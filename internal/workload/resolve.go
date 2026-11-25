package workload

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// Target is one workload selected by one policy.
type Target struct {
	Ref       v1.TargetRef
	Namespace string
	Object    *unstructured.Unstructured // labels/annotations/status for later use
	Policy    v1.PolicyRef
	Conflict  string // "" | "multiple_policies:<a>,<b>" | "hpa:<name>"
}

// ScaleSupport reports whether a kind exposes a scale subresource.
type ScaleSupport func(gvk schema.GroupVersionKind) bool

// Resolver turns policy selectors into concrete targets.
type Resolver struct {
	Client client.Client
	// ScaleSupport is the scale-subresource discovery check. Nil means every
	// kind is assumed to support scale.
	ScaleSupport ScaleSupport
	// Skipped holds, per policy, messages about selected kinds that were
	// skipped for lacking a scale subresource. It is reset by each ResolveAll
	// and is meant to feed a policy condition message.
	Skipped map[v1.PolicyRef][]string
}

// NewDiscoveryScaleSupport builds a ScaleSupport backed by API discovery,
// looking for a "<resource>/scale" entry.
func NewDiscoveryScaleSupport(dc discovery.DiscoveryInterface) ScaleSupport {
	return func(gvk schema.GroupVersionKind) bool {
		list, err := dc.ServerResourcesForGroupVersion(gvk.GroupVersion().String())
		if err != nil || list == nil {
			return false
		}
		var resource string
		for _, ar := range list.APIResources {
			if ar.Kind == gvk.Kind && !strings.Contains(ar.Name, "/") {
				resource = ar.Name
				break
			}
		}
		if resource == "" {
			return false
		}
		for _, ar := range list.APIResources {
			if ar.Name == resource+"/scale" {
				return true
			}
		}
		return false
	}
}

type targetKey struct{ apiVersion, kind, namespace, name string }

// ResolveAll resolves every policy at once so multi-policy conflicts are visible.
func (r *Resolver) ResolveAll(ctx context.Context, policies []v1.TidePolicy) (map[v1.PolicyRef][]Target, error) {
	r.Skipped = map[v1.PolicyRef][]string{}
	out := make(map[v1.PolicyRef][]Target, len(policies))
	owners := map[targetKey][]v1.PolicyRef{}

	for i := range policies {
		p := &policies[i]
		ref := v1.PolicyRef{Namespace: p.Namespace, Name: p.Name}
		out[ref] = nil

		namespaces, err := r.namespaces(ctx, p)
		if err != nil {
			return nil, err
		}
		sel, err := metav1.LabelSelectorAsSelector(&p.Spec.Selector.LabelSelector)
		if err != nil {
			return nil, fmt.Errorf("policy %s/%s: invalid selector: %w", p.Namespace, p.Name, err)
		}
		for _, kind := range p.Spec.Selector.Kinds {
			gvk := schema.FromAPIVersionAndKind(kind.APIVersion, kind.Kind)
			if r.ScaleSupport != nil && !r.ScaleSupport(gvk) {
				r.Skipped[ref] = append(r.Skipped[ref],
					fmt.Sprintf("kind %s/%s skipped: no scale subresource", kind.APIVersion, kind.Kind))
				continue
			}
			for _, ns := range namespaces {
				list := &unstructured.UnstructuredList{}
				list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
				if err := r.Client.List(ctx, list, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: sel}); err != nil {
					return nil, fmt.Errorf("list %s in %s: %w", kind.Kind, ns, err)
				}
				for j := range list.Items {
					obj := &list.Items[j]
					out[ref] = append(out[ref], Target{
						Ref:       v1.TargetRef{APIVersion: kind.APIVersion, Kind: kind.Kind, Name: obj.GetName()},
						Namespace: obj.GetNamespace(),
						Object:    obj,
						Policy:    ref,
					})
					k := targetKey{kind.APIVersion, kind.Kind, obj.GetNamespace(), obj.GetName()}
					owners[k] = append(owners[k], ref)
				}
			}
		}
	}

	hpas, err := r.hpaTargets(ctx, out)
	if err != nil {
		return nil, err
	}
	for ref, ts := range out {
		for i := range ts {
			t := &ts[i]
			k := targetKey{t.Ref.APIVersion, t.Ref.Kind, t.Namespace, t.Ref.Name}
			if refs := owners[k]; len(refs) > 1 {
				names := make([]string, len(refs))
				for n, pr := range refs {
					names[n] = pr.Namespace + "/" + pr.Name
				}
				slices.Sort(names)
				t.Conflict = "multiple_policies:" + strings.Join(names, ",")
			} else if h, ok := hpas[k]; ok {
				t.Conflict = "hpa:" + h
			}
		}
		out[ref] = ts
	}
	return out, nil
}

// namespaces returns the union of listed and selected namespaces, or the
// policy's own namespace when neither is set.
func (r *Resolver) namespaces(ctx context.Context, p *v1.TidePolicy) ([]string, error) {
	s := p.Spec.Selector
	if len(s.Namespaces) == 0 && s.NamespaceSelector == nil {
		return []string{p.Namespace}, nil
	}
	set := map[string]struct{}{}
	for _, n := range s.Namespaces {
		set[n] = struct{}{}
	}
	if s.NamespaceSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(s.NamespaceSelector)
		if err != nil {
			return nil, fmt.Errorf("policy %s/%s: invalid namespaceSelector: %w", p.Namespace, p.Name, err)
		}
		var nsList corev1.NamespaceList
		if err := r.Client.List(ctx, &nsList, client.MatchingLabelsSelector{Selector: sel}); err != nil {
			return nil, fmt.Errorf("list namespaces: %w", err)
		}
		for _, n := range nsList.Items {
			set[n.Name] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	slices.Sort(out)
	return out, nil
}

// hpaTargets maps each target key that an autoscaling/v2 HPA points at to the
// HPA name, looking only in namespaces that contain resolved targets.
func (r *Resolver) hpaTargets(ctx context.Context, resolved map[v1.PolicyRef][]Target) (map[targetKey]string, error) {
	nss := map[string]struct{}{}
	for _, ts := range resolved {
		for _, t := range ts {
			nss[t.Namespace] = struct{}{}
		}
	}
	out := map[targetKey]string{}
	for ns := range nss {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscalerList"})
		if err := r.Client.List(ctx, list, client.InNamespace(ns)); err != nil {
			return nil, fmt.Errorf("list hpas in %s: %w", ns, err)
		}
		for _, h := range list.Items {
			m, found, _ := unstructured.NestedStringMap(h.Object, "spec", "scaleTargetRef")
			if !found {
				continue
			}
			group := schema.FromAPIVersionAndKind(m["apiVersion"], m["kind"]).Group
			out[targetKeyByGroup(group, m["kind"], ns, m["name"])] = h.GetName()
		}
	}
	// Re-key by apiVersion for lookup: match on group, so expand over resolved targets.
	res := map[targetKey]string{}
	for _, ts := range resolved {
		for _, t := range ts {
			g := schema.FromAPIVersionAndKind(t.Ref.APIVersion, t.Ref.Kind).Group
			if name, ok := out[targetKeyByGroup(g, t.Ref.Kind, t.Namespace, t.Ref.Name)]; ok {
				res[targetKey{t.Ref.APIVersion, t.Ref.Kind, t.Namespace, t.Ref.Name}] = name
			}
		}
	}
	return res, nil
}

func targetKeyByGroup(group, kind, ns, name string) targetKey {
	return targetKey{apiVersion: group, kind: kind, namespace: ns, name: name}
}

// TideWorkloadName is the TideWorkload object name for a target.
func TideWorkloadName(ref v1.TargetRef) string {
	switch ref.Kind {
	case "Deployment":
		return ref.Name + "-deploy"
	case "StatefulSet":
		return ref.Name + "-sts"
	}
	return ref.Name + "-" + strings.ToLower(ref.Kind)
}
