package workload

import (
	"context"
	"fmt"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
	Conflict  string // "" | "multiple_policies:<a>,<b>" | "hpa:<name>" | "hpa_unknown"
}

// ScaleSupport reports whether a kind exposes a scale subresource.
type ScaleSupport func(gvk schema.GroupVersionKind) bool

// Resolver turns policy selectors into concrete targets.
type Resolver struct {
	Client client.Client
	// ScaleSupport is the scale-subresource discovery check. Nil means every
	// kind is assumed to support scale.
	ScaleSupport ScaleSupport
}

// Resolution is the outcome of ResolveAll. Issues holds, per policy, messages
// about selectors, kinds or namespaces that were skipped; they are meant to
// feed a policy condition message.
type Resolution struct {
	Targets map[v1.PolicyRef][]Target
	Issues  map[v1.PolicyRef][]string
}

// skippable reports whether a list error means "this kind/namespace is
// unavailable" rather than something unexpected.
func skippable(err error) bool {
	return meta.IsNoMatchError(err) || apierrors.IsNotFound(err) || apierrors.IsForbidden(err)
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
func (r *Resolver) ResolveAll(ctx context.Context, policies []v1.TidePolicy) (Resolution, error) {
	issues := map[v1.PolicyRef][]string{}
	out := make(map[v1.PolicyRef][]Target, len(policies))
	owners := map[targetKey][]v1.PolicyRef{}

	for i := range policies {
		p := &policies[i]
		ref := v1.PolicyRef{Namespace: p.Namespace, Name: p.Name}
		out[ref] = nil

		namespaces, nsIssues, err := r.namespaces(ctx, p)
		issues[ref] = append(issues[ref], nsIssues...)
		if err != nil {
			return Resolution{}, err
		}
		if namespaces == nil { // invalid namespaceSelector: policy gets no targets
			continue
		}
		sel, err := metav1.LabelSelectorAsSelector(&p.Spec.Selector.LabelSelector)
		if err != nil {
			issues[ref] = append(issues[ref], fmt.Sprintf("invalid selector: %v", err))
			continue
		}
		for _, kind := range p.Spec.Selector.Kinds {
			gvk := schema.FromAPIVersionAndKind(kind.APIVersion, kind.Kind)
			if r.ScaleSupport != nil && !r.ScaleSupport(gvk) {
				issues[ref] = append(issues[ref],
					fmt.Sprintf("kind %s/%s skipped: no scale subresource", kind.APIVersion, kind.Kind))
				continue
			}
			for _, ns := range namespaces {
				list := &unstructured.UnstructuredList{}
				list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
				if err := r.Client.List(ctx, list, client.InNamespace(ns), client.MatchingLabelsSelector{Selector: sel}); err != nil {
					if skippable(err) {
						issues[ref] = append(issues[ref],
							fmt.Sprintf("kind %s/%s skipped in namespace %s: %v", kind.APIVersion, kind.Kind, ns, err))
						if meta.IsNoMatchError(err) {
							break // the kind is not installed, so it is unavailable in every namespace
						}
						continue // Forbidden/NotFound may be specific to this namespace
					}
					return Resolution{}, fmt.Errorf("list %s in %s: %w", kind.Kind, ns, err)
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

	hpas, hpaFailed := r.hpaTargets(ctx, out)
	for ref, ts := range out {
		failedNS := map[string]struct{}{}
		for i := range ts {
			if msg, bad := hpaFailed[ts[i].Namespace]; bad {
				if _, seen := failedNS[ts[i].Namespace]; !seen {
					failedNS[ts[i].Namespace] = struct{}{}
					issues[ref] = append(issues[ref], msg)
				}
			}
		}
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
			} else if _, bad := hpaFailed[t.Namespace]; bad {
				t.Conflict = "hpa_unknown" // fail closed: cannot rule out an HPA
			}
		}
		out[ref] = ts
	}
	return Resolution{Targets: out, Issues: issues}, nil
}

// namespaces returns the union of listed and selected namespaces, or the
// policy's own namespace when neither is set. A nil result with a nil error
// means the policy's namespaceSelector is invalid (recorded in issues).
func (r *Resolver) namespaces(ctx context.Context, p *v1.TidePolicy) ([]string, []string, error) {
	s := p.Spec.Selector
	if len(s.Namespaces) == 0 && s.NamespaceSelector == nil {
		return []string{p.Namespace}, nil, nil
	}
	var issues []string
	set := map[string]struct{}{}
	for _, n := range s.Namespaces {
		set[n] = struct{}{}
	}
	if s.NamespaceSelector != nil {
		sel, err := metav1.LabelSelectorAsSelector(s.NamespaceSelector)
		if err != nil {
			return nil, []string{fmt.Sprintf("invalid namespaceSelector: %v", err)}, nil
		}
		var nsList corev1.NamespaceList
		if err := r.Client.List(ctx, &nsList, client.MatchingLabelsSelector{Selector: sel}); err != nil {
			if !skippable(err) {
				return nil, nil, fmt.Errorf("list namespaces: %w", err)
			}
			issues = append(issues, fmt.Sprintf("namespaceSelector not evaluated: %v", err))
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
	return out, issues, nil
}

// hpaTargets maps each target key that an autoscaling/v2 HPA points at to the
// HPA name, looking only in namespaces that contain resolved targets. Namespaces
// whose HPA list failed are returned with a message; their targets are marked
// Conflict "hpa_unknown" (fail closed) by the caller.
func (r *Resolver) hpaTargets(ctx context.Context, resolved map[v1.PolicyRef][]Target) (map[targetKey]string, map[string]string) {
	nss := map[string]struct{}{}
	for _, ts := range resolved {
		for _, t := range ts {
			nss[t.Namespace] = struct{}{}
		}
	}
	out := map[targetKey]string{}
	failed := map[string]string{}
	for ns := range nss {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(schema.GroupVersionKind{Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscalerList"})
		if err := r.Client.List(ctx, list, client.InNamespace(ns)); err != nil {
			failed[ns] = fmt.Sprintf("hpa conflict detection unavailable in namespace %s: %v", ns, err)
			continue
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
	return res, failed
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
