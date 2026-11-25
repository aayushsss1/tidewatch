package workload

import (
	"context"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const (
	kindDeployment = "Deployment"
	webName        = "web"
	kindRollout    = "Rollout"
	argoGroup      = "argoproj.io"
)

var deployKind = v1.TargetKind{APIVersion: appsV1, Kind: kindDeployment}

func newScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	return s
}

func deploy(ns, name string, labels map[string]string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Labels: labels},
	}
}

func policy(ns, name string, spec v1.WorkloadSelector) v1.TidePolicy {
	return v1.TidePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       v1.TidePolicySpec{Selector: spec},
	}
}

func selector(ns []string, labels map[string]string, kinds ...v1.TargetKind) v1.WorkloadSelector {
	return v1.WorkloadSelector{
		Namespaces:    ns,
		LabelSelector: metav1.LabelSelector{MatchLabels: labels},
		Kinds:         kinds,
	}
}

func newResolver(objs ...client.Object) *Resolver {
	c := fake.NewClientBuilder().WithScheme(newScheme()).WithObjects(objs...).Build()
	return &Resolver{Client: c, ScaleSupport: func(schema.GroupVersionKind) bool { return true }}
}

func TestResolveAll_SelectsByLabelAcrossNamespaces(t *testing.T) {
	lbl := map[string]string{"tide": "on"}
	r := newResolver(
		deploy("a", webName, lbl),
		deploy("b", "api", lbl),
		deploy("b", "other", nil),
	)
	p := policy("a", "p", selector([]string{"a", "b"}, lbl, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Targets
	ts := got[v1.PolicyRef{Namespace: "a", Name: "p"}]
	if len(ts) != 2 {
		t.Fatalf("want 2 targets, got %d: %+v", len(ts), ts)
	}
	for _, tg := range ts {
		if tg.Conflict != "" || tg.Object == nil || tg.Ref.Kind != kindDeployment || tg.Namespace == "" {
			t.Errorf("bad target %+v", tg)
		}
	}
}

func TestResolveAll_NamespaceSelector(t *testing.T) {
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "x", Labels: map[string]string{"env": "prod"}}}
	r := newResolver(ns, deploy("x", webName, nil), deploy("y", webName, nil))
	sel := selector(nil, nil, deployKind)
	sel.NamespaceSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"env": "prod"}}
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{policy("z", "p", sel)})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Targets
	ts := got[v1.PolicyRef{Namespace: "z", Name: "p"}]
	if len(ts) != 1 || ts[0].Namespace != "x" {
		t.Fatalf("got %+v", ts)
	}
}

func TestResolveAll_DefaultsToPolicyNamespace(t *testing.T) {
	r := newResolver(deploy("a", webName, nil), deploy("b", webName, nil))
	p := policy("a", "p", selector(nil, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Targets
	ts := got[v1.PolicyRef{Namespace: "a", Name: "p"}]
	if len(ts) != 1 || ts[0].Namespace != "a" || ts[0].Ref.Name != webName {
		t.Fatalf("got %+v", ts)
	}
}

func TestResolveAll_MultiplePoliciesConflict(t *testing.T) {
	r := newResolver(deploy("a", webName, nil))
	p1 := policy("a", "p1", selector(nil, nil, deployKind))
	p2 := policy("a", "p2", selector(nil, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p1, p2})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Targets
	for _, name := range []string{"p1", "p2"} {
		ts := got[v1.PolicyRef{Namespace: "a", Name: name}]
		if len(ts) != 1 {
			t.Fatalf("%s: got %+v", name, ts)
		}
		if !strings.HasPrefix(ts[0].Conflict, "multiple_policies:") ||
			!strings.Contains(ts[0].Conflict, "p1") || !strings.Contains(ts[0].Conflict, "p2") {
			t.Errorf("%s: conflict %q", name, ts[0].Conflict)
		}
	}
}

func TestResolveAll_HPAConflict(t *testing.T) {
	hpa := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "autoscaling/v2",
		"kind":       "HorizontalPodAutoscaler",
		"metadata":   map[string]any{"namespace": "a", "name": webName},
		"spec": map[string]any{"scaleTargetRef": map[string]any{
			"apiVersion": appsV1, "kind": kindDeployment, "name": webName,
		}},
	}}
	r := newResolver(deploy("a", webName, nil), deploy("a", "free", nil), hpa)
	p := policy("a", "p", selector(nil, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Targets
	conflicts := map[string]string{}
	for _, tg := range got[v1.PolicyRef{Namespace: "a", Name: "p"}] {
		conflicts[tg.Ref.Name] = tg.Conflict
	}
	if conflicts[webName] != "hpa:web" || conflicts["free"] != "" {
		t.Fatalf("got %v", conflicts)
	}
}

func TestResolveAll_SkipsKindsWithoutScale(t *testing.T) {
	r := newResolver(deploy("a", webName, nil))
	r.ScaleSupport = func(schema.GroupVersionKind) bool { return false }
	p := policy("a", "p", selector(nil, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Targets
	ref := v1.PolicyRef{Namespace: "a", Name: "p"}
	if len(got[ref]) != 0 {
		t.Fatalf("expected none, got %+v", got[ref])
	}
	if len(res.Issues[ref]) != 1 || !strings.Contains(res.Issues[ref][0], "scale") {
		t.Fatalf("issues: %v", res.Issues)
	}
}

// failingClient fails List for the listed kinds with the given error.
type failingClient struct {
	client.Client
	failKinds map[string]error
	// onlyNS, when set, restricts failKinds to lists in that namespace.
	onlyNS string
}

func (f failingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if err, ok := f.failKinds[list.GetObjectKind().GroupVersionKind().Kind]; ok {
		lo := &client.ListOptions{}
		lo.ApplyOptions(opts)
		if f.onlyNS == "" || lo.Namespace == f.onlyNS {
			return err
		}
	}
	return f.Client.List(ctx, list, opts...)
}

func TestResolveAll_MissingKindSkippedGoodKindResolves(t *testing.T) {
	for name, listErr := range map[string]error{
		"nomatch":   &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: argoGroup, Kind: kindRollout}},
		"notfound":  apierrors.NewNotFound(schema.GroupResource{Group: argoGroup, Resource: "rollouts"}, ""),
		"forbidden": apierrors.NewForbidden(schema.GroupResource{Group: argoGroup, Resource: "rollouts"}, "", errors.New("no")),
	} {
		t.Run(name, func(t *testing.T) {
			r := newResolver(deploy("a", webName, nil))
			r.Client = failingClient{Client: r.Client, failKinds: map[string]error{"RolloutList": listErr}}
			rollout := v1.TargetKind{APIVersion: "argoproj.io/v1alpha1", Kind: kindRollout}
			p := policy("a", "p", selector(nil, nil, rollout, deployKind))
			res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
			if err != nil {
				t.Fatal(err)
			}
			ref := v1.PolicyRef{Namespace: "a", Name: "p"}
			if len(res.Targets[ref]) != 1 || res.Targets[ref][0].Ref.Kind != kindDeployment {
				t.Fatalf("targets: %+v", res.Targets[ref])
			}
			if len(res.Issues[ref]) != 1 || !strings.Contains(res.Issues[ref][0], kindRollout) {
				t.Fatalf("issues: %v", res.Issues[ref])
			}
		})
	}
}

func TestResolveAll_ForbiddenInOneNamespaceKeepsOthers(t *testing.T) {
	r := newResolver(deploy("a", webName, nil), deploy("b", "api", nil))
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "", errors.New("no"))
	r.Client = failingClient{Client: r.Client, failKinds: map[string]error{"DeploymentList": forbidden}, onlyNS: "a"}
	p := policy("p", "pol", selector([]string{"a", "b"}, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
	if err != nil {
		t.Fatal(err)
	}
	ref := v1.PolicyRef{Namespace: "p", Name: "pol"}
	if len(res.Targets[ref]) != 1 || res.Targets[ref][0].Namespace != "b" {
		t.Fatalf("targets: %+v", res.Targets[ref])
	}
	if len(res.Issues[ref]) != 1 || !strings.Contains(res.Issues[ref][0], "namespace a") {
		t.Fatalf("issues: %v", res.Issues[ref])
	}
}

func TestResolveAll_UnexpectedListErrorFails(t *testing.T) {
	r := newResolver(deploy("a", webName, nil))
	r.Client = failingClient{Client: r.Client, failKinds: map[string]error{"DeploymentList": errors.New("boom")}}
	_, err := r.ResolveAll(context.Background(), []v1.TidePolicy{policy("a", "p", selector(nil, nil, deployKind))})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResolveAll_InvalidSelectorIsolatedToPolicy(t *testing.T) {
	r := newResolver(deploy("a", webName, nil))
	bad := policy("a", "bad", selector(nil, nil, deployKind))
	bad.Spec.Selector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: "k", Operator: "Bogus"}}
	badNS := policy("a", "badns", selector(nil, nil, deployKind))
	badNS.Spec.Selector.NamespaceSelector = &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "k", Operator: "Bogus"}},
	}
	good := policy("a", "good", selector(nil, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{bad, badNS, good})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"bad", "badns"} {
		ref := v1.PolicyRef{Namespace: "a", Name: n}
		if len(res.Targets[ref]) != 0 || len(res.Issues[ref]) != 1 {
			t.Errorf("%s: targets=%v issues=%v", n, res.Targets[ref], res.Issues[ref])
		}
	}
	gref := v1.PolicyRef{Namespace: "a", Name: "good"}
	if len(res.Targets[gref]) != 1 || len(res.Issues[gref]) != 0 || res.Targets[gref][0].Conflict != "" {
		t.Fatalf("good: %+v %v", res.Targets[gref], res.Issues[gref])
	}
}

func TestResolveAll_HPAListFailureRecorded(t *testing.T) {
	r := newResolver(deploy("a", webName, nil))
	r.Client = failingClient{Client: r.Client, failKinds: map[string]error{
		"HorizontalPodAutoscalerList": errors.New("hpa api down"),
	}}
	p := policy("a", "p", selector(nil, nil, deployKind))
	res, err := r.ResolveAll(context.Background(), []v1.TidePolicy{p})
	if err != nil {
		t.Fatal(err)
	}
	ref := v1.PolicyRef{Namespace: "a", Name: "p"}
	if len(res.Targets[ref]) != 1 || res.Targets[ref][0].Conflict != "" {
		t.Fatalf("targets: %+v", res.Targets[ref])
	}
	if len(res.Issues[ref]) != 1 || !strings.Contains(res.Issues[ref][0], "hpa") {
		t.Fatalf("issues: %v", res.Issues[ref])
	}
}

func TestTideWorkloadName(t *testing.T) {
	cases := map[string]string{kindDeployment: "web-deploy", "StatefulSet": "web-sts", kindRollout: "web-rollout"}
	for kind, want := range cases {
		if got := TideWorkloadName(v1.TargetRef{Kind: kind, Name: webName}); got != want {
			t.Errorf("%s: got %q want %q", kind, got, want)
		}
	}
}
