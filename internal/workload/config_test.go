package workload

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

func spec(minR *int32) v1.TidePolicySpec {
	return v1.TidePolicySpec{
		Load: v1.LoadSpec{
			DefaultProfile: "http",
			Profiles: map[string]v1.LoadProfile{
				"http": {
					Query:            `sum(rate(r{namespace="{{ .Namespace }}",app="{{ .Name }}"}[1m])) # {{ .Kind }}`,
					TargetPerReplica: "100",
				},
			},
		},
		Guardrails: v1.GuardrailsSpec{MinReplicas: minR, MaxReplicas: 10},
	}
}

func obj(annos map[string]string) metav1.Object {
	return &metav1.ObjectMeta{Namespace: "shop", Name: "frontend", Annotations: annos}
}

func TestEffective_Defaults(t *testing.T) {
	got, err := Effective(spec(new(int32(2))), false, obj(nil), "Deployment")
	if err != nil {
		t.Fatal(err)
	}
	if got.TargetPerReplica != 100 || got.Headroom != 0.8 || got.MinReplicas != 2 || got.MaxReplicas != 10 ||
		got.Paused || len(got.Warnings) != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestEffective_NilMinIsOne(t *testing.T) {
	got, err := Effective(spec(nil), false, obj(nil), "Deployment")
	if err != nil || got.MinReplicas != 1 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestEffective_TightenMaxHonored(t *testing.T) {
	got, err := Effective(spec(new(int32(1))), false, obj(map[string]string{v1.AnnoMaxReplicas: "6"}), "Deployment")
	if err != nil || got.MaxReplicas != 6 || len(got.Warnings) != 0 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestEffective_LoosenMaxIgnored(t *testing.T) {
	got, err := Effective(spec(new(int32(1))), false, obj(map[string]string{v1.AnnoMaxReplicas: "50"}), "Deployment")
	if err != nil || got.MaxReplicas != 10 || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "ignored") {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestEffective_LoosenMinIgnored(t *testing.T) {
	got, err := Effective(spec(new(int32(2))), false, obj(map[string]string{v1.AnnoMinReplicas: "1"}), "Deployment")
	if err != nil || got.MinReplicas != 2 || len(got.Warnings) != 1 || !strings.Contains(got.Warnings[0], "ignored") {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestEffective_TargetPerReplicaOverride(t *testing.T) {
	got, err := Effective(spec(new(int32(1))), false, obj(map[string]string{v1.AnnoTargetPerRep: "80"}), "Deployment")
	if err != nil || got.TargetPerReplica != 80 {
		t.Fatalf("got %+v, %v", got, err)
	}
	for _, bad := range []string{"0", "-5", "abc"} {
		if _, err := Effective(spec(new(int32(1))), false, obj(map[string]string{v1.AnnoTargetPerRep: bad}), "Deployment"); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}

func TestEffective_UnknownProfileErrors(t *testing.T) {
	_, err := Effective(spec(new(int32(1))), false, obj(map[string]string{v1.AnnoLoadProfile: "grpc"}), "Deployment")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestEffective_RendersQuery(t *testing.T) {
	got, err := Effective(spec(new(int32(1))), false, obj(nil), "Deployment")
	if err != nil || !strings.Contains(got.Query, `namespace="shop",app="frontend"`) ||
		!strings.Contains(got.Query, "Deployment") {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestEffective_BadTemplateErrors(t *testing.T) {
	s := spec(new(int32(1)))
	s.Load.Profiles["http"] = v1.LoadProfile{Query: "{{ .Nope }}", TargetPerReplica: "100"}
	if _, err := Effective(s, false, obj(nil), "Deployment"); err == nil {
		t.Fatal("expected error")
	}
}

func TestEffective_PausedByPolicyOrWorkload(t *testing.T) {
	paused := map[string]string{v1.AnnoPaused: "true"}
	for name, c := range map[string]struct {
		policy bool
		annos  map[string]string
		want   bool
	}{
		"neither":  {false, nil, false},
		"policy":   {true, nil, true},
		"workload": {false, paused, true},
	} {
		got, err := Effective(spec(new(int32(1))), c.policy, obj(c.annos), "Deployment")
		if err != nil || got.Paused != c.want {
			t.Fatalf("%s: got %+v, %v", name, got, err)
		}
	}
}
