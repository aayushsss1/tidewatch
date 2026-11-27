package harness

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

func policyFor(t *testing.T, g Guardrails, ns string) v1.TidePolicy {
	t.Helper()
	sc, err := scenario.Load(filepath.Join("..", "scenarios", "example-flash.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	g, err = g.resolve()
	if err != nil {
		t.Fatal(err)
	}
	c, _, err := newClient("")
	if err != nil {
		t.Fatal(err)
	}
	r := &run{sc: sc, cfg: Config{Guardrails: g}, ns: ns}
	if err := r.objects(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	var pol v1.TidePolicy
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: policyName}, &pol); err != nil {
		t.Fatal(err)
	}
	return pol
}

func TestGuardrailsReachPolicySpec(t *testing.T) {
	m := policyFor(t, "", "gr-matched").Spec.Guardrails
	if m.ScaleUp.MaxStep != 60 || m.ScaleDown.MaxStep != 60 {
		t.Errorf("matched max steps = %d/%d, want 60/60", m.ScaleUp.MaxStep, m.ScaleDown.MaxStep)
	}
	if m.ScaleUp.Cooldown.Duration != 15*time.Second || m.ScaleDown.Cooldown.Duration != 15*time.Second {
		t.Errorf("matched cooldowns = %v/%v, want 15s (0 would re-default to 60s)",
			m.ScaleUp.Cooldown.Duration, m.ScaleDown.Cooldown.Duration)
	}
	if m.ScaleDown.StabilizationWindow.Duration != 300*time.Second {
		t.Errorf("matched stabilization = %v", m.ScaleDown.StabilizationWindow.Duration)
	}
	d := policyFor(t, GuardrailsDefault, "gr-default").Spec.Guardrails
	if d.ScaleUp.MaxStep != 5 || d.ScaleUp.Cooldown.Duration != 60*time.Second ||
		d.ScaleDown.MaxStep != 2 || d.ScaleDown.StabilizationWindow.Duration != 10*time.Minute {
		t.Errorf("default profile must keep the CRD defaults: %+v", d)
	}
	if _, err := Guardrails("bogus").resolve(); err == nil {
		t.Error("unknown profile should error")
	}
}
