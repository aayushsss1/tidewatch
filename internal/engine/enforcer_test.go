package engine

import (
	"slices"
	"testing"
	"time"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func enfIn(current, desired int32) EnforceInput {
	return EnforceInput{
		Now:     t0,
		Desired: desired,
		Config:  workload.EffectiveConfig{MinReplicas: 1, MaxReplicas: 50},
		Rails: v1.GuardrailsSpec{
			ScaleUp:   v1.ScaleUpPolicy{MaxStep: 5, Cooldown: metav1.Duration{Duration: time.Minute}},
			ScaleDown: v1.ScaleDownPolicy{MaxStep: 5, Cooldown: metav1.Duration{Duration: time.Minute}},
		},
		Mode:  v1.ModeActive,
		State: EnforceState{Current: current},
	}
}

func hasReason(r EnforceResult, s string) bool { return slices.Contains(r.Decision.Reasons, s) }

func TestEnforce_ClampsToBounds(t *testing.T) {
	in := enfIn(3, 100)
	in.Config.MaxReplicas = 6
	r := Enforce(in)
	if r.Final != 6 || !hasReason(r, "bounds") || r.Decision.Verdict != v1.VerdictClamped || !r.Apply {
		t.Fatalf("%+v", r)
	}
	in = enfIn(3, 0)
	in.Config.MinReplicas = 2
	if r := Enforce(in); r.Final != 2 || !hasReason(r, "bounds") {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_MaxStepUp(t *testing.T) {
	r := Enforce(enfIn(2, 10))
	if r.Final != 7 || !hasReason(r, "max_step") || r.Decision.Verdict != v1.VerdictClamped || !r.Apply {
		t.Fatalf("%+v", r)
	}
	if r.Decision.Desired != 10 || r.Decision.Source != v1.SourceBaseline || !r.Decision.Time.Equal(&metav1.Time{Time: t0}) {
		t.Fatalf("%+v", r.Decision)
	}
}

func TestEnforce_ScaleUpCooldown(t *testing.T) {
	in := enfIn(2, 4)
	in.State.LastScaleUp = t0.Add(-30 * time.Second)
	r := Enforce(in)
	if r.Final != 2 || !hasReason(r, "cooldown") || r.Decision.Verdict != v1.VerdictHeld || r.Apply {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_ScaleDownStabilization(t *testing.T) {
	in := enfIn(10, 4)
	in.Rails.ScaleDown.StabilizationWindow = metav1.Duration{Duration: 5 * time.Minute}
	in.State.Recommendations = []TimedReplicas{
		{T: t0.Add(-4 * time.Minute), Replicas: 8},
		{T: t0.Add(-2 * time.Minute), Replicas: 6},
		{T: t0.Add(-time.Minute), Replicas: 5},
		{T: t0.Add(-10 * time.Minute), Replicas: 20}, // outside window
	}
	r := Enforce(in)
	if r.Final != 8 || !hasReason(r, "stabilization") || !r.Apply {
		t.Fatalf("%+v", r)
	}
	// recommendation above current never raises Final past current
	in.State.Recommendations = []TimedReplicas{{T: t0.Add(-time.Minute), Replicas: 15}}
	if r := Enforce(in); r.Final != 10 || r.Decision.Verdict != v1.VerdictHeld {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_BudgetLimits(t *testing.T) {
	in := enfIn(3, 9)
	in.Rails.MaxTotalReplicas = new(int32(25))
	in.State.OthersTotal = 20
	r := Enforce(in)
	if r.Final != 5 || !r.BudgetLimited || !hasReason(r, "budget") {
		t.Fatalf("%+v", r)
	}
	in.State.OthersTotal = 30 // already over budget: never below current
	if r := Enforce(in); r.Final != 3 {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_ShadowNeverApplies(t *testing.T) {
	in := enfIn(2, 4)
	in.Mode = v1.ModeShadow
	r := Enforce(in)
	if r.Decision.Verdict != v1.VerdictShadow || r.Apply || r.Final != 4 {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_DryRunForcesShadow(t *testing.T) {
	in := enfIn(2, 4)
	in.DryRun = true
	r := Enforce(in)
	if r.Decision.Verdict != v1.VerdictShadow || r.Apply {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_ConflictRejected(t *testing.T) {
	in := enfIn(2, 4)
	in.Conflict = "other-policy"
	r := Enforce(in)
	if r.Decision.Verdict != v1.VerdictRejected || !hasReason(r, "conflict") || r.Apply || r.Final != 2 {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_PausedHeld(t *testing.T) {
	in := enfIn(2, 4)
	in.Config.Paused = true
	r := Enforce(in)
	if r.Decision.Verdict != v1.VerdictHeld || !hasReason(r, "paused") || r.Apply || r.Final != 2 {
		t.Fatalf("%+v", r)
	}
}

func externalIn() EnforceInput {
	in := enfIn(2, 6)
	in.State.LastApplied = new(int32(5)) // kubectl scaled 5 -> 2
	return in
}

func TestEnforce_ExternalChangeRestartsCooldown(t *testing.T) {
	r := Enforce(externalIn())
	if !r.ExternalChange || !hasReason(r, "external_change") || r.Final != 2 ||
		r.Decision.Verdict != v1.VerdictHeld || r.Apply {
		t.Fatalf("%+v", r)
	}
}

func TestEnforce_ExternalChangeThenScalesAfterCooldown(t *testing.T) {
	in := enfIn(2, 6)
	in.Now = t0.Add(61 * time.Second)
	in.State.LastScaleUp = t0 // observed time recorded by the engine; LastApplied now matches reality
	in.State.LastApplied = new(int32(2))
	r := Enforce(in)
	if r.Final != 6 || r.Decision.Verdict != v1.VerdictApplied || !r.Apply || r.ExternalChange {
		t.Fatalf("%+v", r)
	}
}
