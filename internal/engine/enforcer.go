package engine

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

// Enforcement reasons recorded on a Decision.
const (
	reasonConflict       = "conflict"
	reasonPaused         = "paused"
	reasonExternalChange = "external_change"
	reasonBounds         = "bounds"
	reasonMaxStep        = "max_step"
	reasonCooldown       = "cooldown"
	reasonStabilization  = "stabilization"
	reasonBudget         = "budget"
)

// TimedReplicas is a past recommendation, kept for scale-down stabilization.
type TimedReplicas struct {
	T        time.Time
	Replicas int32
}

// EnforceState is the per-workload history the enforcer needs.
type EnforceState struct {
	Current                    int32
	LastApplied                *int32 // replicas the engine last set; nil if never
	LastScaleUp, LastScaleDown time.Time
	Recommendations            []TimedReplicas // own history, for stabilization
	OthersTotal                int32           // final replicas already decided for other workloads in this policy this tick
}

// EnforceInput is one enforcement request.
type EnforceInput struct {
	Now      time.Time
	Desired  int32
	Config   workload.EffectiveConfig
	Rails    v1.GuardrailsSpec
	Mode     v1.PolicyMode
	DryRun   bool
	Conflict string
	State    EnforceState
}

// EnforceResult is the guarded outcome.
type EnforceResult struct {
	Final                         int32
	Apply                         bool
	Decision                      v1.Decision
	BudgetLimited, ExternalChange bool
}

// Enforce applies guardrails to a desired replica count.
func Enforce(in EnforceInput) EnforceResult {
	st := in.State
	res := EnforceResult{Final: st.Current}
	var reasons []string
	finish := func(verdict v1.Verdict) EnforceResult {
		res.Decision = v1.Decision{
			Time:    metav1.NewTime(in.Now),
			Source:  v1.SourceBaseline,
			Desired: in.Desired,
			Final:   res.Final,
			Verdict: verdict,
			Reasons: reasons,
		}
		return res
	}

	switch {
	case in.Conflict != "":
		reasons = append(reasons, reasonConflict)
		return finish(v1.VerdictRejected)
	case in.Config.Paused:
		reasons = append(reasons, reasonPaused)
		return finish(v1.VerdictHeld)
	}

	lastUp, lastDown := st.LastScaleUp, st.LastScaleDown
	if st.LastApplied != nil && *st.LastApplied != st.Current {
		res.ExternalChange = true
		reasons = append(reasons, reasonExternalChange)
		if in.Desired > st.Current {
			lastUp = in.Now
		} else {
			lastDown = in.Now
		}
	}

	final := min(max(in.Desired, in.Config.MinReplicas), in.Config.MaxReplicas)
	if final != in.Desired {
		reasons = append(reasons, reasonBounds)
	}

	up, down := in.Rails.ScaleUp, in.Rails.ScaleDown
	if final > st.Current && up.MaxStep > 0 && final-st.Current > up.MaxStep {
		final = st.Current + up.MaxStep
		reasons = append(reasons, reasonMaxStep)
	} else if final < st.Current && down.MaxStep > 0 && st.Current-final > down.MaxStep {
		final = st.Current - down.MaxStep
		reasons = append(reasons, reasonMaxStep)
	}

	switch {
	case final > st.Current && in.Now.Sub(lastUp) < up.Cooldown.Duration && !lastUp.IsZero():
		final = st.Current
		reasons = append(reasons, reasonCooldown)
	case final < st.Current && in.Now.Sub(lastDown) < down.Cooldown.Duration && !lastDown.IsZero():
		final = st.Current
		reasons = append(reasons, reasonCooldown)
	case final < st.Current:
		if stab := stabilized(final, st, in.Now, down.StabilizationWindow.Duration); stab != final {
			final = stab
			reasons = append(reasons, reasonStabilization)
		}
	}

	if budget := in.Rails.MaxTotalReplicas; budget != nil && final > st.Current && st.OthersTotal+final > *budget {
		final = max(st.Current, *budget-st.OthersTotal)
		res.BudgetLimited = true
		reasons = append(reasons, reasonBudget)
	}
	res.Final = final

	var clamping bool
	for _, r := range reasons {
		clamping = clamping || r != reasonExternalChange
	}
	switch {
	case final == st.Current:
		return finish(v1.VerdictHeld)
	case in.Mode == v1.ModeShadow || in.DryRun:
		return finish(v1.VerdictShadow)
	case clamping:
		res.Apply = true
		return finish(v1.VerdictClamped)
	default:
		res.Apply = true
		return finish(v1.VerdictApplied)
	}
}

// stabilized raises a scale-down target to the highest recommendation in the window, never above current.
func stabilized(final int32, st EnforceState, now time.Time, window time.Duration) int32 {
	if window <= 0 {
		return final
	}
	for _, r := range st.Recommendations {
		if !r.T.Before(now.Add(-window)) {
			final = max(final, r.Replicas)
		}
	}
	return min(final, st.Current)
}
