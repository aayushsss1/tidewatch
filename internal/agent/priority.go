package agent

import (
	"math"
	"strconv"
	"time"

	"github.com/aayushsss1/tidewatch/internal/signal"
)

const (
	minDeadline        = 10 * time.Second
	deadlineMargin     = 30 * time.Second
	overProvUrgency    = 0.01
	disagreementUrgent = 0.3
	changeUrgent       = 0.5
	anomalyFallback    = 0.5 // Anomaly triggers carry no shortfall; see report
)

// Urgency returns the highest per-trigger urgency (spec §7.3) and the
// time-to-breach of the trigger that produced it (nil when it has none).
func Urgency(ts []signal.Trigger) (urgency float64, timeToBreach *time.Duration) {
	for _, t := range ts {
		u, ttb := triggerUrgency(t)
		if u > urgency {
			urgency, timeToBreach = u, ttb
		}
	}
	return urgency, timeToBreach
}

func triggerUrgency(t signal.Trigger) (float64, *time.Duration) {
	switch t.Type {
	case signal.OverProvisioned:
		return overProvUrgency, nil
	case signal.ModelDisagreement:
		return disagreementUrgent, nil
	case signal.WorkloadChange:
		return changeUrgent, nil
	case signal.Anomaly:
		if t.Shortfall > 0 {
			return t.Shortfall, nil
		}
		return anomalyFallback, nil
	}
	if t.TimeToBreach == nil {
		return t.Shortfall, nil
	}
	return t.Shortfall / math.Max(t.TimeToBreach.Minutes(), 1), t.TimeToBreach
}

// Deadline returns now + min(max, ttb − ttr − 30s). It returns false when that
// is under 10s (too late; the baseline covers it). With no ttb it is now + max.
func Deadline(now time.Time, ttb *time.Duration, ttr, max time.Duration) (time.Time, bool) {
	d := max
	if ttb != nil {
		d = min(max, *ttb-ttr-deadlineMargin)
	}
	if d < minDeadline {
		return time.Time{}, false
	}
	return now.Add(d), true
}

// PriorityOf maps a workload to 1..10. A valid 1..10 annotation wins; otherwise
// the PriorityClass value is mapped (nil → 5, ≤0 → 3, ≥1e9 → 10, else log scale).
func PriorityOf(podPriority *int32, annotation string) int {
	if n, err := strconv.Atoi(annotation); err == nil && n >= 1 && n <= 10 {
		return n
	}
	switch {
	case podPriority == nil:
		return 5
	case *podPriority <= 0:
		return 3
	case *podPriority >= 1_000_000_000:
		return 10
	}
	p := int(math.Round(4 + 6*math.Log10(1+float64(*podPriority))/9))
	return min(10, max(1, p))
}
