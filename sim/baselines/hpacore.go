package baselines

import (
	"math"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// Kubernetes HPA defaults used by HPA and KEDA (KEDA drives a stock HPA).
const (
	hpaSync        = 15 * time.Second
	hpaTarget      = 0.8 // target utilization, and AverageValue target as a fraction of capacity
	hpaTolerance   = 0.1
	hpaStabilize   = 300 * time.Second // scale-down stabilization window
	hpaUpPods      = 4                 // scale-up policy: max(100%, 4 pods) per sync
	kedaPollPeriod = 30 * time.Second
	eps            = 1e-9
)

// seriesAt returns the series value at t, clamped to the series' range. A point at T is
// the load over (T-step, T].
func seriesAt(s forecast.Series, t time.Time) float64 {
	n := len(s.Points)
	if n == 0 {
		return 0
	}
	i := int(t.Sub(s.Points[0].T) / s.Step)
	return s.Points[min(max(i, 0), n-1)].V
}

func ceilEps(x float64) int32 { return int32(math.Ceil(x - eps)) } //nolint:gosec // small

// withinTolerance reports whether a usage ratio is close enough to 1 that the HPA does
// not act.
func withinTolerance(ratio float64) bool { return math.Abs(ratio-1) <= hpaTolerance+eps }

type recommendation struct {
	t time.Time
	n int32
}

// hpaState holds one HPA's scale-down stabilization history.
type hpaState struct{ recs []recommendation }

// sync applies the HPA's stabilization and scaling policies to a raw recommendation and
// returns the new replica count, clamped to [lo, hi]. It follows the controller's
// stabilizeRecommendation: with the default scale-up window of 0 the up recommendation is
// the raw one, and the down recommendation is the max over the last 300s.
func (h *hpaState) sync(now time.Time, current, desired, lo, hi int32) int32 {
	desired = min(max(desired, lo), hi)
	h.recs = append(h.recs, recommendation{now, desired})
	downRec := desired
	kept := h.recs[:0]
	for _, r := range h.recs {
		if now.Sub(r.t) < hpaStabilize {
			kept = append(kept, r)
			downRec = max(downRec, r.n)
		}
	}
	h.recs = kept

	n := min(max(current, desired), downRec)
	// Policies, per sync: scale up by at most max(100%, 4 pods); scale down by at most
	// 100% (no practical limit).
	if n > current {
		n = min(n, current+max(current, hpaUpPods))
	}
	return min(max(n, lo), hi)
}

// seriesCache memoizes scenario.Series, which recomputes the series on every call.
type seriesCache map[string]forecast.Series

func (c seriesCache) get(sc *scenario.Scenario, name string) forecast.Series {
	if s, ok := c[name]; ok {
		return s
	}
	s := sc.Series(name)
	c[name] = s
	return s
}
