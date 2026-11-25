package forecast

import (
	"cmp"
	"maps"
	"math"
	"slices"
	"sync"
	"time"
)

// Score is the rolling accuracy of one model at one lead time.
type Score struct {
	Model           string
	Lead            time.Duration
	SMAPE, Coverage float64
	Samples         int
}

type trackerKey struct {
	model string
	lead  time.Duration
}

type pendingPoint struct {
	target time.Time
	fp     ForecastPoint
}

type scoredPoint struct {
	target  time.Time
	smape   float64
	covered bool
}

// Tracker scores past forecasts against actuals. Use one Tracker per workload.
type Tracker struct {
	step, window time.Duration
	leads        []time.Duration
	maxPending   int
	maxScores    int

	mu      sync.Mutex
	pending map[trackerKey][]pendingPoint
	scored  map[trackerKey][]scoredPoint
}

// NewTracker scores forecasts at leads of horizon/3, 2*horizon/3 and horizon,
// each rounded to step, over a rolling window.
func NewTracker(horizon, step, window time.Duration) *Tracker {
	roundLead := func(d time.Duration) time.Duration {
		return max(time.Duration(math.Round(float64(d)/float64(step))), 1) * step
	}
	leads := []time.Duration{roundLead(horizon / 3), roundLead(2 * horizon / 3), roundLead(horizon)}
	leads = slices.Compact(leads)
	return &Tracker{
		step:       step,
		window:     window,
		leads:      leads,
		maxPending: int(horizon/step) + 1,
		maxScores:  max(int(window/step), 1),
		pending:    map[trackerKey][]pendingPoint{},
		scored:     map[trackerKey][]scoredPoint{},
	}
}

// Record stores the forecast points at issuedAt+lead for each lead.
func (t *Tracker) Record(f Forecast, issuedAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, lead := range t.leads {
		target := issuedAt.Add(lead)
		idx, ok := nearest(len(f.Points), func(i int) time.Time { return f.Points[i].T }, target, t.step)
		if !ok {
			continue
		}
		k := trackerKey{f.Model, lead}
		p := append(t.pending[k], pendingPoint{target: target, fp: f.Points[idx]})
		if len(p) > t.maxPending {
			p = slices.Delete(p, 0, len(p)-t.maxPending)
		}
		t.pending[k] = p
	}
}

// Observe scores pending points whose target is at or before now, then drops
// scores older than the window. Pending points with no matching actual are dropped.
func (t *Tracker) Observe(actual Series, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, ps := range t.pending {
		remaining := ps[:0]
		for _, p := range ps {
			if p.target.After(now) {
				remaining = append(remaining, p)
				continue
			}
			idx, ok := nearest(len(actual.Points), func(i int) time.Time { return actual.Points[i].T }, p.target, t.step)
			if !ok || math.IsNaN(actual.Points[idx].V) || math.IsInf(actual.Points[idx].V, 0) {
				continue
			}
			a := actual.Points[idx].V
			t.scored[k] = append(t.scored[k], scoredPoint{
				target:  p.target,
				smape:   smape(p.fp.Point, a),
				covered: p.fp.Lower <= a && a <= p.fp.Upper,
			})
		}
		t.pending[k] = remaining
	}
	cutoff := now.Add(-t.window)
	for k, ss := range t.scored {
		ss = slices.DeleteFunc(ss, func(s scoredPoint) bool { return !s.target.After(cutoff) })
		if len(ss) > t.maxScores {
			ss = slices.Delete(ss, 0, len(ss)-t.maxScores)
		}
		t.scored[k] = ss
	}
}

// Scores returns the current scores, sorted by model then lead.
func (t *Tracker) Scores() []Score {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Score, 0, len(t.scored))
	for k, ss := range t.scored {
		if len(ss) == 0 {
			continue
		}
		var sum float64
		covered := 0
		for _, s := range ss {
			sum += s.smape
			if s.covered {
				covered++
			}
		}
		out = append(out, Score{
			Model:    k.model,
			Lead:     k.lead,
			SMAPE:    sum / float64(len(ss)),
			Coverage: float64(covered) / float64(len(ss)),
			Samples:  len(ss),
		})
	}
	slices.SortFunc(out, func(a, b Score) int {
		return cmp.Or(cmp.Compare(a.Model, b.Model), cmp.Compare(a.Lead, b.Lead))
	})
	return out
}

// Trusted returns the model with the lowest mean sMAPE across leads among
// models having at least minSamples at every lead.
func (t *Tracker) Trusted(minSamples int) (string, bool) {
	byModel := map[string][]Score{}
	for _, s := range t.Scores() {
		byModel[s.Model] = append(byModel[s.Model], s)
	}
	best, bestMean, found := "", 0.0, false
	for _, model := range slices.Sorted(maps.Keys(byModel)) {
		ss := byModel[model]
		if len(ss) != len(t.leads) {
			continue
		}
		var sum float64
		ok := true
		for _, s := range ss {
			if s.Samples < max(minSamples, 1) {
				ok = false
				break
			}
			sum += s.SMAPE
		}
		mean := sum / float64(len(ss))
		if math.IsNaN(mean) || math.IsInf(mean, 0) {
			continue
		}
		if ok && (!found || mean < bestMean) {
			best, bestMean, found = model, mean, true
		}
	}
	return best, found
}

func smape(f, a float64) float64 {
	denom := (math.Abs(f) + math.Abs(a)) / 2
	if denom == 0 {
		return 0
	}
	return math.Abs(f-a) / denom
}

// nearest returns the index of the time-ordered element closest to target, if it
// lies within step/2 of it.
func nearest(n int, at func(int) time.Time, target time.Time, step time.Duration) (int, bool) {
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		if at(mid).Before(target) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	best, bestD, found := 0, time.Duration(0), false
	for _, j := range []int{lo - 1, lo} {
		if j < 0 || j >= n {
			continue
		}
		d := at(j).Sub(target).Abs()
		if !found || d < bestD {
			best, bestD, found = j, d, true
		}
	}
	return best, found && bestD <= step/2
}
