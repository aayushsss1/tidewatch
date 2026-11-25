package forecast

import (
	"math"
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// constForecast builds a forecast of `steps` points at step cadence after issuedAt,
// every point equal to v with +-width interval.
func constForecast(model string, issuedAt time.Time, step time.Duration, steps int, v, width float64) Forecast {
	f := Forecast{Model: model}
	for h := 1; h <= steps; h++ {
		f.Points = append(f.Points, ForecastPoint{
			T: issuedAt.Add(time.Duration(h) * step), Point: v, Lower: v - width, Upper: v + width,
		})
	}
	return f
}

// constActual builds an actual series of n points at step cadence starting at t0.
func constActual(step time.Duration, n int, v float64) Series {
	pts := make([]Point, n)
	for i := range pts {
		pts[i] = Point{T: t0.Add(time.Duration(i) * step), V: v}
	}
	return Series{Step: step, Points: pts}
}

func sampleFor(scores []Score, model string, lead time.Duration) (Score, bool) {
	for _, s := range scores {
		if s.Model == model && s.Lead == lead {
			return s, true
		}
	}
	return Score{}, false
}

func TestTracker_PerfectForecastScoresZero(t *testing.T) {
	step := time.Minute
	tr := NewTracker(6*step, step, time.Hour)
	tr.Record(constForecast("a", t0, step, 6, 100, 5), t0)
	tr.Observe(constActual(step, 10, 100), t0.Add(6*step))
	scores := tr.Scores()
	if len(scores) != 3 {
		t.Fatalf("got %d scores, want 3: %+v", len(scores), scores)
	}
	wantLeads := []time.Duration{2 * step, 4 * step, 6 * step}
	for i, s := range scores {
		if s.Lead != wantLeads[i] {
			t.Errorf("score %d lead = %v, want %v", i, s.Lead, wantLeads[i])
		}
		if s.SMAPE != 0 || s.Coverage != 1 || s.Samples != 1 {
			t.Errorf("score %d = %+v, want sMAPE 0 coverage 1 samples 1", i, s)
		}
	}
}

func TestTracker_SMAPEAndCoverage(t *testing.T) {
	step := time.Minute
	tr := NewTracker(step, step, time.Hour)
	tr.Record(constForecast("a", t0, step, 1, 120, 5), t0)
	tr.Observe(constActual(step, 5, 100), t0.Add(step))
	s, ok := sampleFor(tr.Scores(), "a", step)
	if !ok {
		t.Fatalf("no score: %+v", tr.Scores())
	}
	if want := 20.0 / 110.0; math.Abs(s.SMAPE-want) > 1e-12 {
		t.Errorf("SMAPE = %v, want %v", s.SMAPE, want)
	}
	if s.Coverage != 0 {
		t.Errorf("Coverage = %v, want 0", s.Coverage)
	}
}

// feed records and observes `rounds` consecutive forecasts for a model whose
// point is actual*(1+relErr).
func feed(tr *Tracker, model string, step time.Duration, rounds int, relErr float64) {
	actual := constActual(step, rounds+20, 100)
	for i := range rounds {
		at := t0.Add(time.Duration(i) * step)
		tr.Record(constForecast(model, at, step, 6, 100*(1+relErr), 1), at)
	}
	tr.Observe(actual, t0.Add(time.Duration(rounds+10)*step))
}

func TestTracker_PicksMoreAccurateModel(t *testing.T) {
	step := time.Minute
	tr := NewTracker(6*step, step, 2*time.Hour)
	feed(tr, "a", step, 10, 0.01)
	feed(tr, "b", step, 10, 0.20)
	got, ok := tr.Trusted(5)
	if !ok || got != "a" {
		t.Fatalf("Trusted = %q, %v; want a", got, ok)
	}
}

func TestTracker_NoTrustedBelowMinSamples(t *testing.T) {
	step := time.Minute
	tr := NewTracker(6*step, step, 2*time.Hour)
	feed(tr, "a", step, 3, 0.01)
	if got, ok := tr.Trusted(5); ok {
		t.Fatalf("Trusted = %q, want none", got)
	}
	if _, ok := tr.Trusted(3); !ok {
		t.Error("Trusted(3) should succeed with 3 samples")
	}
}

func TestTracker_DropsScoresOutsideWindow(t *testing.T) {
	step := time.Minute
	tr := NewTracker(step, step, 10*time.Minute)
	actual := constActual(step, 40, 100)
	for i := range 30 {
		at := t0.Add(time.Duration(i) * step)
		tr.Record(constForecast("a", at, step, 1, 100, 1), at)
		tr.Observe(actual, at.Add(step))
	}
	s, ok := sampleFor(tr.Scores(), "a", step)
	if !ok || s.Samples != 10 {
		t.Fatalf("Samples = %d (ok=%v), want 10", s.Samples, ok)
	}
}

func TestTracker_UnmatchedTargetsAreDropped(t *testing.T) {
	step := time.Minute
	tr := NewTracker(step, step, time.Hour)
	tr.Record(constForecast("a", t0, step, 1, 100, 1), t0)
	actual := constActual(step, 5, 100)
	actual.Points[1].V = math.NaN()
	tr.Observe(actual, t0.Add(2*step))
	if scores := tr.Scores(); len(scores) != 0 {
		t.Fatalf("scores = %+v, want none", scores)
	}
	// The pending point was dropped, so a later Observe cannot score it.
	tr.Observe(constActual(step, 5, 100), t0.Add(3*step))
	if scores := tr.Scores(); len(scores) != 0 {
		t.Fatalf("dropped point was scored later: %+v", scores)
	}
}

func TestTracker_BoundedMemory(t *testing.T) {
	step := time.Minute
	horizon := 6 * step
	tr := NewTracker(horizon, step, time.Hour)
	for i := range 10_000 {
		at := t0.Add(time.Duration(i) * step)
		tr.Record(constForecast("a", at, step, 6, 100, 1), at)
	}
	limit := int(horizon/step) + 1
	for k, p := range tr.pending {
		if len(p) > limit {
			t.Errorf("pending[%v] len = %d, want <= %d", k, len(p), limit)
		}
	}
	if len(tr.pending) != 3 {
		t.Errorf("pending keys = %d, want 3", len(tr.pending))
	}
}

func TestTracker_BothZeroIsZeroError(t *testing.T) {
	step := time.Minute
	tr := NewTracker(step, step, time.Hour)
	tr.Record(constForecast("a", t0, step, 1, 0, 0), t0)
	tr.Observe(constActual(step, 5, 0), t0.Add(step))
	s, ok := sampleFor(tr.Scores(), "a", step)
	if !ok || s.SMAPE != 0 || math.IsNaN(s.SMAPE) {
		t.Fatalf("score = %+v, ok=%v", s, ok)
	}
}

func TestTracker_InfActualDoesNotPoisonTrusted(t *testing.T) {
	step := time.Minute
	tr := NewTracker(6*step, step, time.Hour)
	tr.Record(constForecast("a", t0, step, 6, 100, 5), t0)
	act := constActual(step, 10, 100)
	for i := range act.Points {
		if i == 6 {
			act.Points[i].V = math.Inf(1)
		}
	}
	tr.Observe(act, t0.Add(6*step))
	for _, s := range tr.Scores() {
		if math.IsNaN(s.SMAPE) {
			t.Fatalf("NaN score: %+v", s)
		}
	}
	if _, ok := tr.Trusted(1); ok {
		t.Fatalf("lead with only an Inf actual must not count as scored")
	}
}

func TestTrusted_IgnoresNaNMeans(t *testing.T) {
	tr := NewTracker(3*time.Minute, time.Minute, time.Hour)
	k := trackerKey{"bad", time.Minute}
	tr.scored[k] = []scoredPoint{{target: t0, smape: math.NaN()}}
	for _, l := range tr.leads {
		tr.scored[trackerKey{"bad", l}] = []scoredPoint{{target: t0, smape: math.NaN()}}
		tr.scored[trackerKey{"good", l}] = []scoredPoint{{target: t0, smape: 0.5}}
	}
	if m, ok := tr.Trusted(1); !ok || m != "good" {
		t.Fatalf("got %q %v", m, ok)
	}
}
