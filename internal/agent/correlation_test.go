package agent

import (
	"math"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func mkSeries(step time.Duration, vals []float64) forecast.Series {
	s := forecast.Series{Step: step}
	for i, v := range vals {
		s.Points = append(s.Points, forecast.Point{T: t0.Add(time.Duration(i) * step), V: v})
	}
	return s
}

func wave(n int) []float64 {
	v := make([]float64, n)
	for i := range v {
		v[i] = math.Sin(float64(i)*0.5) + float64(i%7)
	}
	return v
}

func TestLaggedCorrelation_FindsLag(t *testing.T) {
	step := 30 * time.Second
	a := wave(60)
	b := make([]float64, 60)
	for i := range b {
		if i >= 6 {
			b[i] = a[i-6]
		} else {
			b[i] = 100 // pre-onset flat region
		}
	}
	corr, lag := LaggedCorrelation(mkSeries(step, a), mkSeries(step, b), 10*time.Minute)
	if corr < 0.95 || lag != 6*step {
		t.Fatalf("corr=%v lag=%v", corr, lag)
	}
}

func TestLaggedCorrelation_NaNSafe(t *testing.T) {
	a := wave(30)
	b := append([]float64(nil), a...)
	b[3], b[10] = math.NaN(), math.NaN()
	corr, _ := LaggedCorrelation(mkSeries(time.Minute, a), mkSeries(time.Minute, b), 5*time.Minute)
	if math.IsNaN(corr) || corr < 0.99 {
		t.Fatalf("corr=%v", corr)
	}
	flat := make([]float64, 30)
	if c, _ := LaggedCorrelation(mkSeries(time.Minute, a), mkSeries(time.Minute, flat), 5*time.Minute); !math.IsNaN(c) {
		t.Fatalf("flat series should give NaN, got %v", c)
	}
}

type fakeSrc struct {
	m     map[types.UID]forecast.Series
	calls int
}

func (f *fakeSrc) Series(u types.UID) (forecast.Series, bool) {
	f.calls++
	s, ok := f.m[u]
	return s, ok
}

func TestCorrelator_CachesAndMissingData(t *testing.T) {
	a := wave(40)
	src := &fakeSrc{m: map[types.UID]forecast.Series{"a": mkSeries(time.Minute, a), "b": mkSeries(time.Minute, a)}}
	clk := clocktesting.NewFakeClock(t0)
	c := NewCorrelator(src, clk)
	if corr, _, ok := c.Related("a", "b"); !ok || corr < 0.99 {
		t.Fatalf("corr=%v ok=%v", corr, ok)
	}
	n := src.calls
	c.Related("a", "b")
	if src.calls != n {
		t.Fatal("expected cache hit")
	}
	clk.Step(11 * time.Minute)
	c.Related("a", "b")
	if src.calls == n {
		t.Fatal("expected refresh after TTL")
	}
	if _, _, ok := c.Related("a", "zzz"); ok {
		t.Fatal("missing series must give ok=false")
	}
}

func TestCorrelator_PrunesStaleEntries(t *testing.T) {
	a := wave(40)
	src := &fakeSrc{m: map[types.UID]forecast.Series{"a": mkSeries(time.Minute, a), "b": mkSeries(time.Minute, a), "c": mkSeries(time.Minute, a)}}
	clk := clocktesting.NewFakeClock(t0)
	c := NewCorrelator(src, clk)
	c.Related("a", "b")
	c.Related("a", "c")
	if len(c.cache) != 2 {
		t.Fatal(len(c.cache))
	}
	clk.Step(11 * time.Minute)
	c.Related("b", "c")
	if len(c.cache) != 1 {
		t.Fatalf("cache=%d", len(c.cache))
	}
}
