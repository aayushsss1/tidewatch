// Package agent holds the pure-Go units between the trigger detector and the
// LLM investigator: incident grouping, queueing, budget and circuit breaking.
package agent

import (
	"math"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

const (
	maxLag       = 10 * time.Minute
	corrCacheTTL = 10 * time.Minute
	minOverlap   = 3
)

// SeriesSource returns the recent load series of a workload.
type SeriesSource interface {
	Series(uid types.UID) (forecast.Series, bool)
}

type pairKey struct{ a, b types.UID }

type pairVal struct {
	corr float64
	lag  time.Duration
	ok   bool
	at   time.Time
}

// Correlator measures lagged correlation between workloads, cached per pair.
type Correlator struct {
	src   SeriesSource
	clk   clock.Clock
	mu    sync.Mutex
	cache map[pairKey]pairVal
}

// NewCorrelator returns a Correlator reading series from src.
func NewCorrelator(src SeriesSource, clk clock.Clock) *Correlator {
	return &Correlator{src: src, clk: clk, cache: map[pairKey]pairVal{}}
}

// Related returns the best lagged correlation of b following a. ok is false
// when either series is unavailable or too short to compare.
func (c *Correlator) Related(a, b types.UID) (corr float64, lag time.Duration, ok bool) {
	if c == nil || c.src == nil {
		return 0, 0, false
	}
	now := c.clk.Now()
	k := pairKey{a, b}
	c.mu.Lock()
	if v, hit := c.cache[k]; hit && now.Sub(v.at) < corrCacheTTL {
		c.mu.Unlock()
		return v.corr, v.lag, v.ok
	}
	c.mu.Unlock()

	sa, okA := c.src.Series(a)
	sb, okB := c.src.Series(b)
	v := pairVal{at: now}
	if okA && okB {
		v.corr, v.lag = LaggedCorrelation(sa, sb, maxLag)
		v.ok = !math.IsNaN(v.corr)
		if !v.ok {
			v.corr = 0
		}
	}
	c.mu.Lock()
	c.cache[k] = v
	c.mu.Unlock()
	return v.corr, v.lag, v.ok
}

// LaggedCorrelation returns the highest Pearson correlation between a and b
// shifted by 0..maxLag (b lagging a), over time-aligned non-NaN points. It
// returns NaN when no lag has enough overlap or variance.
func LaggedCorrelation(a, b forecast.Series, maxLag time.Duration) (corr float64, lag time.Duration) {
	corr = math.NaN()
	if a.Step <= 0 || b.Step != a.Step {
		return corr, 0
	}
	bv := make(map[int64]float64, len(b.Points))
	for _, p := range b.Points {
		if !math.IsNaN(p.V) {
			bv[p.T.UnixNano()] = p.V
		}
	}
	for k := 0; time.Duration(k)*a.Step <= maxLag; k++ {
		shift := time.Duration(k) * a.Step
		var xs, ys []float64
		for _, p := range a.Points {
			if math.IsNaN(p.V) {
				continue
			}
			if y, ok := bv[p.T.Add(shift).UnixNano()]; ok {
				xs, ys = append(xs, p.V), append(ys, y)
			}
		}
		r, ok := pearson(xs, ys)
		if ok && (math.IsNaN(corr) || r > corr) {
			corr, lag = r, shift
		}
	}
	return corr, lag
}

func pearson(xs, ys []float64) (float64, bool) {
	n := len(xs)
	if n < minOverlap {
		return 0, false
	}
	var mx, my float64
	for i := range xs {
		mx += xs[i]
		my += ys[i]
	}
	mx /= float64(n)
	my /= float64(n)
	var sxy, sxx, syy float64
	for i := range xs {
		dx, dy := xs[i]-mx, ys[i]-my
		sxy += dx * dy
		sxx += dx * dx
		syy += dy * dy
	}
	if sxx == 0 || syy == 0 {
		return 0, false
	}
	return sxy / math.Sqrt(sxx*syy), true
}
