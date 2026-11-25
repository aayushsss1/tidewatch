package forecast

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// ErrModelFailed is returned when a model cannot produce finite values.
var ErrModelFailed = errors.New("model failed")

const defaultRefitEvery = 60

var hwGrid = []float64{0.05, 0.1, 0.2, 0.3, 0.5, 0.7, 0.9}

// HoltWintersOptions configures NewHoltWinters.
type HoltWintersOptions struct {
	// RefitEvery is the number of Forecast calls between parameter refits (default 60).
	RefitEvery int
}

type hwParams struct{ alpha, beta, gamma float64 }

// holtWinters keeps its fitted parameters per instance: use one per workload.
type holtWinters struct {
	refitEvery int

	mu     sync.Mutex
	calls  int
	fits   int // number of grid-search fits performed; read by tests
	params hwParams
}

// NewHoltWinters returns an additive Holt-Winters forecaster. Parameters are
// cached in the returned instance, so use one instance per workload.
func NewHoltWinters(opts HoltWintersOptions) Forecaster {
	if opts.RefitEvery <= 0 {
		opts.RefitEvery = defaultRefitEvery
	}
	return &holtWinters{refitEvery: opts.RefitEvery}
}

func (*holtWinters) Name() string { return "holt-winters" }

func (hw *holtWinters) Forecast(ctx context.Context, s Series, horizon, season time.Duration) (Forecast, error) {
	if err := ctx.Err(); err != nil {
		return Forecast{}, err
	}
	if s.Step <= 0 || season < s.Step {
		return Forecast{}, fmt.Errorf("invalid step %v or season %v", s.Step, season)
	}
	m := int(season / s.Step)
	n := len(s.Points)
	if n < 2*m {
		return Forecast{}, fmt.Errorf("%w: have %d points, need %d", ErrInsufficientHistory, n, 2*m)
	}
	y := make([]float64, n)
	for i, p := range s.Points {
		y[i] = p.V
	}
	for _, v := range y {
		if math.IsInf(v, 0) {
			return Forecast{}, fmt.Errorf("%w: non-finite input", ErrModelFailed)
		}
	}
	if _, _, ok := hwInit(y, m); !ok {
		return Forecast{}, fmt.Errorf("%w: an initial season has no valid samples", ErrInsufficientHistory)
	}

	hw.mu.Lock()
	if hw.calls%hw.refitEvery == 0 {
		hw.params = fitHW(y, m)
		hw.fits++
	}
	hw.calls++
	params := hw.params
	hw.mu.Unlock()

	st := runHW(y, m, params)
	sigma := 0.0
	if len(st.errs) >= 2 {
		sigma = stdDev(st.errs)
	}
	last := s.Points[n-1].T
	steps := int(horizon / s.Step)
	f := Forecast{Model: "holt-winters", Points: make([]ForecastPoint, 0, steps)}
	varFactor := 1.0 // 1 + sum_{j=1}^{h-1} c_j^2
	for h := 1; h <= steps; h++ {
		if h > 1 {
			j := h - 1
			c := params.alpha * (1 + float64(j)*params.beta)
			if j%m == 0 {
				c += params.gamma
			}
			varFactor += c * c
		}
		v := st.level + float64(h)*st.trend + st.seasonal[(n+h-1)%m]
		half := z90 * sigma * math.Sqrt(varFactor)
		fp := ForecastPoint{
			T:     last.Add(time.Duration(h) * s.Step),
			Point: math.Max(v, 0),
			Lower: math.Max(v-half, 0),
			Upper: math.Max(v+half, 0),
		}
		if !finite(fp.Point, fp.Lower, fp.Upper) {
			return Forecast{}, fmt.Errorf("%w: holt-winters produced non-finite value", ErrModelFailed)
		}
		f.Points = append(f.Points, fp)
	}
	return f, nil
}

type hwState struct {
	level, trend float64
	seasonal     []float64
	errs         []float64 // in-sample one-step errors for t >= m
	sse          float64
}

// runHW initializes from the first two seasons and runs the additive recursion
// over all of y, collecting one-step errors for t >= m.
func runHW(y []float64, m int, p hwParams) hwState {
	m1, m2, _ := hwInit(y, m)
	st := hwState{level: m1, trend: (m2 - m1) / float64(m), seasonal: make([]float64, m), errs: make([]float64, 0, len(y))}
	for i := range m {
		switch {
		case !math.IsNaN(y[i]):
			st.seasonal[i] = y[i] - m1
		case !math.IsNaN(y[m+i]):
			st.seasonal[i] = y[m+i] - m2
		}
	}
	for t, v := range y {
		si := t % m
		pred := st.level + st.trend + st.seasonal[si]
		if math.IsNaN(v) {
			v = pred // a missing observation: assume the one-step prediction, no error term
		} else if t >= m { // the first season seeds the seasonal terms, so its errors are artifacts
			e := v - pred
			st.errs = append(st.errs, e)
			st.sse += e * e
		}
		prev := st.level
		st.level = p.alpha*(v-st.seasonal[si]) + (1-p.alpha)*(st.level+st.trend)
		st.trend = p.beta*(st.level-prev) + (1-p.beta)*st.trend
		st.seasonal[si] = p.gamma*(v-st.level) + (1-p.gamma)*st.seasonal[si]
	}
	return st
}

// hwInit returns the NaN-aware means of the first two seasons; ok is false when
// either season has no valid sample.
func hwInit(y []float64, m int) (m1, m2 float64, ok bool) {
	mean := func(xs []float64) (float64, bool) {
		var sum float64
		n := 0
		for _, x := range xs {
			if !math.IsNaN(x) {
				sum += x
				n++
			}
		}
		if n == 0 {
			return 0, false
		}
		return sum / float64(n), true
	}
	m1, ok1 := mean(y[:m])
	m2, ok2 := mean(y[m : 2*m])
	return m1, m2, ok1 && ok2
}

// fitHW grid-searches (alpha, beta, gamma) minimizing in-sample one-step SSE.
func fitHW(y []float64, m int) hwParams {
	best := hwParams{hwGrid[0], hwGrid[0], hwGrid[0]}
	bestSSE := math.Inf(1)
	for _, a := range hwGrid {
		for _, b := range hwGrid {
			for _, g := range hwGrid {
				p := hwParams{a, b, g}
				if sse := runHW(y, m, p).sse; sse < bestSSE {
					best, bestSSE = p, sse
				}
			}
		}
	}
	return best
}
