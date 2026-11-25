package forecast

import (
	"context"
	"fmt"
	"math"
	"time"
)

const z90 = 1.645

type seasonalNaive struct{}

// NewSeasonalNaive returns a forecaster that repeats the last observed season.
func NewSeasonalNaive() Forecaster { return seasonalNaive{} }

func (seasonalNaive) Name() string { return "seasonal-naive" }

func (seasonalNaive) Forecast(_ context.Context, s Series, horizon, season time.Duration) (Forecast, error) {
	if s.Step <= 0 || season < s.Step {
		return Forecast{}, fmt.Errorf("invalid step %v or season %v", s.Step, season)
	}
	m := int(season / s.Step)
	n := len(s.Points)
	if n < m {
		return Forecast{}, fmt.Errorf("%w: have %d points, need %d", ErrInsufficientHistory, n, m)
	}
	y := make([]float64, n)
	for i, p := range s.Points {
		y[i] = p.V
	}
	sigma := seasonalDiffStdDev(y, m)
	last := s.Points[n-1].T
	steps := int(horizon / s.Step)
	f := Forecast{Model: "seasonal-naive", Points: make([]ForecastPoint, 0, steps)}
	for h := 1; h <= steps; h++ {
		k := (h + m - 1) / m
		v := y[n-1+h-m*k]
		half := z90 * sigma * math.Sqrt(float64(k))
		fp := ForecastPoint{
			T:     last.Add(time.Duration(h) * s.Step),
			Point: math.Max(v, 0),
			Lower: math.Max(v-half, 0),
			Upper: math.Max(v+half, 0),
		}
		if !finite(fp.Point, fp.Lower, fp.Upper) {
			return Forecast{}, fmt.Errorf("%w: seasonal-naive produced non-finite value", ErrModelFailed)
		}
		f.Points = append(f.Points, fp)
	}
	return f, nil
}

// seasonalDiffStdDev is the sample standard deviation of y[t]-y[t-m], skipping
// missing (NaN) pairs. It is 0 when fewer than two differences exist.
func seasonalDiffStdDev(y []float64, m int) float64 {
	var diffs []float64
	for t := m; t < len(y); t++ {
		d := y[t] - y[t-m]
		if math.IsNaN(d) {
			continue
		}
		diffs = append(diffs, d)
	}
	if len(diffs) < 2 {
		return 0
	}
	return stdDev(diffs)
}

func stdDev(xs []float64) float64 {
	var mean float64
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	var ss float64
	for _, x := range xs {
		ss += (x - mean) * (x - mean)
	}
	return math.Sqrt(ss / float64(len(xs)-1))
}

func finite(vs ...float64) bool {
	for _, v := range vs {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}
	return true
}
