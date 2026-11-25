package forecast

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

const (
	hwStep   = time.Hour
	hwSeason = 24 * time.Hour
)

func sineAt(i int) float64 {
	return 200 + 100*math.Sin(2*math.Pi*float64(i)/24)
}

func hwSeries(n int, f func(i int) float64) Series {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pts := make([]Point, n)
	for i := range pts {
		pts[i] = Point{T: start.Add(time.Duration(i) * hwStep), V: f(i)}
	}
	return Series{Step: hwStep, Points: pts}
}

func TestHoltWinters_RecoversCleanSeasonalPattern(t *testing.T) {
	s := hwSeries(96, sineAt)
	f, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if err != nil {
		t.Fatal(err)
	}
	if f.Model != "holt-winters" || len(f.Points) != 24 {
		t.Fatalf("model %q, %d points", f.Model, len(f.Points))
	}
	var ape float64
	for h, p := range f.Points {
		truth := sineAt(96 + h)
		ape += math.Abs(p.Point-truth) / truth
	}
	if mape := ape / 24; mape >= 0.05 {
		t.Errorf("MAPE = %.4f, want < 0.05", mape)
	}
}

func TestHoltWinters_TracksTrend(t *testing.T) {
	s := hwSeries(96, func(i int) float64 { return sineAt(i) + float64(i) })
	f, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if err != nil {
		t.Fatal(err)
	}
	// Same seasonal phase one season later sits about 24 higher than the last season.
	if got, want := f.Points[23].Point, s.Points[95].V+24; math.Abs(got-want) > 20 {
		t.Errorf("point[m] = %v, want about %v", got, want)
	}
	if f.Points[23].Point <= s.Points[95].V {
		t.Errorf("point[m] = %v should exceed last observed %v", f.Points[23].Point, s.Points[95].V)
	}
}

func TestHoltWinters_InsufficientHistory(t *testing.T) {
	s := hwSeries(47, sineAt)
	_, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if !errors.Is(err, ErrInsufficientHistory) {
		t.Fatalf("err = %v, want ErrInsufficientHistory", err)
	}
}

func TestHoltWinters_IntervalContainsTruthOnNoisyData(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	noisy := func(i int) float64 { return sineAt(i) + rng.NormFloat64()*5 }
	s := hwSeries(96, noisy)
	f, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if err != nil {
		t.Fatal(err)
	}
	inside := 0
	for h, p := range f.Points {
		truth := noisy(96 + h)
		if truth >= p.Lower && truth <= p.Upper {
			inside++
		}
	}
	if frac := float64(inside) / 24; frac < 0.8 {
		t.Errorf("coverage = %.2f, want >= 0.80", frac)
	}
}

func TestHoltWinters_ClampsNegativeForecasts(t *testing.T) {
	declining := hwSeries(96, func(i int) float64 { return 1000 - 10*float64(i) + 20*math.Sin(float64(i)) })
	for _, in := range []Series{declining} {
		f, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), in, hwSeason, hwSeason)
		if err != nil {
			t.Fatal(err)
		}
		for i, p := range f.Points {
			if p.Point < 0 || p.Lower < 0 || p.Upper < 0 {
				t.Fatalf("point %d has negative value: %+v", i, p)
			}
		}
	}
}

func TestHoltWinters_NaNInputYieldsModelFailed(t *testing.T) {
	s := hwSeries(96, func(i int) float64 {
		if i == 50 {
			return math.Inf(1)
		}
		return sineAt(i)
	})
	_, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if !errors.Is(err, ErrModelFailed) {
		t.Fatalf("err = %v, want ErrModelFailed", err)
	}
}

func TestHoltWinters_RefitsOnlyEveryN(t *testing.T) {
	hw := NewHoltWinters(HoltWintersOptions{RefitEvery: 3})
	s := hwSeries(96, sineAt)
	for range 3 {
		if _, err := hw.Forecast(context.Background(), s, hwSeason, hwSeason); err != nil {
			t.Fatal(err)
		}
	}
	if got := hw.(*holtWinters).fits; got != 1 {
		t.Errorf("fits after 3 calls = %d, want 1", got)
	}
	if _, err := hw.Forecast(context.Background(), s, hwSeason, hwSeason); err != nil {
		t.Fatal(err)
	}
	if got := hw.(*holtWinters).fits; got != 2 {
		t.Errorf("fits after 4 calls = %d, want 2", got)
	}
}

func TestHoltWinters_IntervalCoverageWithMinimalHistory(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 9))
	noisy := func(i int) float64 { return sineAt(i) + rng.NormFloat64()*5 }
	s := hwSeries(48, noisy) // exactly 2m points
	f, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if err != nil {
		t.Fatal(err)
	}
	inside := 0
	for h, p := range f.Points {
		if truth := noisy(48 + h); truth >= p.Lower && truth <= p.Upper {
			inside++
		}
	}
	if frac := float64(inside) / 24; frac < 0.7 {
		t.Errorf("coverage = %.2f, want >= 0.70", frac)
	}
}

func TestHoltWinters_HalfWidthReflectsNoiseWithMinimalHistory(t *testing.T) {
	const sigmaTrue = 5.0
	rng := rand.New(rand.NewPCG(7, 9))
	s := hwSeries(48, func(i int) float64 { return sineAt(i) + rng.NormFloat64()*sigmaTrue })
	f, err := NewHoltWinters(HoltWintersOptions{}).Forecast(context.Background(), s, hwSeason, hwSeason)
	if err != nil {
		t.Fatal(err)
	}
	half := f.Points[0].Upper - f.Points[0].Point
	ratio := half / (z90 * sigmaTrue)
	t.Logf("ratio = %.3f", ratio)
	if ratio < 1.0 || ratio > 1.6 {
		t.Errorf("h=1 half-width = %.3f (%.2fx of 1.645*sigma_true), want within [1.0, 1.6]x", half, ratio)
	}
}
