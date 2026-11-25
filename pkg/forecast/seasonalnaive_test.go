package forecast

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestSeasonalNaive_RepeatsLastSeason(t *testing.T) {
	step := time.Minute
	vals := make([]float64, 0, 12)
	for range 3 {
		vals = append(vals, 1, 2, 3, 4)
	}
	s := mkSeries(vals...)
	f, err := NewSeasonalNaive().Forecast(context.Background(), s, 6*step, 4*step)
	if err != nil {
		t.Fatal(err)
	}
	if f.Model != "seasonal-naive" {
		t.Errorf("Model = %q", f.Model)
	}
	want := []float64{1, 2, 3, 4, 1, 2}
	if len(f.Points) != len(want) {
		t.Fatalf("got %d points, want %d", len(f.Points), len(want))
	}
	last, _ := s.Last()
	for i, p := range f.Points {
		if p.Point != want[i] {
			t.Errorf("point %d = %v, want %v", i, p.Point, want[i])
		}
		if wantT := last.T.Add(time.Duration(i+1) * step); !p.T.Equal(wantT) {
			t.Errorf("point %d T = %v, want %v", i, p.T, wantT)
		}
	}
}

func TestSeasonalNaive_InsufficientHistory(t *testing.T) {
	s := mkSeries(1, 2, 3)
	_, err := NewSeasonalNaive().Forecast(context.Background(), s, 4*time.Minute, 4*time.Minute)
	if !errors.Is(err, ErrInsufficientHistory) {
		t.Fatalf("err = %v, want ErrInsufficientHistory", err)
	}
}

func TestSeasonalNaive_IntervalWidensWithSeasonsAhead(t *testing.T) {
	step := time.Minute
	vals := make([]float64, 12)
	for i := range vals {
		vals[i] = 100 + float64((i*7)%5)*3
	}
	s := mkSeries(vals...)
	f, err := NewSeasonalNaive().Forecast(context.Background(), s, 8*step, 4*step)
	if err != nil {
		t.Fatal(err)
	}
	w1 := f.Points[0].Upper - f.Points[0].Point
	w2 := f.Points[4].Upper - f.Points[4].Point
	if w1 <= 0 {
		t.Fatalf("expected positive width, got %v", w1)
	}
	if math.Abs(w2-math.Sqrt2*w1) > 1e-9 {
		t.Errorf("w2 = %v, want sqrt2*w1 = %v", w2, math.Sqrt2*w1)
	}
}

func TestSeasonalNaive_NonFiniteFails(t *testing.T) {
	s := mkSeries(1, 2, math.Inf(1), 4)
	_, err := NewSeasonalNaive().Forecast(context.Background(), s, 4*time.Minute, 4*time.Minute)
	if !errors.Is(err, ErrModelFailed) {
		t.Fatalf("err = %v, want ErrModelFailed", err)
	}
}

func TestForecast_PeakRespectsUntil(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	f := Forecast{Points: []ForecastPoint{
		{T: t0, Point: 1, Upper: 2},
		{T: t0.Add(time.Minute), Point: 5, Upper: 9},
		{T: t0.Add(2 * time.Minute), Point: 100, Upper: 200},
	}}
	p, ok := f.Peak(t0.Add(time.Minute))
	if !ok || p.Point != 5 {
		t.Fatalf("Peak = %v, %v; want 5", p, ok)
	}
	u, ok := f.MaxUpper(t0.Add(time.Minute))
	if !ok || u != 9 {
		t.Fatalf("MaxUpper = %v, %v; want 9", u, ok)
	}
	if _, ok := f.Peak(t0.Add(-time.Minute)); ok {
		t.Error("Peak before first point should be !ok")
	}
	if _, ok := f.MaxUpper(t0.Add(-time.Minute)); ok {
		t.Error("MaxUpper before first point should be !ok")
	}
}

func TestRegistry_UnknownModel(t *testing.T) {
	r := NewRegistry(NewSeasonalNaive())
	fs, err := r.Get([]string{"seasonal-naive"})
	if err != nil || len(fs) != 1 {
		t.Fatalf("Get known: %v, %v", fs, err)
	}
	_, err = r.Get([]string{"nope"})
	if err == nil || !strings.Contains(err.Error(), "nope") {
		t.Fatalf("err = %v, want mention of nope", err)
	}
}

func TestSeasonalNaive_NaNAtSeasonAgoUsesEarlierSeason(t *testing.T) {
	nan := math.NaN()
	s := mkSeries(1, 2, 3, 4, 5, 2, 3, 4, nan, 2, 3, 4)
	f, err := NewSeasonalNaive().Forecast(context.Background(), s, 4*time.Minute, 4*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Points[0].Point; got != 5 {
		t.Errorf("point 0 = %v, want 5 (one season earlier)", got)
	}
}

func TestSeasonalNaive_NoValidSeasonUsesLastValid(t *testing.T) {
	nan := math.NaN()
	s := mkSeries(nan, 2, 3, 4, nan, 2, 3, 4, nan, 2, 3, 4)
	f, err := NewSeasonalNaive().Forecast(context.Background(), s, 4*time.Minute, 4*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Points[0].Point; got != 4 {
		t.Errorf("point 0 = %v, want last valid 4", got)
	}
}
