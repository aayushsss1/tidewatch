package forecast

import (
	"math"
	"testing"
	"time"
)

func mkSeries(vals ...float64) Series {
	step := time.Minute
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pts := make([]Point, len(vals))
	for i, v := range vals {
		pts[i] = Point{T: start.Add(time.Duration(i) * step), V: v}
	}
	return Series{Step: step, Points: pts}
}

func TestFillGaps_InterpolatesShortRuns(t *testing.T) {
	s := mkSeries(1, math.NaN(), 3)
	got, remaining := s.FillGaps(1)
	if remaining != 0 {
		t.Fatalf("remaining = %d, want 0", remaining)
	}
	want := []float64{1, 2, 3}
	for i, p := range got.Points {
		if p.V != want[i] {
			t.Errorf("point %d = %v, want %v", i, p.V, want[i])
		}
	}
	if !math.IsNaN(s.Points[1].V) {
		t.Error("FillGaps mutated the receiver")
	}
}

func TestFillGaps_LeavesLongRuns(t *testing.T) {
	nan := math.NaN()
	s := mkSeries(1, nan, nan, nan, 5)
	got, remaining := s.FillGaps(2)
	if remaining != 3 {
		t.Fatalf("remaining = %d, want 3", remaining)
	}
	for i := 1; i <= 3; i++ {
		if !math.IsNaN(got.Points[i].V) {
			t.Errorf("point %d should remain NaN", i)
		}
	}
}

func TestSeries_Last(t *testing.T) {
	s := mkSeries(1, 2, math.NaN())
	p, ok := s.Last()
	if !ok || p.V != 2 {
		t.Fatalf("Last = %v, %v; want 2, true", p, ok)
	}
	if _, ok := mkSeries().Last(); ok {
		t.Error("empty series should have no last point")
	}
}
