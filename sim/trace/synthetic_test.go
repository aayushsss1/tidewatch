package trace

import (
	"math"
	"testing"
	"time"
)

func TestDiurnalShape(t *testing.T) {
	d := Diurnal{Base: 10, Amp: 40, Period: 24 * time.Hour}
	if got := d.At(0); math.Abs(got-10) > 1e-9 {
		t.Errorf("trough = %v, want 10", got)
	}
	if got := d.At(12 * time.Hour); math.Abs(got-50) > 1e-9 {
		t.Errorf("peak = %v, want 50", got)
	}
	if got := d.At(-12 * time.Hour); math.Abs(got-50) > 1e-9 {
		t.Errorf("negative offset peak = %v, want 50", got)
	}
}

func TestDiurnalNoiseDeterministicBounded(t *testing.T) {
	d := Diurnal{Base: 100, Amp: 0, Period: time.Hour, Noise: 0.1, Seed: 7}
	a, b := d.At(5*time.Minute), d.At(5*time.Minute)
	if a != b {
		t.Fatal("noise not deterministic")
	}
	varied := false
	for i := range 100 {
		v := d.At(time.Duration(i) * time.Minute)
		if v < 90-1e-9 || v > 110+1e-9 {
			t.Fatalf("noise out of bounds: %v", v)
		}
		if v != a {
			varied = true
		}
	}
	if !varied {
		t.Error("noise constant")
	}
	if (Diurnal{Base: 100, Noise: 0.1, Seed: 8}).At(5*time.Minute) == a {
		t.Error("seed ignored")
	}
}

func TestSample(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := Sample(Diurnal{Base: 1, Amp: 1, Period: time.Hour}, epoch, -time.Hour, time.Hour, 10*time.Minute)
	if s.Step != 10*time.Minute || len(s.Points) != 13 {
		t.Fatalf("step %v len %d", s.Step, len(s.Points))
	}
	if !s.Points[0].T.Equal(epoch.Add(-time.Hour)) || !s.Points[12].T.Equal(epoch.Add(time.Hour)) {
		t.Errorf("bounds %v %v", s.Points[0].T, s.Points[12].T)
	}
}
