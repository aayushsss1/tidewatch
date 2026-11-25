package main

import (
	"math"
	"testing"
	"time"
)

func TestPattern_PeakAtHalfPeriod(t *testing.T) {
	p := Pattern{Base: 10, Amp: 90, Period: time.Hour, Epoch: time.Unix(1_700_000_000, 0)}
	if got := p.RPS(p.Epoch.Add(p.Period / 2)); got < 99.999 || got > 100.001 {
		t.Fatalf("peak = %v, want 100", got)
	}
	if got := p.RPS(p.Epoch); got < 9.999 || got > 10.001 {
		t.Fatalf("trough = %v, want 10", got)
	}
}

func TestPattern_Deterministic(t *testing.T) {
	p := Pattern{Base: 10, Amp: 90, Period: time.Hour, Epoch: time.Unix(0, 0), Noise: 0.2, Seed: 7}
	ts := time.Unix(1_700_000_123, 0)
	if p.RPS(ts) != p.RPS(ts) {
		t.Fatal("not deterministic")
	}
	q := p
	q.Seed = 8
	if p.RPS(ts) == q.RPS(ts) {
		t.Fatal("seed has no effect")
	}
}

func TestPattern_SpikeMultipliesWithinWindow(t *testing.T) {
	epoch := time.Unix(1_700_000_000, 0)
	base := Pattern{Base: 10, Amp: 90, Period: time.Hour, Epoch: epoch}
	at := epoch.Add(14 * time.Minute)
	p := base
	p.SpikeAt, p.SpikeMult, p.SpikeRamp, p.SpikeDur = at, 4, 2*time.Minute, 8*time.Minute

	near := func(got, want float64, what string) {
		t.Helper()
		if math.Abs(got-want) > 1e-6*math.Max(1, want) {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	}
	// Unchanged outside the window.
	for _, off := range []time.Duration{-time.Hour, -time.Second, 0, 11 * time.Minute} {
		ts := at.Add(off)
		if off >= 0 && off < 10*time.Minute {
			continue
		}
		near(p.RPS(ts), base.RPS(ts), "outside window")
	}
	near(p.RPS(at), base.RPS(at), "spike start")
	// Halfway up the ramp: 1 + 3*0.5 = 2.5x.
	ts := at.Add(time.Minute)
	near(p.RPS(ts), 2.5*base.RPS(ts), "mid ramp")
	// Holding at the full multiplier.
	ts = at.Add(3 * time.Minute)
	near(p.RPS(ts), 4*base.RPS(ts), "hold")
	// Back to base once ramp+duration has passed.
	ts = at.Add(10*time.Minute + time.Second)
	near(p.RPS(ts), base.RPS(ts), "after spike")
	// No spike configured: identical.
	near(base.RPS(at.Add(3*time.Minute)), Pattern{Base: 10, Amp: 90, Period: time.Hour, Epoch: epoch}.RPS(at.Add(3*time.Minute)), "no spike")
}

func TestPattern_SpikeDefaults(t *testing.T) {
	epoch := time.Unix(1_700_000_000, 0)
	base := Pattern{Base: 10, Period: time.Hour, Epoch: epoch}
	p := base
	p.SpikeAt = epoch.Add(time.Minute)
	ts := epoch.Add(time.Minute + 3*time.Minute)
	if got, want := p.RPS(ts), 4*base.RPS(ts); math.Abs(got-want) > 1e-9 {
		t.Fatalf("default spike = %v, want %v", got, want)
	}
}
