package main

import (
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
