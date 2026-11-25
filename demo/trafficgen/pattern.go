package main

import (
	"hash/fnv"
	"math"
	"time"
)

// Pattern is a sinusoidal request-rate pattern with deterministic noise.
type Pattern struct {
	Base, Amp float64
	Period    time.Duration
	Epoch     time.Time
	Noise     float64
	Seed      int64

	// Optional off-pattern spike. A zero SpikeAt disables it. Zero SpikeMult, SpikeRamp and
	// SpikeDur fall back to 4, 2m and 8m.
	SpikeAt             time.Time
	SpikeMult           float64
	SpikeRamp, SpikeDur time.Duration
}

// spikeFactor returns the spike multiplier at t: 1 before SpikeAt, a linear ramp to SpikeMult
// over SpikeRamp, a hold for SpikeDur, then 1 again.
func (p Pattern) spikeFactor(t time.Time) float64 {
	if p.SpikeAt.IsZero() {
		return 1
	}
	mult, ramp, dur := p.SpikeMult, p.SpikeRamp, p.SpikeDur
	if mult == 0 {
		mult = 4
	}
	if ramp == 0 {
		ramp = 2 * time.Minute
	}
	if dur == 0 {
		dur = 8 * time.Minute
	}
	d := t.Sub(p.SpikeAt)
	switch {
	case d < 0 || d >= ramp+dur:
		return 1
	case d < ramp:
		return 1 + (mult-1)*float64(d)/float64(ramp)
	default:
		return mult
	}
}

// RPS returns the target requests per second at t. The shape peaks at phase 0.5.
func (p Pattern) RPS(t time.Time) float64 {
	phase := 0.0
	if p.Period > 0 {
		d := t.Sub(p.Epoch) % p.Period
		if d < 0 {
			d += p.Period
		}
		phase = float64(d) / float64(p.Period)
	}
	v := (p.Base + p.Amp*(0.5-0.5*math.Cos(2*math.Pi*phase))) * p.spikeFactor(t)
	if p.Noise > 0 {
		h := fnv.New64a()
		var b [16]byte
		putU64(b[:8], uint64(p.Seed))
		putU64(b[8:], uint64(t.Unix()))
		_, _ = h.Write(b[:])
		u := float64(h.Sum64()>>11) / float64(1<<53) // [0,1)
		v *= 1 + p.Noise*(2*u-1)
	}
	return math.Max(v, 0)
}

func putU64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}
