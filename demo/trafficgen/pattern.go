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
	v := p.Base + p.Amp*(0.5-0.5*math.Cos(2*math.Pi*phase))
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
