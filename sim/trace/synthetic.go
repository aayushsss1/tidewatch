package trace

import (
	"hash/fnv"
	"math"
	"time"
)

// Diurnal is the demo traffic pattern: Base + Amp*(0.5-0.5*cos(2*pi*phase)), where phase is
// the position within Period measured from the trace origin. It troughs at offset 0 and
// peaks half a period later. Noise is a deterministic multiplicative jitter of +-Noise.
type Diurnal struct {
	Base, Amp float64
	Period    time.Duration
	Noise     float64
	Seed      int64
}

// At implements Source.
func (d Diurnal) At(off time.Duration) float64 {
	phase := 0.0
	if d.Period > 0 {
		phase = float64(mod(off, d.Period)) / float64(d.Period)
	}
	v := d.Base + d.Amp*(0.5-0.5*math.Cos(2*math.Pi*phase))
	if d.Noise > 0 {
		h := fnv.New64a()
		var b [16]byte
		putU64(b[:8], uint64(d.Seed))
		putU64(b[8:], uint64(off/time.Second))
		_, _ = h.Write(b[:])
		u := float64(h.Sum64()>>11) / float64(1<<53) // [0,1)
		v *= 1 + d.Noise*(2*u-1)
	}
	return math.Max(v, 0)
}

func putU64(b []byte, v uint64) {
	for i := 0; i < 8; i++ {
		b[i] = byte(v >> (8 * i))
	}
}
