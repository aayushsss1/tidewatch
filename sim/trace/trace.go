// Package trace provides load traces for the benchmark simulator: a synthetic diurnal
// pattern and loaders for the Azure Functions 2019 invocation trace. Everything converts
// to forecast.Series, with values in requests per second.
package trace

import (
	"time"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// Source is a load trace addressable by offset from the trace origin. Offsets outside the
// trace's natural extent are allowed (finite traces wrap), so scenarios can ask for
// history before their start.
type Source interface {
	// At returns the offered load in requests per second at the offset.
	At(off time.Duration) float64
}

// Sample evaluates src every step over [from, to] (inclusive) and returns the result as a
// forecast.Series whose timestamps are epoch+offset.
func Sample(src Source, epoch time.Time, from, to, step time.Duration) forecast.Series {
	s := forecast.Series{Step: step}
	for off := from; off <= to; off += step {
		s.Points = append(s.Points, forecast.Point{T: epoch.Add(off), V: src.At(off)})
	}
	return s
}

// mod returns d modulo m in [0, m).
func mod(d, m time.Duration) time.Duration {
	d %= m
	if d < 0 {
		d += m
	}
	return d
}
