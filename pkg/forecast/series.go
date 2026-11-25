// Package forecast provides pure-Go time-series forecasters and an accuracy tracker.
package forecast

import (
	"math"
	"slices"
	"time"
)

// Point is a single sample. V may be NaN to mark a missing value.
type Point struct {
	T time.Time
	V float64
}

// Series is a regularly spaced series with strictly increasing T aligned to Step.
type Series struct {
	Step   time.Duration
	Points []Point
}

// FillGaps linearly interpolates runs of NaN no longer than maxGap points that
// have a valid neighbour on both sides. It returns a new series and the count of
// NaN points remaining. The receiver is not modified.
func (s Series) FillGaps(maxGap int) (Series, int) {
	out := Series{Step: s.Step, Points: make([]Point, len(s.Points))}
	copy(out.Points, s.Points)
	pts := out.Points
	for i := 0; i < len(pts); {
		if !math.IsNaN(pts[i].V) {
			i++
			continue
		}
		j := i
		for j < len(pts) && math.IsNaN(pts[j].V) {
			j++
		}
		if runLen := j - i; i > 0 && j < len(pts) && runLen <= maxGap {
			a, b := pts[i-1].V, pts[j].V
			for k := i; k < j; k++ {
				frac := float64(k-i+1) / float64(runLen+1)
				pts[k].V = a + (b-a)*frac
			}
		}
		i = j
	}
	remaining := 0
	for _, p := range pts {
		if math.IsNaN(p.V) {
			remaining++
		}
	}
	return out, remaining
}

// Last returns the last non-NaN point.
func (s Series) Last() (Point, bool) {
	for _, p := range slices.Backward(s.Points) {
		if !math.IsNaN(p.V) {
			return p, true
		}
	}
	return Point{}, false
}
