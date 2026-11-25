package workload

import (
	"slices"
	"time"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const maxReadySamples = 20

// TimeToReady tracks how long scale-ups take to become ready, keeping the
// last 20 samples. The zero value is ready to use. It is not safe for
// concurrent use.
type TimeToReady struct {
	samples []time.Duration // oldest first
	open    bool
	start   time.Time
	to      int32
}

// ScaledUp starts a measurement, replacing any open one.
func (r *TimeToReady) ScaledUp(at time.Time, to int32) {
	r.open, r.start, r.to = true, at, to
}

// Observe closes the open measurement once ready reaches the target.
func (r *TimeToReady) Observe(now time.Time, ready int32) {
	if !r.open || ready < r.to {
		return
	}
	r.open = false
	r.samples = append(r.samples, now.Sub(r.start))
	if len(r.samples) > maxReadySamples {
		r.samples = r.samples[len(r.samples)-maxReadySamples:]
	}
}

// P90 returns the 90th percentile of recorded samples, or def with none.
func (r *TimeToReady) P90(def time.Duration) time.Duration {
	if len(r.samples) == 0 {
		return def
	}
	return r.percentile(0.9)
}

// Status summarizes the samples for a TideWorkload status.
func (r *TimeToReady) Status() v1.TimeToReadyStatus {
	if len(r.samples) == 0 {
		return v1.TimeToReadyStatus{}
	}
	return v1.TimeToReadyStatus{
		P50Seconds: int32(r.percentile(0.5).Round(time.Second) / time.Second),
		P90Seconds: int32(r.percentile(0.9).Round(time.Second) / time.Second),
		Samples:    int32(len(r.samples)),
	}
}

// percentile uses the nearest-rank method; samples must be non-empty.
func (r *TimeToReady) percentile(p float64) time.Duration {
	sorted := slices.Clone(r.samples)
	slices.Sort(sorted)
	rank := int(p*float64(len(sorted))+0.999999999) - 1
	return sorted[min(max(rank, 0), len(sorted)-1)]
}
