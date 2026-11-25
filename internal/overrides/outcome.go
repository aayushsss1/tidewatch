package overrides

import (
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// Sample is one observation taken while an override was active. Capacities are
// in load units (replicas x targetPerReplica x headroom).
type Sample struct {
	T                time.Time
	Load             float64
	CapacityReady    float64 // what the ready replicas could serve
	BaselineCapacity float64 // what the baseline alone would have provisioned
	Replicas         int32
	NeededReplicas   int32
}

// Score judges an override retrospectively (spec 7.8).
//
// Needed: the load exceeded what the baseline alone would have provisioned.
// Timely: nil unless needed; true iff ready capacity covered the load at every
// sample from the first one that exceeded the baseline. Wasted is the replica
// minutes above what the load needed. ScoredAt is the last sample's time; the
// caller overrides it with the scoring time.
func Score(samples []Sample, _ v1.Override, step time.Duration) v1.Outcome {
	var out v1.Outcome
	first := -1
	var wasted float64
	for i, s := range samples {
		if first < 0 && s.Load > s.BaselineCapacity {
			first = i
		}
		wasted += float64(max(0, s.Replicas-s.NeededReplicas)) * step.Minutes()
	}
	if first >= 0 {
		out.Needed = true
		timely := true
		for _, s := range samples[first:] {
			if s.CapacityReady < s.Load {
				timely = false
				break
			}
		}
		out.Timely = &timely
	}
	out.WastedReplicaMinutes = fmt.Sprintf("%.2f", wasted)
	if n := len(samples); n > 0 {
		out.ScoredAt = metav1.NewTime(samples[n-1].T)
	}
	return out
}
