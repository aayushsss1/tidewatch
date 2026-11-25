package overrides

import (
	"testing"
	"time"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const step = 15 * time.Second

// sample builds a sample at tick i with a baseline capacity of 100.
func sample(i int, load, ready float64, replicas, needed int32) Sample {
	return Sample{
		T: t0.Add(time.Duration(i) * step), Load: load, CapacityReady: ready,
		BaselineCapacity: 100, Replicas: replicas, NeededReplicas: needed,
	}
}

func TestScore_TimelyAndNeeded(t *testing.T) {
	out := Score([]Sample{
		sample(0, 80, 100, 5, 5),
		sample(1, 130, 160, 8, 8), // first point above the baseline: capacity already there
		sample(2, 150, 160, 8, 8),
	}, v1.Override{}, step)
	if out.Timely == nil || !*out.Timely || !out.Needed {
		t.Fatalf("got %+v", out)
	}
}

func TestScore_LateWhenCapacityTrailsLoad(t *testing.T) {
	out := Score([]Sample{
		sample(0, 80, 100, 5, 5),
		sample(1, 130, 160, 8, 8),
		sample(2, 150, 120, 6, 8), // capacity dips below load after the first exceedance
	}, v1.Override{}, step)
	if out.Timely == nil || *out.Timely || !out.Needed {
		t.Fatalf("got %+v", out)
	}
	// Capacity trailing at the very first exceedance is late too.
	out = Score([]Sample{sample(0, 130, 100, 5, 8), sample(1, 130, 160, 8, 8)}, v1.Override{}, step)
	if out.Timely == nil || *out.Timely {
		t.Fatalf("got %+v", out)
	}
}

func TestScore_UnneededHasNilTimely(t *testing.T) {
	out := Score([]Sample{sample(0, 60, 100, 5, 4), sample(1, 100, 100, 5, 5)}, v1.Override{}, step)
	if out.Timely != nil || out.Needed {
		t.Fatalf("load never exceeded the baseline: %+v", out)
	}
	if out := Score(nil, v1.Override{}, step); out.Timely != nil || out.Needed || out.WastedReplicaMinutes != "0.00" {
		t.Fatalf("no samples: %+v", out)
	}
}

func TestScore_WastedReplicaMinutes(t *testing.T) {
	ss := make([]Sample, 0, 5)
	for i := range 4 {
		ss = append(ss, sample(i, 50, 100, 7, 5)) // 2 extra replicas each
	}
	if out := Score(ss, v1.Override{}, step); out.WastedReplicaMinutes != "2.00" {
		t.Fatalf("got %q", out.WastedReplicaMinutes)
	}
	// Under-provisioned samples never subtract.
	ss = append(ss, sample(4, 50, 100, 3, 5))
	if out := Score(ss, v1.Override{}, step); out.WastedReplicaMinutes != "2.00" {
		t.Fatalf("got %q", out.WastedReplicaMinutes)
	}
}
