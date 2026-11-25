package main

import (
	"math"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

func TestEncodeWriteRequest_RoundTrip(t *testing.T) {
	raw := encodeWriteRequest([]tsData{{
		labels:  map[string]string{"__name__": "m", "a": "b"},
		samples: []sample{{value: 12.5, ts: 1700000000123}},
	}})
	tsb, n := protowire.ConsumeBytes(raw[func() int { _, _, m := protowire.ConsumeTag(raw); return m }():])
	if n < 0 {
		t.Fatal("bad timeseries")
	}
	labels, samples := 0, 0
	for len(tsb) > 0 {
		num, _, m := protowire.ConsumeTag(tsb)
		tsb = tsb[m:]
		b, m := protowire.ConsumeBytes(tsb)
		tsb = tsb[m:]
		switch num {
		case 1:
			labels++
		case 2:
			samples++
			_, _, m := protowire.ConsumeTag(b)
			v, m2 := protowire.ConsumeFixed64(b[m:])
			if math.Float64frombits(v) != 12.5 {
				t.Fatalf("value %v", math.Float64frombits(v))
			}
			_, _, m3 := protowire.ConsumeTag(b[m+m2:])
			ts, _ := protowire.ConsumeVarint(b[m+m2+m3:])
			if int64(ts) != 1700000000123 {
				t.Fatalf("ts %d", ts)
			}
		}
	}
	if labels != 2 || samples != 1 {
		t.Fatalf("labels=%d samples=%d", labels, samples)
	}
}

func TestCounterSamples_MonotonicAndAligned(t *testing.T) {
	p := Pattern{Base: 10, Amp: 0, Period: time.Hour, Epoch: time.Unix(0, 0)}
	now := time.Unix(1_700_000_000, 0)
	s := counterSamples(p, now, time.Minute, 15*time.Second)
	if len(s) < 4 {
		t.Fatalf("len=%d", len(s))
	}
	for i := 1; i < len(s); i++ {
		if s[i].value-s[i-1].value != 150 || s[i].ts-s[i-1].ts != 15000 {
			t.Fatalf("bad step at %d: %+v %+v", i, s[i-1], s[i])
		}
	}
}

func TestSchedule(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	for _, n := range []int{0, 1, 7, 300} {
		s := schedule(start, n)
		if len(s) != n {
			t.Fatalf("n=%d len=%d", n, len(s))
		}
		for i, at := range s {
			if at.Before(start) || !at.Before(start.Add(time.Second)) {
				t.Fatalf("n=%d offset %d out of range", n, i)
			}
			if i > 0 && !at.After(s[i-1]) {
				t.Fatalf("n=%d not strictly increasing at %d", n, i)
			}
		}
	}
}
