package workload

import (
	"testing"
	"time"
)

func TestTimeToReady_MeasuresScaleUp(t *testing.T) {
	var r TimeToReady
	t0 := time.Unix(1000, 0)
	r.ScaledUp(t0, 5)
	r.Observe(t0.Add(40*time.Second), 3)
	r.Observe(t0.Add(50*time.Second), 5)
	if got := r.P90(time.Minute); got != 50*time.Second {
		t.Fatalf("P90 = %v", got)
	}
	st := r.Status()
	if st.Samples != 1 || st.P90Seconds != 50 || st.P50Seconds != 50 {
		t.Fatalf("status = %+v", st)
	}
	// Closed measurement: further observations add nothing.
	r.Observe(t0.Add(90*time.Second), 5)
	if r.Status().Samples != 1 {
		t.Fatal("measurement should be closed")
	}
}

func TestTimeToReady_DefaultWithoutSamples(t *testing.T) {
	var r TimeToReady
	if got := r.P90(60 * time.Second); got != 60*time.Second {
		t.Fatalf("got %v", got)
	}
	if r.Status().Samples != 0 {
		t.Fatal("expected no samples")
	}
}

func TestTimeToReady_ScaledUpReplacesOpenMeasurement(t *testing.T) {
	var r TimeToReady
	t0 := time.Unix(1000, 0)
	r.ScaledUp(t0, 5)
	r.ScaledUp(t0.Add(10*time.Second), 8)
	r.Observe(t0.Add(30*time.Second), 8)
	if got := r.P90(time.Minute); got != 20*time.Second {
		t.Fatalf("got %v", got)
	}
}

func TestTimeToReady_KeepsLast20(t *testing.T) {
	var r TimeToReady
	t0 := time.Unix(1000, 0)
	for i := 1; i <= 25; i++ {
		r.ScaledUp(t0, 1)
		r.Observe(t0.Add(time.Duration(i)*time.Second), 1)
	}
	st := r.Status()
	if st.Samples != 20 {
		t.Fatalf("samples = %d", st.Samples)
	}
	// Remaining samples are 6..25s; the oldest (1..5s) were dropped.
	if st.P90Seconds != 23 || st.P50Seconds != 15 {
		t.Fatalf("status = %+v", st)
	}
}
