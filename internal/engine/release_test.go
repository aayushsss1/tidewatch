package engine

import (
	"math"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

// releaseRig drives releaseOverrides one minute at a time. Baseline capacity is
// 2 replicas × 10 × 0.8 = 16.
type releaseRig struct {
	t      *testing.T
	e      *Engine
	store  *overrides.Store
	st     *wlState
	tw     *v1.TideWorkload
	start  time.Time
	events []v1.Decision // decisions of the last tick
}

func newReleaseRig(t *testing.T, start time.Time) *releaseRig {
	store := overrides.NewStore()
	tw := &v1.TideWorkload{}
	tw.UID, tw.Namespace, tw.Name = "uid-r", "ns", "wl"
	if err := store.AddOverride(tw.UID, v1.Override{
		ID: "o1", IncidentID: "inc", MinReplicas: 8,
		Start: metav1.NewTime(start), End: metav1.NewTime(start.Add(90 * time.Minute)),
	}); err != nil {
		t.Fatal(err)
	}
	return &releaseRig{t: t, e: &Engine{ov: store}, store: store, st: &wlState{}, tw: tw, start: start}
}

// tick evaluates minute m after the override start and reports whether it was released.
func (r *releaseRig) tick(m int, actual float64, healthy bool) bool {
	it := &item{
		tw: r.tw, st: r.st, now: r.start.Add(time.Duration(m) * time.Minute), actual: actual,
		cfg:   workload.EffectiveConfig{TargetPerReplica: 10, Headroom: 0.8},
		scale: workload.ScaleState{Replicas: 8},
	}
	it.base.Desired = 2
	r.e.releaseOverrides(it, healthy)
	r.events = it.events
	return len(it.events) > 0
}

func TestReleaseOverrides_AfterTenQuietMinutes(t *testing.T) {
	r := newReleaseRig(t, t0)
	for m := range 10 {
		if r.tick(m, 12, true) {
			t.Fatalf("released too early at minute %d", m)
		}
	}
	if !r.tick(10, 12, true) {
		t.Fatal("must release once 10 quiet minutes have passed")
	}
	now := t0.Add(10 * time.Minute)
	if m, o := r.store.Active(r.tw.UID, now); m != 0 || o != nil {
		t.Fatal("override must no longer be active")
	}
	res := r.store.TakeResolutions()
	if len(res) != 1 || res[0].Verdict != overrides.ResEnded || res[0].By != "early_release" {
		t.Fatalf("resolutions = %+v", res)
	}
	ev := r.events
	if len(ev) != 1 || len(ev[0].Reasons) != 1 || ev[0].Reasons[0] != "early_release" || ev[0].IncidentID != "inc" {
		t.Fatalf("events = %+v", ev)
	}
}

func TestReleaseOverrides_NotWhileLoadExceedsBaseline(t *testing.T) {
	r := newReleaseRig(t, t0)
	for m := range 30 {
		load := 12.0
		if m%8 == 7 { // a spike every 8 minutes keeps resetting the quiet streak
			load = 17
		}
		if r.tick(m, load, true) {
			t.Fatalf("released at minute %d despite load above baseline capacity", m)
		}
	}
	if r.tick(31, 16, true) || !r.tick(41, 16, true) { // exactly at capacity counts as quiet
		t.Fatal("load equal to baseline capacity is quiet: release 10m after the streak began")
	}
}

func TestReleaseOverrides_NotBeforeStartPlusTenMinutes(t *testing.T) {
	// Quiet since before the override began: still no release before start+10m.
	r := newReleaseRig(t, t0.Add(10*time.Minute))
	r.st.quietSince = map[string]time.Time{"o1/inc": t0}
	if r.tick(0, 5, true) || r.tick(9, 5, true) {
		t.Fatal("released before start+10m")
	}
	if !r.tick(10, 5, true) {
		t.Fatal("must release at start+10m when load has been quiet for 10m")
	}
}

func TestReleaseOverrides_DegradedDataNeverCounts(t *testing.T) {
	r := newReleaseRig(t, t0)
	for m := range 9 {
		r.tick(m, 5, true)
	}
	if r.tick(9, math.NaN(), true) || r.tick(10, 5, false) {
		t.Fatal("NaN or unhealthy data must not release")
	}
	// The streak restarted: 10 more clean minutes are needed.
	for m := 11; m < 20; m++ {
		if r.tick(m, 5, true) {
			t.Fatalf("released at minute %d, streak should have restarted at 11", m)
		}
	}
	if !r.tick(21, 5, true) {
		t.Fatal("must release after a fresh 10 quiet minutes")
	}
}
