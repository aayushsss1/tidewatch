package engine

import (
	"math"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

const testIncident = "inc"

// releaseRig drives releaseOverrides one minute at a time with the real
// Baseline(). unit = 10 × 0.8 = 8; the override floor of 8 replicas covers 64
// of load, so the event counts as seen from a load of 44.8.
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
		ID: "o1", IncidentID: testIncident, MinReplicas: 8,
		Start: metav1.NewTime(start), End: metav1.NewTime(start.Add(90 * time.Minute)),
	}); err != nil {
		t.Fatal(err)
	}
	return &releaseRig{t: t, e: &Engine{ov: store}, store: store, st: &wlState{}, tw: tw, start: start}
}

// tick evaluates minute m after the override start. upper is the trusted
// forecast's upper bound inside the baseline lookahead (0 for no forecast).
// It reports whether the override was released.
func (r *releaseRig) tick(m int, actual, upper float64, healthy bool) bool {
	now := r.start.Add(time.Duration(m) * time.Minute)
	cfg := workload.EffectiveConfig{TargetPerReplica: 10, Headroom: 0.8}
	var trusted *forecast.Forecast
	if upper > 0 {
		trusted = &forecast.Forecast{Points: []forecast.ForecastPoint{{T: now.Add(2 * time.Minute), Upper: upper}}}
	}
	it := &item{
		tw: r.tw, st: r.st, now: now, actual: actual, cfg: cfg, scale: workload.ScaleState{Replicas: 8},
	}
	it.base = Baseline(BaselineInput{
		Now: now, Actual: actual, Trusted: trusted, TimeToReady: time.Minute, Margin: 2 * time.Minute, Config: cfg,
	})
	r.e.releaseOverrides(it, healthy)
	r.events = it.events
	return len(it.events) > 0
}

func TestReleaseOverrides_SpikeThenDecline(t *testing.T) {
	r := newReleaseRig(t, t0)
	for m := range 5 { // the event: load needs the whole floor, baseline >= floor
		if r.tick(m, 60, 0, true) {
			t.Fatalf("released during the spike at minute %d", m)
		}
	}
	// The load drops at minute 5: the baseline alone is now 2 replicas.
	for m := 5; m < 15; m++ {
		if r.tick(m, 12, 0, true) {
			t.Fatalf("released at minute %d, before 10 quiet minutes", m)
		}
	}
	if !r.tick(15, 12, 0, true) {
		t.Fatal("must release 10 minutes after the baseline dropped below the floor")
	}
	if m, o := r.store.Active(r.tw.UID, t0.Add(15*time.Minute)); m != 0 || o != nil {
		t.Fatal("override must no longer be active")
	}
	res := r.store.TakeResolutions()
	if len(res) != 1 || res[0].Verdict != overrides.ResEnded || res[0].By != "early_release" {
		t.Fatalf("resolutions = %+v", res)
	}
	ev := r.events
	if len(ev) != 1 || len(ev[0].Reasons) != 1 || ev[0].Reasons[0] != "early_release" || ev[0].IncidentID != testIncident {
		t.Fatalf("events = %+v", ev)
	}
	// Release then the Expire sweep: still exactly one resolution.
	r.store.Expire(t0.Add(16 * time.Minute))
	if res := r.store.TakeResolutions(); len(res) != 0 {
		t.Fatalf("Expire duplicated the resolution: %+v", res)
	}
}

func TestReleaseOverrides_NotBeforeTheEvent(t *testing.T) {
	r := newReleaseRig(t, t0)
	for m := range 31 { // a prescale: low load, baseline below the floor, but no peak yet
		if r.tick(m, 12, 0, true) {
			t.Fatalf("a prescale was released at minute %d", m)
		}
	}
}

func TestReleaseOverrides_NotWhileBaselineBindsAtTheFloor(t *testing.T) {
	r := newReleaseRig(t, t0)
	r.tick(0, 60, 0, true)
	for m := 1; m < 31; m++ {
		// Load is low but the forecast lookahead still wants the floor.
		if r.tick(m, 12, 64, true) {
			t.Fatalf("released at minute %d while the baseline still needs the floor", m)
		}
	}
	for m := 31; m < 41; m++ { // forecast gone: the streak starts now
		if r.tick(m, 12, 0, true) {
			t.Fatalf("released early at minute %d", m)
		}
	}
	if !r.tick(41, 12, 0, true) {
		t.Fatal("must release 10 minutes after the baseline fell below the floor")
	}
}

func TestReleaseOverrides_DegradedDataResetsTheStreak(t *testing.T) {
	r := newReleaseRig(t, t0)
	r.tick(0, 60, 0, true)
	for m := 1; m < 10; m++ {
		r.tick(m, 12, 0, true)
	}
	if r.tick(10, math.NaN(), 0, true) || r.tick(11, 12, 0, false) {
		t.Fatal("NaN or unhealthy data must not release")
	}
	for m := 12; m < 22; m++ { // the streak restarted at 12
		if r.tick(m, 12, 0, true) {
			t.Fatalf("released at minute %d, the streak should have restarted at 12", m)
		}
	}
	if !r.tick(22, 12, 0, true) {
		t.Fatal("must release after a fresh 10 quiet minutes")
	}
}
