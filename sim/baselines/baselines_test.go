package baselines_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/sim/baselines"
	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// flatScenario is a flat 100 rps service (capacity 10/pod, so 13 pods at 0.8 headroom)
// hit by a surge of mult at 1h20m for hold. The run covers 1h..2h; note is optional.
func flatScenario(t *testing.T, step string, mult float64, hold, note string) *scenario.Scenario {
	t.Helper()
	y := fmt.Sprintf(`
name: flat
seed: 1
trace: {type: "synthetic:diurnal", base: 100, amp: 0, noise: 0}
start: 1h
duration: 1h
step: %s
history: 1h
workloads:
  - {name: svc, capacity: 10, startupDelay: 30s, minReplicas: 2, maxReplicas: 1000}
events:
  - {kind: surge, start: 1h20m, ramp: %s, hold: %s, mult: %v}
`, step, step, hold, mult)
	if note != "" {
		y += fmt.Sprintf("notes:\n  - {publishAt: 1h5m, text: %q}\n", note)
	}
	sc, err := scenario.Parse([]byte(y), "")
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func runCtl(t *testing.T, sc *scenario.Scenario, c harness.Controller) []cluster.Sample {
	t.Helper()
	res, err := harness.Run(context.Background(), sc, harness.Config{}, 0, harness.Options{Controller: c})
	if err != nil {
		t.Fatal(err)
	}
	return res.PerWorkload["svc"].Samples
}

// at returns the sample accounted at the given offset from the trace origin.
func at(t *testing.T, ss []cluster.Sample, sc *scenario.Scenario, off time.Duration) cluster.Sample {
	t.Helper()
	for _, s := range ss {
		if s.T.Equal(sc.Epoch.Add(off)) {
			return s
		}
	}
	t.Fatalf("no sample at %v", off)
	return cluster.Sample{}
}

func TestHPA_ScalesUpUnderPolicyLimit(t *testing.T) {
	sc := flatScenario(t, "1m", 20, "30m", "")
	ss := runCtl(t, sc, baselines.NewHPA(sc.Workloads))
	if got := at(t, ss, sc, 1*time.Hour+10*time.Minute).Desired; got != 13 {
		t.Fatalf("steady state replicas = %d, want 13", got)
	}
	// The surge ramps over one step; the first step sees the new load and the policy
	// allows at most max(100%, 4 pods) per 15s: 4 syncs per 1m step is at most x16.
	first := at(t, ss, sc, 1*time.Hour+22*time.Minute).Desired
	if first <= 13 || first > 13*16 {
		t.Fatalf("first step replicas = %d, want in (13, 208]", first)
	}
	// Needed is 250 at 2000 rps: it keeps climbing and is not reached in one step.
	later := at(t, ss, sc, 1*time.Hour+25*time.Minute).Desired
	if later <= first || later < 200 {
		t.Fatalf("replicas %d then %d, want a continued climb toward 250", first, later)
	}
}

func TestHPA_ScaleUpRatePerSync(t *testing.T) {
	// A 15s step is a single sync per step: replicas at most double per step.
	sc := flatScenario(t, "15s", 20, "30m", "")
	ss := runCtl(t, sc, baselines.NewHPA(sc.Workloads))
	prev := int32(13)
	grew := false
	for _, s := range ss {
		if s.Desired > 2*prev && s.Desired > prev+4 {
			t.Fatalf("at %v replicas went %d -> %d, over max(100%%, 4 pods) per 15s", s.T, prev, s.Desired)
		}
		grew = grew || s.Desired > prev
		prev = s.Desired
	}
	if !grew {
		t.Fatal("never scaled up")
	}
}

func TestHPA_HoldsThroughStabilizationBeforeScalingDown(t *testing.T) {
	sc := flatScenario(t, "1m", 4, "10m", "")
	ss := runCtl(t, sc, baselines.NewHPA(sc.Workloads))
	// Load is 400 over [1h20m, 1h31m); the last high slot ends at 1h31m. The first low
	// metric is seen at the step starting 1h32m. Replicas must hold at least 5 minutes.
	peak := int32(0)
	for _, s := range ss {
		peak = max(peak, s.Desired)
	}
	if peak < 40 {
		t.Fatalf("peak %d, want >= 40", peak)
	}
	lowStart := 1*time.Hour + 32*time.Minute
	for off := time.Duration(0); off < 4*time.Minute; off += time.Minute {
		if got := at(t, ss, sc, lowStart+off).Desired; got < peak-2 {
			t.Fatalf("at +%v replicas dropped to %d inside the 300s window (peak %d)", off, got, peak)
		}
	}
	if got := at(t, ss, sc, lowStart+9*time.Minute).Desired; got > 20 {
		t.Fatalf("replicas = %d after the window, want scaled down", got)
	}
}

func firstScaled(ss []cluster.Sample, from int32) time.Time {
	for _, s := range ss {
		if s.Desired > from {
			return s.T
		}
	}
	return time.Time{}
}

func TestKEDAPrometheus_ReactsAt30sGranularity(t *testing.T) {
	// Step 15s: the metric refreshes at :00 and :30 only. Whatever step first shows the
	// surge, KEDA reacts in a step starting on a 30s boundary, and never before the HPA
	// (which sees every 15s step) would on the same metric.
	sc := flatScenario(t, "15s", 20, "30m", "")
	k := firstScaled(runCtl(t, sc, baselines.NewKEDAPrometheus(sc.Workloads)), 13)
	h := firstScaled(runCtl(t, sc, baselines.NewHPA(sc.Workloads)), 13)
	if k.IsZero() || h.IsZero() {
		t.Fatalf("never scaled: keda %v hpa %v", k, h)
	}
	if (k.Unix()-15)%30 != 0 {
		t.Fatalf("keda scaled in the step starting %v, not a 30s boundary", k.Add(-15*time.Second))
	}
	// 1h20m00s is a 30s boundary; the ramp's first step starts at 1h20m, so the surge is
	// first visible in the series to the step starting 1h20m15s: HPA acts there, KEDA waits.
	if !k.After(h) {
		t.Fatalf("keda scaled at %v, hpa at %v: want keda one step later", k, h)
	}
}

func TestKEDACron_AppliesNoteWindow(t *testing.T) {
	note := "Flash sale at 2026-01-01T01:20:00Z, expect ~4× traffic."
	sc := flatScenario(t, "1m", 4, "30m", note)
	ss := runCtl(t, sc, baselines.NewKEDACron(sc.Workloads))
	// desired = ceil(4 x 100 / (10 x 0.8)) = 50 from 01:05 to 02:20.
	if got := at(t, ss, sc, 1*time.Hour+10*time.Minute).Desired; got != 50 {
		t.Fatalf("cron window replicas at 01:10 = %d, want 50", got)
	}
	if got := at(t, ss, sc, 1*time.Hour+4*time.Minute).Desired; got != 13 {
		t.Fatalf("before the window replicas = %d, want 13", got)
	}
}

func TestKEDACron_WrongTimeNoteFiresAtWrongTime(t *testing.T) {
	// The note says 01:45 but the surge is at 01:20: the cron window opens at 01:30.
	note := "Flash sale at 2026-01-01T01:45:00Z, expect ~4× traffic."
	sc := flatScenario(t, "1m", 4, "30m", note)
	ss := runCtl(t, sc, baselines.NewKEDACron(sc.Workloads))
	if got := at(t, ss, sc, 1*time.Hour+15*time.Minute).Desired; got != 13 {
		t.Fatalf("at 01:15 replicas = %d, want 13 (window not open)", got)
	}
	if got := at(t, ss, sc, 1*time.Hour+31*time.Minute).Desired; got != 50 {
		t.Fatalf("at 01:31 replicas = %d, want 50 (window open)", got)
	}
}

func TestOracle_NoBreaches(t *testing.T) {
	sc := flatScenario(t, "1m", 4, "30m", "")
	res, err := harness.Run(context.Background(), sc, harness.Config{}, 0,
		harness.Options{Controller: baselines.NewOracle(sc.Workloads)})
	if err != nil {
		t.Fatal(err)
	}
	if m := res.PerWorkload["svc"]; m.BreachMinutes != 0 {
		t.Fatalf("oracle breach minutes = %v", m.BreachMinutes)
	}
}

func TestByName(t *testing.T) {
	sc := flatScenario(t, "1m", 4, "30m", "")
	for _, n := range baselines.Names() {
		e, err := baselines.ByName(n, sc.Workloads)
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		if e.Config.Name == "" && e.Controller == nil {
			t.Fatalf("%s: empty entry", n)
		}
	}
	if len(baselines.Names()) != 7 {
		t.Fatalf("names = %v", baselines.Names())
	}
	if _, err := baselines.ByName("nope", sc.Workloads); err == nil {
		t.Fatal("unknown name should error")
	}
}
