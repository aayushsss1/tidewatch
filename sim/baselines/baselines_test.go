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

// flatOpts configures a flat 100 rps service (capacity 10/pod, so 13 pods at 0.8 headroom)
// hit by a surge. The measured run is [start, start+dur]; the surge starts 20m in.
type flatOpts struct {
	step, hold, note string
	mult             float64
	start, history   time.Duration // default 1h / 1h
	dur              time.Duration // default 1h
	maxReplicas      int           // default 1000
	startup          string        // default 30s
}

func (o flatOpts) scenario(t *testing.T) *scenario.Scenario {
	t.Helper()
	d := func(v, def time.Duration) time.Duration {
		if v == 0 {
			return def
		}
		return v
	}
	start, hist, dur := d(o.start, time.Hour), d(o.history, time.Hour), d(o.dur, time.Hour)
	if o.maxReplicas == 0 {
		o.maxReplicas = 1000
	}
	if o.startup == "" {
		o.startup = "30s"
	}
	y := fmt.Sprintf(`
name: flat
seed: 1
trace: {type: "synthetic:diurnal", base: 100, amp: 0, noise: 0}
start: %v
duration: %v
step: %s
history: %v
workloads:
  - {name: svc, capacity: 10, startupDelay: %s, minReplicas: 2, maxReplicas: %d}
events:
  - {kind: surge, start: %v, ramp: %s, hold: %s, mult: %v}
`, start, dur, o.step, hist, o.startup, o.maxReplicas, start+20*time.Minute, o.step, o.hold, o.mult)
	if o.note != "" {
		y += fmt.Sprintf("notes:\n  - {publishAt: %v, text: %q}\n", start+5*time.Minute, o.note)
	}
	sc, err := scenario.Parse([]byte(y), "")
	if err != nil {
		t.Fatal(err)
	}
	return sc
}

func flatScenario(t *testing.T, step string, mult float64, hold, note string) *scenario.Scenario {
	return flatOpts{step: step, mult: mult, hold: hold, note: note}.scenario(t)
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

const trueNoteEarly = "Flash sale at 2026-01-01T01:20:00Z, expect ~4× traffic."

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
	note := trueNoteEarly
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

func TestNew(t *testing.T) {
	sc := flatScenario(t, "1m", 4, "30m", "")
	for _, n := range baselines.Names() {
		e, err := baselines.New(n, sc, baselines.Options{Provider: &scriptedProvider{}})
		if err != nil {
			t.Fatalf("%s: %v", n, err)
		}
		if e.Name != n || (e.Controller == nil) == (e.Config.Name == "") {
			t.Fatalf("%s: bad entry %+v", n, e)
		}
	}
	if len(baselines.Names()) != 7 {
		t.Fatalf("names = %v", baselines.Names())
	}
	if _, err := baselines.New("nope", sc, baselines.Options{}); err == nil {
		t.Fatal("unknown name should error")
	}
	if _, err := baselines.New(baselines.NameTWAgent, sc, baselines.Options{}); err == nil {
		t.Fatal("tidewatch-agent without a provider should error")
	}
}

func TestHPA_NoCompoundingWhenStartupExceedsStep(t *testing.T) {
	// Startup 150s is longer than the 1m step: pods ordered in one step are still unready in
	// the next. The target must not compound; needed at 400 rps is 50.
	sc := flatOpts{step: "1m", mult: 4, hold: "30m", startup: "150s"}.scenario(t)
	peak := int32(0)
	for _, s := range runCtl(t, sc, baselines.NewHPA(sc.Workloads)) {
		peak = max(peak, s.Desired)
	}
	if peak > 52 {
		t.Fatalf("peak replicas %d, want about 50 (no compounding)", peak)
	}
}

func breachOf(ss []cluster.Sample) (n float64) {
	for _, s := range ss {
		if s.Load > float64(s.Ready)*10 {
			n++
		}
	}
	return n
}

func TestKEDACron_WrongTimeNoBetterThanHPA(t *testing.T) {
	note := "Flash sale at 2026-01-01T01:45:00Z, expect ~4× traffic." // the surge is at 01:20
	sc := flatScenario(t, "1m", 4, "30m", note)
	cron := breachOf(runCtl(t, sc, baselines.NewKEDACron(sc.Workloads)))
	hpa := breachOf(runCtl(t, sc, baselines.NewHPA(sc.Workloads)))
	if cron < hpa {
		t.Fatalf("cron with a wrong-time note breached %v min, HPA %v: cron must not help", cron, hpa)
	}
}

func TestKEDACron_WindowReleasesAfterHoldAndStabilization(t *testing.T) {
	note := trueNoteEarly
	sc := flatOpts{step: "1m", mult: 4, hold: "10m", note: note, dur: 90 * time.Minute}.scenario(t)
	ss := runCtl(t, sc, baselines.NewKEDACron(sc.Workloads))
	// Window [01:05, 02:20]; load is back to 100 rps by 01:31, so the Prometheus level is 13.
	if got := at(t, ss, sc, 2*time.Hour+15*time.Minute).Desired; got != 50 {
		t.Fatalf("inside the window at 02:15 replicas = %d, want 50", got)
	}
	// 300s after the window closes the stabilization no longer holds the cron level.
	if got := at(t, ss, sc, 2*time.Hour+27*time.Minute).Desired; got != 13 {
		t.Fatalf("at 02:27 replicas = %d, want back at the Prometheus level 13", got)
	}
}

func TestKEDACron_NoLeakage(t *testing.T) {
	// The scenarios differ only after 01:20 (surge x4 vs x8). Same note, same publish
	// time: cron must act identically until the surge shows in the series.
	note := trueNoteEarly
	a := flatScenario(t, "1m", 4, "30m", note)
	b := flatScenario(t, "1m", 8, "30m", note)
	sa := runCtl(t, a, baselines.NewKEDACron(a.Workloads))
	sb := runCtl(t, b, baselines.NewKEDACron(b.Workloads))
	n := 0
	for i := range sa {
		if sa[i].T.After(a.Epoch.Add(time.Hour + 20*time.Minute)) {
			break
		}
		if sa[i].Desired != sb[i].Desired {
			t.Fatalf("at %v desired %d vs %d: cron used post-cutoff information",
				sa[i].T, sa[i].Desired, sb[i].Desired)
		}
		n++
	}
	if n < 20 {
		t.Fatalf("compared only %d steps", n)
	}
}
