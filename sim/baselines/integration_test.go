package baselines_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/sim/baselines"
	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

func TestMain(m *testing.M) {
	code := m.Run()
	_ = harness.Shutdown()
	os.Exit(code)
}

// exampleFlash loads example-flash.yaml trimmed to 8h..10h so a run takes seconds.
func exampleFlash(t *testing.T) *scenario.Scenario {
	t.Helper()
	sc, err := scenario.Load(filepath.Join("..", "scenarios", "example-flash.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	sc.Start, sc.Duration, sc.History = 8*time.Hour, 2*time.Hour, 48*time.Hour
	return sc
}

// scriptedProvider proposes a floor for the frontend TideWorkload (frontend-deploy) when
// the incident includes it, and no_action otherwise.
type scriptedProvider struct {
	floor int
	end   time.Time
}

func (p *scriptedProvider) Name() string  { return "scripted" }
func (p *scriptedProvider) Model() string { return "scripted-1" }

func (p *scriptedProvider) Turn(_ context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	name, in := "no_action", map[string]any{"reason": "nothing for the frontend", "confidence": 0.9}
	if strings.Contains(req.Messages[0].Text, "frontend-deploy: replicas") {
		name, in = "propose_scaling", map[string]any{
			"overrides": []map[string]any{{
				"workload": "frontend-deploy", "minReplicas": p.floor, "end": p.end.Format(time.RFC3339),
			}},
			"reason": "flash sale announced for 09:00", "confidence": 0.9,
		}
	}
	raw, _ := json.Marshal(in)
	return llm.TurnResponse{
		Message:     llm.Message{Role: llm.RoleAssistant, ToolCall: &llm.ToolCall{ID: "c", Name: name, Input: raw}},
		InputTokens: 1000, OutputTokens: 100,
	}, nil
}

func runConfig(t *testing.T, sc *scenario.Scenario, name string) harness.Result {
	t.Helper()
	opts := baselines.Options{}
	if name == baselines.NameTWAgent {
		opts.Provider = &scriptedProvider{floor: 55, end: sc.Epoch.Add(9*time.Hour + 40*time.Minute)}
	}
	e, err := baselines.New(name, sc, opts)
	if err != nil {
		t.Fatal(err)
	}
	res, err := e.Run(context.Background(), sc, 0, harness.Options{})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return res
}

func sumMetrics(r harness.Result) (breach, lift float64, violations int) {
	for _, m := range r.PerWorkload {
		breach += m.BreachMinutes
		lift += m.LiftRM
		violations += m.SafetyViolation
	}
	return
}

func frontendAt(t *testing.T, r harness.Result, sc *scenario.Scenario, off time.Duration) cluster.Sample {
	t.Helper()
	return at(t, r.PerWorkload["frontend"].Samples, sc, off)
}

func TestRegexRule_LiftsBeforeEvent(t *testing.T) {
	sc := exampleFlash(t)
	base := runConfig(t, sc, "tidewatch-baseline")
	rule := runConfig(t, sc, "regex-rule")
	_, lift, viol := sumMetrics(rule)
	if lift <= 0 {
		t.Errorf("lift_rm = %v, want > 0: the stored override must raise desired", lift)
	}
	if viol != 0 || rule.SkippedSteps != 0 {
		t.Errorf("violations %d, skipped steps %d (%v), want 0", viol, rule.SkippedSteps, rule.SkippedReasons)
	}
	// Event at 9h, load ~x3 of ~70 rps: the rule's floor is well above the baseline's
	// desired count before the surge begins.
	off := 8*time.Hour + 58*time.Minute
	if r, b := frontendAt(t, rule, sc, off).Desired, frontendAt(t, base, sc, off).Desired; r <= b {
		t.Errorf("at 08:58 rule desired %d <= baseline %d: no lift before the event", r, b)
	}
	rb, _, _ := sumMetrics(rule)
	bb, _, _ := sumMetrics(base)
	if rb > bb {
		t.Errorf("rule breach %v > baseline %v", rb, bb)
	}
}

func TestOracle_ZeroBreachesOnExampleFlash(t *testing.T) {
	if b, _, _ := sumMetrics(runConfig(t, exampleFlash(t), "oracle")); b != 0 {
		t.Fatalf("oracle breach minutes = %v, want 0", b)
	}
}

func TestAllConfigs_ExampleFlash(t *testing.T) {
	sc := exampleFlash(t)
	breach := map[string]float64{}
	for _, name := range baselines.Names() {
		res := runConfig(t, sc, name)
		if res.Config != name {
			t.Errorf("%s: result config = %q", name, res.Config)
		}
		for w, m := range res.PerWorkload {
			if len(m.Samples) != 120 {
				t.Errorf("%s/%s: %d samples, want 120", name, w, len(m.Samples))
			}
		}
		breach[name], _, _ = sumMetrics(res)
		if name == "tidewatch-agent" && (res.Investigations == 0 || res.LLMCalls == 0) {
			t.Errorf("agent config did no investigations: %+v", res)
		}
		t.Logf("%-18s breach_min=%v", name, breach[name])
	}
	for name, b := range breach {
		if b < breach["oracle"] {
			t.Errorf("%s has fewer breach minutes (%v) than the oracle (%v)", name, b, breach["oracle"])
		}
	}
	if breach["hpa"] <= breach["oracle"] {
		t.Errorf("hpa breach %v should exceed the oracle's %v on a flash sale", breach["hpa"], breach["oracle"])
	}
}

// flatTW is a flat service with enough history for the engine's forecaster: the run is
// 49h..50h and the surge starts at 49h20m (2026-01-03T01:20Z), announced at 49h05m.
func flatTW(t *testing.T, mult float64, note string, maxReplicas int) *scenario.Scenario {
	return flatOpts{
		step: "1m", mult: mult, hold: "30m", note: note, start: 49 * time.Hour, history: 48 * time.Hour,
		maxReplicas: maxReplicas,
	}.scenario(t)
}

const trueNote = "Flash sale at 2026-01-03T01:20:00Z, expect ~4× traffic."

func runNamed(
	t *testing.T, sc *scenario.Scenario, name string, hook func(harness.Config) harness.Config,
) harness.Result {
	t.Helper()
	e, err := baselines.New(name, sc, baselines.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if hook != nil {
		e.Config = hook(e.Config)
	}
	res, err := e.Run(context.Background(), sc, 0, harness.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestRegexRule_BeatsBaselineOnTrueNote(t *testing.T) {
	sc := flatTW(t, 4, trueNote, 1000)
	base := runNamed(t, sc, baselines.NameTWBaseline, nil)
	rule := runNamed(t, sc, baselines.NameRegexRule, nil)
	bb, _, _ := sumMetrics(base)
	rb, lift, viol := sumMetrics(rule)
	if rb >= bb {
		t.Fatalf("rule breach %v, baseline %v: the rule must be strictly better on a true x4 note", rb, bb)
	}
	if viol != 0 || lift <= 0 || rule.SkippedSteps != 0 || rule.Guardrails != harness.GuardrailsMatched {
		t.Fatalf("violations %d lift %v skipped %d guardrails %q", viol, lift, rule.SkippedSteps, rule.Guardrails)
	}
}

func TestRegexRule_WrongMagnitudeClampedToMax(t *testing.T) {
	sc := flatTW(t, 4, "Flash sale at 2026-01-03T01:20:00Z, expect 50× traffic.", 100)
	rule := runNamed(t, sc, baselines.NameRegexRule, nil)
	peak := int32(0)
	for _, s := range rule.PerWorkload["svc"].Samples {
		peak = max(peak, s.Desired)
	}
	// 50 x 100 / 8 = 625 replicas asked for, clamped to the policy maximum.
	if peak != 100 {
		t.Fatalf("peak desired = %d, want exactly the maxReplicas clamp 100", peak)
	}
	if _, _, v := sumMetrics(rule); v != 0 {
		t.Fatalf("safety violations = %d", v)
	}
}

func TestNoLeakage_RuleOverrides(t *testing.T) {
	// Scenarios identical up to the publish time (49h05m), different after it.
	var got [2][]harness.RuleOverride
	for i, mult := range []float64{4, 8} {
		sc := flatTW(t, mult, trueNote, 1000)
		runNamed(t, sc, baselines.NameRegexRule, func(c harness.Config) harness.Config {
			orig := c.RuleOverrides
			c.RuleOverrides = func(
				now time.Time, notes []scenario.Note, snaps map[string]engine.Snapshot,
			) []harness.RuleOverride {
				out := orig(now, notes, snaps)
				got[i] = append(got[i], out...)
				return out
			}
			return c
		})
	}
	if len(got[0]) == 0 || !reflect.DeepEqual(got[0], got[1]) {
		t.Fatalf("rule overrides differ or are empty:\n%+v\n%+v", got[0], got[1])
	}
}

func replicaMinutes(r harness.Result) (rm float64) {
	for _, m := range r.PerWorkload {
		for _, s := range m.Samples {
			rm += float64(s.Desired)
		}
	}
	return rm
}

func TestOracle_FewestReplicaMinutesAmongZeroBreach(t *testing.T) {
	sc := exampleFlash(t)
	oracle := replicaMinutes(runConfig(t, sc, baselines.NameOracle))
	for _, name := range baselines.Names() {
		res := runConfig(t, sc, name)
		if b, _, _ := sumMetrics(res); b == 0 && replicaMinutes(res) < oracle {
			t.Errorf("%s has zero breaches with %v replica-minutes, fewer than the oracle's %v",
				name, replicaMinutes(res), oracle)
		}
	}
}

func TestAgentScriptedVsBaseline(t *testing.T) {
	sc := exampleFlash(t)
	base := runConfig(t, sc, baselines.NameTWBaseline)
	agent := runConfig(t, sc, baselines.NameTWAgent)
	bb, _, _ := sumMetrics(base)
	ab, lift, _ := sumMetrics(agent)
	t.Logf("baseline breach %v, agent breach %v, lift_rm %v, investigations %d", bb, ab, lift, agent.Investigations)
	if ab > bb {
		t.Fatalf("agent breach %v > baseline %v", ab, bb)
	}
	// The breach minutes tie on this scenario by construction: investigations are triggered
	// by anomalies seen 3 steps into the surge (after 09:03), so the scripted override lands
	// after every breach minute (09:01-09:04) and only adds replicas. The agent acting at
	// all is asserted instead.
	if lift <= 0 || agent.Investigations == 0 {
		t.Errorf("scripted agent did not act: lift %v, investigations %d", lift, agent.Investigations)
	}
}
