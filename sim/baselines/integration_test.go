package baselines_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	e, err := baselines.ByNameStep(name, sc.Workloads, sc.Step)
	if err != nil {
		t.Fatal(err)
	}
	if e.Config.Agent {
		e.Config.Provider = &scriptedProvider{floor: 55, end: sc.Epoch.Add(9*time.Hour + 40*time.Minute)}
	}
	res, err := harness.Run(context.Background(), sc, e.Config, 0, harness.Options{Controller: e.Controller})
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
