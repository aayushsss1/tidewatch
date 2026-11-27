package harness_test

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
	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

const (
	keyConfidence = "confidence"
	keyReason     = "reason"
)

var baselineCfg = harness.Config{Name: "tidewatch-baseline"}

func TestMain(m *testing.M) {
	code := m.Run()
	_ = harness.Shutdown()
	os.Exit(code)
}

// loadScenario loads the example flash sale, trimmed so a run takes a few seconds: the
// surge starts at 9h; the run covers 8h to 10h.
func loadScenario(t *testing.T) *scenario.Scenario {
	t.Helper()
	sc, err := scenario.Load(filepath.Join("..", "scenarios", "example-flash.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	sc.Start, sc.Duration, sc.History = 8*time.Hour, 2*time.Hour, 48*time.Hour
	return sc
}

// flashProvider answers every investigation with a floor covering the event on the
// frontend (a TideWorkload named frontend-deploy) when the incident includes it, and with
// no_action otherwise. It is stateless, so concurrent or repeated runs are identical.
type flashProvider struct {
	floor int
	end   time.Time
}

func (p *flashProvider) Name() string  { return "scripted" }
func (p *flashProvider) Model() string { return "scripted-1" }

func (p *flashProvider) Turn(_ context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	name, in := "no_action", map[string]any{keyReason: "nothing for the frontend", keyConfidence: 0.9}
	if strings.Contains(req.Messages[0].Text, "frontend-deploy: replicas") {
		name, in = "propose_scaling", map[string]any{
			"overrides": []map[string]any{{
				"workload": "frontend-deploy", "minReplicas": p.floor, "end": p.end.Format(time.RFC3339),
			}},
			keyReason:     "flash sale announced for 09:00",
			keyConfidence: 0.9,
		}
	}
	raw, _ := json.Marshal(in)
	return llm.TurnResponse{
		Message:     llm.Message{Role: llm.RoleAssistant, ToolCall: &llm.ToolCall{ID: "c", Name: name, Input: raw}},
		InputTokens: 1000, OutputTokens: 100,
	}, nil
}

func agentConfig(sc *scenario.Scenario) harness.Config {
	return harness.Config{
		Name: "tidewatch-agent", Agent: true,
		Provider: &flashProvider{floor: 55, end: sc.Epoch.Add(9*time.Hour + 40*time.Minute)},
	}
}

type sums struct {
	breach, under, over, lift float64
	violations                int
}

func total(r harness.Result) (s sums) {
	for _, m := range r.PerWorkload {
		s.breach += m.BreachMinutes
		s.under += m.UnderprovRM
		s.over += m.OverprovRM
		s.lift += m.LiftRM
		s.violations += m.SafetyViolation
	}
	return
}

func TestRun_TidewatchBaseline(t *testing.T) {
	sc := loadScenario(t)
	res, err := harness.Run(context.Background(), sc, baselineCfg, 0, harness.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Scenario != "example-flash" || res.Config != "tidewatch-baseline" || len(res.PerWorkload) != 2 {
		t.Fatalf("result = %+v", res)
	}
	for name, m := range res.PerWorkload {
		if len(m.Samples) != 120 {
			t.Errorf("%s: %d samples, want 120", name, len(m.Samples))
		}
		if m.ScaleEvents == 0 {
			t.Errorf("%s: the engine never scaled", name)
		}
	}
	got := total(res)
	if got.breach <= 0 {
		t.Errorf("baseline breach minutes = %v, want > 0: a 3x flash sale outruns the forecaster", got.breach)
	}
	if got.lift != 0 || got.violations != 0 || res.Investigations != 0 || res.LLMCalls != 0 {
		t.Errorf("baseline must not lift or use the LLM: %+v", res)
	}
	if res.SkippedSteps != 0 {
		t.Errorf("engine skipped %d workload-steps: %v", res.SkippedSteps, res.SkippedReasons)
	}
}

func TestRun_TidewatchAgentOverrideApplied(t *testing.T) {
	sc := loadScenario(t)
	dir := t.TempDir()
	base, err := harness.Run(context.Background(), sc, baselineCfg, 0, harness.Options{})
	if err != nil {
		t.Fatal(err)
	}
	res, err := harness.Run(context.Background(), sc, agentConfig(sc), 0, harness.Options{TranscriptDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	got, want := total(res), total(base)
	if res.Investigations == 0 || res.LLMCalls == 0 || res.InputTokens == 0 || res.OutputTokens == 0 {
		t.Fatalf("no agent activity: %+v", res)
	}
	if got.lift <= 0 {
		t.Errorf("lift_rm = %v, want > 0: the override must raise desired above the baseline", got.lift)
	}
	if got.violations != 0 {
		t.Errorf("safety_violations = %d, want 0", got.violations)
	}
	if res.SkippedSteps != 0 {
		t.Errorf("engine skipped %d workload-steps: %v", res.SkippedSteps, res.SkippedReasons)
	}
	// Paired comparison (same scenario and seed): the agent never leaves less capacity.
	if got.under > want.under || got.breach > want.breach {
		t.Errorf("agent underprov/breach %v/%v exceed paired baseline %v/%v",
			got.under, got.breach, want.under, want.breach)
	}
	if want := filepath.Join(dir, "example-flash-tidewatch-agent-matched-0.jsonl"); res.TranscriptPath != want {
		t.Errorf("transcript path = %q, want %q", res.TranscriptPath, want)
	}
	if st, err := os.Stat(res.TranscriptPath); err != nil || st.Size() == 0 {
		t.Errorf("transcript missing or empty: %v", err)
	}
	t.Logf("breach base=%v agent=%v; underprov base=%v agent=%v; paired waste=%v; investigations=%d calls=%d",
		want.breach, got.breach, want.under, got.under, got.over-want.over, res.Investigations, res.LLMCalls)
}

func TestRun_Deterministic(t *testing.T) {
	sc := loadScenario(t)
	a, err := harness.Run(context.Background(), sc, agentConfig(sc), 0, harness.Options{Seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	b, err := harness.Run(context.Background(), sc, agentConfig(sc), 0, harness.Options{Seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("runs differ:\n%+v\n%+v", a, b)
	}
	// Startup delays are jittered, so a different seed gives a different run.
	c, err := harness.Run(context.Background(), sc, agentConfig(sc), 0, harness.Options{Seed: 8})
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(a.PerWorkload, c.PerWorkload) {
		t.Fatal("different seeds produced identical results; startup jitter is not applied")
	}
}

type fixedController struct{ n int32 }

func (fixedController) Name() string { return "fixed" }
func (f fixedController) Step(_ context.Context, _ time.Time, sc *scenario.Scenario, cl *cluster.SimCluster) error {
	for _, w := range sc.Workloads {
		if err := cl.SetReplicas(harness.ControllerNamespace, w.Name, f.n); err != nil {
			return err
		}
	}
	return nil
}

func TestRun_ControllerMode(t *testing.T) {
	sc := loadScenario(t)
	opts := harness.Options{Controller: fixedController{n: 60}}
	res, err := harness.Run(context.Background(), sc, harness.Config{}, 0, opts)
	if err != nil {
		t.Fatal(err)
	}
	got := total(res)
	if res.Config != "fixed" || got.breach != 0 || got.lift != 0 || got.violations != 0 || res.Investigations != 0 {
		t.Fatalf("a fixed 60 replicas must never breach: %+v", res)
	}
	if res.PerWorkload["frontend"].OverprovRM <= 0 {
		t.Error("expected over-provisioning")
	}
}

// notesProvider reads the ops notes through get_ops_notes on the first turn, records what
// the tool returned, and concludes with no_action.
type notesProvider struct{ texts []string }

func (p *notesProvider) Name() string  { return "scripted" }
func (p *notesProvider) Model() string { return "scripted-1" }

func (p *notesProvider) Turn(_ context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	name, in := "get_ops_notes", map[string]any{}
	for _, m := range req.Messages {
		if m.ToolResult != nil {
			p.texts = append(p.texts, m.ToolResult.Content)
			name, in = "no_action", map[string]any{keyReason: "none", keyConfidence: 0.9}
		}
	}
	raw, _ := json.Marshal(in)
	return llm.TurnResponse{
		Message: llm.Message{Role: llm.RoleAssistant, ToolCall: &llm.ToolCall{ID: "c", Name: name, Input: raw}},
	}, nil
}

func TestRun_PublishesAllDueNotesNewestLast(t *testing.T) {
	sc := loadScenario(t)
	sc.NoteSpecs[0].PublishAt = 8*time.Hour + 10*time.Minute
	second := sc.NoteSpecs[0]
	second.Text = "SECOND-NOTE-MARKER"
	sc.NoteSpecs = append(sc.NoteSpecs, second)
	p := &notesProvider{}
	cfg := harness.Config{Name: "tidewatch-agent", Agent: true, Provider: p}
	if _, err := harness.Run(context.Background(), sc, cfg, 0, harness.Options{}); err != nil {
		t.Fatal(err)
	}
	for _, txt := range p.texts {
		i, j := strings.Index(txt, "Flash sale starts"), strings.Index(txt, "SECOND-NOTE-MARKER")
		if i >= 0 && j > i && strings.Contains(txt[i:j], `\n\n`) {
			return
		}
	}
	t.Fatalf("no prompt carried both notes (%d prompts): %q", len(p.texts), p.texts)
}

func TestRun_RuleHookDefersUntilFirstSnapshot(t *testing.T) {
	sc := loadScenario(t)
	sc.NoteSpecs[0].PublishAt = sc.Start // due on the very first step, before any snapshot
	var calls []time.Time
	var nsnaps []int
	cfg := harness.Config{
		Name: "rule",
		RuleOverrides: func(now time.Time, notes []scenario.Note, snaps map[string]engine.Snapshot) []harness.RuleOverride {
			calls = append(calls, now)
			nsnaps = append(nsnaps, len(snaps))
			return nil
		},
	}
	if _, err := harness.Run(context.Background(), sc, cfg, 0, harness.Options{}); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 {
		t.Fatalf("hook called %d times, want exactly once", len(calls))
	}
	if want := sc.StartTime().Add(sc.Step); !calls[0].Equal(want) || nsnaps[0] != len(sc.Workloads) {
		t.Fatalf("hook at %v with %d snapshots, want %v with %d", calls[0], nsnaps[0], want, len(sc.Workloads))
	}
}
