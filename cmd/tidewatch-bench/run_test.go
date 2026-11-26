package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

const testScenario = `
name: t
seed: 10
trace: {type: "synthetic:diurnal", base: 10, amp: 10}
start: 2h
duration: 1h
history: 2h
workloads:
  - {name: frontend, capacity: 10, startupDelay: 30s}
  - {name: api, capacity: 10, startupDelay: 30s, lag: 1m}
`

// fixture writes a matrix with two scenarios under .../scenarios/ and returns its path.
func fixture(t *testing.T, repeats string) string {
	t.Helper()
	root := t.TempDir()
	sdir := filepath.Join(root, "scenarios", "grp")
	mdir := filepath.Join(root, "matrices")
	for _, d := range []string{sdir, mdir} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []string{"a-flash", "b-control"} {
		if err := os.WriteFile(filepath.Join(sdir, n+".yaml"), []byte(testScenario), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	m := "scenarios: [../scenarios/grp/*.yaml]\nconfigs: [hpa, regex-rule, tidewatch-agent]\n" + repeats
	p := filepath.Join(mdir, "m.yaml")
	if err := os.WriteFile(p, []byte(m), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func fakeRC(t *testing.T, matrix string, calls *[]plannedRun) runConfig {
	t.Helper()
	return runConfig{
		Matrix: matrix, Out: filepath.Join(t.TempDir(), "out"), Provider: providerScripted, Model: defaultModel,
		Guardrails: "matched",
		load:       func(string) (*scenario.Scenario, error) { return scenario.Parse([]byte(testScenario), t.TempDir()) },
		exec: func(_ context.Context, _ *scenario.Scenario, p plannedRun, _ int64) (harness.Result, error) {
			*calls = append(*calls, p)
			return harness.Result{
				PerWorkload: map[string]cluster.Metrics{
					"frontend": {BreachMinutes: 3, OverprovRM: 5, SafetyViolation: 0, ScaleEvents: 2},
					"api":      {BreachMinutes: 1},
				},
				Investigations: 2, LLMCalls: 5, InputTokens: 1_000_000, OutputTokens: 200_000, SkippedSteps: 0,
			}, nil
		},
	}
}

func readCSV(t *testing.T, dir string) [][]string {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "results.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestPlan(t *testing.T) {
	mf := fixture(t, "repeats: {tidewatch-agent: 5}\n")
	m, err := loadMatrix(mf)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := plan(m, planOptions{MatrixFile: mf, Guardrails: "matched"})
	if err != nil {
		t.Fatal(err)
	}
	// 2 scenarios x (hpa 1 + regex-rule 1 + agent 5)
	if len(runs) != 14 {
		t.Fatalf("%d runs, want 14", len(runs))
	}
	if runs[0].ScenarioID != "grp/a-flash" || runs[0].Guardrails != noGuardrails {
		t.Errorf("first run = %+v", runs[0])
	}
	both, _ := plan(m, planOptions{MatrixFile: mf, Guardrails: "both"})
	// hpa has no profile; regex-rule and agent run under both.
	if want := 2 * (1 + 2*1 + 2*5); len(both) != want {
		t.Errorf("both: %d runs, want %d", len(both), want)
	}
	only, _ := plan(m, planOptions{MatrixFile: mf, Only: "grp/a-*", Configs: []string{"hpa"}})
	if len(only) != 1 || only[0].ScenarioID != "grp/a-flash" || only[0].Config != "hpa" {
		t.Errorf("only = %+v", only)
	}
	rep, _ := plan(m, planOptions{MatrixFile: mf, Repeats: 2, Configs: []string{"tidewatch-agent"}})
	if len(rep) != 4 {
		t.Errorf("--repeats 2: %d runs, want 4", len(rep))
	}
	if _, err := plan(m, planOptions{MatrixFile: mf, Guardrails: "loose"}); err == nil {
		t.Error("bad guardrails must fail")
	}
}

func TestRunWritesRowsAndResumes(t *testing.T) {
	mf := fixture(t, "")
	var calls []plannedRun
	rc := fakeRC(t, mf, &calls)
	rc.Configs = []string{"hpa", "tidewatch-agent"}
	var out bytes.Buffer
	if err := execute(context.Background(), rc, &out); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 4 {
		t.Fatalf("%d runs, want 4", len(calls))
	}
	rows := readCSV(t, rc.Out)
	if len(rows) != 1+4*2 { // 2 workload rows per run
		t.Fatalf("%d csv rows, want 9", len(rows))
	}
	if strings.Join(rows[0], ",") != strings.Join(csvHeader, ",") {
		t.Errorf("header = %v", rows[0])
	}
	col := map[string]int{}
	for i, h := range rows[0] {
		col[h] = i
	}
	// Row 1: hpa, lead workload; row 3 or so: agent. Check the agent's lead row.
	var lead, second []string
	for _, r := range rows[1:] {
		if r[col["config"]] == "tidewatch-agent" && r[col["scenario"]] == "grp/a-flash" {
			if r[col["workload"]] == "frontend" {
				lead = r
			} else {
				second = r
			}
		}
	}
	if lead[col["model"]] != scriptedModel || lead[col["guardrails"]] != "matched" || lead[col["seed"]] != "10" {
		t.Errorf("lead row = %v", lead)
	}
	if lead[col["breach_min"]] != "3" || lead[col["investigations"]] != "2" || lead[col["input_tokens"]] != "1000000" {
		t.Errorf("lead metrics = %v", lead)
	}
	if second[col["investigations"]] != "0" || second[col["breach_min"]] != "1" {
		t.Errorf("run-level columns must be on the first row only: %v", second)
	}
	if !strings.Contains(out.String(), "[1/4]") || !strings.Contains(out.String(), "done: 4 runs") {
		t.Errorf("progress output:\n%s", out.String())
	}

	// Without --resume an existing result is an error; with it nothing reruns.
	calls = nil
	if err := execute(context.Background(), rc, &out); err == nil {
		t.Error("existing results without --resume must fail")
	}
	rc.Resume = true
	if err := execute(context.Background(), rc, &out); err != nil || len(calls) != 0 {
		t.Fatalf("resume reran %d runs (err %v)", len(calls), err)
	}
	// A widened config set only runs the new cells.
	rc.Configs = nil
	if err := execute(context.Background(), rc, &out); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || calls[0].Config != "regex-rule" {
		t.Errorf("resume ran %+v, want the 2 regex-rule runs", calls)
	}
}

func TestRunMaxUSD(t *testing.T) {
	mf := fixture(t, "repeats: {tidewatch-agent: 5}\n")
	var calls []plannedRun
	rc := fakeRC(t, mf, &calls)
	rc.Provider, rc.Model, rc.key = providerAnthropic, "claude-haiku-5-5", "sk-test"
	rc.Configs, rc.MaxUSD = []string{"tidewatch-agent"}, 0.25
	var out bytes.Buffer
	if err := execute(context.Background(), rc, &out); err != nil {
		t.Fatal(err)
	}
	// Each run: 1M in x $0.10 + 0.2M out x $0.50 = $0.20; the second run ends at $0.40 > 0.25.
	if len(calls) != 2 {
		t.Fatalf("%d runs, want 2 before the cap", len(calls))
	}
	if !strings.Contains(out.String(), "stopped: estimated spend") || !strings.Contains(out.String(), "before run 3 of 10") {
		t.Errorf("output:\n%s", out.String())
	}
	rows := readCSV(t, rc.Out)
	if rows[1][col(rows, "model")] != "claude-haiku-5-5" {
		t.Errorf("model column = %q", rows[1][col(rows, "model")])
	}
	if got := rows[1][col(rows, "usd")]; got != "0.200000" {
		t.Errorf("usd = %q", got)
	}
	// The spend of earlier sessions counts on resume.
	calls = nil
	rc.Resume = true
	if err := execute(context.Background(), rc, &out); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 0 {
		t.Errorf("resume over the cap ran %d runs", len(calls))
	}
}

func col(rows [][]string, name string) int {
	for i, h := range rows[0] {
		if h == name {
			return i
		}
	}
	return -1
}

func TestAgentNeedsKey(t *testing.T) {
	mf := fixture(t, "")
	var calls []plannedRun
	rc := fakeRC(t, mf, &calls)
	rc.Provider, rc.key = providerAnthropic, ""
	err := execute(context.Background(), rc, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), keyEnv) {
		t.Fatalf("err = %v, want a message naming %s", err, keyEnv)
	}
	if len(calls) != 0 {
		t.Error("must fail before running anything")
	}
	// Non-agent configs need no key.
	rc.Configs = []string{"hpa", "regex-rule"}
	if err := execute(context.Background(), rc, &bytes.Buffer{}); err != nil {
		t.Fatalf("non-agent run: %v", err)
	}
	// The key never appears in an error.
	rc.key, rc.Model, rc.Configs = "sk-secret-value", "no-such-model", nil
	rc.Out = filepath.Join(t.TempDir(), "o")
	err = execute(context.Background(), rc, &bytes.Buffer{})
	if err == nil || strings.Contains(err.Error(), "sk-secret-value") {
		t.Fatalf("err = %v", err)
	}
}

func TestUSD(t *testing.T) {
	for model, want := range map[string]float64{
		"claude-haiku-5-5": 0.6, "claude-sonnet-5-5": 12, "claude-opus-5-5": 24,
	} {
		if got, ok := usd(model, 1_000_000, 1_000_000); !ok || got != want {
			t.Errorf("%s: %v %v, want %v", model, got, ok, want)
		}
	}
	if _, ok := usd("x", 1, 1); ok {
		t.Error("unknown model must report !ok")
	}
}

func TestScenarioID(t *testing.T) {
	for in, want := range map[string]string{
		"/r/sim/scenarios/headline/w1-flash-3x.yaml": "headline/w1-flash-3x",
		"sim/scenarios/example-flash.yaml":           "example-flash",
		"other/x.yaml":                               "x",
	} {
		if got := scenarioID(in); got != want {
			t.Errorf("%s: %q, want %q", in, got, want)
		}
	}
}

func TestScriptedProvider(t *testing.T) {
	brief := "Incident x (policy a/b). Investigate.\n" +
		"- frontend-deploy: replicas 6 (ready 6, max 10), load 12; triggers: ContextDue(sale)\n" +
		"- api-deploy: replicas 3 (ready 3, max 40), load 4\n"
	resp, err := scriptedProvider{}.Turn(context.Background(), llm.TurnRequest{Messages: []llm.Message{{Text: brief}}})
	if err != nil {
		t.Fatal(err)
	}
	tc := resp.Message.ToolCall
	if tc.Name != "propose_scaling" || !strings.Contains(string(tc.Input), `"workload":"frontend-deploy"`) ||
		!strings.Contains(string(tc.Input), `"minReplicas":10`) || strings.Contains(string(tc.Input), "api-deploy") {
		t.Errorf("call = %s %s", tc.Name, tc.Input)
	}
	resp, _ = scriptedProvider{}.Turn(context.Background(), llm.TurnRequest{Messages: []llm.Message{{Text: "nothing"}}})
	if resp.Message.ToolCall.Name != "no_action" {
		t.Errorf("call = %s", resp.Message.ToolCall.Name)
	}
}

// TestCommittedMatrices checks the shipped matrices plan the expected runs and that the
// synthetic (CI) scenarios load. The headline scenarios need the Azure cache, so they are
// only planned here, not loaded.
func TestCommittedMatrices(t *testing.T) {
	smoke := filepath.Join("..", "..", "sim", "matrices", "smoke.yaml")
	m, err := loadMatrix(smoke)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := plan(m, planOptions{MatrixFile: smoke, Guardrails: "matched"})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 14 {
		t.Errorf("smoke: %d runs, want 2 scenarios x 7 configs", len(runs))
	}
	for _, p := range runs[:1] {
		if _, err := scenario.Load(p.Path); err != nil {
			t.Errorf("%s: %v", p.ScenarioID, err)
		}
	}
	for _, p := range runs {
		if p.Config == "tidewatch-agent" && p.Repeat != 0 {
			t.Errorf("smoke runs the agent once, got repeat %d", p.Repeat)
		}
	}

	head := filepath.Join("..", "..", "sim", "matrices", "headline.yaml")
	hm, err := loadMatrix(head)
	if err != nil {
		t.Fatal(err)
	}
	hr, err := plan(hm, planOptions{MatrixFile: head, Guardrails: "matched"})
	if err != nil {
		t.Fatal(err)
	}
	scenarios, agent := map[string]bool{}, 0
	for _, p := range hr {
		scenarios[p.ScenarioID] = true
		if p.Config == "tidewatch-agent" {
			agent++
		}
	}
	// 6 workloads x 4 scenario kinds + 2 B scenarios; 7 configs, the agent x5.
	if len(scenarios) != 26 || agent != 26*5 || len(hr) != 26*(6+5) {
		t.Errorf("headline: %d scenarios, %d agent runs, %d runs", len(scenarios), agent, len(hr))
	}
}
