package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/anthropic"
	"github.com/aayushsss1/tidewatch/sim/baselines"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

const (
	providerAnthropic = "anthropic"
	providerScripted  = "scripted"
	defaultModel      = "claude-haiku-5-5"
	keyEnv            = "ANTHROPIC_API_KEY"
)

// csvHeader is the results.csv schema: one row per (run x workload). The run-level columns
// (investigations through wall_seconds) are written on the first workload's row only and are
// 0 on the others, so summing a column over a run's rows gives the run total.
var csvHeader = []string{
	"scenario", "config", "guardrails", "repeat", "seed", "model", "workload",
	"breach_min", "overprov_rm", "underprov_rm", "scale_events", "lift_rm", "safety_violations", "skipped_steps",
	"investigations", "llm_calls", "input_tokens", "output_tokens", "usd", "wall_seconds",
}

// executor runs one planned run. The default one drives the real harness.
type executor func(ctx context.Context, sc *scenario.Scenario, p plannedRun, seed int64) (harness.Result, error)

// runConfig is the parsed `run` command.
type runConfig struct {
	Matrix, Out, Provider, Model, Only string
	Configs                            []string
	Repeats                            int
	Resume                             bool
	MaxUSD                             float64
	Guardrails                         string

	// Test seams.
	load func(path string) (*scenario.Scenario, error)
	exec executor
	key  string // API key, read from the environment
}

func runCmd(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	var rc runConfig
	var cfgs string
	fs.StringVar(&rc.Matrix, "matrix", "", "matrix YAML file (required)")
	fs.StringVar(&rc.Out, "out", "", "output directory (required)")
	fs.StringVar(&rc.Provider, "provider", providerAnthropic, "anthropic or scripted")
	fs.StringVar(&rc.Model, "model", defaultModel, "model for the agent config")
	fs.IntVar(&rc.Repeats, "repeats", 0, "override the agent config's repeat count")
	fs.StringVar(&rc.Only, "only", "", "run only scenarios matching this glob (scenario ID or ID/config)")
	fs.StringVar(&cfgs, "config", "", "run only these configs (comma separated)")
	fs.BoolVar(&rc.Resume, "resume", false, "skip runs already present in results.csv")
	fs.Float64Var(&rc.MaxUSD, "max-usd", 0, "stop before a run once the estimated spend exceeds this (0 = no limit)")
	fs.StringVar(&rc.Guardrails, "guardrails", "matched", "matched, default or both")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if rc.Matrix == "" || rc.Out == "" {
		return errors.New("run: --matrix and --out are required")
	}
	if cfgs != "" {
		rc.Configs = strings.Split(cfgs, ",")
	}
	rc.key = os.Getenv(keyEnv)
	defer func() { _ = harness.Shutdown() }()
	return execute(ctx, rc, stdout)
}

// provider builds the LLM provider for the agent config.
func (rc *runConfig) provider() (llm.Provider, error) {
	switch rc.Provider {
	case providerScripted:
		return scriptedProvider{}, nil
	case providerAnthropic:
		if rc.key == "" {
			return nil, fmt.Errorf("the %s config needs %s for --provider anthropic (use --provider scripted for a free run)",
				baselines.NameTWAgent, keyEnv)
		}
		if _, ok := usd(rc.Model, 0, 0); !ok {
			return nil, fmt.Errorf("no price for model %q; add it to the price table in cmd/tidewatch-bench/price.go", rc.Model)
		}
		return anthropic.New(rc.key, rc.Model), nil
	}
	return nil, fmt.Errorf("--provider %q: want anthropic or scripted", rc.Provider)
}

func execute(ctx context.Context, rc runConfig, stdout io.Writer) error {
	m, err := loadMatrix(rc.Matrix)
	if err != nil {
		return err
	}
	runs, err := plan(m, planOptions{
		Only: rc.Only, Configs: rc.Configs, Repeats: rc.Repeats, Guardrails: rc.Guardrails, MatrixFile: rc.Matrix,
	})
	if err != nil {
		return err
	}
	if len(runs) == 0 {
		return errors.New("run: the filters select no runs")
	}
	var prov llm.Provider
	if slices.ContainsFunc(runs, func(p plannedRun) bool { return p.Config == baselines.NameTWAgent }) {
		if prov, err = rc.provider(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(rc.Out, 0o750); err != nil {
		return err
	}
	csvPath := filepath.Join(rc.Out, "results.csv")
	done, spent, err := readDone(csvPath)
	if err != nil {
		return err
	}
	if !rc.Resume && len(done) > 0 {
		return fmt.Errorf("%s already has results; pass --resume to continue it or choose another --out", csvPath)
	}
	out, err := openCSV(csvPath)
	if err != nil {
		return err
	}
	defer func() { _ = out.close() }()

	load := rc.load
	if load == nil {
		load = scenario.Load
	}
	exec := rc.exec
	if exec == nil {
		exec = func(ctx context.Context, sc *scenario.Scenario, p plannedRun, seed int64) (harness.Result, error) {
			td := filepath.Join(rc.Out, "transcripts")
			if rc.Guardrails == "both" && p.Guardrails == string(harness.GuardrailsDefault) {
				td = filepath.Join(td, "default")
			}
			e, err := baselines.New(p.Config, sc, baselines.Options{
				Provider: prov, Model: rc.Model, Guardrails: harness.Guardrails(p.Guardrails),
			})
			if err != nil {
				return harness.Result{}, err
			}
			return e.Run(ctx, sc, p.Repeat, harness.Options{TranscriptDir: td, Seed: seed})
		}
	}

	var (
		cur     *scenario.Scenario
		curID   string
		started int
		failed  []string
	)
	total := len(runs)
	for i, p := range runs {
		if done[p.key()] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("interrupted after %d of %d runs (%d skipped as done): %w", started, total, i-started, err)
		}
		if rc.MaxUSD > 0 && spent > rc.MaxUSD {
			_, _ = fmt.Fprintf(stdout, "stopped: estimated spend $%.4f exceeds --max-usd %.2f; "+
				"stopped before run %d of %d (%d completed this session). Continue with --resume.\n",
				spent, rc.MaxUSD, i+1, total, started)
			return nil
		}
		if curID != p.ScenarioID {
			if cur, err = load(p.Path); err != nil {
				return fmt.Errorf("loading %s: %w", p.ScenarioID, err)
			}
			curID = p.ScenarioID
		}
		// Common random numbers: every config at repeat r shares the startup-delay seed.
		seed := cur.Seed + int64(p.Repeat)
		t0 := time.Now()
		res, err := exec(ctx, cur, p, seed)
		wall := time.Since(t0).Seconds()
		started++
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("interrupted during %s/%s: %w", p.ScenarioID, p.Config, ctx.Err())
			}
			_, _ = fmt.Fprintf(stdout, "[%d/%d] %s %s %s r%d FAILED: %v\n",
				i+1, total, p.ScenarioID, p.Config, p.Guardrails, p.Repeat, err)
			failed = append(failed, p.key())
			continue
		}
		model := ""
		if p.Config == baselines.NameTWAgent {
			model = rc.Model
			if rc.Provider == providerScripted {
				model = scriptedModel
			}
		}
		cost, _ := usd(model, res.InputTokens, res.OutputTokens)
		spent += cost
		if err := out.write(rowsFor(cur, p, seed, model, res, cost, wall)); err != nil {
			return err
		}
		sum := summarize(res)
		_, _ = fmt.Fprintf(stdout, "[%d/%d] %s %s %s r%d breach_min=%.0f overprov_rm=%.0f "+
			"violations=%d skipped=%d inv=%d usd=$%.4f %.1fs\n",
			i+1, total, p.ScenarioID, p.Config, p.Guardrails, p.Repeat,
			sum.breach, sum.overprov, sum.violations, res.SkippedSteps, res.Investigations, cost, wall)
	}
	_, _ = fmt.Fprintf(stdout, "done: %d runs this session, estimated spend $%.4f, results in %s\n",
		started, spent, csvPath)
	if len(failed) > 0 {
		return fmt.Errorf("%d runs failed (rerun with --resume): %s", len(failed), strings.Join(failed, ", "))
	}
	return nil
}

type runSummary struct {
	breach, overprov float64
	violations       int
}

func summarize(res harness.Result) runSummary {
	var s runSummary
	for _, m := range res.PerWorkload {
		s.breach += m.BreachMinutes
		s.overprov += m.OverprovRM
		s.violations += m.SafetyViolation
	}
	return s
}

// rowsFor builds the CSV rows of a run, one per workload in scenario order.
func rowsFor(sc *scenario.Scenario, p plannedRun, seed int64, model string, res harness.Result,
	cost, wall float64) [][]string {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	rows := make([][]string, 0, len(sc.Workloads))
	for i, w := range sc.Workloads {
		m := res.PerWorkload[w.Name]
		row := []string{
			p.ScenarioID, p.Config, p.Guardrails, strconv.Itoa(p.Repeat), strconv.FormatInt(seed, 10), model, w.Name,
			f(m.BreachMinutes), f(m.OverprovRM), f(m.UnderprovRM), strconv.Itoa(m.ScaleEvents), f(m.LiftRM),
			strconv.Itoa(m.SafetyViolation), "0",
			"0", "0", "0", "0", "0", "0",
		}
		if i == 0 {
			row[13] = strconv.Itoa(res.SkippedSteps)
			row[14], row[15] = strconv.Itoa(res.Investigations), strconv.Itoa(res.LLMCalls)
			row[16], row[17] = strconv.Itoa(res.InputTokens), strconv.Itoa(res.OutputTokens)
			row[18], row[19] = strconv.FormatFloat(cost, 'f', 6, 64), strconv.FormatFloat(wall, 'f', 2, 64)
		}
		rows = append(rows, row)
	}
	return rows
}

// csvOut appends flushed rows to results.csv.
type csvOut struct {
	f *os.File
	w *csv.Writer
}

func openCSV(path string) (*csvOut, error) {
	st, err := os.Stat(path)
	fresh := errors.Is(err, os.ErrNotExist) || (err == nil && st.Size() == 0)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // output path
	if err != nil {
		return nil, err
	}
	o := &csvOut{f: f, w: csv.NewWriter(f)}
	if fresh {
		if err := o.write([][]string{csvHeader}); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return o, nil
}

func (o *csvOut) write(rows [][]string) error {
	if err := o.w.WriteAll(rows); err != nil { // WriteAll flushes
		return err
	}
	return o.f.Sync()
}

func (o *csvOut) close() error { return o.f.Close() }

// readDone returns the keys of the runs already in the CSV and their summed usd. A missing
// file is an empty result.
func readDone(path string) (done map[string]bool, spent float64, err error) {
	done = map[string]bool{}
	f, err := os.Open(path) //nolint:gosec // output path
	if errors.Is(err, os.ErrNotExist) {
		return done, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, 0, fmt.Errorf("%s: %w", path, err)
	}
	if len(rows) == 0 {
		return done, 0, nil
	}
	if !slices.Equal(rows[0], csvHeader) {
		return nil, 0, fmt.Errorf("%s: header does not match this tidewatch-bench version", path)
	}
	for _, r := range rows[1:] {
		done[strings.Join([]string{r[0], r[1], r[2], r[3]}, "|")] = true
		if u, err := strconv.ParseFloat(r[18], 64); err == nil {
			spent += u
		}
	}
	return done, spent, nil
}
