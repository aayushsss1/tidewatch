package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/aayushsss1/tidewatch/sim/baselines"
	"github.com/aayushsss1/tidewatch/sim/harness"
)

// Matrix is a list of scenarios crossed with configurations. Scenario paths (globs allowed)
// are relative to the matrix file.
type Matrix struct {
	Scenarios []string `yaml:"scenarios"`
	Configs   []string `yaml:"configs"`
	// Repeats is the repeat count per config; absent configs run once.
	Repeats map[string]int `yaml:"repeats"`
}

// noGuardrails is the guardrails column of controller baselines, which have no profile.
const noGuardrails = "n/a"

// plannedRun is one (scenario, config, guardrails, repeat) cell of the matrix.
type plannedRun struct {
	ScenarioID string // path under the scenarios directory, without extension
	Path       string
	Config     string
	Guardrails string
	Repeat     int
	Model      string // the agent's model; empty for configs that call no model
}

func (p plannedRun) key() string {
	return fmt.Sprintf("%s|%s|%s|%d|%s", p.ScenarioID, p.Config, p.Guardrails, p.Repeat, p.Model)
}

func loadMatrix(file string) (Matrix, error) {
	data, err := os.ReadFile(file) //nolint:gosec // user-supplied matrix path
	if err != nil {
		return Matrix{}, err
	}
	var m Matrix
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&m); err != nil {
		return Matrix{}, fmt.Errorf("%s: %w", file, err)
	}
	if len(m.Scenarios) == 0 || len(m.Configs) == 0 {
		return Matrix{}, fmt.Errorf("%s: scenarios and configs are required", file)
	}
	for _, c := range m.Configs {
		if !slices.Contains(baselines.Names(), c) {
			return Matrix{}, fmt.Errorf("%s: unknown config %q (want one of %v)", file, c, baselines.Names())
		}
	}
	for c, n := range m.Repeats {
		if n < 1 || !slices.Contains(m.Configs, c) {
			return Matrix{}, fmt.Errorf("%s: repeats[%s] = %d: want >= 1 and a listed config", file, c, n)
		}
	}
	return m, nil
}

// scenarioID names a scenario file by its path under the nearest "scenarios" directory
// ("headline/w1-flash-3x"), or by its base name when there is none.
func scenarioID(file string) string {
	slash := filepath.ToSlash(file)
	parts := strings.Split(slash, "/")
	for i := len(parts) - 2; i >= 0; i-- {
		if parts[i] == "scenarios" {
			slash = strings.Join(parts[i+1:], "/")
			break
		}
	}
	if slash == filepath.ToSlash(file) {
		slash = parts[len(parts)-1]
	}
	return strings.TrimSuffix(slash, path.Ext(slash))
}

// planOptions filters and expands a matrix.
type planOptions struct {
	Only        string   // glob over the scenario ID, or scenarioID/config
	Configs     []string // restricts the configs when non-empty
	Repeats     int      // overrides the agent's repeat count when > 0
	Guardrails  string   // matched | default | both
	MatrixFile  string
	AgentConfig string
}

// isTidewatch reports whether the config runs the real engine, and so has a guardrail profile.
func isTidewatch(config string) bool {
	switch config {
	case baselines.NameRegexRule, baselines.NameTWBaseline, baselines.NameTWAgent:
		return true
	}
	return false
}

func guardrailProfiles(g string) ([]string, error) {
	switch g {
	case "", string(harness.GuardrailsMatched):
		return []string{string(harness.GuardrailsMatched)}, nil
	case string(harness.GuardrailsDefault):
		return []string{string(harness.GuardrailsDefault)}, nil
	case "both":
		return []string{string(harness.GuardrailsMatched), string(harness.GuardrailsDefault)}, nil
	}
	return nil, fmt.Errorf("--guardrails %q: want matched, default or both", g)
}

// plan expands the matrix into runs, scenarios outermost, in a deterministic order.
func plan(m Matrix, o planOptions) ([]plannedRun, error) {
	profiles, err := guardrailProfiles(o.Guardrails)
	if err != nil {
		return nil, err
	}
	var files []string
	for _, pat := range m.Scenarios {
		full := pat
		if !filepath.IsAbs(pat) {
			full = filepath.Join(filepath.Dir(o.MatrixFile), pat)
		}
		hits, err := filepath.Glob(full)
		if err != nil {
			return nil, fmt.Errorf("scenario pattern %q: %w", pat, err)
		}
		if len(hits) == 0 {
			return nil, fmt.Errorf("scenario pattern %q matches no files", pat)
		}
		slices.Sort(hits)
		files = append(files, hits...)
	}
	var runs []plannedRun
	seen := map[string]bool{}
	for _, f := range files {
		id := scenarioID(f)
		if seen[id] {
			return nil, fmt.Errorf("scenario %q listed twice", id)
		}
		seen[id] = true
		for _, cfg := range m.Configs {
			if len(o.Configs) > 0 && !slices.Contains(o.Configs, cfg) {
				continue
			}
			if o.Only != "" {
				a, err1 := path.Match(o.Only, id)
				b, err2 := path.Match(o.Only, id+"/"+cfg)
				if err1 != nil {
					return nil, fmt.Errorf("--only %q: %w", o.Only, err1)
				}
				if err2 != nil || (!a && !b) {
					continue
				}
			}
			repeats := max(m.Repeats[cfg], 1)
			if o.Repeats > 0 && cfg == baselines.NameTWAgent {
				repeats = o.Repeats
			}
			gs := []string{noGuardrails}
			if isTidewatch(cfg) {
				gs = profiles
			}
			for _, g := range gs {
				for r := range repeats {
					runs = append(runs, plannedRun{ScenarioID: id, Path: f, Config: cfg, Guardrails: g, Repeat: r})
				}
			}
		}
	}
	return runs, nil
}
