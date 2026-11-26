package baselines

import (
	"context"
	"errors"
	"fmt"

	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// Configuration names.
const (
	NameHPA        = "hpa"
	NameKEDAProm   = "keda-prometheus"
	NameKEDACron   = "keda-cron"
	NameRegexRule  = "regex-rule"
	NameOracle     = "oracle"
	NameTWBaseline = "tidewatch-baseline"
	NameTWAgent    = "tidewatch-agent"
)

// Names lists the registered configuration names in report order.
func Names() []string {
	return []string{
		NameHPA, NameKEDAProm, NameKEDACron, NameRegexRule, NameOracle, NameTWBaseline, NameTWAgent,
	}
}

// Options configures New.
type Options struct {
	// Provider and Model are required for tidewatch-agent and ignored otherwise.
	Provider llm.Provider
	Model    string
	// Guardrails is the Tidewatch profile (regex-rule, tidewatch-*); empty is the
	// benchmark default, harness.GuardrailsMatched.
	Guardrails harness.Guardrails
}

// Entry is a runnable configuration. An Entry holds controller state, so use it for one
// run only and call New again for the next.
type Entry struct {
	Name       string
	Config     harness.Config
	Controller harness.Controller // set for baselines that act on SimCluster directly
}

// Run simulates sc under the entry, whether it is a Controller or a Tidewatch config.
func (e Entry) Run(
	ctx context.Context, sc *scenario.Scenario, repeat int, opts harness.Options,
) (harness.Result, error) {
	if e.Controller != nil {
		opts.Controller = e.Controller
	}
	return harness.Run(ctx, sc, e.Config, repeat, opts)
}

// New builds the named configuration for sc's workloads. Each call returns fresh state.
func New(name string, sc *scenario.Scenario, opts Options) (Entry, error) {
	ws := sc.Workloads
	tw := harness.Config{Name: name, Guardrails: opts.Guardrails}
	switch name {
	case NameHPA:
		return Entry{Name: name, Controller: NewHPA(ws)}, nil
	case NameKEDAProm:
		return Entry{Name: name, Controller: NewKEDAPrometheus(ws)}, nil
	case NameKEDACron:
		return Entry{Name: name, Controller: NewKEDACron(ws)}, nil
	case NameOracle:
		return Entry{Name: name, Controller: NewOracle(ws)}, nil
	case NameRegexRule:
		tw.RuleOverrides = RegexRuleOverrides(sc.Step)
		return Entry{Name: name, Config: tw}, nil
	case NameTWBaseline:
		return Entry{Name: name, Config: tw}, nil
	case NameTWAgent:
		if opts.Provider == nil {
			return Entry{}, errors.New("baselines: tidewatch-agent needs a Provider")
		}
		tw.Agent, tw.Provider, tw.Model = true, opts.Provider, opts.Model
		return Entry{Name: name, Config: tw}, nil
	}
	return Entry{}, fmt.Errorf("baselines: unknown config %q (want one of %v)", name, Names())
}
