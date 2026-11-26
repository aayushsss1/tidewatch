package baselines

import (
	"fmt"
	"time"

	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// Entry is a configuration by name. Controller is set for the baselines that act on
// SimCluster directly (pass it as harness.Options.Controller together with an empty
// Config); otherwise the config is a Tidewatch run. For "tidewatch-agent" the caller must
// set Config.Provider and Config.Model.
type Entry struct {
	Config     harness.Config
	Controller harness.Controller
}

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

// ByName builds the named configuration for a scenario's workloads. Each call returns
// fresh controller state, so call it once per run. step is the scenario step, used by the
// regex rule's window margin.
func ByName(name string, ws []scenario.Workload) (Entry, error) {
	return ByNameStep(name, ws, time.Minute)
}

// ByNameStep is ByName with an explicit scenario step.
func ByNameStep(name string, ws []scenario.Workload, step time.Duration) (Entry, error) {
	switch name {
	case NameHPA:
		return Entry{Controller: NewHPA(ws)}, nil
	case NameKEDAProm:
		return Entry{Controller: NewKEDAPrometheus(ws)}, nil
	case NameKEDACron:
		return Entry{Controller: NewKEDACron(ws)}, nil
	case NameOracle:
		return Entry{Controller: NewOracle(ws)}, nil
	case NameRegexRule:
		return Entry{Config: harness.Config{Name: name, RuleOverrides: RegexRuleOverrides(step)}}, nil
	case NameTWBaseline:
		return Entry{Config: harness.Config{Name: name}}, nil
	case NameTWAgent:
		return Entry{Config: harness.Config{Name: name, Agent: true}}, nil
	}
	return Entry{}, fmt.Errorf("baselines: unknown config %q (want one of %v)", name, Names())
}
