package baselines

import (
	"fmt"
	"time"

	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// RuleFunc is the type of harness.Config.RuleOverrides.
type RuleFunc = func(time.Time, []scenario.Note, map[string]engine.Snapshot) []harness.RuleOverride

const ruleHold = 30 * time.Minute

// RegexRuleOverrides is the harness hook of the regex-rule config: tidewatch-baseline plus
// a deterministic override per workload for each parseable ops note.
//
// For a note "expect ~Nx" at event time E, each workload in the policy gets
// minReplicas = ceil(N x currentLoad / (capacity x headroom)), where currentLoad is the
// engine snapshot's actual load at publish time, over the window
// [E - timeToReady - 2 x step, E + 30m], clamped to the store's 2h maximum. The engine
// enforces the usual guardrails. An unparseable note does nothing, and an event already
// over is ignored.
func RegexRuleOverrides(step time.Duration) RuleFunc {
	return func(now time.Time, notes []scenario.Note, snaps map[string]engine.Snapshot) []harness.RuleOverride {
		var out []harness.RuleOverride
		for _, n := range notes {
			ev, ok := ParseNote(n.Text, n.PublishAt)
			if !ok {
				continue
			}
			for w, sn := range snaps {
				capacity, headroom := sn.Cfg.TargetPerReplica, sn.Cfg.Headroom
				if capacity <= 0 || headroom <= 0 {
					continue
				}
				start := ev.EventTime.Add(-sn.TimeToReady - 2*step)
				end := ev.EventTime.Add(ruleHold)
				if start.Before(now) {
					start = now
				}
				if !end.After(start) {
					continue
				}
				if end.Sub(start) > overrides.MaxWindow {
					end = start.Add(overrides.MaxWindow)
				}
				out = append(out, harness.RuleOverride{
					Workload:    w,
					MinReplicas: ceilEps(ev.Multiplier * sn.Actual / (capacity * headroom)),
					Start:       start, End: end,
					Reason: fmt.Sprintf("rule: note expects %.4gx at %s", ev.Multiplier, ev.EventTime.Format(time.RFC3339)),
				})
			}
		}
		return out
	}
}
