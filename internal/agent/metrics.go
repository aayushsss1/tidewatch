package agent

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/pkg/llm"
)

// Agent metrics of spec §7.10. The outcome metrics live with the engine.
var (
	investigationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tidewatch_agent_investigations_total", Help: "Investigations by trigger type and result.",
	}, []string{"trigger", "result"})
	toolCallsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tidewatch_agent_tool_calls_total", Help: "Agent tool calls by tool.",
	}, []string{"tool"})
	tokensTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tidewatch_agent_tokens_total", Help: "LLM tokens by provider and direction (input|output).",
	}, []string{"provider", "direction"})
	latencySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "tidewatch_agent_latency_seconds", Help: "Wall time of one investigation.",
		Buckets: []float64{1, 2, 5, 10, 20, 30, 45, 60},
	})
	queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tidewatch_agent_queue_depth", Help: "Incidents waiting for investigation.",
	})
	budgetRemaining = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tidewatch_agent_budget_remaining", Help: "LLM tokens left in this hour's budget.",
	})
	circuitOpen = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tidewatch_agent_circuit_open", Help: "1 while the provider circuit breaker is not closed.",
	}, []string{"provider"})
	overridesActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tidewatch_overrides_active", Help: "Overrides whose window covers now.",
	})
	stormGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "tidewatch_agent_storm", Help: "1 after a trigger storm was detected; the runner clears it when storm mode ends.",
	})

	metricsOnce sync.Once
)

func registerAgentMetrics() {
	metricsOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(investigationsTotal, toolCallsTotal, tokensTotal, latencySeconds,
			queueDepth, budgetRemaining, circuitOpen, overridesActive, stormGauge)
	})
}

// triggerLabel names the most urgent trigger of an incident.
func triggerLabel(inc *Incident) string {
	if inc.Storm {
		return "storm"
	}
	label, best := "none", -1.0
	for _, m := range inc.Members {
		for _, t := range m.Triggers {
			if u, _ := triggerUrgency(t); u > best {
				label, best = string(t.Type), u
			}
		}
	}
	return label
}

// resultLabel summarizes how an investigation ended.
func resultLabel(r Result) string {
	switch {
	case r.Skipped != "":
		return "skipped_" + r.Skipped
	case r.Exit != "" && r.Exit != llm.ExitTerminal && r.Exit != llm.ExitForcedFinal:
		return string(r.Exit)
	case r.Verdict != "":
		return string(r.Verdict)
	case len(r.Reasons) > 0:
		return r.Reasons[0]
	}
	return "none"
}

// setOverridesActive recounts the overrides in force at now.
func setOverridesActive(s *overrides.Store, now time.Time) {
	n := 0
	for _, uid := range s.Known() {
		ovs, _, _ := s.Snapshot(uid)
		for _, o := range ovs {
			if !o.Start.After(now) && o.End.After(now) {
				n++
			}
		}
	}
	overridesActive.Set(float64(n))
}
