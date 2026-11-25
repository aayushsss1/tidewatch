package engine

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	wlLabels = []string{"namespace", "workload"}

	forecastGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tidewatch_forecast",
		Help: "Forecast load at the horizon point, by model and bound (point|lower|upper).",
	}, append([]string{"model", "bound"}, wlLabels...))
	forecastErrorGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tidewatch_forecast_error",
		Help: "Windowed sMAPE of a model at a lead time.",
	}, append([]string{"model", "lead"}, wlLabels...))
	capacityRatioGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tidewatch_capacity_ratio",
		Help: "Load divided by ready replicas times target per replica.",
	}, wlLabels)
	desiredReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tidewatch_desired_replicas",
		Help: "Replicas the baseline wants before guardrails.",
	}, wlLabels)
	decisionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tidewatch_decisions_total",
		Help: "Scaling decisions by source and verdict.",
	}, append([]string{"source", "verdict"}, wlLabels...))
	baselineOnlyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "tidewatch_baseline_only_replicas",
		Help: "Replicas the baseline wanted before any agent override, the ghost series to plot against desired.",
	}, wlLabels)
	outcomeTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "tidewatch_decision_outcome_total",
		Help: "Scored agent overrides by outcome (timely|late|unneeded).",
	}, []string{"outcome"})
	stepDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "tidewatch_step_duration_seconds",
		Help:    "Time spent evaluating one workload in a tick.",
		Buckets: prometheus.DefBuckets,
	}, wlLabels)

	registerOnce sync.Once
)

func registerMetrics() {
	registerOnce.Do(func() {
		ctrlmetrics.Registry.MustRegister(
			forecastGauge, forecastErrorGauge, capacityRatioGauge,
			desiredReplicasGauge, decisionsTotal, stepDuration, baselineOnlyGauge, outcomeTotal,
		)
	})
}

// forgetMetrics drops every series of a workload that no longer exists.
func forgetMetrics(namespace, workload string) {
	m := prometheus.Labels{"namespace": namespace, "workload": workload}
	forecastGauge.DeletePartialMatch(m)
	forecastErrorGauge.DeletePartialMatch(m)
	capacityRatioGauge.DeletePartialMatch(m)
	desiredReplicasGauge.DeletePartialMatch(m)
	decisionsTotal.DeletePartialMatch(m)
	stepDuration.DeletePartialMatch(m)
	baselineOnlyGauge.DeletePartialMatch(m)
}
