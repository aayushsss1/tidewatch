// Package tools implements the agent's twelve tools: ten read-only context
// tools and two terminal tools. Every tool is scoped to an incident, bounded
// to 4 KB of output and labels what it returns as data.
package tools

import (
	"context"
	"time"
)

// WorkloadRef identifies a TideWorkload.
type WorkloadRef struct{ Namespace, Name string }

// Scope is what one incident's tools may touch.
type Scope struct {
	Policy       string // ns.name
	Namespaces   []string
	Workloads    []WorkloadRef
	RedactEvents bool
}

// Source supplies tool data. A later task implements it over the engine,
// the override store, Kubernetes and Prometheus.
type Source interface {
	State(ctx context.Context, w WorkloadRef) (WorkloadState, error)
	Forecasts(ctx context.Context, w WorkloadRef) (ForecastSet, error)
	Related(ctx context.Context, w WorkloadRef) ([]Related, error)
	Outcomes(ctx context.Context, w WorkloadRef) ([]Outcome, error)
	Events(ctx context.Context, w WorkloadRef, since time.Duration) ([]Event, error)
	Rollouts(ctx context.Context, w WorkloadRef) ([]Rollout, error)
	OpsNotes(ctx context.Context) (string, error)
	SimilarEpisodes(ctx context.Context, w WorkloadRef) ([]EpisodeSummary, error)
	QueryRange(ctx context.Context, promql string, rng, step time.Duration) (Series, error)
}

// WorkloadState is the current state of a workload.
type WorkloadState struct {
	Replicas         int32             `json:"replicas"`
	Ready            int32             `json:"ready"`
	Requests         map[string]string `json:"requests,omitempty"`
	Limits           map[string]string `json:"limits,omitempty"`
	TimeToReadyP50   float64           `json:"timeToReadyP50Seconds,omitempty"`
	TimeToReadyP90   float64           `json:"timeToReadyP90Seconds,omitempty"`
	Guardrails       map[string]string `json:"guardrails,omitempty"`
	ActiveOverrides  []string          `json:"activeOverrides,omitempty"`
	PendingApprovals []string          `json:"pendingApprovals,omitempty"`
}

// ForecastPoint is one forecast step with its interval.
type ForecastPoint struct {
	T     int64   `json:"t"`
	Value float64 `json:"v"`
	Lower float64 `json:"lo"`
	Upper float64 `json:"hi"`
}

// ModelForecast is one model's forecast and recent accuracy.
type ModelForecast struct {
	Name     string          `json:"name"`
	Peak     float64         `json:"peak"`
	PeakAt   time.Time       `json:"peakAt"`
	PeakLow  float64         `json:"peakLower"`
	PeakHigh float64         `json:"peakUpper"`
	SMAPE    float64         `json:"smape"`
	Coverage float64         `json:"coverage"`
	Points   []ForecastPoint `json:"points,omitempty"`
}

// ForecastSet holds every model's forecast for a workload.
type ForecastSet struct {
	Models []ModelForecast `json:"models"`
}

// Related is another workload in the policy.
type Related struct {
	Workload   string  `json:"workload"`
	Load       float64 `json:"load"`
	Peak       float64 `json:"peak"`
	Corr       float64 `json:"corr"`
	LagSeconds int     `json:"lagSeconds"`
}

// Outcome is a past decision with its verdict.
type Outcome struct {
	At      time.Time `json:"at"`
	Source  string    `json:"source"`
	Verdict string    `json:"verdict"`
	Score   float64   `json:"score"`
}

// Event is a Kubernetes event. Message is dropped when events are redacted.
type Event struct {
	Reason   string    `json:"reason"`
	Type     string    `json:"type"`
	Count    int32     `json:"count"`
	LastSeen time.Time `json:"lastSeen"`
	Message  string    `json:"message,omitempty"`
}

// Rollout is one template change.
type Rollout struct {
	At       time.Time `json:"at"`
	Images   []string  `json:"images"`
	Progress string    `json:"progress,omitempty"`
}

// EpisodeSummary is a similar past episode.
type EpisodeSummary struct {
	Name    string  `json:"name"`
	Score   float64 `json:"score"`
	Summary string  `json:"summary"`
}

// Point is one (unix seconds, value) sample.
type Point struct {
	T int64   `json:"t"`
	V float64 `json:"v"`
}

// Series is a range-query result.
type Series struct {
	Points []Point `json:"points"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Mean   float64 `json:"mean"`
	Last   float64 `json:"last"`
}
