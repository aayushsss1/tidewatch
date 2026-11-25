package engine

import (
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// Snapshot is a read-only copy of what the engine knew about one workload at
// its last successful evaluation. The agent reads it to build investigations.
type Snapshot struct {
	TW          types.NamespacedName
	UID         types.UID
	Policy      v1.PolicyRef
	Now         time.Time
	Actual      float64
	Replicas    int32
	Ready       int32
	Cfg         workload.EffectiveConfig
	TimeToReady time.Duration
	Forecasts   []forecast.Forecast
	Scores      []forecast.Score
	Trusted     string // model name, empty when none
	Series      forecast.Series
}

func (s Snapshot) clone() Snapshot {
	s.Forecasts = slices.Clone(s.Forecasts)
	s.Scores = slices.Clone(s.Scores)
	s.Series.Points = slices.Clone(s.Series.Points)
	s.Cfg.Warnings = slices.Clone(s.Cfg.Warnings)
	return s
}

// saveSnapshot stores the evaluation of it for the agent. The stored value is
// never mutated afterwards; readers get clones.
func (e *Engine) saveSnapshot(it *item, series forecast.Series) {
	s := Snapshot{
		TW:  types.NamespacedName{Namespace: it.tw.Namespace, Name: it.tw.Name},
		UID: it.tw.UID, Policy: it.policyRef(), Now: it.now, Actual: it.actual,
		Replicas: it.scale.Replicas, Ready: it.scale.Ready, Cfg: it.cfg,
		TimeToReady: it.st.ttr.P90(e.opts.DefaultTimeToReady),
		Forecasts:   slices.Clone(it.forecasts), Scores: slices.Clone(it.scores), Series: series,
	}
	if it.trusted != nil {
		s.Trusted = it.trusted.Model
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snaps[s.UID] = s
}

// Snapshot returns a copy of the workload's last evaluation.
func (e *Engine) Snapshot(uid types.UID) (Snapshot, bool) {
	e.mu.Lock()
	s, ok := e.snaps[uid]
	e.mu.Unlock()
	if !ok {
		return Snapshot{}, false
	}
	return s.clone(), true
}

// Series returns a copy of the workload's recent load series.
func (e *Engine) Series(uid types.UID) (forecast.Series, bool) {
	e.mu.Lock()
	s, ok := e.snaps[uid]
	e.mu.Unlock()
	if !ok {
		return forecast.Series{}, false
	}
	s.Series.Points = slices.Clone(s.Series.Points)
	return s.Series, true
}

// DryRun reports whether the engine never writes replicas.
func (e *Engine) DryRun() bool { return e.opts.DryRun }
