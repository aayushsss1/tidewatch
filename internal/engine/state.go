package engine

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

const (
	statusRefresh   = 30 * time.Second
	maxDecisions    = 5
	trustMinSamples = 10

	defaultStep    = time.Minute
	defaultHorizon = 30 * time.Minute
	defaultHistory = 168 * time.Hour
	defaultSeason  = 24 * time.Hour
	defaultWindow  = 24 * time.Hour
)

var defaultModels = []string{"holt-winters", "seasonal-naive"}

// wlState is the engine's in-memory knowledge of one TideWorkload. It is only
// touched by the one goroutine handling that workload in a tick.
type wlState struct {
	namespace, name string

	hw         forecast.Forecaster // stateful: one instance per workload
	tracker    *forecast.Tracker
	trackerSig [3]time.Duration
	ttr        workload.TimeToReady

	lastApplied      *int32
	lastUp, lastDown time.Time
	recs             []TimedReplicas
	seeded           bool // recs seeded from the first observed replica count

	lastRun     time.Time // wall (clock) time of the last evaluation
	lastAligned time.Time // step-aligned time of the last Record/Observe
	replicas    int32     // last known replica count, for policy budgets

	// Trigger detection inputs (only maintained when Options.Triggers is set).
	prevForecasts  []forecast.Forecast // last tick's forecasts, to judge the current step
	outsideSteps   int
	anomalyPoint   float64 // trusted point forecast of the last judged step, 0 when none
	outsideAt      time.Time
	tplSig         uint64
	tplSeen        bool
	externalChange bool // the enforcer saw another actor change replicas last tick
	restarts       []restartSample

	tracks map[string]*track // overrides being sampled for outcome scoring, by override key
	events []v1.Decision     // approval, rejection and expiry decisions awaiting a status write

	statusWritten bool
	lastStatus    time.Time
	lastVerdict   v1.Verdict
	lastFinal     int32
}

// item carries one workload through both phases of a tick.
type item struct {
	tw     *v1.TideWorkload
	pol    *v1.TidePolicy
	target workload.Target
	st     *wlState

	now           time.Time // step-aligned
	step, horizon time.Duration
	cfg           workload.EffectiveConfig
	cfgOK         bool // cfg was resolved, so its annotation warnings are meaningful
	scale         workload.ScaleState
	actual        float64

	forecasts []forecast.Forecast
	trusted   *forecast.Forecast
	scores    []forecast.Score
	base      BaselineResult

	forecasting metav1.Condition
	budget      bool
	decision    v1.Decision
	enforce     bool // phase 2 should run Enforce
	skipStatus  bool // the target vanished mid-tick: do not write status

	events           []v1.Decision // extra decisions for this tick, oldest first
	baselineReplicas int32         // Desired before any override
	override         *v1.Override  // the override that raised Desired, if any
	dur              time.Duration
}

func (i *item) policyRef() v1.PolicyRef { return i.tw.Spec.PolicyRef }

func forecastParams(spec v1.ForecastingSpec) (step, horizon, history, season, window time.Duration) {
	pick := func(d metav1.Duration, def time.Duration) time.Duration {
		if d.Duration > 0 {
			return d.Duration
		}
		return def
	}
	return pick(spec.Step, defaultStep), pick(spec.Horizon, defaultHorizon), pick(spec.History, defaultHistory),
		pick(spec.Seasonality, defaultSeason), pick(spec.AccuracyWindow, defaultWindow)
}
