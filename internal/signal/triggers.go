// Package signal turns per-workload engine observations into the deterministic
// triggers that wake the LLM agent. It performs no I/O and never calls a model.
package signal

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// Type names a kind of trigger.
type Type string

// Trigger types, see spec §7.1.
const (
	ForecastBreach    Type = "ForecastBreach"
	Anomaly           Type = "Anomaly"
	ModelDisagreement Type = "ModelDisagreement"
	WorkloadChange    Type = "WorkloadChange"
	ContextDue        Type = "ContextDue"
	OverProvisioned   Type = "OverProvisioned"
)

// Thresholds and windows from spec §7.1.
const (
	breachShortfall     = 0.2
	anomalySteps        = 3
	disagreementRatio   = 0.4
	restartThreshold    = 3
	overProvisionedFrac = 0.5
	debounceWindow      = 10 * time.Minute
	overProvisionedGap  = time.Hour
	worsenFactor        = 2.0
	worsenMinGap        = 3 * time.Minute
	maxDetail           = 200
)

// Trigger is one reason to investigate a workload.
type Trigger struct {
	Type         Type
	Workload     types.NamespacedName
	UID          types.UID
	Policy       v1.PolicyRef
	At           time.Time
	TimeToBreach *time.Duration
	Shortfall    float64
	Detail       string // at most 200 characters, human-readable
}

// Input is everything Detect needs for one workload at one step.
type Input struct {
	Now                        time.Time
	Step, Horizon, TimeToReady time.Duration
	Actual                     float64
	Capacity                   float64 // current replicas × targetPerReplica × headroom
	Trusted                    *forecast.Forecast
	Forecasts                  []forecast.Forecast
	OutsideSteps               int     // consecutive steps actual was outside the trusted interval
	AnomalyForecast            float64 // trusted point forecast for Now that Actual is judged against; <= 0 if unknown
	TemplateChanged            bool
	OOMKilled                  bool
	ExternalReplicaChange      bool
	Restarts5m                 int
	Notes                      string
	Healthy                    bool
	// Covered means an active override already handles the workload: only
	// WorkloadChange may fire, and nothing else is debounced or recorded.
	Covered bool
}

// Debouncer suppresses repeats of the same trigger type per workload.
type Debouncer struct {
	mu   sync.Mutex
	last map[dkey]dval
}

type dkey struct {
	uid types.UID
	typ Type
}

type dval struct {
	at       time.Time
	severity float64
}

// NewDebouncer returns an empty Debouncer.
func NewDebouncer() *Debouncer { return &Debouncer{last: map[dkey]dval{}} }

// Forget drops everything remembered about a workload.
func (d *Debouncer) Forget(uid types.UID) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.last {
		if k.uid == uid {
			delete(d.last, k)
		}
	}
}

// allow reports whether a trigger may fire, recording it when it does.
// Within the window it is suppressed unless severity at least doubled and the last fire was at least 3 minutes ago.
func (d *Debouncer) allow(uid types.UID, t Type, now time.Time, severity float64) bool {
	if d == nil {
		return true
	}
	window, worsens := debounceWindow, true
	if t == OverProvisioned {
		window, worsens = overProvisionedGap, false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	k := dkey{uid, t}
	if prev, ok := d.last[k]; ok && now.Sub(prev.at) < window {
		grew := now.Sub(prev.at) >= worsenMinGap && severity > prev.severity &&
			(prev.severity <= 0 || severity >= prev.severity*worsenFactor)
		if !worsens || !grew {
			return false
		}
	}
	d.last[k] = dval{at: now, severity: severity}
	return true
}

// Detect returns the triggers that fire for in, after debouncing.
func Detect(in Input, uid types.UID, wl types.NamespacedName, pol v1.PolicyRef, d *Debouncer) []Trigger {
	if !in.Healthy {
		return nil
	}
	var out []Trigger
	emit := func(t Type, severity float64, ttb *time.Duration, shortfall float64, detail string) {
		if in.Covered && t != WorkloadChange {
			return
		}
		if !d.allow(uid, t, in.Now, severity) {
			return
		}
		out = append(out, Trigger{
			Type: t, Workload: wl, UID: uid, Policy: pol, At: in.Now,
			TimeToBreach: ttb, Shortfall: shortfall, Detail: truncate(detail, maxDetail),
		})
	}

	if ttb, shortfall, ok := forecastBreach(in); ok {
		emit(ForecastBreach, shortfall, &ttb, shortfall,
			fmt.Sprintf("trusted upper bound exceeds capacity by %.0f%% in %s", shortfall*100, ttb.Round(time.Second)))
	}
	if in.OutsideSteps >= anomalySteps {
		emit(Anomaly, float64(in.OutsideSteps), nil, anomalyShortfall(in),
			fmt.Sprintf("load outside the trusted interval for %d consecutive steps", in.OutsideSteps))
	}
	if diff, ok := modelDisagreement(in); ok {
		emit(ModelDisagreement, diff, nil, diff,
			fmt.Sprintf("model peaks differ by %.0f%% and straddle current capacity", diff*100))
	}
	if reasons := workloadChange(in); len(reasons) > 0 {
		emit(WorkloadChange, float64(len(reasons)), nil, 0, strings.Join(reasons, "; "))
	}
	if ev, ok := contextDue(in); ok {
		ttb := ev.At.Sub(in.Now)
		shortfall := 1.0
		if ev.Multiplier > 0 {
			shortfall = ev.Multiplier - 1
		}
		emit(ContextDue, shortfall, &ttb, shortfall, ev.Text)
	}
	if overProvisioned(in) {
		emit(OverProvisioned, 0, nil, 0, "every model stays under 50% of capacity for the whole horizon")
	}
	return out
}

// maxAnomalyShortfall bounds the shortfall so a near-zero forecast cannot yield a huge ratio.
const maxAnomalyShortfall = 10

// anomalyShortfall is |actual − forecast| / forecast, capped at maxAnomalyShortfall,
// or 0 without a positive forecast.
func anomalyShortfall(in Input) float64 {
	if in.AnomalyForecast <= 0 || math.IsNaN(in.Actual) || math.IsInf(in.Actual, 0) {
		return 0
	}
	return min(math.Abs(in.Actual-in.AnomalyForecast)/in.AnomalyForecast, maxAnomalyShortfall)
}

func forecastBreach(in Input) (ttb time.Duration, shortfall float64, ok bool) {
	if in.Trusted == nil || in.Capacity <= 0 {
		return 0, 0, false
	}
	end := in.Now.Add(in.Horizon)
	upper, found := in.Trusted.MaxUpper(end)
	if !found || math.IsNaN(upper) || math.IsInf(upper, 0) {
		return 0, 0, false
	}
	shortfall = (upper - in.Capacity) / in.Capacity
	if shortfall <= breachShortfall {
		return 0, 0, false
	}
	var breach time.Time
	for _, p := range in.Trusted.Points {
		if !p.T.After(end) && p.Upper > in.Capacity {
			breach = p.T
			break
		}
	}
	// The baseline already covers up to now + time-to-ready + 2 steps.
	if breach.IsZero() || !breach.After(in.Now.Add(in.TimeToReady+4*in.Step)) {
		return 0, 0, false
	}
	return breach.Sub(in.Now), shortfall, true
}

// modelDisagreement fires when two model peaks differ by more than 40% and one
// of them needs more than the current capacity while the other does not, i.e.
// the replica decision would differ.
func modelDisagreement(in Input) (float64, bool) {
	if len(in.Forecasts) < 2 || in.Capacity <= 0 {
		return 0, false
	}
	end := in.Now.Add(in.Horizon)
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, f := range in.Forecasts {
		p, ok := f.Peak(end)
		if !ok {
			continue
		}
		lo, hi = min(lo, p.Point), max(hi, p.Point)
	}
	if math.IsInf(lo, 0) || hi <= 0 {
		return 0, false
	}
	diff := (hi - lo) / hi
	if diff <= disagreementRatio || !(hi > in.Capacity && lo <= in.Capacity) {
		return 0, false
	}
	return diff, true
}

func workloadChange(in Input) []string {
	var r []string
	if in.TemplateChanged {
		r = append(r, "pod template changed (rollout)")
	}
	if in.Restarts5m >= restartThreshold {
		r = append(r, fmt.Sprintf("%d container restarts in 5m", in.Restarts5m))
	}
	if in.OOMKilled {
		r = append(r, "container OOMKilled")
	}
	if in.ExternalReplicaChange {
		r = append(r, "replicas changed by another actor")
	}
	return r
}

// contextDue returns the earliest ops-note event that falls inside the horizon.
func contextDue(in Input) (NoteEvent, bool) {
	end := in.Now.Add(in.Horizon)
	for _, ev := range ParseNoteEvents(in.Notes, in.Now) { // sorted by time
		if !ev.At.After(end) {
			return ev, true
		}
	}
	return NoteEvent{}, false
}

func overProvisioned(in Input) bool {
	if len(in.Forecasts) == 0 || in.Capacity <= 0 {
		return false
	}
	end := in.Now.Add(in.Horizon)
	for _, f := range in.Forecasts {
		p, ok := f.Peak(end)
		if !ok || p.Point >= overProvisionedFrac*in.Capacity {
			return false
		}
	}
	return true
}

// truncate shortens s to at most n runes.
func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n])
}
