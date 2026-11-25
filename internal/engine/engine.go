// Package engine runs the predictive-autoscaling control loop.
package engine

import (
	"cmp"
	"context"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
)

const (
	reasonMetricsUnavailable = "metrics_unavailable"
	reasonInvalidConfig      = "invalid_config"
	reasonNoData             = "no_data"
	reasonScaleFailed        = "scale_failed"
	seasonalNaiveName        = "seasonal-naive"
	defaultPromTimeout       = 10 * time.Second
)

// Options configures an Engine. Zero values select the documented defaults.
type Options struct {
	Workers            int           // default 8
	Tick               time.Duration // default 5s
	DryRun             bool
	DefaultTimeToReady time.Duration // default 60s
	Clock              clock.Clock
	PromFactory        func(url string, timeout time.Duration) (prom.Querier, error)
	Scale              workload.ScaleClient // default workload.NewScaleClient(c)
}

type promEntry struct {
	q   prom.Querier
	col *Collector
}

// Engine is a manager.Runnable that evaluates every TideWorkload each tick.
type Engine struct {
	c     client.Client
	scale workload.ScaleClient
	opts  Options
	clock clock.Clock

	mu     sync.Mutex
	states map[types.UID]*wlState
	proms  map[string]*promEntry
}

// New builds an Engine, applying defaults to opts.
func New(c client.Client, opts Options) *Engine {
	if opts.Workers <= 0 {
		opts.Workers = 8
	}
	if opts.Tick <= 0 {
		opts.Tick = 5 * time.Second
	}
	if opts.DefaultTimeToReady <= 0 {
		opts.DefaultTimeToReady = 60 * time.Second
	}
	if opts.Clock == nil {
		opts.Clock = clock.RealClock{}
	}
	if opts.PromFactory == nil {
		opts.PromFactory = prom.New
	}
	if opts.Scale == nil {
		opts.Scale = workload.NewScaleClient(c)
	}
	registerMetrics()
	return &Engine{
		c: c, scale: opts.Scale, opts: opts, clock: opts.Clock,
		states: map[types.UID]*wlState{}, proms: map[string]*promEntry{},
	}
}

// NeedLeaderElection makes the manager run the engine on the leader only.
func (e *Engine) NeedLeaderElection() bool { return true }

// Start ticks until ctx is cancelled.
func (e *Engine) Start(ctx context.Context) error {
	for {
		e.Step(ctx)
		t := e.clock.NewTimer(e.opts.Tick)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C():
		}
	}
}

func (e *Engine) stateCount() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.states)
}

// Step runs one tick: phase 1 gathers and forecasts in parallel, phase 2
// enforces guardrails and writes status sequentially per policy.
func (e *Engine) Step(ctx context.Context) {
	log := logf.Log.WithName("engine")
	var list v1.TideWorkloadList
	if err := e.c.List(ctx, &list); err != nil {
		log.Error(err, "Listing TideWorkloads")
		return
	}
	e.prune(&list)

	results := make([]*item, len(list.Items))
	jobs := make(chan int)
	var wg sync.WaitGroup
	for range min(e.opts.Workers, max(len(list.Items), 1)) {
		wg.Go(func() {
			for i := range jobs {
				results[i] = e.prepare(ctx, &list.Items[i])
			}
		})
	}
	for i := range list.Items {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	byPolicy := map[v1.PolicyRef][]*item{}
	for _, it := range results {
		if it != nil {
			byPolicy[it.policyRef()] = append(byPolicy[it.policyRef()], it)
		}
	}
	known := e.knownReplicas(&list)
	for _, ref := range slices.SortedFunc(maps.Keys(byPolicy), func(a, b v1.PolicyRef) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	}) {
		e.phase2(ctx, byPolicy[ref], known[ref])
	}
}

// prune drops state and metrics of TideWorkloads that no longer exist.
func (e *Engine) prune(list *v1.TideWorkloadList) {
	live := make(map[types.UID]struct{}, len(list.Items))
	for i := range list.Items {
		live[list.Items[i].UID] = struct{}{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for uid, st := range e.states {
		if _, ok := live[uid]; ok {
			continue
		}
		delete(e.states, uid)
		for _, p := range e.proms {
			p.col.Forget(string(uid))
		}
		forgetMetrics(st.namespace, st.name)
	}
}

func (e *Engine) stateFor(tw *v1.TideWorkload) *wlState {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.states[tw.UID]
	if st == nil {
		st = &wlState{
			namespace: tw.Namespace, name: tw.Name,
			hw: forecast.NewHoltWinters(forecast.HoltWintersOptions{}),
		}
		// Survive operator restarts: cooldowns keep counting from the recorded times.
		if t := tw.Status.LastScaleUpTime; t != nil {
			st.lastUp = t.Time
		}
		if t := tw.Status.LastScaleDownTime; t != nil {
			st.lastDown = t.Time
		}
		e.states[tw.UID] = st
	}
	return st
}

// knownReplicas returns the last known replica count of every workload,
// grouped by policy, so budgets account for workloads that are not due.
func (e *Engine) knownReplicas(list *v1.TideWorkloadList) map[v1.PolicyRef]map[types.UID]int32 {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[v1.PolicyRef]map[types.UID]int32{}
	for i := range list.Items {
		tw := &list.Items[i]
		if out[tw.Spec.PolicyRef] == nil {
			out[tw.Spec.PolicyRef] = map[types.UID]int32{}
		}
		if st := e.states[tw.UID]; st != nil {
			out[tw.Spec.PolicyRef][tw.UID] = st.replicas
		}
	}
	return out
}

func (e *Engine) querier(url string, timeout time.Duration) (*promEntry, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.proms[url]; ok {
		return p, nil
	}
	if timeout <= 0 {
		timeout = defaultPromTimeout
	}
	q, err := e.opts.PromFactory(url, timeout)
	if err != nil {
		return nil, err
	}
	p := &promEntry{q: q, col: NewCollector(q)}
	e.proms[url] = p
	return p, nil
}

// prepare is phase 1 for one workload. It returns nil when the workload is
// skipped (not due, or its policy or target is missing).
func (e *Engine) prepare(ctx context.Context, tw *v1.TideWorkload) *item {
	log := logf.Log.WithName("engine").WithValues("workload", client.ObjectKeyFromObject(tw))
	var pol v1.TidePolicy
	ref := tw.Spec.PolicyRef
	if err := e.c.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &pol); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "Getting policy")
		}
		return nil
	}
	step, horizon, history, season, window := forecastParams(pol.Spec.Forecasting)
	st := e.stateFor(tw)
	wall := e.clock.Now()
	if !st.lastRun.IsZero() && wall.Before(st.lastRun.Add(step)) {
		return nil
	}

	target, ok := e.fetchTarget(ctx, tw)
	if !ok {
		return nil
	}
	scale, err := e.scale.Get(ctx, target)
	if err != nil {
		log.Error(err, "Reading scale")
		return nil
	}
	started := time.Now()
	st.lastRun = wall
	st.replicas = scale.Replicas
	if !st.seeded {
		// A fresh state has no recommendation history; seeding it with the current
		// replicas makes a scale-down wait one stabilization window after a restart.
		st.seeded = true
		st.recs = []TimedReplicas{{T: wall.Truncate(step), Replicas: scale.Replicas}}
	}
	it := &item{
		tw: tw, pol: &pol, target: target, st: st, scale: scale, step: step, horizon: horizon,
		now: wall.Truncate(step), actual: math.NaN(),
	}
	defer func() { it.dur = time.Since(started) }()
	st.ttr.Observe(it.now, scale.Ready)

	cfg, err := workload.Effective(pol.Spec, pol.Annotations[v1.AnnoPaused] == "true", target.Object, target.Ref.Kind)
	if err != nil {
		e.unavailable(it, reasonInvalidConfig, err.Error())
		return it
	}
	it.cfg = cfg
	pe, err := e.querier(pol.Spec.Prometheus.URL, pol.Spec.Prometheus.Timeout.Duration)
	if err != nil {
		e.unavailable(it, reasonMetricsUnavailable, err.Error())
		return it
	}
	if err := pe.q.Ready(ctx); err != nil {
		e.unavailable(it, reasonMetricsUnavailable, err.Error())
		return it
	}
	series, gaps, err := pe.col.Update(ctx, string(tw.UID), cfg.Query, history, step, it.now)
	if err != nil {
		e.unavailable(it, reasonMetricsUnavailable, err.Error())
		return it
	}
	series, _ = series.FillGaps(2)
	if n := len(series.Points); n > 0 {
		it.actual = series.Points[n-1].V
	}

	it.forecasting = metav1.Condition{Type: v1.CondForecasting, Status: metav1.ConditionTrue, Reason: "forecast_ok"}
	if gaps.Healthy() {
		if err := e.runModels(ctx, it, series, season, window); err != nil {
			e.unavailable(it, reasonInvalidConfig, err.Error())
			return it
		}
	} else {
		it.forecasting = condFalse("insufficient_data", "too many gaps in the load series")
	}
	if it.trusted == nil && it.forecasting.Status == metav1.ConditionTrue {
		it.forecasting = condFalse("insufficient_history", "no model produced a forecast")
	}
	it.base = Baseline(BaselineInput{
		Now: it.now, Actual: it.actual, Trusted: it.trusted,
		TimeToReady: st.ttr.P90(e.opts.DefaultTimeToReady), Margin: 2 * step, Config: cfg,
	})
	if !gaps.Healthy() || math.IsNaN(it.actual) {
		// Degraded data may only scale up: never shrink on a partial view.
		it.base.Desired = max(it.base.Desired, scale.Replicas)
	}
	if !it.base.OK {
		e.unavailable(it, reasonNoData, "load series has no data")
		return it
	}
	it.enforce = true
	e.recordPhase1Metrics(it)
	return it
}

func (e *Engine) fetchTarget(ctx context.Context, tw *v1.TideWorkload) (workload.Target, bool) {
	ref := tw.Spec.TargetRef
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.FromAPIVersionAndKind(ref.APIVersion, ref.Kind))
	if err := e.c.Get(ctx, types.NamespacedName{Namespace: tw.Namespace, Name: ref.Name}, obj); err != nil {
		if !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			logf.Log.WithName("engine").Error(err, "Getting target", "workload", client.ObjectKeyFromObject(tw))
		}
		return workload.Target{}, false
	}
	t := workload.Target{Ref: ref, Namespace: tw.Namespace, Object: obj, Policy: tw.Spec.PolicyRef}
	c := meta.FindStatusCondition(tw.Status.Conditions, v1.CondConflict)
	if c == nil {
		// The policy controller has not yet initialized this TideWorkload; it may
		// be in conflict, so do nothing until it says otherwise.
		return workload.Target{}, false
	}
	if c.Status == metav1.ConditionTrue {
		t.Conflict = c.Message
	}
	return t, true
}

// runModels forecasts with every configured model, scores them and picks the
// trusted one. Only models that succeeded this tick are eligible.
func (e *Engine) runModels(ctx context.Context, it *item, series forecast.Series, season, window time.Duration) error {
	spec := it.pol.Spec.Forecasting
	names := spec.Models
	if len(names) == 0 {
		names = defaultModels
	}
	st := it.st
	models, err := forecast.NewRegistry(st.hw, forecast.NewSeasonalNaive()).Get(names)
	if err != nil {
		return err
	}
	sig := [3]time.Duration{it.horizon, it.step, window}
	if st.tracker == nil || st.trackerSig != sig {
		st.tracker, st.trackerSig = forecast.NewTracker(it.horizon, it.step, window), sig
	}
	byName := map[string]*forecast.Forecast{}
	for _, m := range models {
		f, err := m.Forecast(ctx, series, it.horizon, season)
		if err != nil {
			continue
		}
		it.forecasts = append(it.forecasts, f)
		byName[f.Model] = &it.forecasts[len(it.forecasts)-1]
	}
	if !st.lastAligned.Equal(it.now) { // the tracker assumes one Record/Observe per step
		for _, f := range it.forecasts {
			st.tracker.Record(f, it.now)
		}
		st.tracker.Observe(series, it.now)
		st.lastAligned = it.now
	}
	it.scores = st.tracker.Scores()
	if name, ok := st.tracker.Trusted(trustMinSamples); ok && byName[name] != nil {
		it.trusted = byName[name]
	} else {
		it.trusted = byName[seasonalNaiveName]
	}
	return nil
}

// unavailable records a held decision and skips phase 2 for the workload.
func (e *Engine) unavailable(it *item, reason, msg string) {
	it.forecasting = condFalse(reason, msg)
	it.decision = v1.Decision{
		Time: metav1.NewTime(it.now), Source: v1.SourceBaseline,
		Desired: it.scale.Replicas, Final: it.scale.Replicas,
		Verdict: v1.VerdictHeld, Reasons: []string{reason},
	}
}

func condFalse(reason, msg string) metav1.Condition {
	return metav1.Condition{Type: v1.CondForecasting, Status: metav1.ConditionFalse, Reason: reason, Message: msg}
}

func (e *Engine) recordPhase1Metrics(it *item) {
	ns, name := it.tw.Namespace, it.tw.Name
	for _, f := range it.forecasts {
		if n := len(f.Points); n > 0 {
			p := f.Points[n-1]
			forecastGauge.WithLabelValues(f.Model, "point", ns, name).Set(p.Point)
			forecastGauge.WithLabelValues(f.Model, "lower", ns, name).Set(p.Lower)
			forecastGauge.WithLabelValues(f.Model, "upper", ns, name).Set(p.Upper)
		}
	}
	for _, s := range it.scores {
		forecastErrorGauge.WithLabelValues(s.Model, s.Lead.String(), ns, name).Set(s.SMAPE)
	}
	if capacity := float64(it.scale.Ready) * it.cfg.TargetPerReplica; capacity > 0 && !math.IsNaN(it.actual) {
		capacityRatioGauge.WithLabelValues(ns, name).Set(it.actual / capacity)
	}
}
