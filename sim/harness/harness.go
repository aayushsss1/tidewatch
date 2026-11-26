// Package harness runs the real Tidewatch operator code (policy controller, engine, agent
// runner, investigator, tools and override store) against the simulated world: SimCluster,
// SimProm and a fake clock. Only the world is simulated; an in-process envtest API server
// holds the Deployment, TidePolicy and TideWorkload objects.
//
// Runs share that API server and the process-global engine/agent metrics, so they run
// serially: Run holds a package-level mutex and concurrent calls queue.
//
// Safety accounting. Each step in which the engine evaluated a workload, the harness
// compares the TideWorkload's status DesiredReplicas (after overrides, before
// enforcement) with BaselineReplicas (the forecast's desire without overrides) via
// SimCluster.AccountLift: a violation is Desired < Baseline, and LiftRM accumulates the
// replica-minutes the agent added. This lift check is a near-tautological regression
// guard on engine.applyOverride, which can only raise the baseline. It is not the
// headline claim. "The agent never leaves less capacity than the baseline" is shown by the
// paired comparison of an agent run with the baseline run of the same scenario and seed
// (common random numbers): agent underprov_rm and breach_min must be <= the baseline's,
// and override waste is agent overprov_rm minus baseline overprov_rm. That comparison is
// empirical, not a proof, because enforcer state (cooldowns, rate limits) diverges between
// the runs after the first override.
package harness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/agent"
	"github.com/aayushsss1/tidewatch/internal/controller"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/transcript"
	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/promsim"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// ControllerNamespace is the SimCluster namespace in Controller mode, where no Kubernetes
// objects exist. Controllers address workloads as {ControllerNamespace, workload name}.
const ControllerNamespace = "sim"

const (
	loadQuery    = `sum(rate(http_requests_total{app="{{.Name}}"}[1m]))`
	promURL      = "http://promsim.invalid"
	labelEnabled = "tidewatch.dev/enabled"
	labelApp     = "app"
	policyName   = "policy"
	reconcileMax = 20
)

// Config selects what drives the cluster in one run.
type Config struct {
	Name     string // "tidewatch-baseline" | "tidewatch-agent" | a baseline's name
	Agent    bool
	Provider llm.Provider // nil when !Agent
	Model    string
}

// Controller is a non-Tidewatch baseline that acts on SimCluster directly. Workloads are
// addressed as {ControllerNamespace, name}.
type Controller interface {
	Name() string
	Step(ctx context.Context, now time.Time, sc *scenario.Scenario, cl *cluster.SimCluster) error
}

// Options tunes a run.
type Options struct {
	Epoch         time.Time  // absolute time of trace offset 0; default scenario.DefaultEpoch
	TranscriptDir string     // agent transcripts are written here when set
	EnvtestAssets string     // envtest binaries; default $KUBEBUILDER_ASSETS or <repo>/bin/k8s/*
	Controller    Controller // when set, Tidewatch is NOT run and the controller drives the cluster
	Seed          int64      // startup-delay seed; default the scenario's
}

// Result is the outcome of one run.
type Result struct {
	Scenario, Config string
	Repeat           int
	Model            string
	PerWorkload      map[string]cluster.Metrics
	// Investigations counts only investigations that reached the provider (not skipped for
	// budget, breaker, or a missed deadline).
	Investigations int
	LLMCalls       int
	InputTokens    int
	OutputTokens   int
	TranscriptPath string
	// SkippedSteps counts workload-steps the engine did not evaluate (see checkEvaluated),
	// with SkippedReasons breaking them down. Tidewatch configs only; expected to be 0.
	SkippedSteps   int
	SkippedReasons map[string]int
}

var (
	runMu      sync.Mutex
	runCounter atomic.Int64
)

// countingProvider counts model calls and tokens for one run.
type countingProvider struct {
	llm.Provider
	calls, in, out atomic.Int64
}

func (c *countingProvider) Turn(ctx context.Context, req llm.TurnRequest) (llm.TurnResponse, error) {
	c.calls.Add(1)
	resp, err := c.Provider.Turn(ctx, req)
	c.in.Add(int64(resp.InputTokens))
	c.out.Add(int64(resp.OutputTokens))
	return resp, err
}

// lateSink forwards triggers to the runner, which is built after the engine it needs.
type lateSink struct{ r atomic.Pointer[agent.Runner] }

func (s *lateSink) Submit(t signal.Trigger) {
	if r := s.r.Load(); r != nil {
		r.Submit(t)
	}
}

// loadFn returns the offered load of workload w over the slot ending at t.
type loadFn func(w string, t time.Time) float64

func newLoadFn(sc *scenario.Scenario) loadFn {
	series := map[string][]float64{}
	var first time.Time
	for _, w := range sc.Workloads {
		s := sc.Series(w.Name)
		vals := make([]float64, len(s.Points))
		for i, p := range s.Points {
			vals[i] = p.V
		}
		series[w.Name] = vals
		if len(s.Points) > 0 {
			first = s.Points[0].T
		}
	}
	return func(w string, t time.Time) float64 {
		vals := series[w]
		i := int(t.Sub(first) / sc.Step)
		if i < 0 || i >= len(vals) {
			return 0
		}
		return vals[i]
	}
}

// Run simulates sc under cfg and returns its metrics.
func Run(ctx context.Context, sc *scenario.Scenario, cfg Config, repeat int, opts Options) (Result, error) {
	runMu.Lock()
	defer runMu.Unlock()
	epoch := opts.Epoch
	if epoch.IsZero() {
		epoch = scenario.DefaultEpoch
	}
	seed := opts.Seed
	if seed == 0 {
		seed = sc.Seed
	}
	cp := *sc // shallow copy: Series only reads shared state
	cp.Epoch = epoch
	sc = &cp

	name := cfg.Name
	if opts.Controller != nil && name == "" {
		name = opts.Controller.Name()
	}
	res := Result{Scenario: sc.Name, Config: name, Repeat: repeat, Model: cfg.Model}
	if cfg.Agent && cfg.Provider == nil {
		return res, errors.New("harness: Agent config needs a Provider")
	}
	if cfg.Agent && opts.Controller != nil {
		return res, errors.New("harness: Agent and Controller are mutually exclusive")
	}
	if cfg.Agent && res.Model == "" {
		res.Model = cfg.Provider.Model()
	}

	clk := clocktesting.NewFakeClock(sc.StartTime())
	cl := cluster.New(seed, clk)
	load := newLoadFn(sc)

	r := &run{
		sc: sc, cfg: cfg, clk: clk, cl: cl, load: load, res: &res, ns: ControllerNamespace,
	}
	if opts.Controller == nil {
		r.ns = fmt.Sprintf("run-%d", runCounter.Add(1))
	}
	for _, w := range sc.Workloads {
		l0 := load(w.Name, sc.StartTime())
		needed := cluster.Needed(l0, w.Capacity, cluster.DefaultHeadroom)
		initial := min(max(needed, int32(w.MinReplicas)), int32(w.MaxReplicas)) //nolint:gosec // small
		cl.AddWorkload(r.ns, w.Name, cluster.WorkloadSpec{
			CapacityRPS: w.Capacity, StartupDelay: cluster.Delay{Mean: w.StartupDelay}, Initial: initial,
		})
	}

	var err error
	if opts.Controller != nil {
		err = r.loopController(ctx, opts.Controller)
	} else {
		err = r.runTidewatch(ctx, opts, repeat)
	}
	if err != nil {
		return res, err
	}
	res.PerWorkload = map[string]cluster.Metrics{}
	for _, w := range sc.Workloads {
		m, err := cl.Metrics(r.ns, w.Name)
		if err != nil {
			return res, err
		}
		res.PerWorkload[w.Name] = m
	}
	return res, nil
}

// run is the state of one simulation.
type run struct {
	sc   *scenario.Scenario
	cfg  Config
	clk  *clocktesting.FakeClock
	cl   *cluster.SimCluster
	load loadFn
	res  *Result
	ns   string
}

// advance moves the clock one step and accounts the slot that just elapsed.
func (r *run) advance(t time.Time) {
	r.clk.SetTime(t.Add(r.sc.Step))
	next := r.clk.Now()
	loads := make(map[cluster.Key]float64, len(r.sc.Workloads))
	for _, w := range r.sc.Workloads {
		loads[cluster.Key{Namespace: r.ns, Name: w.Name}] = r.load(w.Name, next)
	}
	r.cl.Account(next, r.sc.Step, loads)
}

func (r *run) loopController(ctx context.Context, c Controller) error {
	for t := r.sc.StartTime(); t.Before(r.sc.EndTime()); t = t.Add(r.sc.Step) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.Step(ctx, t, r.sc, r.cl); err != nil {
			return fmt.Errorf("controller %s at %s: %w", c.Name(), t.Format(time.RFC3339), err)
		}
		r.advance(t)
	}
	return nil
}

func dur(d time.Duration) metav1.Duration { return metav1.Duration{Duration: d} }

func (r *run) objects(ctx context.Context, c client.Client) error {
	sc := r.sc
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: r.ns}}); err != nil {
		return err
	}
	minR, maxR := int32(sc.Workloads[0].MinReplicas), int32(sc.Workloads[0].MaxReplicas) //nolint:gosec // small
	for _, w := range sc.Workloads {
		minR, maxR = min(minR, int32(w.MinReplicas)), max(maxR, int32(w.MaxReplicas)) //nolint:gosec // small
	}
	for _, w := range sc.Workloads {
		labels := map[string]string{labelApp: w.Name, labelEnabled: "true"}
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name: w.Name, Namespace: r.ns, Labels: labels,
				Annotations: map[string]string{
					v1.AnnoTargetPerRep: strconv.FormatFloat(w.Capacity, 'f', -1, 64),
					v1.AnnoMinReplicas:  strconv.Itoa(w.MinReplicas),
					v1.AnnoMaxReplicas:  strconv.Itoa(w.MaxReplicas),
				},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: new(int32(w.MinReplicas)), //nolint:gosec // small
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{labelApp: w.Name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{labelApp: w.Name}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.invalid/app:1"}}},
				},
			},
		}
		if err := c.Create(ctx, d); err != nil {
			return err
		}
	}
	pol := &v1.TidePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: r.ns},
		Spec: v1.TidePolicySpec{
			Mode:       v1.ModeActive,
			Prometheus: v1.PrometheusSpec{URL: promURL},
			Selector: v1.WorkloadSelector{
				LabelSelector: metav1.LabelSelector{MatchLabels: map[string]string{labelEnabled: "true"}},
				Kinds:         []v1.TargetKind{{APIVersion: "apps/v1", Kind: "Deployment"}},
			},
			Load: v1.LoadSpec{
				DefaultProfile: "rps",
				Profiles: map[string]v1.LoadProfile{"rps": {
					Query:            loadQuery,
					TargetPerReplica: strconv.FormatFloat(sc.Workloads[0].Capacity, 'f', -1, 64),
				}},
			},
			Forecasting: v1.ForecastingSpec{History: dur(sc.History), Step: dur(sc.Step)},
			Guardrails:  v1.GuardrailsSpec{MinReplicas: &minR, MaxReplicas: maxR},
		},
	}
	if r.cfg.Agent {
		pol.Spec.Agent = v1.AgentSpec{Enabled: true, Approval: "Auto"}
	}
	return c.Create(ctx, pol)
}

// reconcile runs the policy controller until each workload has a TideWorkload whose
// Conflict condition is set, and returns the TideWorkloads by workload name.
func (r *run) reconcile(
	ctx context.Context, rec *controller.TidePolicyReconciler,
) (map[string]types.NamespacedName, error) {
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: r.ns, Name: policyName}}
	out := map[string]types.NamespacedName{}
	for range reconcileMax {
		if _, err := rec.Reconcile(ctx, req); err != nil {
			return nil, err
		}
		var list v1.TideWorkloadList
		if err := rec.List(ctx, &list, client.InNamespace(r.ns)); err != nil {
			return nil, err
		}
		for i := range list.Items {
			tw := &list.Items[i]
			if meta.FindStatusCondition(tw.Status.Conditions, v1.CondConflict) != nil {
				out[tw.Spec.TargetRef.Name] = client.ObjectKeyFromObject(tw)
			}
		}
		if len(out) == len(r.sc.Workloads) {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return nil, fmt.Errorf("harness: policy controller did not create %d TideWorkloads", len(r.sc.Workloads))
}

// cleanup removes the run's policy and TideWorkloads so later runs' engines never see them.
func (r *run) cleanup(rec *controller.TidePolicyReconciler) {
	ctx := context.Background()
	pol := &v1.TidePolicy{ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: r.ns}}
	_ = rec.Delete(ctx, pol)
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pol)}
	for range 3 {
		_, _ = rec.Reconcile(ctx, req)
	}
	var list v1.TideWorkloadList
	if rec.List(ctx, &list, client.InNamespace(r.ns)) == nil {
		for i := range list.Items {
			tw := &list.Items[i]
			if len(tw.Finalizers) > 0 {
				tw.Finalizers = nil
				_ = rec.Update(ctx, tw)
			}
			_ = rec.Delete(ctx, tw)
		}
	}
}

func (r *run) publishNote(ctx context.Context, c client.Client, text string) error {
	pol := &v1.TidePolicy{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: r.ns, Name: policyName}, pol); err != nil {
		return err
	}
	pol.Spec.Agent.Context.Notes = text
	return c.Update(ctx, pol)
}

func (r *run) runTidewatch(ctx context.Context, opts Options, repeat int) error {
	c, sch, err := newClient(opts.EnvtestAssets)
	if err != nil {
		return err
	}
	if err := r.objects(ctx, c); err != nil {
		return fmt.Errorf("harness: creating objects: %w", err)
	}
	rec := &controller.TidePolicyReconciler{Client: c, Scheme: sch}
	defer r.cleanup(rec)
	tws, err := r.reconcile(ctx, rec)
	if err != nil {
		return err
	}
	uids := map[string]types.UID{}
	for w, key := range tws {
		var tw v1.TideWorkload
		if err := c.Get(ctx, key, &tw); err != nil {
			return err
		}
		uids[w] = tw.UID
	}

	sim := promsim.New(r.sc, r.clk, r.sc.Epoch, nil)
	store := overrides.NewStore()
	eo := engine.Options{
		Clock: r.clk, PromFactory: sim.Factory(), Scale: r.cl, Overrides: store, APIReader: c,
	}
	sink := &lateSink{}
	if r.cfg.Agent {
		eo.Triggers = sink
	}
	eng := engine.New(c, eo)

	var runner *agent.Runner
	var counter *countingProvider
	var tw *transcript.Writer
	if r.cfg.Agent {
		counter = &countingProvider{Provider: r.cfg.Provider}
		var investigations atomic.Int64
		defer func() { r.res.Investigations = int(investigations.Load()) }()
		ro := agent.RunnerOptions{
			Workers: 1, Clock: r.clk, Provider: counter, PromFactory: sim.Factory(),
			Observer: func(incident string) func(llm.Turn) {
				investigations.Add(1)
				if tw == nil {
					return nil
				}
				return tw.Observer(incident)
			},
		}
		if opts.TranscriptDir != "" {
			if err := os.MkdirAll(opts.TranscriptDir, 0o750); err != nil {
				return err
			}
			path := filepath.Join(opts.TranscriptDir, fmt.Sprintf("%s-%s-%d.jsonl", r.sc.Name, r.res.Config, repeat))
			f, err := os.Create(path) //nolint:gosec // harness output path
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()
			tw, r.res.TranscriptPath = transcript.NewWriter(f), path
		}
		runner = agent.NewRunner(c, c, eng, store, &record.FakeRecorder{}, ro)
		sink.r.Store(runner)
		defer func() {
			r.res.LLMCalls = int(counter.calls.Load())
			r.res.InputTokens, r.res.OutputTokens = int(counter.in.Load()), int(counter.out.Load())
		}()
	}

	notes := r.sc.Notes()
	for t := r.sc.StartTime(); t.Before(r.sc.EndTime()); t = t.Add(r.sc.Step) {
		if err := ctx.Err(); err != nil {
			return err
		}
		// 1. Publish the newest note due; triggers see it at the next engine.Step.
		due := -1
		for i, n := range notes {
			if !n.PublishAt.After(t) {
				due = i
			}
		}
		if due >= 0 {
			text := notes[due].Text
			notes = notes[due+1:]
			if err := r.publishNote(ctx, c, text); err != nil {
				return fmt.Errorf("harness: publishing note: %w", err)
			}
		}
		// 2. The engine evaluates the slot ending at t.
		eng.Step(ctx)
		evaluated, err := r.checkEvaluated(ctx, c, eng, tws, uids, t)
		if err != nil {
			return err
		}
		// 3. Agent: incident loop, then investigations, with the clock frozen.
		if runner != nil {
			runner.RunOnce(ctx)
			runner.RunPending(ctx)
		}
		// 4+5. Advance one step and account the slot; then account the agent's lift.
		r.advance(t)
		r.accountLift(evaluated)
	}
	if tw != nil {
		return tw.Err()
	}
	return nil
}

// Reasons reported in Result.SkippedReasons besides the Forecasting condition's own.
const (
	skipNoConflict   = "no_conflict_condition"
	skipNotEvaluated = "not_evaluated"
)

// checkEvaluated verifies, after engine.Step at t, that the engine evaluated every
// workload this step, and returns the TideWorkloads that were. The signal is the engine
// Snapshot, which is saved only on a successful evaluation, and whose Now is the step it
// evaluated. (TideWorkload status Decisions[0].Time is not usable: the engine only adds a
// decision when it differs from the previous one, so its time is stale in steady state.)
// A workload that was not evaluated is counted in SkippedSteps with the reason taken from
// its Forecasting condition (metrics_unavailable, insufficient_data, no_data, ...), a
// missing Conflict condition, or "not_evaluated".
func (r *run) checkEvaluated(
	ctx context.Context, c client.Client, eng *engine.Engine,
	tws map[string]types.NamespacedName, uids map[string]types.UID, t time.Time,
) (map[string]v1.TideWorkload, error) {
	ok := map[string]v1.TideWorkload{}
	for _, w := range r.sc.Workloads {
		var tw v1.TideWorkload
		if err := c.Get(ctx, tws[w.Name], &tw); err != nil {
			return nil, err
		}
		reason := ""
		switch sn, have := eng.Snapshot(uids[w.Name]); {
		case meta.FindStatusCondition(tw.Status.Conditions, v1.CondConflict) == nil:
			reason = skipNoConflict
		case !have || !sn.Now.Equal(t.Truncate(r.sc.Step)):
			reason = skipNotEvaluated
			if f := meta.FindStatusCondition(tw.Status.Conditions, v1.CondForecasting); f != nil &&
				f.Status == metav1.ConditionFalse {
				reason = f.Reason
			}
		}
		if reason != "" {
			r.res.SkippedSteps++
			if r.res.SkippedReasons == nil {
				r.res.SkippedReasons = map[string]int{}
			}
			r.res.SkippedReasons[reason]++
			continue
		}
		ok[w.Name] = tw
	}
	return ok, nil
}

// accountLift records the agent's lift and safety violations for the step that began at t,
// for the workloads the engine evaluated this step.
func (r *run) accountLift(evaluated map[string]v1.TideWorkload) {
	for _, w := range r.sc.Workloads {
		if tw, ok := evaluated[w.Name]; ok {
			r.cl.AccountLift(r.sc.Step, cluster.Key{Namespace: r.ns, Name: w.Name},
				tw.Status.BaselineReplicas, tw.Status.DesiredReplicas)
		}
	}
}
