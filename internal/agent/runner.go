package agent

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/episodes"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/prom"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

// +kubebuilder:rbac:groups=scheduling.k8s.io,resources=priorityclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get,namespace=system

const (
	triggerBuffer  = 1024
	runnerTick     = time.Second
	maxQueueWait   = 5 * time.Minute
	stormWindow    = 2 * time.Minute
	breakerTrips   = 5
	breakerOpen    = 2 * time.Minute
	finderWindow   = 30 * time.Minute
	defaultWorkers = 4
	defaultBudget  = 200000
	unknownTTR     = time.Minute
	resultNoPolicy = "skipped_policy_unavailable"
	resultDisabled = "skipped_agent_disabled"
	resultNoLLM    = "skipped_no_provider"
)

// RunnerOptions configures a Runner.
type RunnerOptions struct {
	Workers            int
	TokenBudgetPerHour int
	Provider           llm.Provider // nil disables the agent: incidents and episodes are still recorded
	WebhookURL         string
	Clock              clock.Clock
}

type recentTrigger struct {
	at     time.Time
	policy v1.PolicyRef
}

// Runner is the leader-only runnable that turns engine triggers into incidents,
// investigations and recorded episodes.
type Runner struct {
	c        client.Client
	rd       client.Reader
	eng      *engine.Engine
	store    *overrides.Store
	clk      clock.Clock
	workers  int
	provider llm.Provider

	in       chan signal.Trigger
	mgr      *Manager
	queue    *Queue
	inv      *Investigator
	rec      *episodes.Recorder
	notifier Notifier

	recent  []recentTrigger // triggers within the storm window; RunOnce only
	inStorm bool            // RunOnce only
}

var _ engine.TriggerSink = (*Runner)(nil)

// NewRunner wires the agent pipeline. c is the cached client, r the uncached reader.
func NewRunner(
	c client.Client, r client.Reader, eng *engine.Engine, store *overrides.Store,
	rec record.EventRecorder, o RunnerOptions,
) *Runner {
	registerAgentMetrics()
	if o.Clock == nil {
		o.Clock = clock.RealClock{}
	}
	if store != nil {
		store.SetClock(o.Clock.Now)
	}
	if o.Workers <= 0 {
		o.Workers = defaultWorkers
	}
	if o.TokenBudgetPerHour <= 0 {
		o.TokenBudgetPerHour = defaultBudget
	}
	run := &Runner{
		c: c, rd: r, eng: eng, store: store, clk: o.Clock, workers: o.Workers, provider: o.Provider,
		in:    make(chan signal.Trigger, triggerBuffer),
		queue: NewQueue(WithClock(o.Clock)),
		rec:   &episodes.Recorder{C: c, R: r, Clock: o.Clock},
	}
	corr := NewCorrelator(eng, o.Clock)
	run.mgr = NewManager(o.Clock, corr, Options{})
	run.notifier = NewNotifier(rec, o.WebhookURL, run.notifyOnAuto)
	run.inv = &Investigator{
		Provider: o.Provider, Store: store, Client: c, Reader: r, Engine: eng,
		Episodes: episodes.Finder(&episodes.Reader{R: r}, run.finderQuery),
		Notifier: run.notifier, Budget: NewBudget(o.TokenBudgetPerHour, o.Clock),
		Breaker: NewBreaker(breakerTrips, breakerOpen, o.Clock), Clock: o.Clock,
		Correlator: corr, PromFactory: prom.New,
	}
	return run
}

// NeedLeaderElection makes the manager run the agent on the leader only.
func (r *Runner) NeedLeaderElection() bool { return true }

// Submit queues a trigger for the runner. It never blocks: when the buffer is
// full the trigger is dropped and counted. It is safe for concurrent use.
func (r *Runner) Submit(t signal.Trigger) {
	select {
	case r.in <- t:
	default:
		triggersDropped.Inc()
	}
}

// Start runs the incident loop and the investigation workers until ctx is done.
func (r *Runner) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	for range r.workers {
		wg.Go(func() { r.worker(ctx) })
	}
	for {
		r.RunOnce(ctx)
		t := r.clk.NewTimer(runnerTick)
		select {
		case <-ctx.Done():
			t.Stop()
			wg.Wait()
			return nil
		case <-t.C():
		}
	}
}

// RunOnce is one pass of the incident loop: drain triggers, close quiet
// incidents, queue ready ones and report the ones the queue dropped. It must
// not be called concurrently with itself.
func (r *Runner) RunOnce(ctx context.Context) {
	r.announceResolutions(ctx)
	r.drain(ctx)
	now := r.clk.Now()
	for _, inc := range r.mgr.Tick(now) {
		r.recordEpisode(ctx, inc)
	}
	if r.inStorm && !r.mgr.InStorm() {
		r.inStorm = false
		stormGauge.Set(0)
	}
	for _, inc := range r.mgr.TakeReady() {
		r.enqueue(ctx, inc, now)
	}
	for _, inc := range r.queue.Dropped() {
		r.mgr.SetStatus(inc.ID, StatusDecided)
		r.notifier.Skipped(ctx, inc, SkipTooLate)
	}
	queueDepth.Set(float64(r.queue.Len()))
}

// announceResolutions reports the approvals, rejections, expiries and
// supersessions the store queued since the last pass. Approvals also get the
// Applied notification a human-approved override has not had yet.
func (r *Runner) announceResolutions(ctx context.Context) {
	for _, res := range r.store.TakeResolutions() {
		sn, ok := r.eng.Snapshot(res.UID)
		if !ok { // the workload is gone: nothing to tell
			continue
		}
		w := workloadKey{ns: sn.TW.Namespace, name: sn.TW.Name, uid: res.UID}
		var verdict v1.Verdict
		switch res.Verdict {
		case overrides.ResApproved:
			verdict = v1.VerdictApplied
		case overrides.ResRejected:
			verdict = v1.VerdictRejected
		case overrides.ResSuperseded:
			verdict = v1.VerdictSuperseded
		case overrides.ResEnded:
			verdict = verdictEnded
		default:
			verdict = v1.VerdictExpired
		}
		r.notifier.Resolved(ctx, res.IncidentID, w, verdict, res.By)
		if res.Verdict != overrides.ResApproved {
			continue
		}
		ovs, _, _ := r.store.Snapshot(res.UID)
		for _, o := range ovs {
			if o.IncidentID != res.IncidentID {
				continue
			}
			inc := &Incident{ID: res.IncidentID, Policy: sn.Policy, Members: map[types.UID]*Member{
				res.UID: {Workload: sn.TW, UID: res.UID},
			}}
			r.notifier.Applied(ctx, inc, map[types.UID]v1.Override{res.UID: o}, false)
		}
	}
}

func (r *Runner) drain(ctx context.Context) {
	for {
		select {
		case t := <-r.in:
			now := r.clk.Now()
			r.trackRecent(t, now)
			if _, started := r.mgr.Add(t); started {
				r.inStorm = true
				r.stormStarted(ctx)
			}
		default:
			return
		}
	}
}

func (r *Runner) trackRecent(t signal.Trigger, now time.Time) {
	r.recent = append(r.recent, recentTrigger{at: now, policy: t.Policy})
	cut := 0
	for cut < len(r.recent) && now.Sub(r.recent[cut].at) > stormWindow {
		cut++
	}
	r.recent = r.recent[cut:]
}

// stormStarted notifies about a new storm: per policy when every trigger of
// the storm window came from one policy, else across all policies.
func (r *Runner) stormStarted(ctx context.Context) {
	var pol *v1.PolicyRef
	if len(r.recent) > 0 {
		first := r.recent[0].policy
		if slices.IndexFunc(r.recent, func(t recentTrigger) bool { return t.policy != first }) < 0 {
			pol = &first
		}
	}
	r.notifier.Storm(ctx, pol, len(r.recent))
}

func (r *Runner) enqueue(ctx context.Context, inc *Incident, now time.Time) {
	var all []signal.Trigger
	var ttr time.Duration
	prio := 1
	for _, uid := range sortedUIDs(inc.Members) {
		m := inc.Members[uid]
		all = append(all, m.Triggers...)
		prio = max(prio, r.priority(ctx, m))
		if sn, ok := r.eng.Snapshot(uid); ok {
			ttr = max(ttr, sn.TimeToReady)
		}
	}
	if ttr == 0 {
		ttr = unknownTTR
	}
	urgency, ttb := Urgency(all)
	deadline, ok := Deadline(now, ttb, ttr, maxQueueWait)
	if !ok {
		r.mgr.SetStatus(inc.ID, StatusDecided)
		r.notifier.Skipped(ctx, inc, SkipTooLate)
		return
	}
	r.mgr.SetStatus(inc.ID, StatusInvestigating)
	r.queue.Push(inc, urgency*float64(prio), deadline)
}

// priority resolves a member's 1..10 priority from its target's annotation and PriorityClass.
func (r *Runner) priority(ctx context.Context, m *Member) int {
	var tw v1.TideWorkload
	if err := r.c.Get(ctx, client.ObjectKey{Namespace: m.Workload.Namespace, Name: m.Workload.Name}, &tw); err != nil {
		return PriorityOf(nil, "")
	}
	key := client.ObjectKey{Namespace: m.Workload.Namespace, Name: tw.Spec.TargetRef.Name}
	var anno, class string
	switch tw.Spec.TargetRef.Kind {
	case kindDeployment:
		var d appsv1.Deployment
		if err := r.c.Get(ctx, key, &d); err == nil {
			anno, class = d.Annotations[v1.AnnoPriority], d.Spec.Template.Spec.PriorityClassName
		}
	case kindStatefulSet:
		var d appsv1.StatefulSet
		if err := r.c.Get(ctx, key, &d); err == nil {
			anno, class = d.Annotations[v1.AnnoPriority], d.Spec.Template.Spec.PriorityClassName
		}
	}
	var pp *int32
	if class != "" {
		var pc schedulingv1.PriorityClass
		if err := r.c.Get(ctx, client.ObjectKey{Name: class}, &pc); err == nil {
			pp = &pc.Value
		}
	}
	return PriorityOf(pp, anno)
}

func (r *Runner) worker(ctx context.Context) {
	for {
		inc, deadline, ok := r.queue.Pop(ctx)
		if !ok {
			return
		}
		r.process(ctx, inc, deadline)
		queueDepth.Set(float64(r.queue.Len()))
	}
}

// policyRefOf returns the policy that governs inc; a storm uses its lead's.
func (r *Runner) policyRefOf(inc *Incident) (v1.PolicyRef, bool) {
	if !inc.Storm && inc.Policy != (v1.PolicyRef{}) {
		return inc.Policy, true
	}
	if sn, ok := r.eng.Snapshot(inc.Lead); ok {
		return sn.Policy, true
	}
	for _, uid := range sortedUIDs(inc.Members) {
		if sn, ok := r.eng.Snapshot(uid); ok {
			return sn.Policy, true
		}
	}
	return v1.PolicyRef{}, false
}

// process investigates one popped incident, or skips it silently (metric only)
// when the agent is off for its policy or no provider is configured.
func (r *Runner) process(ctx context.Context, inc *Incident, deadline time.Time) {
	defer r.mgr.SetStatus(inc.ID, StatusDecided)
	skip := func(result string) { investigationsTotal.WithLabelValues(triggerLabel(inc), result).Inc() }
	if r.provider == nil {
		skip(resultNoLLM)
		return
	}
	ref, ok := r.policyRefOf(inc)
	var pol v1.TidePolicy
	if !ok || r.c.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &pol) != nil {
		skip(resultNoPolicy)
		return
	}
	if !pol.Spec.Agent.Enabled {
		skip(resultDisabled)
		return
	}
	r.inv.Investigate(ctx, inc, deadline)
}

func (r *Runner) notifyOnAuto(ref v1.PolicyRef) bool {
	var pol v1.TidePolicy
	if err := r.c.Get(context.Background(), client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, &pol); err != nil {
		return false
	}
	return pol.Spec.Agent.NotifyOnAuto
}

// finderQuery describes the current situation of w for similar-episode retrieval.
func (r *Runner) finderQuery(w tools.WorkloadRef) (string, []string, episodes.Query, bool) {
	ctx := context.Background()
	var tw v1.TideWorkload
	if err := r.c.Get(ctx, client.ObjectKey{Namespace: w.Namespace, Name: w.Name}, &tw); err != nil {
		return "", nil, episodes.Query{}, false
	}
	pol := tw.Spec.PolicyRef
	now := r.clk.Now()
	q := episodes.Query{Weekday: now.UTC().Weekday(), Hour: now.UTC().Hour()}
	if series, ok := r.eng.Series(tw.UID); ok {
		q.Shape = episodes.Shape(series, now.Add(-finderWindow), now)
	}
	return pol.Namespace + "." + pol.Name, []string{pol.Namespace}, q, true
}

// recordEpisode stores a closed incident as an episode when it is notable.
func (r *Runner) recordEpisode(ctx context.Context, inc *Incident) {
	view, ok := r.view(inc)
	if !ok || !r.rec.Notable(view) {
		return
	}
	if err := r.rec.Record(ctx, r.rec.Build(view)); err != nil {
		logf.FromContext(ctx).WithName("agent").Error(err, "Recording episode", "incident", inc.ID)
	}
}

// view converts a closed incident into the recorder's input.
func (r *Runner) view(inc *Incident) (*episodes.IncidentView, bool) {
	ref, ok := r.policyRefOf(inc)
	lead := inc.Members[inc.Lead]
	if !ok || lead == nil {
		return nil, false
	}
	series, _ := r.eng.Series(inc.Lead)
	v := &episodes.IncidentView{
		ID: inc.ID, Policy: ref, Lead: lead.Workload.Name, LeadNamespace: lead.Workload.Namespace, Start: inc.Opened, End: inc.LastTrigger,
		LeadSeries: series,
	}
	if v.End.Before(v.Start) {
		v.End = v.Start
	}
	v.Peak, v.PeakRatio = leadPeak(series, v.Start, v.End)
	step := series.Step
	if step <= 0 {
		step = time.Minute
	}
	var notes []string
	for _, uid := range sortedUIDs(inc.Members) {
		m := inc.Members[uid]
		v.Members = append(v.Members, v1.EpisodeMember{
			Workload: m.Workload.Name, LagSeconds: int32(min(inc.Lags[uid].Seconds(), math.MaxInt32)), //nolint:gosec // bounded
		})
		var first, last time.Time
		for _, t := range m.Triggers {
			switch t.Type {
			case signal.ContextDue:
				notes = append(notes, t.Detail)
			case signal.WorkloadChange:
				v.Deploy = v.Deploy || strings.Contains(t.Detail, "template changed")
			case signal.Anomaly:
				if first.IsZero() || t.At.Before(first) {
					first = t.At
				}
				last = maxTime(last, t.At)
			}
		}
		if !first.IsZero() { // it had already lasted three steps when it first fired
			v.LongestAnomaly = max(v.LongestAnomaly, last.Sub(first)+3*step)
		}
		r.applyOutcomes(v, inc.ID, uid)
	}
	v.OpsNote = strings.Join(notes, "; ")
	return v, true
}

// applyOutcomes folds the store's recorded outcomes of incident id for one member into v.
func (r *Runner) applyOutcomes(v *episodes.IncidentView, id string, uid types.UID) {
	_, _, outs := r.store.Snapshot(uid)
	sn, haveSnap := r.eng.Snapshot(uid)
	for _, o := range outs {
		if o.IncidentID != id {
			continue
		}
		if o.Verdict == v1.VerdictApplied || o.Verdict == v1.VerdictClamped {
			v.ProposalApplied = true
			if haveSnap && sn.Cfg.MinReplicas > 0 {
				v.MaxReplicaChange = max(v.MaxReplicaChange, float64(o.MinReplicas)/float64(sn.Cfg.MinReplicas)-1)
			}
		}
		if v.Action == "" || o.Verdict == v1.VerdictApplied {
			v.Action = fmt.Sprintf("replica floor %d (%s)", o.MinReplicas, o.Verdict)
		}
		if o.Outcome != nil && v.Outcome == nil {
			v.Outcome = o.Outcome.DeepCopy()
		}
	}
}

// leadPeak returns the time and ratio (to the series' mean) of the highest load in [from, to].
func leadPeak(s forecast.Series, from, to time.Time) (time.Time, float64) {
	var sum float64
	var n int
	peak, at := math.Inf(-1), from
	for _, p := range s.Points {
		if math.IsNaN(p.V) {
			continue
		}
		sum += p.V
		n++
		if !p.T.Before(from) && !p.T.After(to) && p.V > peak {
			peak, at = p.V, p.T
		}
	}
	if n == 0 || sum <= 0 || math.IsInf(peak, 0) {
		return at, 1
	}
	return at, max(peak/(sum/float64(n)), 0)
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
