package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/llm"
	"github.com/aayushsss1/tidewatch/pkg/llm/llmtest"
	"github.com/aayushsss1/tidewatch/pkg/prom"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

const (
	kOverrides    = "overrides"
	scriptedName  = "scripted"
	scriptedModel = "scripted-1"
	testNS        = "default"
	testInc       = "inc-1"
	kConf         = "confidence"
	kReason       = "reason"
	appsAPI       = "apps/v1"
)

var (
	envOnce   sync.Once
	testEnv   *envtest.Environment
	k8sClient client.Client
	envErr    error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if testEnv != nil {
		_ = testEnv.Stop()
	}
	os.Exit(code)
}

// cluster starts envtest on first use so the pure unit tests stay fast.
func cluster(t *testing.T) client.Client {
	t.Helper()
	envOnce.Do(func() {
		if err := v1.AddToScheme(scheme.Scheme); err != nil {
			envErr = err
			return
		}
		testEnv = &envtest.Environment{
			CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
			ErrorIfCRDPathMissing: true,
		}
		if entries, err := os.ReadDir(filepath.Join("..", "..", "bin", "k8s")); err == nil {
			for _, e := range entries {
				if e.IsDir() && os.Getenv("KUBEBUILDER_ASSETS") == "" {
					testEnv.BinaryAssetsDirectory = filepath.Join("..", "..", "bin", "k8s", e.Name())
					break
				}
			}
		}
		cfg, err := testEnv.Start()
		if err != nil {
			envErr = err
			return
		}
		k8sClient, envErr = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	})
	if envErr != nil {
		t.Fatalf("envtest: %v", envErr)
	}
	return k8sClient
}

type loadQuerier struct{}

func (loadQuerier) Ready(context.Context) error { return nil }
func (loadQuerier) RangeSeries(_ context.Context, _ string, start, end time.Time, step time.Duration) (forecast.Series, error) {
	s := forecast.Series{Step: step}
	for t := start; !t.After(end); t = t.Add(step) {
		s.Points = append(s.Points, forecast.Point{T: t, V: 50 + 40*math.Sin(2*math.Pi*float64(t.Unix())/3600)})
	}
	return s, nil
}

// recNotifier records notifications.
type recNotifier struct {
	mu       sync.Mutex
	pending  []map[types.UID]v1.PendingApproval
	applied  []bool
	skipped  []string
	resolved []string
}

func (r *recNotifier) Pending(_ context.Context, _ *Incident, p map[types.UID]v1.PendingApproval) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending = append(r.pending, p)
}

func (r *recNotifier) Applied(_ context.Context, _ *Incident, _ map[types.UID]v1.Override, auto bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, auto)
}
func (r *recNotifier) Resolved(_ context.Context, id string, _ workloadKey, v v1.Verdict, _ string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.resolved = append(r.resolved, id+":"+string(v))
}
func (r *recNotifier) Storm(context.Context, *v1.PolicyRef, int) {}
func (r *recNotifier) Skipped(_ context.Context, _ *Incident, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.skipped = append(r.skipped, reason)
}

type fixture struct {
	t     *testing.T
	ctx   context.Context
	c     client.Client
	clk   *clocktesting.FakeClock
	store *overrides.Store
	eng   *engine.Engine
	pol   string
	uids  map[string]types.UID
	notif *recNotifier
	prov  *llmtest.Scripted
	iv    *Investigator
}

func newFixture(t *testing.T, mode v1.PolicyMode, approval string, workloads ...string) *fixture {
	t.Helper()
	c := cluster(t)
	clk := clocktesting.NewFakeClock(time.Date(2026, 1, 1, 0, 45, 0, 0, time.UTC))
	f := &fixture{
		t: t, ctx: context.Background(), c: c, clk: clk, store: overrides.NewStore(),
		pol: fmt.Sprintf("pol-%d", time.Now().UnixNano()), uids: map[string]types.UID{},
		notif: &recNotifier{}, prov: &llmtest.Scripted{},
	}
	pol := &v1.TidePolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: f.pol},
		Spec: v1.TidePolicySpec{
			Mode:       mode,
			Prometheus: v1.PrometheusSpec{URL: "http://fake"},
			Selector:   v1.WorkloadSelector{Kinds: []v1.TargetKind{{APIVersion: appsAPI, Kind: kindDeployment}}},
			Load: v1.LoadSpec{
				DefaultProfile: "rps",
				Profiles:       map[string]v1.LoadProfile{"rps": {Query: "up", TargetPerReplica: "10"}},
			},
			Forecasting: v1.ForecastingSpec{
				Horizon: metav1.Duration{Duration: 30 * time.Minute}, History: metav1.Duration{Duration: 3 * time.Hour},
				Step: metav1.Duration{Duration: time.Minute}, Seasonality: metav1.Duration{Duration: time.Hour},
			},
			Guardrails: v1.GuardrailsSpec{MaxReplicas: 20},
			Agent: v1.AgentSpec{
				Enabled: true, Approval: approval, MinConfidence: "0.6", MaxToolCalls: 8,
				ApprovalTimeout:      metav1.Duration{Duration: 10 * time.Minute},
				InvestigationTimeout: metav1.Duration{Duration: time.Minute},
			},
		},
	}
	if err := c.Create(f.ctx, pol); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Delete(f.ctx, pol) })
	for _, w := range workloads {
		f.addWorkload(w)
	}
	f.eng = engine.New(c, engine.Options{
		Clock: clk, Overrides: f.store, DefaultTimeToReady: 5 * time.Minute,
		PromFactory: func(string, time.Duration) (prom.Querier, error) { return loadQuerier{}, nil },
	})
	if len(workloads) > 0 {
		f.eng.Step(f.ctx)
		for _, w := range workloads {
			f.uids[w] = f.uidOf(w)
		}
	}
	f.iv = &Investigator{
		Provider: f.prov, Store: f.store, Client: c, Reader: c, Engine: f.eng, Notifier: f.notif,
		Budget: NewBudget(200000, clk), Breaker: NewBreaker(5, 2*time.Minute, clk), Clock: clk,
		PromFactory: func(string, time.Duration) (prom.Querier, error) { return loadQuerier{}, nil },
	}
	return f
}

func (f *fixture) uidOf(name string) types.UID {
	var tw v1.TideWorkload
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: name}, &tw); err != nil {
		f.t.Fatal(err)
	}
	return tw.UID
}

func (f *fixture) addWorkload(name string) {
	labels := map[string]string{"app": name}
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
			},
		},
	}
	tw := &v1.TideWorkload{
		ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
		Spec: v1.TideWorkloadSpec{
			PolicyRef: v1.PolicyRef{Namespace: testNS, Name: f.pol},
			TargetRef: v1.TargetRef{APIVersion: appsAPI, Kind: kindDeployment, Name: name},
		},
	}
	for _, o := range []client.Object{d, tw} {
		if err := f.c.Create(f.ctx, o); err != nil {
			f.t.Fatal(err)
		}
	}
	meta.SetStatusCondition(&tw.Status.Conditions, metav1.Condition{Type: v1.CondConflict, Status: metav1.ConditionFalse, Reason: "none"})
	if err := f.c.Status().Update(f.ctx, tw); err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() { _ = f.c.Delete(f.ctx, tw); _ = f.c.Delete(f.ctx, d) })
}

func (f *fixture) replicas(name string) int32 {
	var d appsv1.Deployment
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: name}, &d); err != nil {
		f.t.Fatal(err)
	}
	return *d.Spec.Replicas
}

func (f *fixture) incident(names ...string) *Incident {
	inc := &Incident{
		ID: testInc, Policy: v1.PolicyRef{Namespace: testNS, Name: f.pol}, Opened: f.clk.Now(),
		Members: map[types.UID]*Member{}, Lags: map[types.UID]time.Duration{}, Status: StatusInvestigating,
	}
	for i, n := range names {
		uid := f.uids[n]
		inc.Members[uid] = &Member{
			Workload: types.NamespacedName{Namespace: testNS, Name: n}, UID: uid,
			Triggers: []signal.Trigger{{Type: signal.ForecastBreach, Detail: "breach in 5m"}},
		}
		if i == 0 {
			inc.Lead = uid
		}
	}
	return inc
}

func (f *fixture) script(name string, input any, tokens int) {
	b, _ := json.Marshal(input)
	f.prov.Turns = append(f.prov.Turns, llm.TurnResponse{
		Message:     llm.Message{Role: llm.RoleAssistant, ToolCall: &llm.ToolCall{ID: "c1", Name: name, Input: b}},
		InputTokens: tokens, OutputTokens: tokens / 2,
	})
}

type ovIn struct {
	Workload    string `json:"workload"`
	MinReplicas int32  `json:"minReplicas"`
	End         string `json:"end"`
}

func (f *fixture) propose(conf float64, ovs ...ovIn) {
	end := f.clk.Now().Add(30 * time.Minute).Format(time.RFC3339)
	for i := range ovs {
		if ovs[i].End == "" {
			ovs[i].End = end
		}
	}
	f.script("propose_scaling", map[string]any{kOverrides: ovs, kReason: "payday surge", kConf: conf}, 100)
}

func (f *fixture) deadline() time.Time { return f.clk.Now().Add(time.Minute) }

func TestInvestigator_AutoModeBecomesOverrides(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-auto", "cart-auto")
	f.propose(0.9, ovIn{Workload: "frontend-auto", MinReplicas: 12}, ovIn{Workload: "cart-auto", MinReplicas: 8})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-auto", "cart-auto"), f.deadline())

	if res.Exit != llm.ExitTerminal || res.Verdict != v1.VerdictApplied || len(res.Overrides) != 2 {
		t.Fatalf("unexpected result: %+v", res)
	}
	for w, want := range map[string]int32{"frontend-auto": 12, "cart-auto": 8} {
		if got, o := f.store.Active(f.uids[w], f.clk.Now()); o == nil || got != want || o.IncidentID != testInc {
			t.Errorf("%s: active override = %d, %+v; want %d", w, got, o, want)
		}
	}
	if len(f.notif.applied) != 1 || !f.notif.applied[0] {
		t.Errorf("want one auto Applied notification, got %v", f.notif.applied)
	}
	if got := f.iv.Budget.Remaining(); got != 200000-150 {
		t.Errorf("budget remaining = %d, want %d", got, 200000-150)
	}
	_, _, outs := f.store.Snapshot(f.uids["frontend-auto"])
	if len(outs) != 1 || outs[0].Verdict != v1.VerdictApplied || outs[0].MinReplicas != 12 || outs[0].IncidentID != testInc {
		t.Errorf("outcome = %+v", outs)
	}
}

func TestInvestigator_ManualModeBecomesPendingAndNotifies(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Manual", "frontend-man")
	f.propose(0.8, ovIn{Workload: "frontend-man", MinReplicas: 9})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-man"), f.deadline())

	if res.Verdict != v1.VerdictPending {
		t.Fatalf("verdict = %q, reasons %v", res.Verdict, res.Reasons)
	}
	uid := f.uids["frontend-man"]
	ovs, pend, outs := f.store.Snapshot(uid)
	if len(ovs) != 0 || len(pend) != 1 {
		t.Fatalf("overrides=%d pending=%d, want 0 and 1", len(ovs), len(pend))
	}
	// The window (30m) outlasts the 10m approval timeout, which is therefore the bound.
	if want := f.clk.Now().Add(10 * time.Minute); !pend[0].Expires.Time.Equal(want) {
		t.Errorf("expires = %v, want %v", pend[0].Expires, want)
	}
	if pend[0].Override.MinReplicas != 9 || pend[0].Confidence != "0.80" || pend[0].Reason != "payday surge" {
		t.Errorf("pending = %+v", pend[0])
	}
	if len(outs) != 1 || outs[0].Verdict != v1.VerdictPending {
		t.Errorf("outcomes = %+v", outs)
	}
	if len(f.notif.pending) != 1 || len(f.notif.pending[0]) != 1 {
		t.Errorf("pending notifications = %v", f.notif.pending)
	}
}

func TestInvestigator_ManualExpiryCappedByTimeout(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Manual", "frontend-late-start")
	// Starts in 1h, so the 10m approval timeout is the earlier bound.
	start := f.clk.Now().Add(time.Hour)
	f.script("propose_scaling", map[string]any{
		kReason: "r", kConf: 0.9,
		kOverrides: []map[string]any{{
			"workload": "frontend-late-start", "minReplicas": 5,
			"start": start.Format(time.RFC3339), "end": start.Add(30 * time.Minute).Format(time.RFC3339),
		}},
	}, 100)
	f.iv.Investigate(f.ctx, f.incident("frontend-late-start"), f.deadline())
	_, pend, _ := f.store.Snapshot(f.uids["frontend-late-start"])
	if len(pend) != 1 || !pend[0].Expires.Time.Equal(f.clk.Now().Add(10*time.Minute)) {
		t.Fatalf("pending = %+v", pend)
	}
}

func TestInvestigator_ManualExpiryCappedByWindowEnd(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Manual", "frontend-short")
	end := f.clk.Now().Add(5 * time.Minute) // closes before the 10m approval timeout
	f.script("propose_scaling", map[string]any{
		kReason: "r", kConf: 0.9,
		kOverrides: []map[string]any{{
			"workload": "frontend-short", "minReplicas": 5, "end": end.Format(time.RFC3339),
		}},
	}, 100)
	f.iv.Investigate(f.ctx, f.incident("frontend-short"), f.deadline())
	_, pend, _ := f.store.Snapshot(f.uids["frontend-short"])
	if len(pend) != 1 || !pend[0].Expires.Time.Equal(end) {
		t.Fatalf("pending = %+v, want expiry at the window end %v", pend, end)
	}
}

func TestInvestigator_ShadowModeRecordsOnly(t *testing.T) {
	f := newFixture(t, v1.ModeShadow, "Manual", "frontend-sh")
	f.propose(0.9, ovIn{Workload: "frontend-sh", MinReplicas: 7})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-sh"), f.deadline())

	if res.Verdict != v1.VerdictShadow {
		t.Fatalf("verdict = %q", res.Verdict)
	}
	ovs, pend, outs := f.store.Snapshot(f.uids["frontend-sh"])
	if len(ovs) != 0 || len(pend) != 0 {
		t.Errorf("shadow wrote overrides=%d pending=%d", len(ovs), len(pend))
	}
	if len(outs) != 1 || outs[0].Verdict != v1.VerdictShadow || outs[0].MinReplicas != 7 {
		t.Errorf("outcomes = %+v", outs)
	}
	if len(f.notif.pending) != 0 || len(f.notif.applied) != 0 {
		t.Error("shadow must not send approval or applied notifications")
	}
}

func TestInvestigator_DropsOutOfScopeAndClampsMax(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-clamp")
	f.propose(0.9, ovIn{Workload: "checkout-deploy", MinReplicas: 5}, ovIn{Workload: "frontend-clamp", MinReplicas: 1000})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-clamp"), f.deadline())

	if len(res.Overrides) != 1 {
		t.Fatalf("overrides = %+v", res.Overrides)
	}
	if got, _ := f.store.Active(f.uids["frontend-clamp"], f.clk.Now()); got != 20 {
		t.Errorf("clamped minReplicas = %d, want policy max 20", got)
	}
	for _, r := range []string{"out_of_scope", "clamped_max"} {
		if !strings.Contains(strings.Join(res.Reasons, ","), r) {
			t.Errorf("reasons %v missing %s", res.Reasons, r)
		}
	}
}

func TestInvestigator_RejectsLowConfidence(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-low")
	f.propose(0.3, ovIn{Workload: "frontend-low", MinReplicas: 9})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-low"), f.deadline())

	if res.Verdict != v1.VerdictRejected || len(res.Overrides) != 0 || res.Reasons[0] != "low_confidence" {
		t.Fatalf("result = %+v", res)
	}
	ovs, pend, outs := f.store.Snapshot(f.uids["frontend-low"])
	if len(ovs)+len(pend) != 0 || len(outs) != 1 || outs[0].Verdict != v1.VerdictRejected {
		t.Errorf("store: ovs=%d pend=%d outs=%+v", len(ovs), len(pend), outs)
	}
}

func TestInvestigator_NoActionRoutesNothing(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-na")
	f.script("no_action", map[string]any{kReason: "weak evidence", kConf: 0.9}, 100)
	res := f.iv.Investigate(f.ctx, f.incident("frontend-na"), f.deadline())
	if res.Exit != llm.ExitTerminal || res.Verdict != "" || len(res.Overrides) != 0 || res.Reasons[0] != reasonNoAction {
		t.Fatalf("result = %+v", res)
	}
	if f.iv.Breaker.Open() {
		t.Error("breaker must stay closed")
	}
}

func TestInvestigator_SkipsWhenBreakerOpen(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-br")
	for range 5 {
		if !f.iv.Breaker.Allow() {
			t.Fatal("breaker opened early")
		}
		f.iv.Breaker.Failure()
	}
	f.propose(0.9, ovIn{Workload: "frontend-br", MinReplicas: 9})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-br"), f.deadline())

	if res.Skipped != "provider_unavailable" {
		t.Fatalf("skipped = %q", res.Skipped)
	}
	if len(f.prov.Calls) != 0 {
		t.Error("provider must not be called while the breaker is open")
	}
	if len(f.notif.skipped) != 1 || f.notif.skipped[0] != "provider_unavailable" {
		t.Errorf("skipped notifications = %v", f.notif.skipped)
	}
	if ovs, _, _ := f.store.Snapshot(f.uids["frontend-br"]); len(ovs) != 0 {
		t.Error("no override expected")
	}
}

func TestInvestigator_BudgetSkipReleasesBreakerTrial(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-bud")
	for range 5 {
		f.iv.Breaker.Allow()
		f.iv.Breaker.Failure()
	}
	f.clk.Step(3 * time.Minute) // the next Allow is the half-open trial
	f.iv.Budget = NewBudget(100, f.clk)

	res := f.iv.Investigate(f.ctx, f.incident("frontend-bud"), f.clk.Now().Add(time.Minute))

	if res.Skipped != "budget_exhausted" || len(f.notif.skipped) != 1 {
		t.Fatalf("result = %+v, skipped = %v", res, f.notif.skipped)
	}
	if !f.iv.Breaker.Allow() {
		t.Fatal("a budget skip must release the half-open trial")
	}
}

func TestInvestigator_ProviderErrorOpensBreakerAfterThreshold(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-err")
	f.iv.Breaker = NewBreaker(1, time.Minute, f.clk)
	f.prov.Errs = []error{fmt.Errorf("boom")}

	res := f.iv.Investigate(f.ctx, f.incident("frontend-err"), f.deadline())

	if res.Exit != llm.ExitProviderError || !f.iv.Breaker.Open() {
		t.Fatalf("exit = %s, breaker open = %v", res.Exit, f.iv.Breaker.Open())
	}
}

func TestInvestigator_TooLateSkips(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-tl")
	res := f.iv.Investigate(f.ctx, f.incident("frontend-tl"), f.clk.Now())
	if res.Skipped != "too_late" || len(f.notif.skipped) != 1 {
		t.Fatalf("result = %+v", res)
	}
}

func TestInvestigator_DeadlineLeavesBaselineUnaffected(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-dl")
	f.addWorkload("late-dl") // first evaluated by the concurrent Step below
	f.propose(0.9, ovIn{Workload: "frontend-dl", MinReplicas: 9})
	f.prov.Delay = 5 * time.Second
	f.clk.Step(time.Minute)

	var wg sync.WaitGroup
	var res Result
	wg.Go(func() {
		res = f.iv.Investigate(f.ctx, f.incident("frontend-dl"), f.clk.Now().Add(200*time.Millisecond))
	})
	f.eng.Step(f.ctx)
	wg.Wait()

	if res.Exit != llm.ExitDeadline || res.Verdict != "" || len(res.Overrides) != 0 {
		t.Fatalf("result = %+v", res)
	}
	if ovs, pend, _ := f.store.Snapshot(f.uids["frontend-dl"]); len(ovs)+len(pend) != 0 {
		t.Error("deadline must leave no override")
	}
	if got := f.replicas("late-dl"); got <= 2 {
		t.Errorf("baseline did not scale: late-dl replicas = %d", got)
	}
	if !f.iv.Breaker.Allow() {
		t.Error("deadline must release, not fail, the breaker")
	}
}

func TestValidateProposal_Windows(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	inc := &Incident{ID: "i", Members: map[types.UID]*Member{
		"u1": {Workload: types.NamespacedName{Namespace: "ns", Name: "a"}, UID: "u1"},
	}}
	maxFor := func(types.UID) int32 { return 10 }
	run := func(po ...tools.ProposedOverride) (map[types.UID]v1.Override, []string) {
		return ValidateProposal(proposal(0.9, po...), inc, maxFor, 0.6, now)
	}

	out, reasons := run(tools.ProposedOverride{Workload: "a", MinReplicas: 3})
	o := out["u1"]
	if !o.Start.Time.Equal(now) || !o.End.Time.Equal(now.Add(30*time.Minute)) || len(reasons) != 1 || reasons[0] != reasonWindowClamp {
		t.Errorf("defaults: %+v %v", o, reasons)
	}
	out, reasons = run(tools.ProposedOverride{Workload: "ns/a", MinReplicas: 3, End: now.Add(5 * time.Hour)})
	if got := out["u1"].End.Time; !got.Equal(now.Add(2*time.Hour)) || reasons[0] != reasonWindowClamp {
		t.Errorf("clamp: end=%v %v", got, reasons)
	}
	out, reasons = run(tools.ProposedOverride{Workload: "a", MinReplicas: 3, End: now.Add(-time.Minute)})
	if len(out) != 0 || reasons[0] != "ended" {
		t.Errorf("ended: %v %v", out, reasons)
	}
	out, reasons = run(tools.ProposedOverride{Workload: "a", MinReplicas: 99, End: now.Add(time.Hour)})
	if out["u1"].MinReplicas != 10 || reasons[0] != "clamped_max" {
		t.Errorf("clamp max: %+v %v", out, reasons)
	}
	out, reasons = ValidateProposal(proposal(0.5, tools.ProposedOverride{Workload: "a", MinReplicas: 3, End: now.Add(time.Hour)}), inc, maxFor, 0.6, now)
	if len(out) != 0 || reasons[0] != "low_confidence" {
		t.Errorf("low confidence: %v %v", out, reasons)
	}
}

func TestBrief_BoundedAndMentionsTriggers(t *testing.T) {
	inc := &Incident{ID: "inc-9", Policy: v1.PolicyRef{Namespace: "ns", Name: "p"},
		Members: map[types.UID]*Member{}, Lags: map[types.UID]time.Duration{}}
	snaps := make([]engine.Snapshot, 0, 40)
	for i := range 40 {
		uid := types.UID(fmt.Sprintf("u%02d", i))
		name := fmt.Sprintf("workload-%02d", i)
		inc.Members[uid] = &Member{UID: uid, Workload: types.NamespacedName{Namespace: "ns", Name: name},
			Triggers: []signal.Trigger{{Type: signal.Anomaly, Detail: strings.Repeat("x", 300)}}}
		inc.Lags[uid] = time.Minute
		snaps = append(snaps, engine.Snapshot{TW: types.NamespacedName{Namespace: "ns", Name: name}, UID: uid, Replicas: 3})
	}
	got := Brief(inc, snaps)
	if len(got) > 1500 {
		t.Errorf("brief is %d bytes", len(got))
	}
	if !strings.Contains(got, "Anomaly") || !strings.Contains(got, "inc-9") {
		t.Errorf("brief missing trigger or incident id: %q", got[:200])
	}
	short := Brief(&Incident{ID: "i", Members: map[types.UID]*Member{"u": {UID: "u", Triggers: []signal.Trigger{{Type: signal.ForecastBreach, Detail: "d"}}}}},
		[]engine.Snapshot{{TW: types.NamespacedName{Name: "w"}, UID: "u", Replicas: 4}})
	if !strings.Contains(short, "ForecastBreach") || !strings.Contains(short, "replicas 4") {
		t.Errorf("short brief = %q", short)
	}
}

func proposal(conf float64, po ...tools.ProposedOverride) tools.Proposal {
	return tools.Proposal{Overrides: po, Reason: "r", Confidence: conf}
}

func TestInvestigator_AutoDecisionReachesStatus(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-dec")
	f.script("get_ops_notes", map[string]any{}, 10)
	f.propose(0.9, ovIn{Workload: "frontend-dec", MinReplicas: 12})

	res := f.iv.Investigate(f.ctx, f.incident("frontend-dec"), f.deadline())
	if res.Verdict != v1.VerdictApplied || res.Provider != scriptedName || res.Model != scriptedModel || res.PromptVersion != PromptVersion {
		t.Fatalf("result = %+v", res)
	}
	f.clk.Step(time.Minute)
	f.eng.Step(f.ctx)

	var tw v1.TideWorkload
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: "frontend-dec"}, &tw); err != nil {
		t.Fatal(err)
	}
	var got *v1.Decision
	for i := range tw.Status.Decisions {
		if tw.Status.Decisions[i].Source == v1.SourceAgent && tw.Status.Decisions[i].IncidentID == testInc &&
			tw.Status.Decisions[i].PromptVersion != "" { // the engine's own enforcement decision has none
			got = &tw.Status.Decisions[i]
			break
		}
	}
	if got == nil {
		t.Fatalf("no agent decision in status: %+v", tw.Status.Decisions)
	}
	if got.PromptVersion != PromptVersion || got.Provider != "scripted" || got.Model != "scripted-1" ||
		got.Verdict != v1.VerdictApplied || got.Desired != 12 || got.Final != 12 {
		t.Errorf("decision = %+v", got)
	}
	if len(got.Trace) != 1 || got.Trace[0].Tool != "get_ops_notes" {
		t.Errorf("trace = %+v", got.Trace)
	}
	if len(tw.Status.Decisions) > 5 {
		t.Errorf("decisions = %d, want at most 5", len(tw.Status.Decisions))
	}
	if again := f.store.TakeDecisions(f.uids["frontend-dec"]); len(again) != 0 {
		t.Errorf("decisions not drained: %+v", again)
	}
}

func TestInvestigator_EmptyVerdictWhenNothingStored(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-nostore")
	// Auto with an invalid stored window: AddOverride fails, so nothing is stored.
	inc := f.incident("frontend-nostore")
	uid := f.uids["frontend-nostore"]
	var pol v1.TidePolicy
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: f.pol}, &pol); err != nil {
		t.Fatal(err)
	}
	bad := map[types.UID]v1.Override{uid: {IncidentID: testInc}} // zero start: normalize rejects it
	got := f.iv.route(f.ctx, inc, &pol, tools.Proposal{Confidence: 0.9}, bad, decisionInfo{incident: testInc, now: f.clk.Now()})
	if got != "" {
		t.Errorf("auto verdict = %q, want empty", got)
	}
	pol.Spec.Agent.Approval = "Manual"
	if got := f.iv.route(f.ctx, inc, &pol, tools.Proposal{Confidence: 0.9}, bad, decisionInfo{incident: testInc, now: f.clk.Now()}); got != "" {
		t.Errorf("manual verdict = %q, want empty", got)
	}
	if d := f.store.TakeDecisions(uid); len(d) != 0 {
		t.Errorf("decisions recorded for nothing stored: %+v", d)
	}
}

func TestValidateProposal_InvalidWindowAndStartCap(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	inc := &Incident{ID: "i", Members: map[types.UID]*Member{
		"u1": {Workload: types.NamespacedName{Namespace: "ns", Name: "a"}, UID: "u1"},
	}}
	maxFor := func(types.UID) int32 { return 10 }
	// Start far ahead is capped to now+2h; an end before that cap is then invalid.
	out, reasons := ValidateProposal(proposal(0.9, tools.ProposedOverride{
		Workload: "a", MinReplicas: 3, Start: now.Add(10 * time.Hour), End: now.Add(11 * time.Hour)}), inc, maxFor, 0.6, now)
	if got := out["u1"].Start.Time; !got.Equal(now.Add(2*time.Hour)) || len(reasons) != 1 || reasons[0] != reasonWindowClamp {
		t.Errorf("start cap: start=%v reasons=%v", got, reasons)
	}
	out, reasons = ValidateProposal(proposal(0.9, tools.ProposedOverride{
		Workload: "a", MinReplicas: 3, Start: now.Add(10 * time.Hour), End: now.Add(time.Hour + time.Minute)}), inc, maxFor, 0.6, now)
	if len(out) != 0 || reasons[0] != reasonWindowClamp || reasons[len(reasons)-1] != "invalid_window" {
		t.Errorf("invalid window: %v %v", out, reasons)
	}
}

func TestInvestigator_SupersededPendingBecomesDecision(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Manual", "frontend-sup")
	f.propose(0.8, ovIn{Workload: "frontend-sup", MinReplicas: 9})
	f.iv.Investigate(f.ctx, f.incident("frontend-sup"), f.deadline())
	f.store.TakeDecisions(f.uids["frontend-sup"]) // the first proposal's own decision
	f.prov.Calls = nil
	f.propose(0.8, ovIn{Workload: "frontend-sup", MinReplicas: 11})
	f.prov.Turns = f.prov.Turns[1:] // replay only the second proposal
	f.iv.Investigate(f.ctx, f.incident("frontend-sup"), f.deadline())

	found := false
	for _, d := range f.store.TakeDecisions(f.uids["frontend-sup"]) {
		found = found || (d.Verdict == v1.VerdictSuperseded && d.IncidentID == testInc && d.Desired == 9)
	}
	if !found {
		t.Fatal("no superseded decision for the replaced pending approval")
	}
}

func (f *fixture) heldDecision(name string) (v1.Decision, bool) {
	for _, d := range f.store.TakeDecisions(f.uids[name]) {
		if d.Verdict == v1.VerdictHeld && d.Source == v1.SourceAgent {
			return d, true
		}
	}
	return v1.Decision{}, false
}

func TestInvestigator_AllDroppedLeavesHeldDecisionOnLead(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-held")
	f.propose(0.9, ovIn{Workload: "checkout-deploy", MinReplicas: 1000}) // out of scope: dropped

	res := f.iv.Investigate(f.ctx, f.incident("frontend-held"), f.deadline())
	if len(res.Overrides) != 0 {
		t.Fatalf("overrides = %+v", res.Overrides)
	}
	// The engine writes the queued decision to the lead's status.
	f.clk.Step(time.Minute)
	f.eng.Step(f.ctx)
	var tw v1.TideWorkload
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: "frontend-held"}, &tw); err != nil {
		t.Fatal(err)
	}
	var d *v1.Decision
	for i := range tw.Status.Decisions {
		if tw.Status.Decisions[i].Verdict == v1.VerdictHeld && tw.Status.Decisions[i].Source == v1.SourceAgent {
			d = &tw.Status.Decisions[i]
		}
	}
	if d == nil {
		t.Fatalf("no held decision in status: %+v", tw.Status.Decisions)
	}
	if d.Source != v1.SourceAgent || d.IncidentID != testInc || !slices.Contains(d.Reasons, "out_of_scope") ||
		d.Provider != scriptedName || d.Model != scriptedModel || d.PromptVersion != PromptVersion {
		t.Errorf("decision = %+v", d)
	}
}

func TestInvestigator_NoActionAndFailedExitsLeaveHeldDecision(t *testing.T) {
	cases := map[string]struct {
		setup  func(f *fixture)
		reason string
		skip   bool
	}{
		"no_action": {func(f *fixture) { f.script("no_action", map[string]any{kReason: "weak", kConf: 0.9}, 100) }, reasonNoAction, false},
		"provider_error": {func(f *fixture) {
			f.prov.Errs = []error{errors.New("boom")}
		}, "provider_error", true},
		"no_tool_call": {func(f *fixture) {
			f.prov.Turns = []llm.TurnResponse{{Message: llm.Message{Role: llm.RoleAssistant, Text: "hm"}}}
		}, "no_tool_call", false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			wl := "frontend-x-" + strings.ReplaceAll(name, "_", "-")
			f := newFixture(t, v1.ModeActive, "Auto", wl)
			tc.setup(f)
			f.iv.Investigate(f.ctx, f.incident(wl), f.deadline())
			d, ok := f.heldDecision(wl)
			if !ok || !slices.Contains(d.Reasons, tc.reason) || d.PromptVersion != PromptVersion {
				t.Fatalf("held decision = %+v ok=%v", d, ok)
			}
			if got := slices.Contains(f.notif.skipped, tc.reason); got != tc.skip {
				t.Errorf("InvestigationSkipped(%s) = %v, want %v (%v)", tc.reason, got, tc.skip, f.notif.skipped)
			}
		})
	}
}

func TestInvestigator_DeadlineLeavesHeldDecisionAndSkippedEvent(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-dl")
	f.prov.Delay = 5 * time.Second
	f.propose(0.9, ovIn{Workload: "frontend-dl", MinReplicas: 9})
	var pol v1.TidePolicy
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: f.pol}, &pol); err != nil {
		t.Fatal(err)
	}
	pol.Spec.Agent.InvestigationTimeout = metav1.Duration{Duration: 50 * time.Millisecond}
	if err := f.c.Update(f.ctx, &pol); err != nil {
		t.Fatal(err)
	}
	res := f.iv.Investigate(f.ctx, f.incident("frontend-dl"), f.deadline())
	if res.Exit != llm.ExitDeadline {
		t.Fatalf("exit = %s", res.Exit)
	}
	d, ok := f.heldDecision("frontend-dl")
	if !ok || !slices.Contains(d.Reasons, "deadline") || !slices.Contains(f.notif.skipped, "deadline") {
		t.Fatalf("held=%+v ok=%v skipped=%v", d, ok, f.notif.skipped)
	}
}

func TestSystemPrompt_OverrideDurationGuidance(t *testing.T) {
	if PromptVersion != "m2.2" {
		t.Fatalf("PromptVersion = %q, want m2.2", PromptVersion)
	}
	for _, want := range []string{"20 minutes after", "never longer than needed"} {
		if !strings.Contains(systemPrompt, want) {
			t.Fatalf("system prompt lacks %q", want)
		}
	}
}
