package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
)

const inc1 = "inc-1"

const (
	busybox    = "busybox"
	deployKind = "Deployment"
	profileRPS = "rps"
	appsAPI    = "apps/v1"
)

const (
	testNS   = "default"
	testStep = time.Minute
	season   = time.Hour
)

// spyScale reports a valid state and counts calls.
type spyScale struct{ gets, sets atomic.Int32 }

func (s *spyScale) Get(context.Context, workload.Target) (workload.ScaleState, error) {
	s.gets.Add(1)
	return workload.ScaleState{Replicas: 2, Ready: 2}, nil
}

func (s *spyScale) Set(context.Context, workload.Target, int32) error {
	s.sets.Add(1)
	return nil
}

// notFoundScale reads a state that needs a scale-up and fails every Set with NotFound.
type notFoundScale struct{ sets atomic.Int32 }

func (*notFoundScale) Get(context.Context, workload.Target) (workload.ScaleState, error) {
	return workload.ScaleState{Replicas: 2, Ready: 2}, nil
}

func (s *notFoundScale) Set(context.Context, workload.Target, int32) error {
	s.sets.Add(1)
	return apierrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, "gone")
}

// loadQuerier serves load(t) sampled on the requested grid.
type loadQuerier struct {
	load     func(t time.Time) float64
	readyErr error
}

func (f *loadQuerier) Ready(context.Context) error { return f.readyErr }

func (f *loadQuerier) RangeSeries(_ context.Context, _ string, start, end time.Time, step time.Duration) (forecast.Series, error) {
	s := forecast.Series{Step: step}
	for t := start; !t.After(end); t = t.Add(step) {
		s.Points = append(s.Points, forecast.Point{T: t, V: f.load(t)})
	}
	return s, nil
}

func cosine(t time.Time) float64 {
	return 50 + 40*math.Sin(2*math.Pi*float64(t.Unix())/season.Seconds())
}

// outcomeCount reads tidewatch_decision_outcome_total for one outcome label.
func outcomeCount(outcome string) float64 {
	mfs, err := ctrlmetrics.Registry.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, mf := range mfs {
		if mf.GetName() != "tidewatch_decision_outcome_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "outcome" && l.GetValue() == outcome {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// gaugeFor reads a per-workload gauge, or -1 when the series is absent.
func gaugeFor(name, wl string) float64 {
	mfs, err := ctrlmetrics.Registry.Gather()
	Expect(err).NotTo(HaveOccurred())
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "workload" && l.GetValue() == wl {
					return m.GetGauge().GetValue()
				}
			}
		}
	}
	return -1
}

// sink captures submitted triggers.
type sink struct {
	mu  sync.Mutex
	got []signal.Trigger
}

func (s *sink) Submit(t signal.Trigger) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, t)
}

func (s *sink) types() []signal.Type {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]signal.Type, 0, len(s.got))
	for _, t := range s.got {
		out = append(out, t.Type)
	}
	return out
}

var _ = Describe("Engine", func() {
	var (
		ctx   context.Context
		clk   *clocktesting.FakeClock
		q     *loadQuerier
		eng   *Engine
		names []string
		pol   string
	)

	// 45 minutes into the hour: the cosine trough, just before the ramp.
	trough := time.Date(2026, 1, 1, 0, 45, 0, 0, time.UTC)

	newPolicy := func(mode v1.PolicyMode, budget *int32) {
		p := &v1.TidePolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: pol},
			Spec: v1.TidePolicySpec{
				Mode:       mode,
				Prometheus: v1.PrometheusSpec{URL: "http://fake"},
				Selector:   v1.WorkloadSelector{Kinds: []v1.TargetKind{{APIVersion: appsAPI, Kind: deployKind}}},
				Load: v1.LoadSpec{
					DefaultProfile: profileRPS,
					Profiles:       map[string]v1.LoadProfile{profileRPS: {Query: "up", TargetPerReplica: "10"}},
				},
				Forecasting: v1.ForecastingSpec{
					Horizon:     metav1.Duration{Duration: 30 * time.Minute},
					History:     metav1.Duration{Duration: 3 * time.Hour},
					Step:        metav1.Duration{Duration: testStep},
					Seasonality: metav1.Duration{Duration: season},
				},
				Guardrails: v1.GuardrailsSpec{MaxReplicas: 50, MaxTotalReplicas: budget},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
	}

	newWorkloadInit := func(name string, replicas int32, initialized bool) {
		labels := map[string]string{"app": name}
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
			Spec: appsv1.DeploymentSpec{
				Replicas: new(replicas),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: busybox}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, d)).To(Succeed())
		tw := &v1.TideWorkload{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
			Spec: v1.TideWorkloadSpec{
				PolicyRef: v1.PolicyRef{Namespace: testNS, Name: pol},
				TargetRef: v1.TargetRef{APIVersion: appsAPI, Kind: deployKind, Name: name},
			},
		}
		Expect(k8sClient.Create(ctx, tw)).To(Succeed())
		names = append(names, name)
		if initialized { // what the policy controller does on creation
			meta.SetStatusCondition(&tw.Status.Conditions, metav1.Condition{
				Type: v1.CondConflict, Status: metav1.ConditionFalse, Reason: "none"})
			Expect(k8sClient.Status().Update(ctx, tw)).To(Succeed())
		}
	}
	newWorkload := func(name string, replicas int32) { newWorkloadInit(name, replicas, true) }

	replicasOf := func(name string) int32 {
		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, d)).To(Succeed())
		return *d.Spec.Replicas
	}
	statusOf := func(name string) v1.TideWorkloadStatus {
		tw := &v1.TideWorkload{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tw)).To(Succeed())
		return tw.Status
	}

	BeforeEach(func() {
		ctx = context.Background()
		clk = clocktesting.NewFakeClock(trough)
		q = &loadQuerier{load: cosine}
		pol = fmt.Sprintf("pol-%d", time.Now().UnixNano())
		names = nil
		eng = New(k8sClient, Options{
			Clock:              clk,
			DefaultTimeToReady: 5 * time.Minute,
			PromFactory:        func(string, time.Duration) (prom.Querier, error) { return q, nil },
		})
	})

	AfterEach(func() {
		for _, n := range names {
			_ = k8sClient.Delete(ctx, &v1.TideWorkload{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: n}})
			_ = k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: n}})
		}
		_ = k8sClient.Delete(ctx, &v1.TidePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: pol}})
	})

	It("scales a workload ahead of a forecast peak in Active mode", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkload("ahead", 2) // load 10 / (10*0.8) → reactive need is 2
		eng.Step(ctx)

		Expect(replicasOf("ahead")).To(BeNumerically(">", 2))
		st := statusOf("ahead")
		Expect(st.Decisions).NotTo(BeEmpty())
		Expect(st.Decisions[0].Source).To(Equal(v1.SourceBaseline))
		Expect(st.Decisions[0].Verdict).To(Or(Equal(v1.VerdictApplied), Equal(v1.VerdictClamped)))
		Expect(st.Mode).To(Equal(v1.ModeActive))
		Expect(st.LastAction).To(HavePrefix("scale 2→"))
		Expect(st.LastScaleUpTime).NotTo(BeNil())
		Expect(meta.IsStatusConditionTrue(st.Conditions, v1.CondForecasting)).To(BeTrue())
		Expect(st.ForecastPeak).NotTo(BeEmpty())
		Expect(st.Forecasts).NotTo(BeEmpty())
	})

	Context("trigger detection", func() {
		It("submits ForecastBreach when the forecast outgrows capacity, and is inert with a nil sink", func() {
			// 6 replicas: capacity 48; the ramp breaches it beyond what the baseline already covers.
			newPolicy(v1.ModeShadow, nil)
			newWorkload("breach", 6)
			pl := &v1.TidePolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: pol}, pl)).To(Succeed())
			pl.Spec.Agent.Enabled = true
			Expect(k8sClient.Update(ctx, pl)).To(Succeed())
			s := &sink{}
			withSink := New(k8sClient, Options{
				Clock: clk, DefaultTimeToReady: 5 * time.Minute, Triggers: s,
				PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
			})
			withSink.Step(ctx)
			Expect(s.types()).To(ContainElement(signal.ForecastBreach))
			s.mu.Lock()
			tr := s.got[0]
			s.mu.Unlock()
			Expect(tr.Workload).To(Equal(types.NamespacedName{Namespace: testNS, Name: "breach"}))
			Expect(tr.Policy).To(Equal(v1.PolicyRef{Namespace: testNS, Name: pol}))
			Expect(tr.TimeToBreach).NotTo(BeNil())

			// A second tick inside the debounce window raises nothing new of that type.
			n := len(s.types())
			clk.Step(testStep)
			withSink.Step(ctx)
			Expect(s.types()).To(HaveLen(n))

			// Nil Triggers: the same scenario still produces a decision and nothing else.
			Expect(eng.opts.Triggers).To(BeNil())
			eng.Step(ctx)
			Expect(statusOf("breach").Decisions).NotTo(BeEmpty())
		})
	})

	It("submits no triggers for a policy whose agent is disabled", func() {
		newPolicy(v1.ModeShadow, nil)
		newWorkload("quiet", 6) // would breach, as in the enabled case
		s := &sink{}
		New(k8sClient, Options{
			Clock: clk, DefaultTimeToReady: 5 * time.Minute, Triggers: s,
			PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
		}).Step(ctx)
		Expect(s.types()).To(BeEmpty())
	})

	Context("trigger detection inputs", func() {
		var s *sink
		var reads atomic.Int32
		var podNames []string

		var cr client.Reader

		enableAgent := func(mut func(*v1.TidePolicy)) {
			p := &v1.TidePolicy{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: pol}, p)).To(Succeed())
			p.Spec.Agent.Enabled = true
			if mut != nil {
				mut(p)
			}
			Expect(k8sClient.Update(ctx, p)).To(Succeed())
		}
		trigEngine := func() *Engine {
			return New(k8sClient, Options{
				Clock: clk, DefaultTimeToReady: 5 * time.Minute, Triggers: s, APIReader: cr,
				PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
			})
		}
		has := func(t signal.Type) bool { return slices.Contains(s.types(), t) }
		detail := func(t signal.Type) string {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, g := range s.got {
				if g.Type == t {
					return g.Detail
				}
			}
			return ""
		}
		mkPod := func(wl, name string, mut func(*corev1.Pod)) {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name, Labels: map[string]string{"app": wl}},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: busybox}}},
			}
			Expect(k8sClient.Create(ctx, p)).To(Succeed())
			podNames = append(podNames, name)
			if mut != nil {
				mut(p)
				Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
			}
		}

		BeforeEach(func() {
			s = &sink{}
			podNames = nil
			reads.Store(0)
			cr = countingReader{Reader: k8sClient, n: &reads}
		})
		AfterEach(func() {
			for _, n := range podNames {
				_ = k8sClient.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: n}},
					client.GracePeriodSeconds(0))
			}
			_ = k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "notes-" + pol}})
		})

		It("raises Anomaly after 3 consecutive steps outside the trusted interval", func() {
			newPolicy(v1.ModeShadow, nil)
			enableAgent(nil)
			newWorkload("anom", 6)
			e := trigEngine()
			e.Step(ctx) // first tick: no previous forecast to judge
			// From now on the real load is wildly above what any model predicted.
			q.load = func(t time.Time) float64 { return cosine(t) + 500 }
			for range 5 {
				clk.Step(testStep)
				e.Step(ctx)
			}
			Expect(has(signal.Anomaly)).To(BeTrue())
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, g := range s.got {
				if g.Type == signal.Anomaly {
					Expect(g.Shortfall).To(BeNumerically(">", 0), "Anomaly carries |actual-forecast|/forecast")
				}
			}
		})

		It("raises WorkloadChange from restarts and OOMKills read from pods", func() {
			newPolicy(v1.ModeShadow, nil)
			enableAgent(nil)
			newWorkload("pods", 2)
			mkPod("pods", "pods-a", func(p *corev1.Pod) {
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "c", Image: busybox, ImageID: "x"}}
			})
			e := trigEngine()
			e.Step(ctx)
			Expect(has(signal.WorkloadChange)).To(BeFalse())

			clk.Step(testStep)
			var pod corev1.Pod
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "pods-a"}, &pod)).To(Succeed())
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
				Name: "c", Image: busybox, ImageID: "x", RestartCount: 3,
			}}
			Expect(k8sClient.Status().Update(ctx, &pod)).To(Succeed())
			e.Step(ctx)
			Expect(has(signal.WorkloadChange)).To(BeTrue())
			Expect(detail(signal.WorkloadChange)).To(ContainSubstring("3 container restarts"))
		})

		It("raises WorkloadChange for a recent OOMKill", func() {
			newPolicy(v1.ModeShadow, nil)
			enableAgent(nil)
			newWorkload("oom", 2)
			mkPod("oom", "oom-a", func(p *corev1.Pod) {
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: "c", Image: busybox, ImageID: "x", RestartCount: 1,
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.NewTime(clk.Now().Add(-time.Minute)),
					}},
				}}
			})
			trigEngine().Step(ctx)
			Expect(detail(signal.WorkloadChange)).To(ContainSubstring("OOMKilled"))
		})

		It("ignores pods when the agent is disabled", func() {
			newPolicy(v1.ModeShadow, nil)
			newWorkload("noagent", 2)
			mkPod("noagent", "noagent-a", func(p *corev1.Pod) {
				p.Status.ContainerStatuses = []corev1.ContainerStatus{{
					Name: "c", Image: busybox, ImageID: "x",
					LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
						Reason: "OOMKilled", ExitCode: 137, FinishedAt: metav1.NewTime(clk.Now()),
					}},
				}}
			})
			trigEngine().Step(ctx)
			Expect(has(signal.WorkloadChange)).To(BeFalse())
		})

		It("raises ContextDue from a ConfigMap read once per policy per minute", func() {
			newPolicy(v1.ModeShadow, nil)
			cmName := "notes-" + pol
			enableAgent(func(p *v1.TidePolicy) {
				p.Spec.Agent.Context.ConfigMapRef = &v1.LocalObjectRef{Name: cmName}
			})
			when := clk.Now().Add(10 * time.Minute).UTC().Format(time.RFC3339)
			Expect(k8sClient.Create(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: cmName},
				Data:       map[string]string{"sale": "flash sale at " + when + " expect 3x"},
			})).To(Succeed())
			newWorkload("note-a", 2)
			newWorkload("note-b", 2)
			e := trigEngine()
			e.Step(ctx)
			Expect(has(signal.ContextDue)).To(BeTrue())
			Expect(reads.Load()).To(Equal(int32(1)))
		})

		It("skips the notes ConfigMap when no APIReader is configured", func() {
			newPolicy(v1.ModeShadow, nil)
			enableAgent(func(p *v1.TidePolicy) {
				p.Spec.Agent.Context.ConfigMapRef = &v1.LocalObjectRef{Name: "missing"}
			})
			newWorkload("note-c", 2)
			e := trigEngine()
			e.opts.APIReader = nil
			e.Step(ctx)
			Expect(has(signal.ContextDue)).To(BeFalse())
		})

		It("fires WorkloadChange on an image change but not on a replica change", func() {
			newPolicy(v1.ModeShadow, nil)
			enableAgent(nil)
			newWorkload("tpl", 2)
			e := trigEngine()
			e.Step(ctx)

			get := func() *appsv1.Deployment {
				d := &appsv1.Deployment{}
				Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "tpl"}, d)).To(Succeed())
				return d
			}
			d := get()
			d.Spec.Replicas = new(int32(3))
			Expect(k8sClient.Update(ctx, d)).To(Succeed())
			clk.Step(testStep)
			e.Step(ctx)
			Expect(has(signal.WorkloadChange)).To(BeFalse())

			d = get()
			d.Spec.Template.Spec.Containers[0].Image = "busybox:1.36"
			Expect(k8sClient.Update(ctx, d)).To(Succeed())
			clk.Step(testStep)
			e.Step(ctx)
			Expect(detail(signal.WorkloadChange)).To(ContainSubstring("template changed"))
		})
	})

	It("never scales a TideWorkload the controller has not initialized with a Conflict condition", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkloadInit("uninit", 2, false)
		spy := &spyScale{}
		e := New(k8sClient, Options{
			Clock: clk, Scale: spy, DefaultTimeToReady: 5 * time.Minute,
			PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
		})
		e.Step(ctx)
		Expect(spy.gets.Load()).To(BeZero())
		Expect(spy.sets.Load()).To(BeZero())
		Expect(replicasOf("uninit")).To(Equal(int32(2)))
		Expect(statusOf("uninit").Decisions).To(BeEmpty())
	})

	It("records shadow decisions without patching in Shadow mode", func() {
		newPolicy(v1.ModeShadow, nil)
		newWorkload("shadow", 2)
		eng.Step(ctx)

		Expect(replicasOf("shadow")).To(Equal(int32(2)))
		st := statusOf("shadow")
		Expect(st.Decisions).NotTo(BeEmpty())
		Expect(st.Decisions[0].Verdict).To(Equal(v1.VerdictShadow))
		Expect(st.Decisions[0].Final).To(BeNumerically(">", 2))
	})

	It("holds and reports metrics_unavailable when Prometheus is down", func() {
		q.readyErr = errors.New("down")
		newPolicy(v1.ModeActive, nil)
		newWorkload("down", 3)
		eng.Step(ctx)

		Expect(replicasOf("down")).To(Equal(int32(3)))
		st := statusOf("down")
		Expect(st.Decisions).NotTo(BeEmpty())
		Expect(st.Decisions[0].Verdict).To(Equal(v1.VerdictHeld))
		Expect(st.Decisions[0].Reasons).To(ContainElement("metrics_unavailable"))
		Expect(meta.IsStatusConditionFalse(st.Conditions, v1.CondForecasting)).To(BeTrue())
	})

	It("enforces maxTotalReplicas across workloads in one policy", func() {
		q.load = func(time.Time) float64 { return 80 } // wants 10 replicas each
		newPolicy(v1.ModeActive, ptr.To[int32](12))
		for _, n := range []string{"b-one", "b-two", "b-three"} {
			newWorkload(n, 1)
		}
		eng.Step(ctx)

		var total int32
		limited := 0
		for _, n := range names {
			total += replicasOf(n)
			if meta.IsStatusConditionTrue(statusOf(n).Conditions, v1.CondBudgetLimited) {
				limited++
			}
		}
		Expect(total).To(BeNumerically("<=", 12))
		Expect(limited).To(BeNumerically(">=", 1))
	})

	It("forgets engine state when a TideWorkload is deleted", func() {
		newPolicy(v1.ModeShadow, nil)
		newWorkload("gone", 2)
		eng.Step(ctx)
		Expect(eng.stateCount()).To(Equal(1))

		Expect(k8sClient.Delete(ctx, &v1.TideWorkload{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "gone"}})).To(Succeed())
		eng.Step(ctx)
		Expect(eng.stateCount()).To(Equal(0))
	})

	It("exports tidewatch_capacity_ratio", func() {
		newPolicy(v1.ModeShadow, nil)
		newWorkload("ratio", 2)
		d := &appsv1.Deployment{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "ratio"}, d)).To(Succeed())
		d.Status.Replicas, d.Status.ReadyReplicas = 2, 2
		Expect(k8sClient.Status().Update(ctx, d)).To(Succeed())
		eng.Step(ctx)

		mfs, err := ctrlmetrics.Registry.Gather()
		Expect(err).NotTo(HaveOccurred())
		found := false
		for _, mf := range mfs {
			if mf.GetName() != "tidewatch_capacity_ratio" {
				continue
			}
			for _, m := range mf.GetMetric() {
				for _, l := range m.GetLabel() {
					found = found || (l.GetName() == "workload" && l.GetValue() == "ratio")
				}
			}
		}
		Expect(found).To(BeTrue())
	})

	Context("after a restart (fresh engine state)", func() {
		BeforeEach(func() { q.load = func(time.Time) float64 { return 10 } }) // wants 2 replicas

		It("holds a scale-down by cooldown when status records a recent scale-down", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("restart-cd", 6)
			tw := &v1.TideWorkload{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "restart-cd"}, tw)).To(Succeed())
			tw.Status.LastScaleDownTime = new(metav1.NewTime(clk.Now().Add(-10 * time.Second)))
			Expect(k8sClient.Status().Update(ctx, tw)).To(Succeed())
			eng.Step(ctx)

			Expect(replicasOf("restart-cd")).To(Equal(int32(6)))
			st := statusOf("restart-cd")
			Expect(st.Decisions[0].Reasons).To(ContainElement(reasonCooldown))
			Expect(st.LastScaleDownTime).NotTo(BeNil(), "existing timestamp must be preserved")
		})

		It("waits one stabilization window before the first scale-down", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("restart-stab", 6)
			eng.Step(ctx)

			Expect(replicasOf("restart-stab")).To(Equal(int32(6)))
			Expect(statusOf("restart-stab").Decisions[0].Verdict).To(Equal(v1.VerdictHeld))
		})
	})

	Context("with degraded data", func() {
		// 40 of 181 points missing in the middle: unhealthy, but the last point is valid.
		gappy := func(last float64) func(time.Time) float64 {
			return func(t time.Time) float64 {
				if t.Equal(trough) {
					return last
				}
				if age := trough.Sub(t); age >= 60*time.Minute && age < 100*time.Minute {
					return math.NaN()
				}
				return cosine(t)
			}
		}

		It("never scales down on unhealthy gaps and reports Forecasting=False", func() {
			q.load = gappy(5) // reactive floor 1, current 6
			newPolicy(v1.ModeActive, nil)
			newWorkload("gappy-low", 6)
			eng.Step(ctx)

			Expect(replicasOf("gappy-low")).To(Equal(int32(6)))
			st := statusOf("gappy-low")
			Expect(st.Decisions[0].Verdict).To(Equal(v1.VerdictHeld))
			cond := meta.FindStatusCondition(st.Conditions, v1.CondForecasting)
			Expect(cond).NotTo(BeNil())
			Expect(cond.Status).To(Equal(metav1.ConditionFalse))
			Expect(cond.Reason).To(Equal("insufficient_data"))
		})

		It("never scales down when the latest point is NaN", func() {
			q.load = func(t time.Time) float64 {
				if trough.Sub(t) < 2*time.Minute {
					return math.NaN()
				}
				return cosine(t)
			}
			newPolicy(v1.ModeActive, nil)
			newWorkload("nan-last", 6)
			eng.Step(ctx)

			Expect(replicasOf("nan-last")).To(Equal(int32(6)))
		})

		It("still scales up on unhealthy gaps", func() {
			q.load = gappy(80) // reactive floor 10
			newPolicy(v1.ModeActive, nil)
			newWorkload("gappy-high", 6)
			eng.Step(ctx)

			Expect(replicasOf("gappy-high")).To(BeNumerically(">", 6))
		})
	})

	Context("with an override store", func() {
		var store *overrides.Store

		BeforeEach(func() {
			q.load = func(time.Time) float64 { return 10 } // baseline wants 2 replicas
			store = overrides.NewStore()
			eng = New(k8sClient, Options{
				Clock: clk, Overrides: store, DefaultTimeToReady: 5 * time.Minute,
				PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
			})
		})

		override := func(incident string, minReplicas int32) v1.Override {
			return v1.Override{
				ID: "ov-" + incident, IncidentID: incident, MinReplicas: minReplicas,
				Start: metav1.NewTime(trough.Add(-time.Minute)), End: metav1.NewTime(trough.Add(time.Hour)),
			}
		}
		uidOf := func(name string) types.UID {
			tw := &v1.TideWorkload{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tw)).To(Succeed())
			return tw.UID
		}
		annotate := func(name, key, val string) {
			tw := &v1.TideWorkload{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tw)).To(Succeed())
			orig := tw.DeepCopy()
			if tw.Annotations == nil {
				tw.Annotations = map[string]string{}
			}
			tw.Annotations[key] = val
			Expect(k8sClient.Patch(ctx, tw, client.MergeFrom(orig))).To(Succeed())
		}
		annotationsOf := func(name string) map[string]string {
			tw := &v1.TideWorkload{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, tw)).To(Succeed())
			return tw.Annotations
		}
		pending := func(incident string, minReplicas int32, expires time.Time) v1.PendingApproval {
			return v1.PendingApproval{
				IncidentID: incident, Override: override(incident, minReplicas),
				Confidence: "0.9", Expires: metav1.NewTime(expires),
			}
		}

		It("lifts replicas to an active override but never lowers below the baseline", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("lift", 2)
			Expect(store.AddOverride(uidOf("lift"), override(inc1, 6))).To(Succeed())
			eng.Step(ctx)

			Expect(replicasOf("lift")).To(Equal(int32(6)))
			st := statusOf("lift")
			Expect(st.Decisions[0].Source).To(Equal(v1.SourceAgent))
			Expect(st.Decisions[0].IncidentID).To(Equal(inc1))
			Expect(st.Decisions[0].Desired).To(Equal(int32(6)))

			// A floor below what the baseline wants changes nothing.
			q.load = func(time.Time) float64 { return 80 } // baseline wants 10
			newWorkload("nolower", 8)
			Expect(store.AddOverride(uidOf("nolower"), override("inc-2", 3))).To(Succeed())
			eng.Step(ctx)
			Expect(replicasOf("nolower")).To(Equal(int32(10)))
			Expect(statusOf("nolower").Decisions[0].Source).To(Equal(v1.SourceBaseline))
			Expect(statusOf("nolower").Decisions[0].IncidentID).To(BeEmpty())
		})

		It("still runs the enforcer on an override (maxReplicas clamps it)", func() {
			newPolicy(v1.ModeActive, nil) // maxReplicas 50
			newWorkload("clamped", 48)
			Expect(store.AddOverride(uidOf("clamped"), override(inc1, 80))).To(Succeed())
			eng.Step(ctx)

			Expect(replicasOf("clamped")).To(Equal(int32(50)))
			Expect(statusOf("clamped").Decisions[0].Verdict).To(Equal(v1.VerdictClamped))
		})

		It("consumes an approve annotation, applies the override, and removes the annotation", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("approve", 2)
			Expect(store.AddPending(uidOf("approve"), pending(inc1, 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
			annotate("approve", v1.AnnoApprove, "inc-1@alice")
			eng.Step(ctx)

			Expect(replicasOf("approve")).To(Equal(int32(6)))
			st := statusOf("approve")
			Expect(st.Overrides).To(HaveLen(1))
			Expect(st.Overrides[0].ApprovedBy).To(Equal("alice"))
			Expect(st.PendingApprovals).To(BeEmpty())
			Expect(annotationsOf("approve")).NotTo(HaveKey(v1.AnnoApprove))
			verdicts := []v1.Verdict{}
			for _, d := range st.Decisions {
				if d.Source == v1.SourceAgent && d.IncidentID == inc1 {
					verdicts = append(verdicts, d.Verdict)
				}
			}
			Expect(verdicts).To(ContainElement(v1.VerdictApplied))
		})

		It("defaults the approver to kubectl and consumes reject annotations", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("kc", 2)
			newWorkload("rej", 2)
			Expect(store.AddPending(uidOf("kc"), pending(inc1, 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
			Expect(store.AddPending(uidOf("rej"), pending("inc-2", 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
			annotate("kc", v1.AnnoApprove, inc1)
			annotate("rej", v1.AnnoReject, "inc-2@bob")
			eng.Step(ctx)

			Expect(statusOf("kc").Overrides[0].ApprovedBy).To(Equal("kubectl"))
			st := statusOf("rej")
			Expect(st.Overrides).To(BeEmpty())
			Expect(st.PendingApprovals).To(BeEmpty())
			verdicts := make([]v1.Verdict, 0, len(st.Decisions))
			for _, d := range st.Decisions {
				verdicts = append(verdicts, d.Verdict)
			}
			Expect(verdicts).To(ContainElement(v1.VerdictRejected))
			Expect(replicasOf("rej")).To(Equal(int32(2)))
			Expect(annotationsOf("rej")).NotTo(HaveKey(v1.AnnoReject))
		})

		It("ignores an approve annotation for an expired approval and records verdict expired", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("expired", 2)
			Expect(store.AddPending(uidOf("expired"), pending(inc1, 6, trough.Add(-time.Minute)))).Error().NotTo(HaveOccurred())
			annotate("expired", v1.AnnoApprove, "inc-1@alice")
			eng.Step(ctx)

			Expect(replicasOf("expired")).To(Equal(int32(2)))
			st := statusOf("expired")
			Expect(st.Overrides).To(BeEmpty())
			Expect(st.PendingApprovals).To(BeEmpty())
			found := false
			for _, d := range st.Decisions {
				found = found || (d.Verdict == v1.VerdictExpired && d.IncidentID == inc1)
			}
			Expect(found).To(BeTrue())
			Expect(annotationsOf("expired")).NotTo(HaveKey(v1.AnnoApprove))
		})

		It("restores overrides and pending approvals from status on a fresh engine", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("restored", 2)
			tw := &v1.TideWorkload{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "restored"}, tw)).To(Succeed())
			tw.Status.Overrides = []v1.Override{override(inc1, 6)}
			tw.Status.PendingApprovals = []v1.PendingApproval{pending("inc-2", 8, trough.Add(10*time.Minute))}
			Expect(k8sClient.Status().Update(ctx, tw)).To(Succeed())
			eng.Step(ctx) // store is empty before this step: everything comes from status

			Expect(replicasOf("restored")).To(Equal(int32(6)))
			st := statusOf("restored")
			Expect(st.Overrides).To(HaveLen(1))
			Expect(st.PendingApprovals).To(HaveLen(1))
			Expect(st.PendingApprovals[0].IncidentID).To(Equal("inc-2"))

			// The restored pending approval must still be approvable.
			annotate("restored", v1.AnnoApprove, "inc-2@tester")
			clk.Step(testStep)
			eng.Step(ctx)

			Expect(replicasOf("restored")).To(Equal(int32(8)))
			st = statusOf("restored")
			Expect(st.PendingApprovals).To(BeEmpty())
			Expect(st.Overrides).To(HaveLen(2))
			var approved *v1.Override
			for i := range st.Overrides {
				if st.Overrides[i].IncidentID == "inc-2" {
					approved = &st.Overrides[i]
				}
			}
			Expect(approved).NotTo(BeNil())
			Expect(approved.ApprovedBy).To(Equal("tester"))
			Expect(annotationsOf("restored")).NotTo(HaveKey(v1.AnnoApprove))
			m, active := store.Active(uidOf("restored"), clk.Now())
			Expect(active).NotTo(BeNil())
			Expect(m).To(Equal(int32(8)))
		})

		It("keeps approval decisions for the next write when the status write fails", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("retry", 2)
			fail := true
			wc, err := client.NewWithWatch(cfg, client.Options{Scheme: k8sClient.Scheme()})
			Expect(err).NotTo(HaveOccurred())
			c := interceptor.NewClient(wc, interceptor.Funcs{
				SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if fail {
						return errors.New("boom")
					}
					return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
				},
			})
			e := New(c, Options{
				Clock: clk, Overrides: store, DefaultTimeToReady: 5 * time.Minute,
				PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
			})
			Expect(store.AddPending(uidOf("retry"), pending(inc1, 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
			annotate("retry", v1.AnnoApprove, "inc-1@alice")
			e.Step(ctx)
			Expect(statusOf("retry").Decisions).To(BeEmpty())

			fail = false
			clk.Step(testStep)
			e.Step(ctx)
			found := false
			for _, d := range statusOf("retry").Decisions {
				found = found || (d.Verdict == v1.VerdictApplied && d.IncidentID == inc1)
			}
			Expect(found).To(BeTrue())
		})

		It("records a RecentOutcome after the override window plus horizon", func() {
			newPolicy(v1.ModeActive, nil) // horizon 30m
			newWorkload("scored", 2)
			o := override(inc1, 12)
			o.Start, o.End = metav1.NewTime(trough), metav1.NewTime(trough.Add(2*time.Minute))
			Expect(store.AddOverride(uidOf("scored"), o)).To(Succeed())
			before := outcomeCount("unneeded")

			eng.Step(ctx) // samples inside the window
			Expect(statusOf("scored").RecentOutcomes).To(BeEmpty())
			clk.Step(3 * time.Minute)
			eng.Step(ctx) // window over, horizon not yet
			Expect(statusOf("scored").Overrides).To(BeEmpty())
			Expect(statusOf("scored").RecentOutcomes).To(BeEmpty())
			clk.Step(30 * time.Minute)
			eng.Step(ctx) // End + horizon reached

			st := statusOf("scored")
			Expect(st.RecentOutcomes).To(HaveLen(1))
			ro := st.RecentOutcomes[0]
			Expect(ro.IncidentID).To(Equal(inc1))
			Expect(ro.MinReplicas).To(Equal(int32(12)))
			Expect(ro.Outcome).NotTo(BeNil())
			Expect(ro.Outcome.Timely).To(BeNil(), "constant load 10 never exceeds the baseline's capacity")
			Expect(ro.Outcome.Needed).To(BeFalse())
			Expect(ro.Outcome.WastedReplicaMinutes).NotTo(BeEmpty())
			Expect(outcomeCount("unneeded")).To(Equal(before + 1))
			Expect(gaugeFor("tidewatch_baseline_only_replicas", "scored")).To(BeNumerically(">", 0))
		})

		It("writes baselineReplicas and overrides to status", func() {
			newPolicy(v1.ModeShadow, nil)
			newWorkload("persist", 2)
			Expect(store.AddOverride(uidOf("persist"), override(inc1, 6))).To(Succeed())
			eng.Step(ctx)

			st := statusOf("persist")
			Expect(st.BaselineReplicas).To(Equal(int32(2)))
			Expect(st.Overrides).To(HaveLen(1))
			Expect(st.Overrides[0].MinReplicas).To(Equal(int32(6)))
		})
	})

	It("skips a workload whose labels no longer match the policy", func() {
		p := &v1.TidePolicy{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: pol},
			Spec: v1.TidePolicySpec{
				Mode:       v1.ModeActive,
				Prometheus: v1.PrometheusSpec{URL: "http://fake"},
				Selector: v1.WorkloadSelector{
					LabelSelector: metav1.LabelSelector{MatchLabels: map[string]string{"tier": "web"}},
					Kinds:         []v1.TargetKind{{APIVersion: appsAPI, Kind: deployKind}},
				},
				Load: v1.LoadSpec{
					DefaultProfile: profileRPS,
					Profiles:       map[string]v1.LoadProfile{profileRPS: {Query: "up", TargetPerReplica: "10"}},
				},
				Forecasting: v1.ForecastingSpec{
					Horizon: metav1.Duration{Duration: 30 * time.Minute}, History: metav1.Duration{Duration: 3 * time.Hour},
					Step: metav1.Duration{Duration: testStep}, Seasonality: metav1.Duration{Duration: season},
				},
				Guardrails: v1.GuardrailsSpec{MaxReplicas: 50},
			},
		}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		label := func(name, tier string) {
			d := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, d)).To(Succeed())
			d.Labels = map[string]string{"tier": tier}
			Expect(k8sClient.Update(ctx, d)).To(Succeed())
		}
		newWorkload("still-selected", 2)
		newWorkload("deselected", 2)
		label("still-selected", "web")
		label("deselected", "batch") // relabeled after the TideWorkload was created
		eng.Step(ctx)

		Expect(replicasOf("still-selected")).To(BeNumerically(">", 2))
		Expect(replicasOf("deselected")).To(Equal(int32(2)))
		st := statusOf("deselected")
		Expect(st.Decisions).NotTo(BeEmpty())
		Expect(st.Decisions[0].Verdict).To(Equal(v1.VerdictHeld))
		Expect(st.Decisions[0].Reasons).To(ContainElement("not_selected"))
	})

	It("held path clears stale forecast fields", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkload("stale", 2)
		eng.Step(ctx)
		before := statusOf("stale")
		Expect(before.ForecastPeak).NotTo(BeEmpty())
		Expect(before.Forecasts).NotTo(BeEmpty())
		Expect(before.Load).NotTo(BeEmpty())

		q.readyErr = errors.New("down")
		clk.Step(2 * testStep)
		eng.Step(ctx)
		after := statusOf("stale")
		Expect(after.Decisions[0].Verdict).To(Equal(v1.VerdictHeld))
		Expect(after.ForecastPeak).To(BeEmpty())
		Expect(after.PeakTime).To(BeNil())
		Expect(after.Forecasts).To(BeEmpty())
		Expect(after.Load).To(BeEmpty())
	})

	It("skips the status write when the scale target vanished (Set NotFound)", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkload("vanished", 2)
		spy := &notFoundScale{}
		e := New(k8sClient, Options{
			Clock: clk, Scale: spy, DefaultTimeToReady: 5 * time.Minute,
			PromFactory: func(string, time.Duration) (prom.Querier, error) { return q, nil },
		})
		e.Step(ctx)
		Expect(spy.sets.Load()).To(BeNumerically(">", 0))
		Expect(statusOf("vanished").Decisions).To(BeEmpty())
	})

	It("skips workloads whose target is missing", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkload("orphan", 2)
		Expect(k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "orphan"}})).To(Succeed())
		eng.Step(ctx)
		Expect(statusOf("orphan").Decisions).To(BeEmpty())
	})
})

// countingReader counts Get calls on the wrapped reader.
type countingReader struct {
	client.Reader
	n *atomic.Int32
}

func (c countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.n.Add(1)
	return c.Reader.Get(ctx, key, obj, opts...)
}

func TestQuerierCacheKeyIncludesTimeout(t *testing.T) {
	var built atomic.Int32
	e := New(nil, Options{PromFactory: func(string, time.Duration) (prom.Querier, error) {
		built.Add(1)
		return &loadQuerier{}, nil
	}})
	for _, tc := range []struct {
		timeout time.Duration
		want    int32
	}{{5 * time.Second, 1}, {5 * time.Second, 1}, {10 * time.Second, 2}, {0, 2}, {defaultPromTimeout, 2}, {time.Second, 3}} {
		if _, err := e.querier("http://p", tc.timeout); err != nil {
			t.Fatal(err)
		}
		if got := built.Load(); got != tc.want {
			t.Fatalf("after timeout %v built = %d, want %d", tc.timeout, got, tc.want)
		}
	}
}
