package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
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
				Selector:   v1.WorkloadSelector{Kinds: []v1.TargetKind{{APIVersion: "apps/v1", Kind: "Deployment"}}},
				Load: v1.LoadSpec{
					DefaultProfile: "rps",
					Profiles:       map[string]v1.LoadProfile{"rps": {Query: "up", TargetPerReplica: "10"}},
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
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, d)).To(Succeed())
		tw := &v1.TideWorkload{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name},
			Spec: v1.TideWorkloadSpec{
				PolicyRef: v1.PolicyRef{Namespace: testNS, Name: pol},
				TargetRef: v1.TargetRef{APIVersion: "apps/v1", Kind: "Deployment", Name: name},
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
			Expect(store.AddOverride(uidOf("lift"), override("inc-1", 6))).To(Succeed())
			eng.Step(ctx)

			Expect(replicasOf("lift")).To(Equal(int32(6)))
			st := statusOf("lift")
			Expect(st.Decisions[0].Source).To(Equal(v1.SourceAgent))
			Expect(st.Decisions[0].IncidentID).To(Equal("inc-1"))
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
			Expect(store.AddOverride(uidOf("clamped"), override("inc-1", 80))).To(Succeed())
			eng.Step(ctx)

			Expect(replicasOf("clamped")).To(Equal(int32(50)))
			Expect(statusOf("clamped").Decisions[0].Verdict).To(Equal(v1.VerdictClamped))
		})

		It("consumes an approve annotation, applies the override, and removes the annotation", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("approve", 2)
			Expect(store.AddPending(uidOf("approve"), pending("inc-1", 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
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
				if d.Source == v1.SourceAgent && d.IncidentID == "inc-1" {
					verdicts = append(verdicts, d.Verdict)
				}
			}
			Expect(verdicts).To(ContainElement(v1.VerdictApplied))
		})

		It("defaults the approver to kubectl and consumes reject annotations", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("kc", 2)
			newWorkload("rej", 2)
			Expect(store.AddPending(uidOf("kc"), pending("inc-1", 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
			Expect(store.AddPending(uidOf("rej"), pending("inc-2", 6, trough.Add(10*time.Minute)))).Error().NotTo(HaveOccurred())
			annotate("kc", v1.AnnoApprove, "inc-1")
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
			Expect(store.AddPending(uidOf("expired"), pending("inc-1", 6, trough.Add(-time.Minute)))).Error().NotTo(HaveOccurred())
			annotate("expired", v1.AnnoApprove, "inc-1@alice")
			eng.Step(ctx)

			Expect(replicasOf("expired")).To(Equal(int32(2)))
			st := statusOf("expired")
			Expect(st.Overrides).To(BeEmpty())
			Expect(st.PendingApprovals).To(BeEmpty())
			found := false
			for _, d := range st.Decisions {
				found = found || (d.Verdict == v1.VerdictExpired && d.IncidentID == "inc-1")
			}
			Expect(found).To(BeTrue())
			Expect(annotationsOf("expired")).NotTo(HaveKey(v1.AnnoApprove))
		})

		It("restores overrides and pending approvals from status on a fresh engine", func() {
			newPolicy(v1.ModeActive, nil)
			newWorkload("restored", 2)
			tw := &v1.TideWorkload{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: "restored"}, tw)).To(Succeed())
			tw.Status.Overrides = []v1.Override{override("inc-1", 6)}
			tw.Status.PendingApprovals = []v1.PendingApproval{pending("inc-2", 8, trough.Add(10*time.Minute))}
			Expect(k8sClient.Status().Update(ctx, tw)).To(Succeed())
			eng.Step(ctx) // store is empty before this step: everything comes from status

			Expect(replicasOf("restored")).To(Equal(int32(6)))
			st := statusOf("restored")
			Expect(st.Overrides).To(HaveLen(1))
			Expect(st.PendingApprovals).To(HaveLen(1))
			Expect(st.PendingApprovals[0].IncidentID).To(Equal("inc-2"))
		})

		It("writes baselineReplicas and overrides to status", func() {
			newPolicy(v1.ModeShadow, nil)
			newWorkload("persist", 2)
			Expect(store.AddOverride(uidOf("persist"), override("inc-1", 6))).To(Succeed())
			eng.Step(ctx)

			st := statusOf("persist")
			Expect(st.BaselineReplicas).To(Equal(int32(2)))
			Expect(st.Overrides).To(HaveLen(1))
			Expect(st.Overrides[0].MinReplicas).To(Equal(int32(6)))
		})
	})

	It("skips workloads whose target is missing", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkload("orphan", 2)
		Expect(k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "orphan"}})).To(Succeed())
		eng.Step(ctx)
		Expect(statusOf("orphan").Decisions).To(BeEmpty())
	})
})
