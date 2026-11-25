package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
)

const (
	testNS   = "default"
	testStep = time.Minute
	season   = time.Hour
)

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

	newWorkload := func(name string, replicas int32) {
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
	}

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

	It("skips workloads whose target is missing", func() {
		newPolicy(v1.ModeActive, nil)
		newWorkload("orphan", 2)
		Expect(k8sClient.Delete(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: "orphan"}})).To(Succeed())
		eng.Step(ctx)
		Expect(statusOf("orphan").Decisions).To(BeEmpty())
	})
})
