package workload

import (
	"context"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const (
	testNS = "default"
	appsV1 = "apps/v1"
)

var _ = Describe("ScaleClient", func() {
	var (
		ctx    context.Context
		sc     ScaleClient
		target Target
		name   string
	)

	newDeployment := func(n string) *appsv1.Deployment {
		labels := map[string]string{"app": n}
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: n},
			Spec: appsv1.DeploymentSpec{
				Replicas: ptr.To[int32](2),
				Selector: &metav1.LabelSelector{MatchLabels: labels},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: labels},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
				},
			},
		}
	}

	BeforeEach(func() {
		ctx = context.Background()
		name = fmt.Sprintf("web-%d", GinkgoRandomSeed()+int64(GinkgoParallelProcess()))
		Expect(k8sClient.Create(ctx, newDeployment(name))).To(Succeed())
		sc = NewScaleClient(k8sClient)
		target = Target{
			Ref:       v1.TargetRef{APIVersion: appsV1, Kind: "Deployment", Name: name},
			Namespace: testNS,
		}
	})

	AfterEach(func() {
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Namespace: testNS, Name: name}}
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, d))).To(Succeed())
	})

	It("reads and sets replicas through the scale subresource", func() {
		st, err := sc.Get(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(st.Replicas).To(Equal(int32(2)))
		Expect(st.Selector).To(Equal("app=" + name))

		Expect(sc.Set(ctx, target, 4)).To(Succeed())

		var d appsv1.Deployment
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &d)).To(Succeed())
		Expect(*d.Spec.Replicas).To(Equal(int32(4)))
	})

	It("reports readyReplicas from status", func() {
		var d appsv1.Deployment
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: testNS, Name: name}, &d)).To(Succeed())
		d.Status.Replicas = 2
		d.Status.ReadyReplicas = 2
		Expect(k8sClient.Status().Update(ctx, &d)).To(Succeed())

		st, err := sc.Get(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(st.Ready).To(Equal(int32(2)))
	})

	It("counts Ready pods by selector when readyReplicas is absent", func() {
		for i, ready := range []bool{true, true, false} {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: testNS, Name: fmt.Sprintf("%s-pod-%d", name, i),
					Labels: map[string]string{"app": name},
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
			}
			Expect(k8sClient.Create(ctx, p)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, p) })
			status := corev1.ConditionFalse
			if ready {
				status = corev1.ConditionTrue
			}
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}
			Expect(k8sClient.Status().Update(ctx, p)).To(Succeed())
		}

		st, err := sc.Get(ctx, target)
		Expect(err).NotTo(HaveOccurred())
		Expect(st.Ready).To(Equal(int32(2)))
	})

})
