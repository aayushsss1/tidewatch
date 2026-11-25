/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/prom"
)

type spyScale struct{ sets atomic.Int32 }

func (s *spyScale) Get(context.Context, workload.Target) (workload.ScaleState, error) {
	return workload.ScaleState{}, errors.New("not found")
}

func (s *spyScale) Set(context.Context, workload.Target, int32) error {
	s.sets.Add(1)
	return nil
}

const (
	deploymentKind = "Deployment"
	appsAPIVersion = "apps/v1"
	webName        = "web"
	tierKey        = "tier"
)

var nsCounter atomic.Int32

var _ = Describe("TidePolicy Controller", func() {
	var (
		r  *TidePolicyReconciler
		ns string
	)

	newNS := func() string {
		name := fmt.Sprintf("tp-%d", nsCounter.Add(1))
		Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}})).To(Succeed())
		return name
	}
	policyIn := func(namespace, name string, sel map[string]string) *v1.TidePolicy {
		p := minimalPolicy(name)
		p.Namespace = namespace
		p.Spec.Selector.MatchLabels = sel
		p.Spec.Selector.Kinds = []v1.TargetKind{{APIVersion: appsAPIVersion, Kind: deploymentKind}}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		return p
	}
	deploy := func(namespace, name string, labels map[string]string) *appsv1.Deployment {
		d := &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
			Spec: appsv1.DeploymentSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
				},
			},
		}
		Expect(k8sClient.Create(ctx, d)).To(Succeed())
		return d
	}
	reconcileP := func(p *v1.TidePolicy) reconcile.Result {
		res, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(p)})
		Expect(err).NotTo(HaveOccurred())
		return res
	}
	// reconcileP twice: the first pass adds the finalizer, the second does the work.
	settle := func(ps ...*v1.TidePolicy) {
		for range 2 {
			for _, p := range ps {
				reconcileP(p)
			}
		}
	}
	twKey := func(namespace, dep string) types.NamespacedName {
		return types.NamespacedName{Namespace: namespace, Name: workload.TideWorkloadName(
			v1.TargetRef{APIVersion: appsAPIVersion, Kind: deploymentKind, Name: dep})}
	}
	getTW := func(namespace, dep string) (*v1.TideWorkload, error) {
		tw := &v1.TideWorkload{}
		return tw, k8sClient.Get(ctx, twKey(namespace, dep), tw)
	}
	cond := func(conds []metav1.Condition, t string) *metav1.Condition {
		return meta.FindStatusCondition(conds, t)
	}
	teardown := func(ps ...*v1.TidePolicy) {
		for _, p := range ps {
			_ = k8sClient.Delete(ctx, p)
			_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(p)})
		}
	}

	BeforeEach(func() {
		r = &TidePolicyReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		ns = newNS()
	})

	It("creates a TideWorkload per selected Deployment with label and ownerRef", func() {
		p := policyIn(ns, "pol", map[string]string{tierKey: webName})
		DeferCleanup(teardown, p)
		d := deploy(ns, webName, map[string]string{tierKey: webName})
		deploy(ns, "other", nil)
		settle(p)

		tw, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())
		Expect(tw.Labels).To(HaveKeyWithValue(v1.LabelPolicy, ns+".pol"))
		Expect(tw.Spec.PolicyRef).To(Equal(v1.PolicyRef{Namespace: ns, Name: "pol"}))
		Expect(tw.Spec.TargetRef).To(Equal(v1.TargetRef{APIVersion: appsAPIVersion, Kind: deploymentKind, Name: webName}))
		Expect(tw.OwnerReferences).To(HaveLen(1))
		or := tw.OwnerReferences[0]
		Expect(or.UID).To(Equal(d.UID))
		Expect(or.Kind).To(Equal(deploymentKind))
		Expect(or.Controller).To(HaveValue(BeFalse()))
		Expect(or.BlockOwnerDeletion).To(HaveValue(BeFalse()))
		_, err = getTW(ns, "other")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(p), p)).To(Succeed())
		Expect(p.Finalizers).To(ContainElement("tidewatch.dev/cleanup"))
		Expect(p.Status.SelectedWorkloads).To(Equal(int32(1)))
		Expect(cond(p.Status.Conditions, v1.CondReady)).To(HaveField("Status", metav1.ConditionTrue))
		Expect(cond(p.Status.Conditions, v1.CondReady)).To(HaveField("Reason", "resolved"))
		Expect(cond(p.Status.Conditions, v1.CondConflict)).To(HaveField("Status", metav1.ConditionFalse))
		Expect(cond(tw.Status.Conditions, v1.CondConflict)).To(HaveField("Status", metav1.ConditionFalse))
		Expect(cond(tw.Status.Conditions, v1.CondPaused)).To(HaveField("Status", metav1.ConditionFalse))
	})

	It("deletes the TideWorkload when the Deployment is relabeled out", func() {
		p := policyIn(ns, "pol", map[string]string{tierKey: webName})
		DeferCleanup(teardown, p)
		d := deploy(ns, webName, map[string]string{tierKey: webName})
		settle(p)
		_, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())

		d.Labels = map[string]string{tierKey: "batch"}
		Expect(k8sClient.Update(ctx, d)).To(Succeed())
		settle(p)
		_, err = getTW(ns, webName)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("deletes the TideWorkload when the Deployment is deleted, and the engine attempts no scale on it", func() {
		p := policyIn(ns, "pol", nil)
		DeferCleanup(teardown, p)
		d := deploy(ns, webName, nil)
		settle(p)
		_, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Delete(ctx, d)).To(Succeed())
		settle(p)
		_, err = getTW(ns, webName)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())

		spy := &spyScale{}
		e := engine.New(k8sClient, engine.Options{
			Scale: spy,
			PromFactory: func(string, time.Duration) (prom.Querier, error) {
				return nil, errors.New("no prometheus in tests")
			},
		})
		Expect(func() { e.Step(ctx) }).NotTo(Panic())
		Expect(spy.sets.Load()).To(BeZero())
	})

	It("marks Conflict on both policies when a Deployment matches two", func() {
		p1 := policyIn(ns, "pol-a", nil)
		p2 := policyIn(ns, "pol-b", nil)
		DeferCleanup(teardown, p1, p2)
		deploy(ns, webName, nil)
		settle(p1, p2)

		for _, p := range []*v1.TidePolicy{p1, p2} {
			Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(p), p)).To(Succeed())
			c := cond(p.Status.Conditions, v1.CondConflict)
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
			Expect(c.Message).To(ContainSubstring("multiple_policies"))
		}
		tw, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())
		c := cond(tw.Status.Conditions, v1.CondConflict)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Reason).To(Equal("conflict"))
		Expect(c.Message).To(ContainSubstring("multiple_policies:"))
	})

	It("marks Conflict when an HPA targets the Deployment", func() {
		p := policyIn(ns, "pol", nil)
		DeferCleanup(teardown, p)
		deploy(ns, webName, nil)
		hpa := &autoscalingv2.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "web-hpa", Namespace: ns},
			Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: appsAPIVersion, Kind: deploymentKind, Name: webName},
				MaxReplicas:    3,
			},
		}
		Expect(k8sClient.Create(ctx, hpa)).To(Succeed())
		settle(p)

		tw, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())
		c := cond(tw.Status.Conditions, v1.CondConflict)
		Expect(c).NotTo(BeNil())
		Expect(c.Status).To(Equal(metav1.ConditionTrue))
		Expect(c.Message).To(Equal("hpa:web-hpa"))
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(p), p)).To(Succeed())
		Expect(cond(p.Status.Conditions, v1.CondConflict)).To(HaveField("Status", metav1.ConditionTrue))
	})

	It("cleans up TideWorkloads across namespaces on policy deletion", func() {
		other := newNS()
		p := policyIn(ns, "pol", nil)
		p.Spec.Selector.Namespaces = []string{ns, other}
		Expect(k8sClient.Update(ctx, p)).To(Succeed())
		deploy(ns, webName, nil)
		deploy(other, "api", nil)
		settle(p)
		_, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())
		_, err = getTW(other, "api")
		Expect(err).NotTo(HaveOccurred())

		Expect(k8sClient.Delete(ctx, p)).To(Succeed())
		reconcileP(p)
		_, err = getTW(ns, webName)
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		_, err = getTW(other, "api")
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(p), &v1.TidePolicy{})
		Expect(apierrors.IsNotFound(err)).To(BeTrue())
	})

	It("sets Paused when the workload has tidewatch.dev/paused=true", func() {
		p := policyIn(ns, "pol", nil)
		DeferCleanup(teardown, p)
		d := deploy(ns, webName, nil)
		d.Annotations = map[string]string{v1.AnnoPaused: "true"}
		Expect(k8sClient.Update(ctx, d)).To(Succeed())
		deploy(ns, "free", nil)
		settle(p)

		tw, err := getTW(ns, webName)
		Expect(err).NotTo(HaveOccurred())
		Expect(cond(tw.Status.Conditions, v1.CondPaused)).To(HaveField("Status", metav1.ConditionTrue))
		tw, err = getTW(ns, "free")
		Expect(err).NotTo(HaveOccurred())
		Expect(cond(tw.Status.Conditions, v1.CondPaused)).To(HaveField("Status", metav1.ConditionFalse))
	})

	It("requeues periodically so kinds without a watch are re-resolved", func() {
		p := policyIn(ns, "pol", nil)
		DeferCleanup(teardown, p)
		Expect(reconcileP(p).RequeueAfter).To(Equal(30 * time.Second))
	})
})
