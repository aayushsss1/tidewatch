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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	tidewatchv1alpha1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

func minimalPolicy(name string) *tidewatchv1alpha1.TidePolicy {
	return &tidewatchv1alpha1.TidePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: tidewatchv1alpha1.TidePolicySpec{
			Prometheus: tidewatchv1alpha1.PrometheusSpec{URL: "http://prometheus.monitoring.svc:9090"},
			Selector: tidewatchv1alpha1.WorkloadSelector{
				Kinds: []tidewatchv1alpha1.TargetKind{{APIVersion: appsAPIVersion, Kind: deploymentKind}},
			},
			Load: tidewatchv1alpha1.LoadSpec{
				DefaultProfile: "http",
				Profiles: map[string]tidewatchv1alpha1.LoadProfile{
					"http": {Query: "up", TargetPerReplica: "50"},
				},
			},
			Guardrails: tidewatchv1alpha1.GuardrailsSpec{MaxReplicas: 5},
		},
	}
}

var _ = Describe("CRD validation", func() {
	It("applies defaults to a minimal TidePolicy", func() {
		p := minimalPolicy("defaults")
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(p), p)).To(Succeed())
		Expect(p.Spec.Mode).To(Equal(tidewatchv1alpha1.PolicyMode("Shadow")))
		Expect(p.Spec.Load.Headroom).To(Equal("0.8"))
		Expect(p.Spec.Forecasting.Step.Duration).To(Equal(time.Minute))
		Expect(p.Spec.Forecasting.History.Duration).To(Equal(168 * time.Hour))
		Expect(p.Spec.Forecasting.Horizon.Duration).To(Equal(30 * time.Minute))
		Expect(p.Spec.Forecasting.Seasonality.Duration).To(Equal(24 * time.Hour))
		Expect(p.Spec.Forecasting.AccuracyWindow.Duration).To(Equal(24 * time.Hour))
		Expect(p.Spec.Forecasting.Models).To(Equal([]string{"holt-winters", "seasonal-naive"}))
		Expect(p.Spec.Guardrails.MinReplicas).To(HaveValue(Equal(int32(1))))
		Expect(p.Spec.Guardrails.ScaleUp.MaxStep).To(Equal(int32(5)))
		Expect(p.Spec.Guardrails.ScaleUp.Cooldown.Duration).To(Equal(60 * time.Second))
		Expect(p.Spec.Guardrails.ScaleDown.MaxStep).To(Equal(int32(2)))
		Expect(p.Spec.Guardrails.ScaleDown.Cooldown.Duration).To(Equal(300 * time.Second))
		Expect(p.Spec.Guardrails.ScaleDown.StabilizationWindow.Duration).To(Equal(10 * time.Minute))
		Expect(p.Spec.Prometheus.Timeout.Duration).To(Equal(5 * time.Second))
	})
	It("rejects a non-decimal targetPerReplica", func() {
		p := minimalPolicy("bad-decimal")
		p.Spec.Load.Profiles["http"] = tidewatchv1alpha1.LoadProfile{Query: "up", TargetPerReplica: "fifty"}
		err := k8sClient.Create(ctx, p)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got: %v", err)
		Expect(err.Error()).To(ContainSubstring("targetPerReplica"))
	})
	It("rejects an empty kinds list", func() {
		p := minimalPolicy("no-kinds")
		p.Spec.Selector.Kinds = []tidewatchv1alpha1.TargetKind{}
		err := k8sClient.Create(ctx, p)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got: %v", err)
		Expect(err.Error()).To(ContainSubstring("kinds"))
		Expect(err.Error()).To(ContainSubstring("should have at least 1 items"))
	})
	It("rejects mode outside the enum", func() {
		p := minimalPolicy("bad-mode")
		p.Spec.Mode = "Yolo"
		err := k8sClient.Create(ctx, p)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got: %v", err)
		Expect(err.Error()).To(ContainSubstring("mode"))
	})
	It("rejects forecasting history/step above the Prometheus point limit", func() {
		p := minimalPolicy("too-many-points")
		p.Spec.Forecasting.History = metav1.Duration{Duration: 168 * time.Hour}
		p.Spec.Forecasting.Step = metav1.Duration{Duration: 30 * time.Second} // 20160 points
		err := k8sClient.Create(ctx, p)
		Expect(apierrors.IsInvalid(err)).To(BeTrue(), "got: %v", err)
		Expect(err.Error()).To(ContainSubstring("11000"))
	})
	It("accepts forecasting history/step at the Prometheus point limit", func() {
		p := minimalPolicy("max-points")
		p.Spec.Forecasting.History = metav1.Duration{Duration: 11000 * time.Minute}
		p.Spec.Forecasting.Step = metav1.Duration{Duration: time.Minute}
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
	})
	It("allows minReplicas 0", func() {
		p := minimalPolicy("min-zero")
		p.Spec.Guardrails.MinReplicas = new(int32(0))
		Expect(k8sClient.Create(ctx, p)).To(Succeed())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(p), p)).To(Succeed())
		Expect(p.Spec.Guardrails.MinReplicas).To(HaveValue(Equal(int32(0))))
	})
})
