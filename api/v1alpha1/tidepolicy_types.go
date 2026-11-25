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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// TidePolicySpec defines the desired state of TidePolicy
type TidePolicySpec struct {
	// +kubebuilder:default=Shadow
	// +optional
	Mode PolicyMode `json:"mode,omitempty"`
	// +required
	Prometheus PrometheusSpec `json:"prometheus"`
	// +required
	Selector WorkloadSelector `json:"selector"`
	// +required
	Load LoadSpec `json:"load"`
	// +kubebuilder:default={}
	// +optional
	Forecasting ForecastingSpec `json:"forecasting,omitzero"`
	// +required
	Guardrails GuardrailsSpec `json:"guardrails"`
}

// PrometheusSpec locates the Prometheus used for load history.
type PrometheusSpec struct {
	// +kubebuilder:validation:MinLength=1
	URL string `json:"url"`
	// +kubebuilder:default="5s"
	// +optional
	Timeout metav1.Duration `json:"timeout,omitzero"`
}

// WorkloadSelector chooses the workloads a policy governs.
type WorkloadSelector struct {
	// +optional
	Namespaces []string `json:"namespaces,omitempty"`
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty"`
	// LabelSelector matches labels on the workloads themselves.
	metav1.LabelSelector `json:",inline"`
	// +kubebuilder:validation:MinItems=1
	Kinds []TargetKind `json:"kinds"`
}

// LoadSpec describes how load is measured for each workload.
type LoadSpec struct {
	// +kubebuilder:validation:MinLength=1
	DefaultProfile string `json:"defaultProfile"`
	// +kubebuilder:validation:MinProperties=1
	Profiles map[string]LoadProfile `json:"profiles"`
	// +kubebuilder:default="0.8"
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	// +optional
	Headroom string `json:"headroom,omitempty"`
}

// LoadProfile is a named load query and the capacity of one replica.
type LoadProfile struct {
	// +kubebuilder:validation:MinLength=1
	Query string `json:"query"`
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	TargetPerReplica string `json:"targetPerReplica"`
}

// ForecastingSpec configures forecasting.
type ForecastingSpec struct {
	// +kubebuilder:default="30m"
	// +optional
	Horizon metav1.Duration `json:"horizon,omitzero"`
	// +kubebuilder:default="168h"
	// +optional
	History metav1.Duration `json:"history,omitzero"`
	// +kubebuilder:default="1m"
	// +optional
	Step metav1.Duration `json:"step,omitzero"`
	// +kubebuilder:default="24h"
	// +optional
	Seasonality metav1.Duration `json:"seasonality,omitzero"`
	// +kubebuilder:default="24h"
	// +optional
	AccuracyWindow metav1.Duration `json:"accuracyWindow,omitzero"`
	// +kubebuilder:default={"holt-winters","seasonal-naive"}
	// +optional
	Models []string `json:"models,omitempty"`
}

// GuardrailsSpec bounds what the operator may do.
type GuardrailsSpec struct {
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	MinReplicas int32 `json:"minReplicas,omitempty"`
	// +kubebuilder:validation:Minimum=1
	MaxReplicas int32 `json:"maxReplicas"`
	// MaxTotalReplicas is a budget across all selected workloads.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxTotalReplicas *int32 `json:"maxTotalReplicas,omitempty"`
	// +kubebuilder:default={}
	// +optional
	ScaleUp ScaleUpPolicy `json:"scaleUp,omitzero"`
	// +kubebuilder:default={}
	// +optional
	ScaleDown ScaleDownPolicy `json:"scaleDown,omitzero"`
}

// ScaleUpPolicy limits scale-up speed.
type ScaleUpPolicy struct {
	// +kubebuilder:default=5
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxStep int32 `json:"maxStep,omitempty"`
	// +kubebuilder:default="60s"
	// +optional
	Cooldown metav1.Duration `json:"cooldown,omitzero"`
}

// ScaleDownPolicy limits scale-down speed.
type ScaleDownPolicy struct {
	// +kubebuilder:default=2
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxStep int32 `json:"maxStep,omitempty"`
	// +kubebuilder:default="300s"
	// +optional
	Cooldown metav1.Duration `json:"cooldown,omitzero"`
	// +kubebuilder:default="10m"
	// +optional
	StabilizationWindow metav1.Duration `json:"stabilizationWindow,omitzero"`
}

// TidePolicyStatus defines the observed state of TidePolicy.
type TidePolicyStatus struct {
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// +optional
	SelectedWorkloads int32 `json:"selectedWorkloads"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.spec.mode`
// +kubebuilder:printcolumn:name="Workloads",type=integer,JSONPath=`.status.selectedWorkloads`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TidePolicy is the Schema for the tidepolicies API
type TidePolicy struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec TidePolicySpec `json:"spec"`

	// +optional
	Status TidePolicyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TidePolicyList contains a list of TidePolicy
type TidePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TidePolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TidePolicy{}, &TidePolicyList{})
		return nil
	})
}
