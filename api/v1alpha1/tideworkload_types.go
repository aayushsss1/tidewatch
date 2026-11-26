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

// TideWorkloadSpec binds a workload to its policy. It is operator-owned.
type TideWorkloadSpec struct {
	// +required
	PolicyRef PolicyRef `json:"policyRef"`
	// +required
	TargetRef TargetRef `json:"targetRef"`
}

// TimeToReadyStatus summarizes observed pod time-to-ready.
type TimeToReadyStatus struct {
	// +optional
	P50Seconds int32 `json:"p50Seconds,omitempty"`
	// +optional
	P90Seconds int32 `json:"p90Seconds,omitempty"`
	// +optional
	Samples int32 `json:"samples,omitempty"`
}

// ModelForecast is one model's forecast summary over the horizon.
type ModelForecast struct {
	Model string `json:"model"`
	// +optional
	Peak string `json:"peak,omitempty"`
	// +optional
	PeakTime *metav1.Time `json:"peakTime,omitempty"`
	// +optional
	Lower string `json:"lower,omitempty"`
	// +optional
	Upper string `json:"upper,omitempty"`
}

// ModelAccuracy is one model's measured accuracy at a lead time.
type ModelAccuracy struct {
	Model string `json:"model"`
	// +optional
	LeadSeconds int32 `json:"leadSeconds,omitempty"`
	// +optional
	SMAPE string `json:"smape,omitempty"`
	// +optional
	Coverage string `json:"coverage,omitempty"`
	// +optional
	Samples int32 `json:"samples,omitempty"`
}

// TideWorkloadStatus defines the observed state of TideWorkload.
type TideWorkloadStatus struct {
	// +optional
	CurrentReplicas int32 `json:"currentReplicas"`
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`
	// +optional
	DesiredReplicas int32 `json:"desiredReplicas"`
	// +optional
	Mode PolicyMode `json:"mode,omitempty"`
	// Load is the latest observed load.
	// +optional
	Load string `json:"load,omitempty"`
	// +optional
	TrustedModel string `json:"trustedModel,omitempty"`
	// ForecastPeak is the trusted model's point peak over the horizon.
	// +optional
	ForecastPeak string `json:"forecastPeak,omitempty"`
	// +optional
	PeakTime *metav1.Time `json:"peakTime,omitempty"`
	// +optional
	TimeToReady TimeToReadyStatus `json:"timeToReady,omitzero"`
	// +optional
	Forecasts []ModelForecast `json:"forecasts,omitempty"`
	// +optional
	Accuracy []ModelAccuracy `json:"accuracy,omitempty"`
	// Decisions are newest first, at most 5.
	// +kubebuilder:validation:MaxItems=5
	// +optional
	Decisions []Decision `json:"decisions,omitempty"`
	// LastInvestigation is the newest decision of the LLM investigator, kept
	// after it rotates out of Decisions so its trace stays auditable.
	// +optional
	LastInvestigation *Decision `json:"lastInvestigation,omitempty"`
	// +kubebuilder:validation:MaxItems=3
	// +optional
	Overrides []Override `json:"overrides,omitempty"`
	// +kubebuilder:validation:MaxItems=3
	// +optional
	PendingApprovals []PendingApproval `json:"pendingApprovals,omitempty"`
	// +kubebuilder:validation:MaxItems=10
	// +optional
	RecentOutcomes []RecentOutcome `json:"recentOutcomes,omitempty"`
	// +optional
	BaselineReplicas int32 `json:"baselineReplicas,omitempty"`
	// LastAction is a human-readable summary, e.g. "scale 2→5 (baseline)".
	// +optional
	LastAction string `json:"lastAction,omitempty"`
	// +optional
	LastScaleUpTime *metav1.Time `json:"lastScaleUpTime,omitempty"`
	// +optional
	LastScaleDownTime *metav1.Time `json:"lastScaleDownTime,omitempty"`
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Kind",type=string,JSONPath=`.spec.targetRef.kind`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.status.currentReplicas`
// +kubebuilder:printcolumn:name="Desired",type=integer,JSONPath=`.status.desiredReplicas`
// +kubebuilder:printcolumn:name="Peak",type=string,JSONPath=`.status.forecastPeak`
// +kubebuilder:printcolumn:name="Model",type=string,JSONPath=`.status.trustedModel`
// +kubebuilder:printcolumn:name="Last-Action",type=string,JSONPath=`.status.lastAction`
// +kubebuilder:printcolumn:name="Mode",type=string,JSONPath=`.status.mode`
// +kubebuilder:printcolumn:name="Overrides",type=string,priority=1,JSONPath=`.status.overrides[*].minReplicas`

// TideWorkload is the Schema for the tideworkloads API
type TideWorkload struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec TideWorkloadSpec `json:"spec"`

	// +optional
	Status TideWorkloadStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TideWorkloadList contains a list of TideWorkload
type TideWorkloadList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TideWorkload `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TideWorkload{}, &TideWorkloadList{})
		return nil
	})
}
