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

// EpisodeMember is a workload that moved with the lead, lagging it.
type EpisodeMember struct {
	// +kubebuilder:validation:MinLength=1
	Workload string `json:"workload"`
	// +optional
	LagSeconds int32 `json:"lagSeconds,omitempty"`
}

// EpisodeCalendar locates an episode in the weekly cycle.
type EpisodeCalendar struct {
	// +optional
	Weekday string `json:"weekday,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=23
	// +optional
	Hour int32 `json:"hour,omitempty"`
	// +optional
	Timezone string `json:"timezone,omitempty"`
}

// EpisodeContext holds operator-supplied context about an episode.
type EpisodeContext struct {
	// +kubebuilder:validation:MaxLength=200
	// +optional
	OpsNote string `json:"opsNote,omitempty"`
	// +optional
	Deploy bool `json:"deploy,omitempty"`
}

// TideEpisodeSpec defines a recorded or manually entered load episode.
// +kubebuilder:validation:XValidation:rule="!has(self.shape) || size(self.shape) == 0 || size(self.shape) == 32",message="shape must have 0 or 32 points"
type TideEpisodeSpec struct {
	// +required
	PolicyRef PolicyRef `json:"policyRef"`
	// Lead is the workload whose load rose first.
	// +kubebuilder:validation:MinLength=1
	Lead string `json:"lead"`
	// +optional
	Members []EpisodeMember `json:"members,omitempty"`
	// +required
	Start metav1.Time `json:"start"`
	// +required
	PeakAt metav1.Time `json:"peakAt"`
	// +required
	End metav1.Time `json:"end"`
	// Shape is the normalized load curve: 0 or 32 decimal points.
	// +kubebuilder:validation:items:Pattern=`^[0-9]+(\.[0-9]+)?$`
	// +optional
	Shape []string `json:"shape,omitempty"`
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	PeakRatio string `json:"peakRatio"`
	// +optional
	Calendar EpisodeCalendar `json:"calendar,omitzero"`
	// +optional
	Context EpisodeContext `json:"context,omitzero"`
	// Action is what was done during the episode.
	// +optional
	Action string `json:"action,omitempty"`
	// +optional
	Outcome *Outcome `json:"outcome,omitempty"`
	// +kubebuilder:validation:Enum=recorded;manual
	Source string `json:"source"`
	// +optional
	Notes string `json:"notes,omitempty"`
	// +optional
	HistoryCoverageHours int32 `json:"historyCoverageHours,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:name="Lead",type=string,JSONPath=`.spec.lead`
// +kubebuilder:printcolumn:name="Peak-Ratio",type=string,JSONPath=`.spec.peakRatio`
// +kubebuilder:printcolumn:name="Start",type=date,JSONPath=`.spec.start`
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// TideEpisode is the Schema for the tideepisodes API
type TideEpisode struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec TideEpisodeSpec `json:"spec"`
}

// +kubebuilder:object:root=true

// TideEpisodeList contains a list of TideEpisode
type TideEpisodeList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TideEpisode `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TideEpisode{}, &TideEpisodeList{})
		return nil
	})
}
