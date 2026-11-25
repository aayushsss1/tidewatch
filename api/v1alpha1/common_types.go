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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// PolicyMode selects whether a policy only records decisions or enforces them.
// +kubebuilder:validation:Enum=Shadow;Active
type PolicyMode string

const (
	// ModeShadow records decisions without changing replicas.
	ModeShadow PolicyMode = "Shadow"
	// ModeActive enforces decisions on the scale subresource.
	ModeActive PolicyMode = "Active"
)

// TargetKind identifies a workload kind a policy may select.
type TargetKind struct {
	// +kubebuilder:validation:MinLength=1
	APIVersion string `json:"apiVersion"`
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`
}

// TargetRef points at a scalable workload.
type TargetRef struct {
	// +kubebuilder:validation:MinLength=1
	APIVersion string `json:"apiVersion"`
	// +kubebuilder:validation:MinLength=1
	Kind string `json:"kind"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// PolicyRef points at the TidePolicy governing a workload.
type PolicyRef struct {
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`
}

// DecisionSource says which component produced a decision.
type DecisionSource string

const (
	// SourceBaseline is the deterministic forecast-driven baseline.
	SourceBaseline DecisionSource = "baseline"
)

// Verdict is the enforcer's outcome for a decision.
type Verdict string

const (
	VerdictApplied  Verdict = "applied"
	VerdictClamped  Verdict = "clamped"
	VerdictShadow   Verdict = "shadow"
	VerdictHeld     Verdict = "held"
	VerdictRejected Verdict = "rejected"
)

// Decision records one scaling decision and what the enforcer did with it.
type Decision struct {
	Time   metav1.Time    `json:"time"`
	Source DecisionSource `json:"source"`
	// Desired is the replica count before enforcement.
	Desired int32 `json:"desired"`
	// Final is the replica count after enforcement.
	Final   int32   `json:"final"`
	Verdict Verdict `json:"verdict"`
	// Reasons lists enforcement reasons, e.g. max_step, cooldown, budget,
	// external_change, metrics_unavailable.
	// +optional
	Reasons []string `json:"reasons,omitempty"`
}

// Condition types and annotation/label keys.
const (
	CondForecasting   = "Forecasting"
	CondConflict      = "Conflict"
	CondPaused        = "Paused"
	CondBudgetLimited = "BudgetLimited"
	CondReady         = "Ready"

	AnnoPaused       = "tidewatch.dev/paused"
	AnnoMinReplicas  = "tidewatch.dev/min-replicas"
	AnnoMaxReplicas  = "tidewatch.dev/max-replicas"
	AnnoTargetPerRep = "tidewatch.dev/target-per-replica"
	AnnoLoadProfile  = "tidewatch.dev/load-profile"
	// LabelPolicy is set on TideWorkloads; its value is "<ns>.<name>".
	LabelPolicy = "tidewatch.dev/policy"
)
