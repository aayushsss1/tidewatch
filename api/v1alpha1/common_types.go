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
	// SourceAgent is an override proposed by the AI agent.
	SourceAgent DecisionSource = "agent"
)

// Verdict is the enforcer's outcome for a decision.
type Verdict string

const (
	VerdictApplied  Verdict = "applied"
	VerdictClamped  Verdict = "clamped"
	VerdictShadow   Verdict = "shadow"
	VerdictHeld     Verdict = "held"
	VerdictRejected Verdict = "rejected"
	// VerdictPending awaits human approval.
	VerdictPending Verdict = "pending"
	// VerdictExpired means a pending approval timed out.
	VerdictExpired Verdict = "expired"
	// VerdictSuperseded means a newer proposal replaced this one.
	VerdictSuperseded Verdict = "superseded"
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
	// +optional
	IncidentID string `json:"incidentId,omitempty"`
	// +optional
	PromptVersion string `json:"promptVersion,omitempty"`
	// +optional
	Provider string `json:"provider,omitempty"`
	// +optional
	Model string `json:"model,omitempty"`
	// +kubebuilder:validation:MaxItems=8
	// +optional
	Trace []TraceEntry `json:"trace,omitempty"`
	// +optional
	Outcome *Outcome `json:"outcome,omitempty"`
}

// TraceEntry records one agent tool call.
type TraceEntry struct {
	Tool string `json:"tool"`
	// +kubebuilder:validation:MaxLength=200
	// +optional
	Args string `json:"args,omitempty"`
	// +optional
	ResultBytes int32 `json:"resultBytes,omitempty"`
	// +optional
	LatencyMs int32 `json:"latencyMs,omitempty"`
}

// Outcome is the retrospective score of an agent decision.
type Outcome struct {
	// Timely is nil until it can be judged.
	// +optional
	Timely *bool `json:"timely,omitempty"`
	// +optional
	Needed bool `json:"needed,omitempty"`
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	// +optional
	WastedReplicaMinutes string      `json:"wastedReplicaMinutes,omitempty"`
	ScoredAt             metav1.Time `json:"scoredAt"`
}

// Override is a time-boxed minimum replica floor.
type Override struct {
	ID          string      `json:"id"`
	IncidentID  string      `json:"incidentId"`
	MinReplicas int32       `json:"minReplicas"`
	Start       metav1.Time `json:"start"`
	End         metav1.Time `json:"end"`
	// +optional
	Reason string `json:"reason,omitempty"`
	// +optional
	ApprovedBy string `json:"approvedBy,omitempty"`
}

// PendingApproval is an agent proposal awaiting a human.
type PendingApproval struct {
	IncidentID string   `json:"incidentId"`
	Override   Override `json:"override"`
	// +kubebuilder:validation:Pattern=`^[0-9]+(\.[0-9]+)?$`
	Confidence string `json:"confidence"`
	// +optional
	Reason  string      `json:"reason,omitempty"`
	Expires metav1.Time `json:"expires"`
	// +kubebuilder:validation:MaxItems=8
	// +optional
	Trace []TraceEntry `json:"trace,omitempty"`
}

// RecentOutcome summarizes a past agent proposal and how it turned out.
type RecentOutcome struct {
	IncidentID  string      `json:"incidentId"`
	Time        metav1.Time `json:"time"`
	Verdict     Verdict     `json:"verdict"`
	MinReplicas int32       `json:"minReplicas"`
	// +optional
	Outcome *Outcome `json:"outcome,omitempty"`
}

// Condition types and annotation/label keys.
const (
	CondForecasting        = "Forecasting"
	CondConflict           = "Conflict"
	CondPaused             = "Paused"
	CondBudgetLimited      = "BudgetLimited"
	CondAnnotationsIgnored = "AnnotationsIgnored"
	CondReady              = "Ready"
	CondAgentAvailable     = "AgentAvailable"

	AnnoPaused       = "tidewatch.dev/paused"
	AnnoMinReplicas  = "tidewatch.dev/min-replicas"
	AnnoMaxReplicas  = "tidewatch.dev/max-replicas"
	AnnoTargetPerRep = "tidewatch.dev/target-per-replica"
	AnnoLoadProfile  = "tidewatch.dev/load-profile"
	AnnoPriority     = "tidewatch.dev/priority"
	AnnoApprove      = "tidewatch.dev/approve"
	AnnoReject       = "tidewatch.dev/reject"
	// LabelPolicy is set on TideWorkloads; its value is "<ns>.<name>".
	LabelPolicy = "tidewatch.dev/policy"
)
