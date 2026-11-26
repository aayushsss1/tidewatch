package engine

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

func statusItem(d v1.Decision, warnings ...string) *item {
	return &item{
		st: &wlState{}, pol: &v1.TidePolicy{}, decision: d, cfgOK: true,
		cfg: workload.EffectiveConfig{Warnings: warnings},
	}
}

func TestFillStatus_DoesNotDuplicateUnchangedDecisions(t *testing.T) {
	e := &Engine{}
	var s v1.TideWorkloadStatus
	d := v1.Decision{Time: metav1.NewTime(t0), Verdict: v1.VerdictHeld, Desired: 2, Final: 2, Reasons: []string{"cooldown"}}
	e.fillStatus(&s, statusItem(d))
	d.Time = metav1.NewTime(t0.Add(30 * time.Second))
	d.Desired = 3 // desired alone does not make a new entry
	e.fillStatus(&s, statusItem(d))
	if len(s.Decisions) != 1 {
		t.Fatalf("got %d decisions, want 1", len(s.Decisions))
	}
	d.Reasons = []string{"stabilization"}
	e.fillStatus(&s, statusItem(d))
	d.Verdict, d.Final = v1.VerdictApplied, 4
	e.fillStatus(&s, statusItem(d))
	if len(s.Decisions) != 3 || s.Decisions[0].Final != 4 {
		t.Fatalf("got %+v", s.Decisions)
	}
}

func TestFillStatus_AnnotationsIgnoredCondition(t *testing.T) {
	e := &Engine{}
	var s v1.TideWorkloadStatus
	e.fillStatus(&s, statusItem(v1.Decision{}, "max-replicas \"x\" is not an integer; ignored", "other"))
	var got *metav1.Condition
	for i := range s.Conditions {
		if s.Conditions[i].Type == v1.CondAnnotationsIgnored {
			got = &s.Conditions[i]
		}
	}
	if got == nil || got.Status != metav1.ConditionTrue ||
		got.Message != "max-replicas \"x\" is not an integer; ignored; other" {
		t.Fatalf("got %+v", got)
	}
	e.fillStatus(&s, statusItem(v1.Decision{}))
	for _, c := range s.Conditions {
		if c.Type == v1.CondAnnotationsIgnored && c.Status != metav1.ConditionFalse {
			t.Fatalf("got %+v", c)
		}
	}
}

func TestFillStatus_LastInvestigationSurvivesLaterDecisions(t *testing.T) {
	e := &Engine{}
	var s v1.TideWorkloadStatus
	inv := v1.Decision{
		Time: metav1.NewTime(t0), Source: v1.SourceAgent, IncidentID: "inc-1", Verdict: v1.VerdictApplied,
		PromptVersion: "m2.2", Trace: []v1.TraceEntry{{Tool: "query_range"}},
	}
	it := statusItem(v1.Decision{Time: metav1.NewTime(t0), Verdict: v1.VerdictHeld, Final: 2})
	it.events = []v1.Decision{
		{Time: metav1.NewTime(t0), Source: v1.SourceAgent, PromptVersion: "m2.1", IncidentID: "old"},
		inv,
		{Time: metav1.NewTime(t0), Source: v1.SourceAgent, IncidentID: "inc-1", Verdict: v1.VerdictExpired}, // no prompt version
	}
	e.fillStatus(&s, it)
	if s.LastInvestigation == nil || s.LastInvestigation.IncidentID != "inc-1" || s.LastInvestigation.PromptVersion != "m2.2" {
		t.Fatalf("LastInvestigation = %+v", s.LastInvestigation)
	}

	// Enough engine decisions to push the investigation out of Decisions.
	for i := range 2 * maxDecisions {
		d := v1.Decision{Time: metav1.NewTime(t0.Add(time.Duration(i+1) * time.Minute)), Verdict: v1.VerdictApplied, Final: int32(3 + i)}
		e.fillStatus(&s, statusItem(d))
	}
	for _, d := range s.Decisions {
		if d.PromptVersion != "" {
			t.Fatal("the investigation should have rotated out of Decisions")
		}
	}
	if s.LastInvestigation == nil || s.LastInvestigation.IncidentID != "inc-1" ||
		s.LastInvestigation.PromptVersion != "m2.2" || len(s.LastInvestigation.Trace) != 1 {
		t.Fatalf("LastInvestigation lost: %+v", s.LastInvestigation)
	}
}
