package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/pkg/llm"
)

const testInc7 = "inc-7"

type slackSink struct {
	mu     sync.Mutex
	bodies []string
	ctype  string
}

func (s *slackSink) handler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]string
		_ = json.Unmarshal(b, &m)
		s.mu.Lock()
		s.bodies = append(s.bodies, m["text"])
		s.ctype = r.Header.Get("Content-Type")
		s.mu.Unlock()
		w.WriteHeader(status)
	}
}

func notifIncident() (*Incident, map[types.UID]v1.PendingApproval) {
	inc := &Incident{
		ID: testInc7, Policy: v1.PolicyRef{Namespace: "shop", Name: "pol"},
		Members: map[types.UID]*Member{
			"u1": {UID: "u1", Workload: types.NamespacedName{Namespace: "shop", Name: "frontend"},
				Triggers: []signal.Trigger{{Type: signal.ForecastBreach}}},
		},
	}
	start := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	pend := map[types.UID]v1.PendingApproval{"u1": {
		IncidentID: testInc7, Confidence: "0.80", Reason: strings.Repeat("r", 500) + "<!channel>",
		Expires: metav1.NewTime(start.Add(2 * time.Minute)),
		Override: v1.Override{IncidentID: testInc7, MinReplicas: 9, Start: metav1.NewTime(start),
			End: metav1.NewTime(start.Add(30 * time.Minute))},
	}}
	return inc, pend
}

func TestNotifier_PendingPostsSlackWithApproveCommand(t *testing.T) {
	sink := &slackSink{}
	srv := httptest.NewServer(sink.handler(200))
	defer srv.Close()
	n := NewNotifier(record.NewFakeRecorder(10), srv.URL, nil)
	inc, pend := notifIncident()

	n.Pending(context.Background(), inc, pend)

	if len(sink.bodies) != 1 || sink.ctype != "application/json" {
		t.Fatalf("posts = %d, content-type %q", len(sink.bodies), sink.ctype)
	}
	text := sink.bodies[0]
	for _, want := range []string{testInc7, "shop/frontend", "min replicas 9", "12:00 to 12:30 UTC", "Confidence: 0.80",
		"kubectl tidewatch approve inc-7", "kubectl tidewatch reject inc-7"} {
		if !strings.Contains(text, want) {
			t.Errorf("message missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "<!channel>") || strings.Contains(text, strings.Repeat("r", 301)) {
		t.Errorf("reason not truncated/escaped:\n%s", text)
	}
}

func TestNotifier_NoWebhookNoPost(t *testing.T) {
	sink := &slackSink{}
	srv := httptest.NewServer(sink.handler(200))
	defer srv.Close()
	n := NewNotifier(record.NewFakeRecorder(10), "", func(v1.PolicyRef) bool { return true })
	inc, pend := notifIncident()
	n.Pending(context.Background(), inc, pend)
	n.Applied(context.Background(), inc, map[types.UID]v1.Override{"u1": pend["u1"].Override}, true)
	n.Resolved(context.Background(), testInc7, v1.VerdictExpired, "")
	n.Storm(context.Background(), 25)
	if len(sink.bodies) != 0 {
		t.Fatalf("posted %d messages without a webhook", len(sink.bodies))
	}
}

func TestNotifier_SlackFailureDoesNotPanicOrReturn(t *testing.T) {
	sink := &slackSink{}
	srv := httptest.NewServer(sink.handler(500))
	inc, pend := notifIncident()
	n := NewNotifier(record.NewFakeRecorder(10), srv.URL, nil)
	n.Pending(context.Background(), inc, pend) // 500 response
	if len(sink.bodies) != 1 {
		t.Fatalf("want one attempt, got %d", len(sink.bodies))
	}
	srv.Close()
	n.Pending(context.Background(), inc, pend) // connection refused
	n.Storm(context.Background(), 30)
}

func TestNotifier_EmitsEvents(t *testing.T) {
	rec := record.NewFakeRecorder(20)
	n := NewNotifier(rec, "", nil)
	inc, pend := notifIncident()
	ctx := context.Background()
	ovs := map[types.UID]v1.Override{"u1": pend["u1"].Override}

	n.Pending(ctx, inc, pend)
	n.Applied(ctx, inc, ovs, false)
	n.Resolved(ctx, testInc7, v1.VerdictApplied, "alice")
	n.Skipped(ctx, inc, "budget_exhausted")
	n.Pending(ctx, inc, pend)
	n.Resolved(ctx, testInc7, v1.VerdictRejected, "bob")
	n.Pending(ctx, inc, pend)
	n.Resolved(ctx, testInc7, v1.VerdictExpired, "")
	n.(*notifier).StormOnPolicy(inc.Policy, 25)

	close(rec.Events)
	var got []string
	for e := range rec.Events {
		got = append(got, e)
	}
	all := strings.Join(got, "\n")
	for _, want := range []string{"Normal OverrideProposed", "Normal OverrideApplied", "Normal OverrideApproved",
		"Normal InvestigationSkipped", "Warning OverrideRejected", "Normal OverrideExpired", "Warning StormDetected"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing event %q in:\n%s", want, all)
		}
	}
}

func TestNotifier_AutoFYIOnlyWhenEnabled(t *testing.T) {
	sink := &slackSink{}
	srv := httptest.NewServer(sink.handler(200))
	defer srv.Close()
	enabled := false
	n := NewNotifier(record.NewFakeRecorder(10), srv.URL, func(p v1.PolicyRef) bool { return enabled && p.Name == "pol" })
	inc, pend := notifIncident()
	ovs := map[types.UID]v1.Override{"u1": pend["u1"].Override}
	ctx := context.Background()

	n.Applied(ctx, inc, ovs, true)
	if len(sink.bodies) != 0 {
		t.Fatal("FYI sent while disabled")
	}
	enabled = true
	n.Applied(ctx, inc, ovs, false) // approved by a human: no FYI
	if len(sink.bodies) != 0 {
		t.Fatal("FYI sent for a manual approval")
	}
	n.Applied(ctx, inc, ovs, true)
	if len(sink.bodies) != 1 || !strings.Contains(sink.bodies[0], "auto-applied") {
		t.Fatalf("bodies = %v", sink.bodies)
	}
}

func TestMetrics_RegisterOnceAndLabels(t *testing.T) {
	NewNotifier(record.NewFakeRecorder(1), "", nil)
	NewNotifier(record.NewFakeRecorder(1), "", nil) // must not panic on re-registration
	inc, _ := notifIncident()
	if got := triggerLabel(inc); got != "ForecastBreach" {
		t.Errorf("trigger label = %q", got)
	}
	if got := triggerLabel(&Incident{Storm: true}); got != "storm" {
		t.Errorf("storm label = %q", got)
	}
	for r, want := range map[string]Result{
		"skipped_too_late": {Skipped: "too_late"}, "deadline": {Exit: llm.ExitDeadline},
		"pending": {Exit: llm.ExitTerminal, Verdict: v1.VerdictPending}, reasonNoAction: {Exit: llm.ExitTerminal, Reasons: []string{reasonNoAction}},
	} {
		if got := resultLabel(want); got != r {
			t.Errorf("resultLabel(%+v) = %q, want %q", want, got, r)
		}
	}
}
