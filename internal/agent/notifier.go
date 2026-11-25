package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// Notifier tells humans and the cluster what the agent did. Implementations
// must not block for long and must never fail the caller.
type Notifier interface {
	Pending(ctx context.Context, inc *Incident, pend map[types.UID]v1.PendingApproval)
	Applied(ctx context.Context, inc *Incident, ovs map[types.UID]v1.Override, auto bool)
	Resolved(ctx context.Context, incidentID string, verdict v1.Verdict, by string)
	Skipped(ctx context.Context, inc *Incident, reason string)
	// Storm reports a trigger storm. A nil policy means it spans all policies:
	// then only Slack and the metric are used, with no Event.
	Storm(ctx context.Context, policy *v1.PolicyRef, triggers int)
}

// NoopNotifier discards every notification.
type NoopNotifier struct{}

func (NoopNotifier) Pending(context.Context, *Incident, map[types.UID]v1.PendingApproval) {}
func (NoopNotifier) Applied(context.Context, *Incident, map[types.UID]v1.Override, bool)  {}
func (NoopNotifier) Resolved(context.Context, string, v1.Verdict, string)                 {}
func (NoopNotifier) Skipped(context.Context, *Incident, string)                           {}
func (NoopNotifier) Storm(context.Context, *v1.PolicyRef, int)                            {}

// slackTimeout bounds every webhook call.
const slackTimeout = 5 * time.Second

const (
	maxSlackReason  = 300
	maxStormEvents  = 10
	maxKnownPending = 256

	reasonOverrideProposed = "OverrideProposed"
	reasonOverrideApplied  = "OverrideApplied"
	reasonOverrideApproved = "OverrideApproved"
	reasonOverrideRejected = "OverrideRejected"
	reasonOverrideExpired  = "OverrideExpired"
	reasonSkippedEvent     = "InvestigationSkipped"
	reasonStormEvent       = "StormDetected"
)

type workloadKey struct {
	ns, name string
	uid      types.UID
}

type notifier struct {
	rec          record.EventRecorder
	url          string
	notifyOnAuto func(v1.PolicyRef) bool
	http         *http.Client

	mu    sync.Mutex
	known map[string][]workloadKey // pending incident id -> workloads, for follow-up events
	order []string
}

// NewNotifier returns the Notifier that emits Kubernetes Events through rec
// and, when webhookURL is set, posts summaries to a Slack incoming webhook.
// notifyOnAuto says whether auto-applied overrides of a policy get an FYI.
func NewNotifier(rec record.EventRecorder, webhookURL string, notifyOnAuto func(policy v1.PolicyRef) bool) Notifier {
	registerAgentMetrics()
	if notifyOnAuto == nil {
		notifyOnAuto = func(v1.PolicyRef) bool { return false }
	}
	return &notifier{
		rec: rec, url: webhookURL, notifyOnAuto: notifyOnAuto,
		http:  &http.Client{Timeout: slackTimeout},
		known: map[string][]workloadKey{},
	}
}

func (n *notifier) twObject(w workloadKey) *v1.TideWorkload {
	return &v1.TideWorkload{ObjectMeta: metav1.ObjectMeta{Namespace: w.ns, Name: w.name, UID: w.uid}}
}

func memberKeys(inc *Incident, uids []types.UID) []workloadKey {
	out := make([]workloadKey, 0, len(uids))
	for _, uid := range uids {
		if m := inc.Members[uid]; m != nil {
			out = append(out, workloadKey{ns: m.Workload.Namespace, name: m.Workload.Name, uid: uid})
		}
	}
	return out
}

func (n *notifier) remember(id string, ws []workloadKey) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.known[id]; !ok {
		n.order = append(n.order, id)
		if len(n.order) > maxKnownPending {
			delete(n.known, n.order[0])
			n.order = n.order[1:]
		}
	}
	n.known[id] = ws
}

func (n *notifier) forget(id string) []workloadKey {
	n.mu.Lock()
	defer n.mu.Unlock()
	ws := n.known[id]
	delete(n.known, id)
	n.order = slices.DeleteFunc(n.order, func(s string) bool { return s == id })
	return ws
}

// post sends text to Slack. Failures are logged, never returned.
func (n *notifier) post(ctx context.Context, text string) {
	if n.url == "" {
		return
	}
	log := logf.FromContext(ctx).WithName("notifier")
	body, _ := json.Marshal(map[string]string{"text": text})
	ctx, cancel := context.WithTimeout(ctx, slackTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.url, bytes.NewReader(body))
	if err != nil {
		log.Error(err, "Building Slack request")
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.http.Do(req)
	if err != nil {
		// A *url.Error carries the full webhook URL, which is a secret: log only its cause.
		if ue, ok := errors.AsType[*url.Error](err); ok {
			err = ue.Err
		} else {
			err = errors.New("request failed")
		}
		log.Error(err, "Posting to Slack")
		return
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode/100 != 2 {
		log.Info("Slack webhook rejected the message", "status", resp.StatusCode)
	}
}

var slackEscaper = strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;")

// slackText escapes Slack control characters, so model output cannot ping or link.
func slackText(s string) string { return slackEscaper.Replace(s) }

func window(o v1.Override) string {
	return fmt.Sprintf("%s to %s UTC", o.Start.UTC().Format("15:04"), o.End.UTC().Format("15:04"))
}

func (n *notifier) Pending(ctx context.Context, inc *Incident, pend map[types.UID]v1.PendingApproval) {
	uids := sortedUIDs(pend)
	ws := memberKeys(inc, uids)
	n.remember(inc.ID, ws)
	for _, w := range ws {
		p := pend[w.uid]
		n.rec.Eventf(n.twObject(w), corev1.EventTypeNormal, reasonOverrideProposed,
			"Agent proposes min %d replicas until %s (incident %s, approve with kubectl tidewatch approve %s)",
			p.Override.MinReplicas, p.Override.End.UTC().Format(time.RFC3339), inc.ID, inc.ID)
	}
	if len(uids) == 0 {
		return
	}
	first := pend[uids[0]]
	var b strings.Builder
	fmt.Fprintf(&b, "Tidewatch proposes scaling for incident %s\n", slackText(inc.ID))
	for _, uid := range uids {
		if m := inc.Members[uid]; m != nil {
			p := pend[uid]
			fmt.Fprintf(&b, "- %s/%s: min replicas %d, %s\n", slackText(m.Workload.Namespace),
				slackText(m.Workload.Name), p.Override.MinReplicas, window(p.Override))
		}
	}
	fmt.Fprintf(&b, "Reason: %s\nConfidence: %s\n", slackText(truncRunes(first.Reason, maxSlackReason)), first.Confidence)
	ns := ""
	if inc.Policy.Namespace != "" {
		ns = " -n " + inc.Policy.Namespace
	}
	fmt.Fprintf(&b, "kubectl tidewatch approve %s%s\nkubectl tidewatch reject %s%s", inc.ID, ns, inc.ID, ns)
	n.post(ctx, b.String())
}

func (n *notifier) Applied(ctx context.Context, inc *Incident, ovs map[types.UID]v1.Override, auto bool) {
	uids := sortedUIDs(ovs)
	for _, w := range memberKeys(inc, uids) {
		o := ovs[w.uid]
		how := "approved"
		if auto {
			how = "auto-applied"
		}
		n.rec.Eventf(n.twObject(w), corev1.EventTypeNormal, reasonOverrideApplied,
			"Override %s: min %d replicas until %s (incident %s)", how, o.MinReplicas, o.End.UTC().Format(time.RFC3339), inc.ID)
	}
	if !auto || len(uids) == 0 || !n.notifyOnAuto(inc.Policy) {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Tidewatch auto-applied scaling for incident %s\n", slackText(inc.ID))
	for _, uid := range uids {
		if m := inc.Members[uid]; m != nil {
			fmt.Fprintf(&b, "- %s/%s: min replicas %d, %s\n", slackText(m.Workload.Namespace),
				slackText(m.Workload.Name), ovs[uid].MinReplicas, window(ovs[uid]))
		}
	}
	n.post(ctx, strings.TrimRight(b.String(), "\n"))
}

func (n *notifier) Resolved(ctx context.Context, incidentID string, verdict v1.Verdict, by string) {
	reason, evType, word := reasonOverrideExpired, corev1.EventTypeNormal, string(verdict)
	switch verdict {
	case v1.VerdictApplied:
		reason, word = reasonOverrideApproved, "approved"
	case v1.VerdictRejected:
		reason, evType, word = reasonOverrideRejected, corev1.EventTypeWarning, "rejected"
	case v1.VerdictSuperseded:
		word = "superseded"
	case v1.VerdictExpired:
		word = "expired"
	}
	for _, w := range n.forget(incidentID) {
		n.rec.Eventf(n.twObject(w), evType, reason, "Proposal for incident %s %s", incidentID, word)
	}
	msg := fmt.Sprintf("Tidewatch proposal for incident %s %s", slackText(incidentID), word)
	if by != "" && (verdict == v1.VerdictApplied || verdict == v1.VerdictRejected) {
		msg += " by " + slackText(by)
	}
	n.post(ctx, msg)
}

func (n *notifier) Skipped(_ context.Context, inc *Incident, reason string) {
	keys := memberKeys(inc, sortedUIDs(inc.Members))
	for _, w := range keys[:min(len(keys), maxStormEvents)] {
		n.rec.Eventf(n.twObject(w), corev1.EventTypeNormal, reasonSkippedEvent,
			"Investigation of incident %s skipped: %s", inc.ID, reason)
	}
}

func (n *notifier) Storm(ctx context.Context, policy *v1.PolicyRef, triggers int) {
	stormGauge.Set(1)
	if policy != nil {
		n.rec.Eventf(&v1.TidePolicy{ObjectMeta: metav1.ObjectMeta{Namespace: policy.Namespace, Name: policy.Name}},
			corev1.EventTypeWarning, reasonStormEvent, "Trigger storm: %d triggers within 2 minutes", triggers)
	}
	n.post(ctx, fmt.Sprintf("Tidewatch storm mode: %d triggers within 2 minutes; investigating once for all policies", triggers))
}

func truncRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}
