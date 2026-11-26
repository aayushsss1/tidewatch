package overrides

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

func ov(id, incident string, min int32, start, end time.Time) v1.Override {
	return v1.Override{
		ID: id, IncidentID: incident, MinReplicas: min,
		Start: metav1.NewTime(start), End: metav1.NewTime(end),
	}
}

func pend(incident string, min int32, start, end, expires time.Time) v1.PendingApproval {
	return v1.PendingApproval{
		IncidentID: incident, Override: ov("o-"+incident, incident, min, start, end),
		Confidence: "0.9", Expires: metav1.NewTime(expires),
	}
}

func mustAdd(t *testing.T, s *Store, o v1.Override) {
	t.Helper()
	if err := s.AddOverride("u", o); err != nil {
		t.Fatalf("AddOverride: %v", err)
	}
}

func mustPend(t *testing.T, s *Store, p v1.PendingApproval) {
	t.Helper()
	if _, err := s.AddPending("u", p); err != nil {
		t.Fatalf("AddPending: %v", err)
	}
}

func TestStore_ActiveTakesMaxWithinWindow(t *testing.T) {
	s := NewStore()
	mustAdd(t, s, ov("a", "i1", 4, t0, t0.Add(time.Hour)))
	mustAdd(t, s, ov("b", "i2", 7, t0, t0.Add(30*time.Minute)))
	m, o := s.Active("u", t0.Add(time.Minute))
	if m != 7 || o == nil || o.ID != "b" {
		t.Fatalf("got %d %+v", m, o)
	}
	m, o = s.Active("u", t0.Add(45*time.Minute)) // b has ended (End exclusive)
	if m != 4 || o.ID != "a" {
		t.Fatalf("got %d %+v", m, o)
	}
	if m, o = s.Active("u", t0.Add(time.Hour)); m != 0 || o != nil {
		t.Fatalf("expected none at End, got %d %+v", m, o)
	}
}

func TestStore_FutureOverrideNotActiveUntilStart(t *testing.T) {
	s := NewStore()
	mustAdd(t, s, ov("a", "i1", 5, t0.Add(10*time.Minute), t0.Add(time.Hour)))
	if m, o := s.Active("u", t0); m != 0 || o != nil {
		t.Fatalf("active too early: %d", m)
	}
	if m, _ := s.Active("u", t0.Add(10*time.Minute)); m != 5 {
		t.Fatalf("not active at start: %d", m)
	}
}

func TestStore_ApproveMovesPendingToOverride(t *testing.T) {
	s := NewStore()
	mustPend(t, s, pend("i1", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	if verdict := s.Approve("i1", "u", "alice", t0.Add(time.Minute)); verdict != v1.VerdictApplied {
		t.Fatalf("got %v", verdict)
	}
	ovs, pend, _ := s.Snapshot("u")
	if len(pend) != 0 || len(ovs) != 1 || ovs[0].ApprovedBy != "alice" || ovs[0].MinReplicas != 6 {
		t.Fatalf("snapshot: %+v %+v", ovs, pend)
	}
	if !s.TakeDirty("u") || s.TakeDirty("u") {
		t.Fatal("dirty should be set once and cleared by TakeDirty")
	}
}

func TestStore_ApproveAfterExpiryIsExpired(t *testing.T) {
	s := NewStore()
	mustPend(t, s, pend("i1", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	if verdict := s.Approve("i1", "u", "alice", t0.Add(11*time.Minute)); verdict != v1.VerdictExpired {
		t.Fatalf("got %v", verdict)
	}
	if ovs, p, _ := s.Snapshot("u"); len(ovs) != 0 || len(p) != 0 {
		t.Fatal("expired approval must not be applied or kept")
	}
	// Once Expire has swept it, the verdict is still remembered.
	mustPend(t, s, pend("i2", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	exp, _ := s.Expire(t0.Add(12 * time.Minute))
	if len(exp["u"]) != 1 {
		t.Fatalf("expected one expired pending, got %v", exp)
	}
	if v := s.Approve("i2", "u", "alice", t0.Add(13*time.Minute)); v != v1.VerdictExpired {
		t.Fatalf("got %v", v)
	}
}

func TestStore_ApproveSupersededIsSuperseded(t *testing.T) {
	s := NewStore()
	mustPend(t, s, pend("i1", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	mustAdd(t, s, ov("direct", "i1", 8, t0, t0.Add(time.Hour))) // newer outcome for the same incident
	if v := s.Approve("i1", "u", "alice", t0.Add(time.Minute)); v != v1.VerdictSuperseded {
		t.Fatalf("got %v", v)
	}
	// A re-proposal for the same incident replaces the old pending one.
	mustPend(t, s, pend("i2", 3, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	mustPend(t, s, pend("i2", 9, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	if _, p, _ := s.Snapshot("u"); len(p) != 1 || p[0].Override.MinReplicas != 9 {
		t.Fatalf("pending: %+v", p)
	}
}

func TestStore_ApproveUnknownIncidentNoop(t *testing.T) {
	s := NewStore()
	if verdict := s.Approve("nope", "u", "bob", t0); verdict != "" {
		t.Fatalf("got %q", verdict)
	}
	if s.Reject("nope", "u", "bob") {
		t.Fatal("unknown incident rejected something")
	}
}

func TestStore_RejectRemovesPending(t *testing.T) {
	s := NewStore()
	mustPend(t, s, pend("i1", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	if !s.Reject("i1", "u", "bob") {
		t.Fatal("reject found nothing")
	}
	if _, p, _ := s.Snapshot("u"); len(p) != 0 {
		t.Fatal("pending should be gone")
	}
}

func TestStore_CapsThreeOverridesAndTenOutcomes(t *testing.T) {
	s := NewStore()
	for i := range 5 {
		mustAdd(t, s, ov(string(rune('a'+i)), string(rune('a'+i)), int32(i+1), t0, t0.Add(time.Hour)))
	}
	mustAdd(t, s, ov("a2", "e", 9, t0, t0.Add(time.Hour))) // same incident as "e": newest wins
	ovs, _, _ := s.Snapshot("u")
	if len(ovs) != 3 || ovs[2].ID != "a2" {
		t.Fatalf("overrides: %+v", ovs)
	}
	for i := range ovs {
		if ovs[i].ID == "e" {
			t.Fatal("older override of the same incident must be replaced")
		}
	}
	for i := range 12 {
		s.AddOutcome("u", v1.RecentOutcome{IncidentID: "x", MinReplicas: int32(i)})
	}
	_, _, outs := s.Snapshot("u")
	if len(outs) != 10 || outs[0].MinReplicas != 11 || outs[9].MinReplicas != 2 {
		t.Fatalf("outcomes (newest first): %+v", outs)
	}
}

func TestStore_RestoreFromStatus(t *testing.T) {
	s := NewStore()
	st := v1.TideWorkloadStatus{
		Overrides:        []v1.Override{ov("a", "i1", 4, t0, t0.Add(time.Hour))},
		PendingApprovals: []v1.PendingApproval{pend("i2", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute))},
		RecentOutcomes:   []v1.RecentOutcome{{IncidentID: "i0"}},
	}
	s.Restore("u", st)
	if m, _ := s.Active("u", t0.Add(time.Minute)); m != 4 {
		t.Fatalf("got %d", m)
	}
	if s.TakeDirty("u") {
		t.Fatal("restore must not mark dirty")
	}
	// Known uid: a second restore (e.g. stale status) is a no-op.
	s.Restore("u", v1.TideWorkloadStatus{})
	if ovs, p, o := s.Snapshot("u"); len(ovs) != 1 || len(p) != 1 || len(o) != 1 {
		t.Fatalf("restore overwrote state: %v %v %v", ovs, p, o)
	}
	if v := s.Approve("i2", "u", "x", t0.Add(time.Minute)); v != v1.VerdictApplied {
		t.Fatalf("restored pending not approvable: %v", v)
	}
}

func TestStore_ClampsWindowToTwoHours(t *testing.T) {
	s := NewStore()
	mustAdd(t, s, ov("a", "i1", 4, t0, t0.Add(5*time.Hour)))
	mustPend(t, s, pend("i2", 4, t0, t0.Add(9*time.Hour), t0.Add(time.Hour)))
	ovs, p, _ := s.Snapshot("u")
	if got := ovs[0].End.Sub(ovs[0].Start.Time); got != 2*time.Hour {
		t.Fatalf("override window %v", got)
	}
	if got := p[0].Override.End.Sub(p[0].Override.Start.Time); got != 2*time.Hour {
		t.Fatalf("pending window %v", got)
	}
}

func TestStore_ZeroEndDefaultsToThirtyMinutes(t *testing.T) {
	s := NewStore()
	o := ov("a", "i1", 4, t0, time.Time{})
	o.End = metav1.Time{}
	mustAdd(t, s, o)
	ovs, _, _ := s.Snapshot("u")
	if got := ovs[0].End.Sub(ovs[0].Start.Time); got != 30*time.Minute {
		t.Fatalf("window %v", got)
	}
}

func TestStore_RejectsInvalidWindow(t *testing.T) {
	s := NewStore()
	if err := s.AddOverride("u", ov("a", "i1", 4, t0, t0)); err == nil {
		t.Fatal("End == Start must be rejected")
	}
	if err := s.AddOverride("u", ov("a", "i1", 4, t0, t0.Add(-time.Minute))); err == nil {
		t.Fatal("End < Start must be rejected")
	}
	if _, err := s.AddPending("u", pend("i2", 4, t0, t0.Add(-time.Minute), t0.Add(time.Hour))); err == nil {
		t.Fatal("pending with End < Start must be rejected")
	}
	if ovs, p, _ := s.Snapshot("u"); len(ovs) != 0 || len(p) != 0 {
		t.Fatal("invalid entries must not be stored")
	}
}

func TestStore_TruncatesReasonTo300Runes(t *testing.T) {
	s := NewStore()
	long := strings.Repeat("é", 400)
	o := ov("a", "i1", 4, t0, t0.Add(time.Hour))
	o.Reason = long
	mustAdd(t, s, o)
	p := pend("i2", 4, t0, t0.Add(time.Hour), t0.Add(time.Hour))
	p.Reason, p.Override.Reason = long, long
	mustPend(t, s, p)
	ovs, ps, _ := s.Snapshot("u")
	for _, r := range []string{ovs[0].Reason, ps[0].Reason, ps[0].Override.Reason} {
		if n := len([]rune(r)); n != 300 {
			t.Fatalf("reason has %d runes", n)
		}
	}
}

func TestStore_ExpireReturnsEndedAndExpired(t *testing.T) {
	s := NewStore()
	mustAdd(t, s, ov("a", "i1", 4, t0, t0.Add(10*time.Minute)))
	mustPend(t, s, pend("i2", 6, t0, t0.Add(time.Hour), t0.Add(5*time.Minute)))
	exp, ended := s.Expire(t0.Add(11 * time.Minute))
	if len(exp["u"]) != 1 || len(ended["u"]) != 1 || ended["u"][0].ID != "a" {
		t.Fatalf("%v %v", exp, ended)
	}
	if ovs, p, _ := s.Snapshot("u"); len(ovs) != 0 || len(p) != 0 {
		t.Fatal("expected both removed")
	}
	s.Forget("u")
	if s.TakeDirty("u") {
		t.Fatal("forget clears dirty")
	}
}

func TestStore_DecisionsQueueBoundedAndDrained(t *testing.T) {
	s := NewStore()
	for i := range MaxDecisions + 3 {
		s.AddDecision("u", v1.Decision{Desired: int32(i), IncidentID: "inc"})
	}
	if !s.TakeDirty("u") {
		t.Error("AddDecision must mark the workload dirty")
	}
	got := s.TakeDecisions("u")
	if len(got) != MaxDecisions || got[0].Desired != 3 || got[len(got)-1].Desired != int32(MaxDecisions+2) {
		t.Fatalf("decisions = %+v", got)
	}
	if again := s.TakeDecisions("u"); len(again) != 0 {
		t.Errorf("queue not cleared: %+v", again)
	}
	if s.TakeDecisions("unknown") != nil {
		t.Error("unknown uid must yield nil")
	}
}

func TestStore_ApproveAndRejectActOnOneWorkload(t *testing.T) {
	s := NewStore()
	for _, uid := range []types.UID{"a", "b"} {
		if _, err := s.AddPending(uid, pend("i1", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if v := s.Approve("i1", "a", "alice", t0.Add(time.Minute)); v != v1.VerdictApplied {
		t.Fatalf("a: %v", v)
	}
	if ovs, p, _ := s.Snapshot("b"); len(ovs) != 0 || len(p) != 1 {
		t.Fatalf("approving a must leave b pending: %+v %+v", ovs, p)
	}
	if !s.Reject("i1", "b", "bob") {
		t.Fatal("b reject")
	}
	if ovs, _, _ := s.Snapshot("a"); len(ovs) != 1 {
		t.Fatal("rejecting b must not touch a")
	}
	// Approving both works.
	for _, uid := range []types.UID{"c", "d"} {
		if _, err := s.AddPending(uid, pend("i2", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute))); err != nil {
			t.Fatal(err)
		}
		if v := s.Approve("i2", uid, "x", t0.Add(time.Minute)); v != v1.VerdictApplied {
			t.Fatalf("%s: %v", uid, v)
		}
	}
	// Tombstones are per (incident, uid).
	if _, err := s.AddPending("e", pend("i3", 6, t0, t0.Add(time.Hour), t0.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	s.Expire(t0.Add(2 * time.Minute))
	if v := s.Approve("i3", "e", "x", t0.Add(3*time.Minute)); v != v1.VerdictExpired {
		t.Fatalf("e: %v", v)
	}
	if v := s.Approve("i3", "f", "x", t0.Add(3*time.Minute)); v != "" {
		t.Fatalf("f must not inherit e's tombstone: %q", v)
	}
}

func TestStore_QueuesResolutions(t *testing.T) {
	s := NewStore()
	s.SetClock(func() time.Time { return t0 })
	add := func(inc string, uid types.UID, exp time.Time) {
		t.Helper()
		if _, err := s.AddPending(uid, pend(inc, 6, t0, t0.Add(time.Hour), exp)); err != nil {
			t.Fatal(err)
		}
	}
	add("ap", "a", t0.Add(10*time.Minute))
	s.Approve("ap", "a", "alice", t0.Add(time.Minute))
	add("rj", "a", t0.Add(10*time.Minute))
	s.Reject("rj", "a", "bob")
	add("ex", "a", t0.Add(2*time.Minute))
	add("sp", "a", t0.Add(10*time.Minute))
	add("sp", "a", t0.Add(10*time.Minute)) // supersedes the first sp
	s.Expire(t0.Add(3 * time.Minute))      // ex expires; the approved override "ap" is still active
	s.Expire(t0.Add(2 * time.Hour))        // the approved override ends, sp expires

	got := map[string][]ResolutionVerdict{}
	for _, r := range s.TakeResolutions() {
		got[r.IncidentID] = append(got[r.IncidentID], r.Verdict)
		if r.UID != "a" || r.Time.IsZero() {
			t.Fatalf("bad resolution %+v", r)
		}
	}
	want := map[string][]ResolutionVerdict{
		"ap": {ResApproved, ResEnded}, "rj": {ResRejected}, "ex": {ResExpired},
		"sp": {ResSuperseded, ResExpired},
	}
	for k, w := range want {
		if len(got[k]) != len(w) || got[k][0] != w[0] || (len(w) > 1 && got[k][1] != w[1]) {
			t.Fatalf("%s: got %v want %v", k, got[k], w)
		}
	}
	if len(s.TakeResolutions()) != 0 {
		t.Fatal("queue must be drained")
	}
	for range 300 {
		s.Reject("x", "a", "b") // no-op: nothing pending
	}
	for i := range 300 {
		add("n"+string(rune('0'+i%10)), types.UID("u"+string(rune('a'+i%26))), t0.Add(time.Hour))
		s.Reject("n"+string(rune('0'+i%10)), types.UID("u"+string(rune('a'+i%26))), "b")
	}
	if n := len(s.TakeResolutions()); n != 256 {
		t.Fatalf("queue should be bounded at 256, got %d", n)
	}
}

func TestStore_AwaitingOutcome(t *testing.T) {
	s := NewStore()
	if s.AwaitingOutcome("i1", "u") {
		t.Fatal("nothing created yet")
	}
	mustPend(t, s, pend("i1", 6, t0, t0.Add(time.Hour), t0.Add(10*time.Minute)))
	if s.AwaitingOutcome("i1", "u") {
		t.Fatal("a pending approval is not an override yet")
	}
	s.Approve("i1", "u", "alice", t0.Add(time.Minute))
	if !s.AwaitingOutcome("i1", "u") {
		t.Fatal("approved override must await its outcome")
	}
	s.Expire(t0.Add(2 * time.Hour)) // the override ended, but it is not scored yet
	if !s.AwaitingOutcome("i1", "u") || s.AwaitingOutcome("i1", "other") {
		t.Fatal("ended override still awaits scoring, only for its workload")
	}
	s.AddOutcome("u", v1.RecentOutcome{IncidentID: "i1", Verdict: v1.VerdictApplied}) // unscored entry
	if !s.AwaitingOutcome("i1", "u") {
		t.Fatal("an outcome without a score does not count")
	}
	s.AddOutcome("u", v1.RecentOutcome{IncidentID: "i1", Verdict: v1.VerdictApplied, Outcome: &v1.Outcome{}})
	if s.AwaitingOutcome("i1", "u") {
		t.Fatal("scored")
	}
}

func TestStore_ReleaseEndsOverrideOnceWithResolution(t *testing.T) {
	s := NewStore()
	mustAdd(t, s, ov("a", "i1", 4, t0, t0.Add(time.Hour)))
	now := t0.Add(15 * time.Minute)
	if !s.Release("u", "a", now) {
		t.Fatal("Release of an active override must succeed")
	}
	if m, o := s.Active("u", now); m != 0 || o != nil {
		t.Fatalf("released override still active: %d %+v", m, o)
	}
	ovs, _, _ := s.Snapshot("u")
	if len(ovs) != 1 || !ovs[0].End.Time.Equal(now) {
		t.Fatalf("End must be set to now, got %+v", ovs)
	}
	if !s.TakeDirty("u") {
		t.Fatal("Release must mark the workload dirty")
	}
	res := s.TakeResolutions()
	if len(res) != 1 || res[0].Verdict != ResEnded || res[0].By != "early_release" || res[0].IncidentID != "i1" {
		t.Fatalf("resolutions = %+v", res)
	}
	// The later Expire sweep drops it without a second resolution.
	s.Expire(now.Add(time.Second))
	if res := s.TakeResolutions(); len(res) != 0 {
		t.Fatalf("Expire duplicated the resolution: %+v", res)
	}
	if ovs, _, _ := s.Snapshot("u"); len(ovs) != 0 {
		t.Fatalf("released override must be swept, got %+v", ovs)
	}
	if s.Release("u", "a", now) || s.Release("u", "nope", now) {
		t.Fatal("releasing an ended or unknown override must report false")
	}
}
