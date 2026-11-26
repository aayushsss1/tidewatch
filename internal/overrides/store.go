// Package overrides holds the time-boxed replica floors proposed by the agent,
// the approvals waiting on a human, and the scored outcomes of past proposals.
// The engine is the only writer of TideWorkload status; the store is its
// in-memory source of truth and is restored from status after a failover.
package overrides

import (
	"errors"
	"slices"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// Invariants enforced here because the CRD intentionally has no CEL rule on status.
const (
	MaxWindow      = 2 * time.Hour
	DefaultWindow  = 30 * time.Minute
	MaxReasonLen   = 300 // runes
	MaxOverrides   = 3
	MaxPending     = 3
	MaxOutcomes    = 10
	MaxDecisions   = 10
	maxTombstones  = 128
	maxResolutions = 256
)

// ResolutionVerdict says how a pending approval or an override ended.
type ResolutionVerdict string

// Resolution verdicts.
const (
	ResApproved   ResolutionVerdict = "approved"
	ResRejected   ResolutionVerdict = "rejected"
	ResExpired    ResolutionVerdict = "expired"
	ResSuperseded ResolutionVerdict = "superseded"
	ResEnded      ResolutionVerdict = "ended"
)

// Resolution is a queued record that a proposal of one workload was settled,
// so the agent can announce it. See TakeResolutions.
type Resolution struct {
	IncidentID string
	UID        types.UID
	Verdict    ResolutionVerdict
	By         string
	Time       time.Time
}

type workloadData struct {
	overrides []v1.Override
	pending   []v1.PendingApproval
	outcomes  []v1.RecentOutcome // newest first
	decisions []v1.Decision      // oldest first, awaiting a status write
}

type tombstone struct {
	incident string
	uid      types.UID
	verdict  v1.Verdict
}

// Store is safe for concurrent use.
type Store struct {
	mu    sync.Mutex
	data  map[types.UID]*workloadData
	dirty map[types.UID]struct{}
	tombs []tombstone // verdicts of pending approvals that can no longer be applied
	res   []Resolution
	now   func() time.Time
	made  map[madeKey]struct{} // overrides ever created, by incident and workload
	rel   map[madeKey]struct{} // overrides released early and not yet swept by Expire
	madeQ []madeKey
}

type madeKey struct {
	incident string
	uid      types.UID
}

const maxMade = 512

// AwaitingOutcome reports whether an override was created for incidentID on
// uid and has no scored RecentOutcome yet. It is false for unknown workloads.
func (s *Store) AwaitingOutcome(incidentID string, uid types.UID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.made[madeKey{incidentID, uid}]; !ok {
		return false
	}
	d := s.data[uid]
	if d == nil {
		return false
	}
	return !slices.ContainsFunc(d.outcomes, func(r v1.RecentOutcome) bool {
		return r.IncidentID == incidentID && r.Outcome != nil
	})
}

// SetClock sets the time source used to stamp resolutions that carry no time
// of their own. It defaults to time.Now.
func (s *Store) SetClock(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

func (s *Store) resolve(incident string, uid types.UID, v ResolutionVerdict, by string, at time.Time) {
	if at.IsZero() {
		at = time.Now()
		if s.now != nil {
			at = s.now()
		}
	}
	s.res = append(s.res, Resolution{IncidentID: incident, UID: uid, Verdict: v, By: by, Time: at})
	if len(s.res) > maxResolutions {
		s.res = s.res[len(s.res)-maxResolutions:]
	}
}

// TakeResolutions returns the queued resolutions, oldest first, and clears the queue.
func (s *Store) TakeResolutions() []Resolution {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.res
	s.res = nil
	return out
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: map[types.UID]*workloadData{}, dirty: map[types.UID]struct{}{}}
}

func (s *Store) wl(uid types.UID) *workloadData {
	d := s.data[uid]
	if d == nil {
		d = &workloadData{}
		s.data[uid] = d
	}
	return d
}

func truncateRunes(s string, n int) string {
	if r := []rune(s); len(r) > n {
		return string(r[:n])
	}
	return s
}

// normalize enforces the window and reason invariants on o.
func normalize(o v1.Override) (v1.Override, error) {
	if o.Start.IsZero() {
		return o, errors.New("override start is required")
	}
	if o.End.IsZero() {
		o.End = metav1.NewTime(o.Start.Add(DefaultWindow))
	}
	if !o.End.After(o.Start.Time) {
		return o, errors.New("override end must be after start")
	}
	if o.End.Sub(o.Start.Time) > MaxWindow {
		o.End = metav1.NewTime(o.Start.Add(MaxWindow))
	}
	o.Reason = truncateRunes(o.Reason, MaxReasonLen)
	return o, nil
}

func (s *Store) tombstone(incident string, uid types.UID, v v1.Verdict) {
	s.tombs = append(s.tombs, tombstone{incident, uid, v})
	if len(s.tombs) > maxTombstones {
		s.tombs = s.tombs[len(s.tombs)-maxTombstones:]
	}
}

func (s *Store) verdictFor(incident string, uid types.UID) v1.Verdict {
	for _, t := range slices.Backward(s.tombs) {
		if t.incident == incident && t.uid == uid {
			return t.verdict
		}
	}
	return ""
}

// AddPending stores a proposal awaiting approval. A pending approval of the
// same incident is replaced and returned. The oldest one is dropped past three.
func (s *Store) AddPending(uid types.UID, p v1.PendingApproval) (superseded []v1.PendingApproval, err error) {
	o, err := normalize(p.Override)
	if err != nil {
		return nil, err
	}
	if p.Expires.IsZero() {
		return nil, errors.New("pending approval expiry is required")
	}
	p.Override = o
	p.Reason = truncateRunes(p.Reason, MaxReasonLen)

	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.wl(uid)
	d.pending = slices.DeleteFunc(d.pending, func(x v1.PendingApproval) bool {
		if x.IncidentID == p.IncidentID {
			superseded = append(superseded, x)
			s.resolve(x.IncidentID, uid, ResSuperseded, "", time.Time{})
			return true
		}
		return false
	})
	d.pending = append(d.pending, p)
	for len(d.pending) > MaxPending {
		s.tombstone(d.pending[0].IncidentID, uid, v1.VerdictSuperseded)
		s.resolve(d.pending[0].IncidentID, uid, ResSuperseded, "", time.Time{})
		superseded = append(superseded, d.pending[0])
		d.pending = d.pending[1:]
	}
	s.dirty[uid] = struct{}{}
	return superseded, nil
}

func (s *Store) addOverrideLocked(uid types.UID, o v1.Override) {
	if k := (madeKey{o.IncidentID, uid}); s.made == nil || !hasKey(s.made, k) {
		if s.made == nil {
			s.made = map[madeKey]struct{}{}
		}
		s.made[k] = struct{}{}
		s.madeQ = append(s.madeQ, k)
		if len(s.madeQ) > maxMade {
			delete(s.made, s.madeQ[0])
			s.madeQ = s.madeQ[1:]
		}
	}
	d := s.wl(uid)
	delete(s.rel, madeKey{o.IncidentID, uid}) // a replaced released override is never swept
	d.overrides = slices.DeleteFunc(d.overrides, func(x v1.Override) bool { return x.IncidentID == o.IncidentID })
	d.overrides = append(d.overrides, o)
	if len(d.overrides) > MaxOverrides {
		d.overrides = d.overrides[len(d.overrides)-MaxOverrides:]
	}
	s.dirty[uid] = struct{}{}
}

// AddOverride activates o directly. It keeps at most three overrides, and the
// newest wins for one incident; a pending approval of that incident is superseded.
func (s *Store) AddOverride(uid types.UID, o v1.Override) error {
	o, err := normalize(o)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.wl(uid)
	if slices.ContainsFunc(d.pending, func(p v1.PendingApproval) bool { return p.IncidentID == o.IncidentID }) {
		d.pending = slices.DeleteFunc(d.pending, func(p v1.PendingApproval) bool { return p.IncidentID == o.IncidentID })
		s.tombstone(o.IncidentID, uid, v1.VerdictSuperseded)
		s.resolve(o.IncidentID, uid, ResSuperseded, "", time.Time{})
	}
	s.addOverrideLocked(uid, o)
	return nil
}

// Approve applies uid's pending approval of incidentID, and only that one: an
// incident can span several workloads and each is approved on its own. The
// verdict is applied, expired or superseded, or empty when uid has no such
// approval and none is remembered.
func (s *Store) Approve(incidentID string, uid types.UID, by string, now time.Time) v1.Verdict {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data[uid]
	if d == nil {
		return s.verdictFor(incidentID, uid)
	}
	i := slices.IndexFunc(d.pending, func(p v1.PendingApproval) bool { return p.IncidentID == incidentID })
	if i < 0 {
		return s.verdictFor(incidentID, uid)
	}
	p := d.pending[i]
	d.pending = slices.Delete(d.pending, i, i+1)
	s.dirty[uid] = struct{}{}
	if !p.Expires.After(now) {
		s.tombstone(incidentID, uid, v1.VerdictExpired)
		s.resolve(incidentID, uid, ResExpired, "", now)
		return v1.VerdictExpired
	}
	p.Override.ApprovedBy = by
	s.addOverrideLocked(uid, p.Override)
	s.resolve(incidentID, uid, ResApproved, by, now)
	return v1.VerdictApplied
}

// Reject drops uid's pending approval of incidentID and reports whether there was one.
func (s *Store) Reject(incidentID string, uid types.UID, by string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data[uid]
	if d == nil {
		return false
	}
	n := len(d.pending)
	d.pending = slices.DeleteFunc(d.pending, func(p v1.PendingApproval) bool { return p.IncidentID == incidentID })
	if len(d.pending) == n {
		return false
	}
	s.dirty[uid] = struct{}{}
	s.resolve(incidentID, uid, ResRejected, by, time.Time{})
	return true
}

// Release ends the active override overrideID of uid early by setting its End
// to now, and queues an ended resolution by "early_release". The override stays
// until the next Expire sweep, which does not resolve it a second time. It
// reports whether an active override was released.
func (s *Store) Release(uid types.UID, overrideID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data[uid]
	if d == nil {
		return false
	}
	for i := range d.overrides {
		o := &d.overrides[i]
		if o.ID != overrideID || o.Start.After(now) || !o.End.After(now) {
			continue
		}
		o.End = metav1.NewTime(now)
		if s.rel == nil {
			s.rel = map[madeKey]struct{}{}
		}
		s.rel[madeKey{o.IncidentID, uid}] = struct{}{}
		s.dirty[uid] = struct{}{}
		s.resolve(o.IncidentID, uid, ResEnded, "early_release", now)
		return true
	}
	return false
}

// Expire removes pending approvals past their expiry and overrides past their end.
func (s *Store) Expire(now time.Time) (expiredPending map[types.UID][]v1.PendingApproval, endedOverrides map[types.UID][]v1.Override) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expiredPending = map[types.UID][]v1.PendingApproval{}
	endedOverrides = map[types.UID][]v1.Override{}
	for uid, d := range s.data {
		d.pending = slices.DeleteFunc(d.pending, func(p v1.PendingApproval) bool {
			if p.Expires.After(now) {
				return false
			}
			expiredPending[uid] = append(expiredPending[uid], p)
			s.tombstone(p.IncidentID, uid, v1.VerdictExpired)
			s.resolve(p.IncidentID, uid, ResExpired, "", now)
			return true
		})
		d.overrides = slices.DeleteFunc(d.overrides, func(o v1.Override) bool {
			if o.End.After(now) {
				return false
			}
			endedOverrides[uid] = append(endedOverrides[uid], o)
			if k := (madeKey{o.IncidentID, uid}); hasKey(s.rel, k) {
				delete(s.rel, k) // already resolved by Release
				return true
			}
			s.resolve(o.IncidentID, uid, ResEnded, "", now)
			return true
		})
		if len(expiredPending[uid]) > 0 || len(endedOverrides[uid]) > 0 {
			s.dirty[uid] = struct{}{}
		}
	}
	return expiredPending, endedOverrides
}

// Active returns the largest MinReplicas among overrides with Start <= now < End,
// and that override.
func (s *Store) Active(uid types.UID, now time.Time) (minReplicas int32, o *v1.Override) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data[uid]
	if d == nil {
		return 0, nil
	}
	for i := range d.overrides {
		c := &d.overrides[i]
		if c.Start.After(now) || !c.End.After(now) {
			continue
		}
		if o == nil || c.MinReplicas > minReplicas {
			minReplicas, o = c.MinReplicas, c.DeepCopy()
		}
	}
	return minReplicas, o
}

// AddOutcome records a scored proposal, keeping the newest ten (newest first).
func (s *Store) AddOutcome(uid types.UID, r v1.RecentOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.wl(uid)
	d.outcomes = append([]v1.RecentOutcome{*r.DeepCopy()}, d.outcomes...)
	if len(d.outcomes) > MaxOutcomes {
		d.outcomes = d.outcomes[:MaxOutcomes]
	}
	s.dirty[uid] = struct{}{}
}

// Snapshot returns copies of everything the store holds for uid.
func (s *Store) Snapshot(uid types.UID) (ovs []v1.Override, pending []v1.PendingApproval, outcomes []v1.RecentOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data[uid]
	if d == nil {
		return nil, nil, nil
	}
	for i := range d.overrides {
		ovs = append(ovs, *d.overrides[i].DeepCopy())
	}
	for i := range d.pending {
		pending = append(pending, *d.pending[i].DeepCopy())
	}
	for i := range d.outcomes {
		outcomes = append(outcomes, *d.outcomes[i].DeepCopy())
	}
	return ovs, pending, outcomes
}

// Restore loads uid's state from persisted status. It is a no-op when uid is
// already known, so a stale status never overwrites live state.
func (s *Store) Restore(uid types.UID, st v1.TideWorkloadStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[uid]; ok {
		return
	}
	d := s.wl(uid)
	for _, o := range st.Overrides {
		if o, err := normalize(o); err == nil {
			d.overrides = append(d.overrides, *o.DeepCopy())
		}
	}
	if len(d.overrides) > MaxOverrides {
		d.overrides = d.overrides[len(d.overrides)-MaxOverrides:]
	}
	for _, p := range st.PendingApprovals {
		o, err := normalize(p.Override)
		if err != nil || p.Expires.IsZero() {
			continue
		}
		p = *p.DeepCopy()
		p.Override, p.Reason = o, truncateRunes(p.Reason, MaxReasonLen)
		d.pending = append(d.pending, p)
	}
	if len(d.pending) > MaxPending {
		d.pending = d.pending[len(d.pending)-MaxPending:]
	}
	for _, r := range st.RecentOutcomes {
		d.outcomes = append(d.outcomes, *r.DeepCopy())
	}
	if len(d.outcomes) > MaxOutcomes {
		d.outcomes = d.outcomes[:MaxOutcomes]
	}
}

// TakeDirty reports whether uid changed since the last call, and clears the flag.
func (s *Store) TakeDirty(uid types.UID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dirty[uid]
	delete(s.dirty, uid)
	return ok
}

// MarkDirty flags uid for a status write, e.g. after a failed one.
func (s *Store) MarkDirty(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirty[uid] = struct{}{}
}

// Known returns the workloads the store holds state for.
func (s *Store) Known() []types.UID {
	s.mu.Lock()
	defer s.mu.Unlock()
	uids := make([]types.UID, 0, len(s.data))
	for uid := range s.data {
		uids = append(uids, uid)
	}
	return uids
}

// Forget drops everything held for uid.
func (s *Store) Forget(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, uid)
	delete(s.dirty, uid)
	for k := range s.rel {
		if k.uid == uid {
			delete(s.rel, k)
		}
	}
}

// AddDecision queues an agent decision for the engine's next status write of
// uid. At most MaxDecisions are kept; the oldest are dropped.
func (s *Store) AddDecision(uid types.UID, d v1.Decision) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.wl(uid)
	w.decisions = append(w.decisions, *d.DeepCopy())
	if len(w.decisions) > MaxDecisions {
		w.decisions = w.decisions[len(w.decisions)-MaxDecisions:]
	}
	s.dirty[uid] = struct{}{}
}

// TakeDecisions returns the queued decisions of uid, oldest first, and clears
// the queue.
func (s *Store) TakeDecisions(uid types.UID) []v1.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.data[uid]
	if d == nil || len(d.decisions) == 0 {
		return nil
	}
	out := d.decisions
	d.decisions = nil
	return out
}

func hasKey(m map[madeKey]struct{}, k madeKey) bool { _, ok := m[k]; return ok }
