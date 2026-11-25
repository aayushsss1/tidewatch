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
	MaxWindow     = 2 * time.Hour
	DefaultWindow = 30 * time.Minute
	MaxReasonLen  = 300 // runes
	MaxOverrides  = 3
	MaxPending    = 3
	MaxOutcomes   = 10
	maxTombstones = 128
)

type workloadData struct {
	overrides []v1.Override
	pending   []v1.PendingApproval
	outcomes  []v1.RecentOutcome // newest first
}

type tombstone struct {
	incident string
	verdict  v1.Verdict
}

// Store is safe for concurrent use.
type Store struct {
	mu    sync.Mutex
	data  map[types.UID]*workloadData
	dirty map[types.UID]struct{}
	tombs []tombstone // verdicts of pending approvals that can no longer be applied
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

func (s *Store) tombstone(incident string, v v1.Verdict) {
	s.tombs = append(s.tombs, tombstone{incident, v})
	if len(s.tombs) > maxTombstones {
		s.tombs = s.tombs[len(s.tombs)-maxTombstones:]
	}
}

func (s *Store) verdictFor(incident string) v1.Verdict {
	for _, t := range slices.Backward(s.tombs) {
		if t.incident == incident {
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
			return true
		}
		return false
	})
	d.pending = append(d.pending, p)
	for len(d.pending) > MaxPending {
		s.tombstone(d.pending[0].IncidentID, v1.VerdictSuperseded)
		superseded = append(superseded, d.pending[0])
		d.pending = d.pending[1:]
	}
	s.dirty[uid] = struct{}{}
	return superseded, nil
}

func (s *Store) addOverrideLocked(uid types.UID, o v1.Override) {
	d := s.wl(uid)
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
		s.tombstone(o.IncidentID, v1.VerdictSuperseded)
	}
	s.addOverrideLocked(uid, o)
	return nil
}

// Approve applies the pending approval of incidentID on every workload that has
// one. The verdict is applied, expired or superseded, or empty for an unknown incident.
func (s *Store) Approve(incidentID, by string, now time.Time) (applied []types.UID, verdict v1.Verdict) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expired := false
	for uid, d := range s.data {
		i := slices.IndexFunc(d.pending, func(p v1.PendingApproval) bool { return p.IncidentID == incidentID })
		if i < 0 {
			continue
		}
		p := d.pending[i]
		d.pending = slices.Delete(d.pending, i, i+1)
		s.dirty[uid] = struct{}{}
		if !p.Expires.After(now) {
			expired = true
			continue
		}
		p.Override.ApprovedBy = by
		s.addOverrideLocked(uid, p.Override)
		applied = append(applied, uid)
	}
	slices.Sort(applied)
	switch {
	case len(applied) > 0:
		return applied, v1.VerdictApplied
	case expired:
		s.tombstone(incidentID, v1.VerdictExpired)
		return nil, v1.VerdictExpired
	}
	return nil, s.verdictFor(incidentID)
}

// Reject drops the pending approval of incidentID and returns the affected workloads.
func (s *Store) Reject(incidentID, _ string) []types.UID {
	s.mu.Lock()
	defer s.mu.Unlock()
	var uids []types.UID
	for uid, d := range s.data {
		n := len(d.pending)
		d.pending = slices.DeleteFunc(d.pending, func(p v1.PendingApproval) bool { return p.IncidentID == incidentID })
		if len(d.pending) != n {
			uids = append(uids, uid)
			s.dirty[uid] = struct{}{}
		}
	}
	slices.Sort(uids)
	return uids
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
			s.tombstone(p.IncidentID, v1.VerdictExpired)
			return true
		})
		d.overrides = slices.DeleteFunc(d.overrides, func(o v1.Override) bool {
			if o.End.After(now) {
				return false
			}
			endedOverrides[uid] = append(endedOverrides[uid], o)
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
}
