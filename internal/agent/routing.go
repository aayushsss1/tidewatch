package agent

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

const (
	defaultApprovalTimeout = 10 * time.Minute
	approvalStartGrace     = 2 * time.Minute
	approvalAuto           = "Auto"
)

func sortedUIDs[V any](m map[types.UID]V) []types.UID {
	uids := make([]types.UID, 0, len(m))
	for uid := range m {
		uids = append(uids, uid)
	}
	slices.Sort(uids)
	return uids
}

func (iv *Investigator) outcome(uid types.UID, incident string, verdict v1.Verdict, min int32, now time.Time) {
	iv.Store.AddOutcome(uid, v1.RecentOutcome{
		IncidentID: incident, Time: metav1.NewTime(now), Verdict: verdict, MinReplicas: min,
	})
}

// recordRejected notes a proposal that failed validation as a rejected outcome.
func (iv *Investigator) recordRejected(inc *Incident, p tools.Proposal, now time.Time) {
	for _, po := range p.Overrides {
		if uid, ok := memberUID(inc, po.Workload); ok {
			iv.outcome(uid, inc.ID, v1.VerdictRejected, po.MinReplicas, now)
		}
	}
}

// route applies validated overrides according to the policy mode (spec §7.7)
// and records an outcome for each. It returns the verdict.
func (iv *Investigator) route(
	ctx context.Context, inc *Incident, pol *v1.TidePolicy, p tools.Proposal,
	ovs map[types.UID]v1.Override, trace []v1.TraceEntry, now time.Time,
) v1.Verdict {
	log := logf.FromContext(ctx).WithName("agent").WithValues("incident", inc.ID)
	notifier := iv.notifier()
	uids := sortedUIDs(ovs)

	switch {
	case pol.Spec.Mode == v1.ModeShadow || iv.Engine.DryRun():
		for _, uid := range uids {
			iv.outcome(uid, inc.ID, v1.VerdictShadow, ovs[uid].MinReplicas, now)
		}
		return v1.VerdictShadow

	case pol.Spec.Agent.Approval == approvalAuto:
		applied := map[types.UID]v1.Override{}
		for _, uid := range uids {
			if err := iv.Store.AddOverride(uid, ovs[uid]); err != nil {
				log.Error(err, "Storing override", "workload", uid)
				continue
			}
			applied[uid] = ovs[uid]
			iv.outcome(uid, inc.ID, v1.VerdictApplied, ovs[uid].MinReplicas, now)
		}
		if len(applied) > 0 {
			notifier.Applied(ctx, inc, applied, true)
		}
		return v1.VerdictApplied
	}

	timeout := pol.Spec.Agent.ApprovalTimeout.Duration
	if timeout <= 0 {
		timeout = defaultApprovalTimeout
	}
	pend := map[types.UID]v1.PendingApproval{}
	for _, uid := range uids {
		o := ovs[uid]
		pa := v1.PendingApproval{
			IncidentID: inc.ID, Override: o, Confidence: strconv.FormatFloat(p.Confidence, 'f', 2, 64),
			Reason: p.Reason, Trace: trace,
			Expires: metav1.NewTime(minTime(now.Add(timeout), o.Start.Add(approvalStartGrace))),
		}
		if _, err := iv.Store.AddPending(uid, pa); err != nil {
			log.Error(err, "Storing pending approval", "workload", uid)
			continue
		}
		pend[uid] = pa
		iv.outcome(uid, inc.ID, v1.VerdictPending, o.MinReplicas, now)
	}
	if len(pend) > 0 {
		notifier.Pending(ctx, inc, pend)
	}
	return v1.VerdictPending
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func overrideID(inc *Incident, uid types.UID) string {
	return fmt.Sprintf("%s-%s", inc.ID, inc.Members[uid].Workload.Name)
}
