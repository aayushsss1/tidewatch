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

// outcome records the proposal for the outcome history and, as a decision, for
// the engine to write to the workload's status.
func (iv *Investigator) outcome(uid types.UID, d decisionInfo, verdict v1.Verdict, min int32) {
	iv.Store.AddOutcome(uid, v1.RecentOutcome{
		IncidentID: d.incident, Time: metav1.NewTime(d.now), Verdict: verdict, MinReplicas: min,
	})
	iv.Store.AddDecision(uid, v1.Decision{
		Time: metav1.NewTime(d.now), Source: v1.SourceAgent, Desired: min, Final: min, Verdict: verdict,
		Reasons: slices.Clone(d.reasons), IncidentID: d.incident, PromptVersion: PromptVersion,
		Provider: iv.Provider.Name(), Model: iv.Provider.Model(), Trace: slices.Clone(d.trace),
	})
}

// decisionInfo is what every recorded decision of one investigation shares.
type decisionInfo struct {
	incident string
	reasons  []string
	trace    []v1.TraceEntry
	now      time.Time
}

// recordRejected notes a proposal that failed validation as a rejected outcome.
func (iv *Investigator) recordRejected(inc *Incident, p tools.Proposal, d decisionInfo) {
	for _, po := range p.Overrides {
		if uid, ok := memberUID(inc, po.Workload); ok {
			iv.outcome(uid, d, v1.VerdictRejected, po.MinReplicas)
		}
	}
}

// route applies validated overrides according to the policy mode (spec §7.7)
// and records an outcome for each. It returns the verdict.
func (iv *Investigator) route(
	ctx context.Context, inc *Incident, pol *v1.TidePolicy, p tools.Proposal,
	ovs map[types.UID]v1.Override, d decisionInfo,
) v1.Verdict {
	now, trace := d.now, d.trace
	log := logf.FromContext(ctx).WithName("agent").WithValues("incident", inc.ID)
	notifier := iv.notifier()
	uids := sortedUIDs(ovs)

	switch {
	case pol.Spec.Mode == v1.ModeShadow || iv.Engine.DryRun():
		for _, uid := range uids {
			iv.outcome(uid, d, v1.VerdictShadow, ovs[uid].MinReplicas)
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
			iv.outcome(uid, d, v1.VerdictApplied, ovs[uid].MinReplicas)
		}
		if len(applied) == 0 {
			return ""
		}
		notifier.Applied(ctx, inc, applied, true)
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
			Expires: metav1.NewTime(minTime(now.Add(timeout), o.End.Time)),
		}
		superseded, err := iv.Store.AddPending(uid, pa)
		if err != nil {
			log.Error(err, "Storing pending approval", "workload", uid)
			continue
		}
		for _, old := range superseded { // the audit trail shows what the new proposal replaced
			iv.Store.AddDecision(uid, v1.Decision{
				Time: metav1.NewTime(now), Source: v1.SourceAgent, Desired: old.Override.MinReplicas,
				Final: old.Override.MinReplicas, Verdict: v1.VerdictSuperseded, IncidentID: old.IncidentID,
				Reasons: []string{"superseded"}, PromptVersion: PromptVersion,
				Provider: iv.Provider.Name(), Model: iv.Provider.Model(),
			})
		}
		pend[uid] = pa
		iv.outcome(uid, d, v1.VerdictPending, o.MinReplicas)
	}
	if len(pend) == 0 {
		return ""
	}
	notifier.Pending(ctx, inc, pend)
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
