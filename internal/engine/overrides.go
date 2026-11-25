package engine

import (
	"context"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

const defaultApprover = "kubectl"

// syncOverrides runs once per Step before phase 1: it restores unknown
// workloads from status (so a failover resumes where the last leader stopped),
// expires approvals and overrides, and queues the resulting decisions on each
// workload's state for its next status write.
func (e *Engine) syncOverrides(list *v1.TideWorkloadList) {
	if e.ov == nil {
		return
	}
	for i := range list.Items {
		e.ov.Restore(list.Items[i].UID, list.Items[i].Status)
	}
	now := e.clock.Now()
	expired, _ := e.ov.Expire(now)
	if len(expired) == 0 {
		return
	}
	for i := range list.Items {
		tw := &list.Items[i]
		pend := expired[tw.UID]
		if len(pend) == 0 {
			continue
		}
		st := e.stateFor(tw)
		for _, p := range pend {
			st.events = append(st.events, v1.Decision{
				Time: metav1.NewTime(now), Source: v1.SourceAgent, IncidentID: p.IncidentID,
				Desired: p.Override.MinReplicas, Final: st.replicas, Verdict: v1.VerdictExpired,
			})
		}
	}
}

// splitApproval parses an annotation value "<incidentID>@<who>".
func splitApproval(v string) (incident, who string) {
	incident, who, _ = strings.Cut(strings.TrimSpace(v), "@")
	if who == "" {
		who = defaultApprover
	}
	return incident, who
}

// consumeAnnotations applies and removes the approve/reject annotations of a
// workload. Annotations are removed with a metadata merge patch; status stays
// owned by the status writer.
func (e *Engine) consumeAnnotations(ctx context.Context, it *item) {
	tw := it.tw
	approve, hasApprove := tw.Annotations[v1.AnnoApprove]
	reject, hasReject := tw.Annotations[v1.AnnoReject]
	if e.ov == nil || (!hasApprove && !hasReject) {
		return
	}
	log := logf.Log.WithName("engine").WithValues("workload", client.ObjectKeyFromObject(tw))
	at := metav1.NewTime(it.now)
	current := it.scale.Replicas
	event := func(incident string, min int32, v v1.Verdict) {
		it.events = append(it.events, v1.Decision{
			Time: at, Source: v1.SourceAgent, IncidentID: incident,
			Desired: min, Final: current, Verdict: v,
		})
	}

	if hasApprove {
		incident, who := splitApproval(approve)
		applied, verdict := e.ov.Approve(incident, who, e.clock.Now())
		switch verdict {
		case v1.VerdictApplied:
			if slicesContainsUID(applied, tw) {
				ovs, _, _ := e.ov.Snapshot(tw.UID)
				for _, o := range ovs {
					if o.IncidentID == incident {
						event(incident, o.MinReplicas, v1.VerdictApplied)
					}
				}
			}
		case v1.VerdictExpired, v1.VerdictSuperseded:
			event(incident, 0, verdict)
		default:
			log.Info("Approval for an unknown incident ignored", "incident", incident)
		}
	}
	if hasReject {
		incident, who := splitApproval(reject)
		if uids := e.ov.Reject(incident, who); slicesContainsUID(uids, tw) {
			event(incident, 0, v1.VerdictRejected)
		}
	}

	patched := tw.DeepCopy()
	delete(patched.Annotations, v1.AnnoApprove)
	delete(patched.Annotations, v1.AnnoReject)
	if err := e.c.Patch(ctx, patched, client.MergeFrom(tw)); err != nil {
		log.Error(err, "Removing approval annotations")
	}
}

// applyOverride lifts the baseline to the active override's floor. It never
// lowers: the baseline is what the forecast wants, the override only raises it.
func (e *Engine) applyOverride(it *item) {
	it.baselineReplicas = it.base.Desired
	if e.ov == nil {
		return
	}
	if m, o := e.ov.Active(it.tw.UID, it.now); o != nil && m > it.base.Desired {
		it.base.Desired = m
		it.override = o
	}
}

func slicesContainsUID(uids []types.UID, tw *v1.TideWorkload) bool {
	return slices.Contains(uids, tw.UID)
}
