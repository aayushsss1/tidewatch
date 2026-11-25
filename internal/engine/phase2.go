package engine

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// phase2 enforces guardrails for one policy: scale-downs first, then
// scale-ups by descending Desired/Current, ties by name.
func (e *Engine) phase2(ctx context.Context, items []*item, known map[types.UID]int32) {
	var total int32
	for _, n := range known {
		total += n
	}
	var enforced []*item
	for _, it := range items {
		if it.enforce {
			enforced = append(enforced, it)
		} else {
			e.finish(ctx, it)
		}
	}
	slices.SortFunc(enforced, func(a, b *item) int {
		ad, bd := a.base.Desired < a.scale.Replicas, b.base.Desired < b.scale.Replicas
		if ad != bd {
			if ad {
				return -1
			}
			return 1
		}
		ra := float64(a.base.Desired) / float64(max(a.scale.Replicas, 1))
		rb := float64(b.base.Desired) / float64(max(b.scale.Replicas, 1))
		return cmp.Or(cmp.Compare(rb, ra), strings.Compare(a.tw.Name, b.tw.Name))
	})
	for _, it := range enforced {
		uid := it.tw.UID
		e.decide(ctx, it, total-known[uid])
		total += it.decision.Final - known[uid]
		known[uid] = it.decision.Final
		it.st.replicas = it.decision.Final
		e.finish(ctx, it)
	}
}

// decide runs Enforce and applies its result to the workload and its state.
func (e *Engine) decide(ctx context.Context, it *item, others int32) {
	st := it.st
	mode := it.pol.Spec.Mode
	if mode == "" {
		mode = v1.ModeShadow
	}
	rails := it.pol.Spec.Guardrails
	res := Enforce(EnforceInput{
		Now: it.now, Desired: it.base.Desired, Config: it.cfg, Rails: rails, Mode: mode,
		DryRun: e.opts.DryRun, Conflict: it.target.Conflict,
		State: EnforceState{
			Current: it.scale.Replicas, LastApplied: st.lastApplied,
			LastScaleUp: st.lastUp, LastScaleDown: st.lastDown,
			Recommendations: st.recs, OthersTotal: others,
		},
	})
	it.decision, it.budget = res.Decision, res.BudgetLimited
	if it.override != nil {
		it.decision.Source, it.decision.IncidentID = v1.SourceAgent, it.override.IncidentID
	}

	st.recs = append(st.recs, TimedReplicas{T: it.now, Replicas: it.base.Desired})
	cutoff := it.now.Add(-rails.ScaleDown.StabilizationWindow.Duration)
	st.recs = slices.DeleteFunc(st.recs, func(r TimedReplicas) bool { return r.T.Before(cutoff) })

	if res.ExternalChange {
		st.lastUp, st.lastDown = it.now, it.now
		st.lastApplied = new(it.scale.Replicas)
	}
	if !res.Apply {
		return
	}
	if err := e.scale.Set(ctx, it.target, res.Final); err != nil {
		logf.Log.WithName("engine").Error(err, "Scaling target", "workload", client.ObjectKeyFromObject(it.tw))
		it.decision.Verdict, it.decision.Final = v1.VerdictHeld, it.scale.Replicas
		it.decision.Reasons = append(it.decision.Reasons, reasonScaleFailed)
		return
	}
	st.lastApplied = new(res.Final)
	if res.Final > it.scale.Replicas {
		st.lastUp = it.now
		st.ttr.ScaledUp(it.now, res.Final)
	} else {
		st.lastDown = it.now
	}
}

// finish records metrics for the decision and writes status when due.
func (e *Engine) finish(ctx context.Context, it *item) {
	d := it.decision
	decisionsTotal.WithLabelValues(string(d.Source), string(d.Verdict), it.tw.Namespace, it.tw.Name).Inc()
	desiredReplicasGauge.WithLabelValues(it.tw.Namespace, it.tw.Name).Set(float64(d.Desired))
	e.writeStatus(ctx, it)
	stepDuration.WithLabelValues(it.tw.Namespace, it.tw.Name).Observe(it.dur.Seconds())
}
