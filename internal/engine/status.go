package engine

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// writeStatus patches the TideWorkload status when the decision changed, a
// condition changed, or statusRefresh has elapsed since the last write.
func (e *Engine) writeStatus(ctx context.Context, it *item) {
	st, d := it.st, it.decision
	now := e.clock.Now()
	dirty := e.ov != nil && e.ov.TakeDirty(it.tw.UID)
	if !dirty && len(it.events) == 0 && st.statusWritten && st.lastVerdict == d.Verdict && st.lastFinal == d.Final &&
		now.Sub(st.lastStatus) < statusRefresh && !conditionsChanged(it) {
		return
	}
	key := client.ObjectKeyFromObject(it.tw)
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		tw := &v1.TideWorkload{}
		if err := e.c.Get(ctx, key, tw); err != nil {
			return err
		}
		orig := tw.DeepCopy()
		e.fillStatus(&tw.Status, it)
		return e.c.Status().Patch(ctx, tw, client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{}))
	})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logf.Log.WithName("engine").Error(err, "Writing status", "workload", key)
		}
		if dirty {
			e.ov.MarkDirty(it.tw.UID)
		}
		// Keep approve/reject/expiry decisions for the next write.
		st.events = append(slices.Clone(it.events), st.events...)
		return
	}
	st.statusWritten, st.lastStatus, st.lastVerdict, st.lastFinal = true, now, d.Verdict, d.Final
}

func (it *item) budgetCondition() metav1.Condition {
	c := metav1.Condition{Type: v1.CondBudgetLimited, Status: metav1.ConditionFalse, Reason: "within_budget"}
	if it.budget {
		c.Status, c.Reason = metav1.ConditionTrue, reasonBudget
		c.Message = "maxTotalReplicas limited the scale-up"
	}
	return c
}

func conditionsChanged(it *item) bool {
	differs := func(c metav1.Condition) bool {
		old := meta.FindStatusCondition(it.tw.Status.Conditions, c.Type)
		return old == nil || old.Status != c.Status || old.Reason != c.Reason
	}
	if it.cfgOK && func() bool {
		c := it.annotationsCondition()
		old := meta.FindStatusCondition(it.tw.Status.Conditions, c.Type)
		return old == nil || old.Status != c.Status || old.Message != c.Message
	}() {
		return true
	}
	return (it.forecasting.Type != "" && differs(it.forecasting)) || (it.enforce && differs(it.budgetCondition()))
}

// decisionChanged reports whether d differs from the newest stored decision in
// verdict, final replicas or reasons, so periodic refreshes do not add duplicates.
func decisionChanged(prev, d v1.Decision) bool {
	return prev.Verdict != d.Verdict || prev.Source != d.Source || prev.Final != d.Final || !slices.Equal(prev.Reasons, d.Reasons)
}

func (it *item) annotationsCondition() metav1.Condition {
	c := metav1.Condition{Type: v1.CondAnnotationsIgnored, Status: metav1.ConditionFalse, Reason: "none"}
	if len(it.cfg.Warnings) > 0 {
		c.Status, c.Reason = metav1.ConditionTrue, "annotations_ignored"
		c.Message = strings.Join(it.cfg.Warnings, "; ")
	}
	return c
}

func (e *Engine) fillStatus(s *v1.TideWorkloadStatus, it *item) {
	d, st := it.decision, it.st
	s.CurrentReplicas, s.ReadyReplicas, s.DesiredReplicas = it.scale.Replicas, it.scale.Ready, d.Desired
	s.Mode = it.pol.Spec.Mode
	s.TimeToReady = st.ttr.Status()
	for _, ev := range it.events {
		s.Decisions = append([]v1.Decision{ev}, s.Decisions...)
		if ev.PromptVersion != "" { // events are oldest first, so the newest wins
			s.LastInvestigation = ev.DeepCopy()
		}
	}
	if len(s.Decisions) == 0 || decisionChanged(s.Decisions[0], d) {
		s.Decisions = append([]v1.Decision{d}, s.Decisions...)
	}
	if len(s.Decisions) > maxDecisions {
		s.Decisions = s.Decisions[:maxDecisions]
	}
	if d.Final != it.scale.Replicas {
		verb := "scale"
		if d.Verdict == v1.VerdictShadow {
			verb = "shadow scale"
		}
		s.LastAction = verb + " " + strconv.Itoa(int(it.scale.Replicas)) + "→" +
			strconv.Itoa(int(d.Final)) + " (" + string(d.Source) + ")"
	}
	if t := optTime(st.lastUp); t != nil {
		s.LastScaleUpTime = t
	}
	if t := optTime(st.lastDown); t != nil {
		s.LastScaleDownTime = t
	}

	if it.forecasting.Type != "" {
		meta.SetStatusCondition(&s.Conditions, it.forecasting)
	}
	if it.cfgOK {
		meta.SetStatusCondition(&s.Conditions, it.annotationsCondition())
	}
	if e.ov != nil {
		s.Overrides, s.PendingApprovals, s.RecentOutcomes = e.ov.Snapshot(it.tw.UID)
		if it.enforce {
			s.BaselineReplicas = it.baselineReplicas
		}
	}
	if it.enforce {
		meta.SetStatusCondition(&s.Conditions, it.budgetCondition())
		e.fillForecastStatus(s, it)
	} else {
		// A held tick has no fresh forecast: do not leave the last one looking current.
		s.ForecastPeak, s.PeakTime, s.Forecasts, s.Load = "", nil, nil, ""
	}
}

func (e *Engine) fillForecastStatus(s *v1.TideWorkloadStatus, it *item) {
	s.Load = num(it.actual)
	s.TrustedModel, s.ForecastPeak, s.PeakTime = "", "", nil
	if it.trusted != nil {
		s.TrustedModel = it.trusted.Model
		if p, ok := it.trusted.Peak(it.now.Add(it.horizon)); ok {
			s.ForecastPeak, s.PeakTime = num(p.Point), new(metav1.NewTime(p.T))
		}
	}
	s.Forecasts = nil
	for _, f := range it.forecasts {
		mf := v1.ModelForecast{Model: f.Model}
		if p, ok := f.Peak(it.now.Add(it.horizon)); ok {
			mf.Peak, mf.Lower, mf.Upper = num(p.Point), num(p.Lower), num(p.Upper)
			mf.PeakTime = new(metav1.NewTime(p.T))
		}
		s.Forecasts = append(s.Forecasts, mf)
	}
	s.Accuracy = nil
	for _, sc := range it.scores {
		s.Accuracy = append(s.Accuracy, v1.ModelAccuracy{
			Model: sc.Model, LeadSeconds: int32(sc.Lead.Seconds()), //nolint:gosec // lead is bounded by the horizon
			SMAPE: num(sc.SMAPE), Coverage: num(sc.Coverage), Samples: int32(sc.Samples), //nolint:gosec // bounded by window/step
		})
	}
}

func num(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }

func optTime(t time.Time) *metav1.Time {
	if t.IsZero() {
		return nil
	}
	return new(metav1.NewTime(t))
}
