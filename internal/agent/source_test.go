package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

type multiQuerier struct{ loadQuerier }

func (multiQuerier) RangeSeries(context.Context, string, time.Time, time.Time, time.Duration) (forecast.Series, error) {
	return forecast.Series{}, fmt.Errorf("%w, got 3", prom.ErrMultipleSeries)
}

func TestSource_QueryRangeRejectsMultipleSeries(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "src-q")
	src := f.iv.source(&v1.TidePolicy{}, f.incident("src-q"))
	src.factory = func(string, time.Duration) (prom.Querier, error) { return multiQuerier{}, nil }
	_, err := src.QueryRange(f.ctx, `sum(up{namespace="default"})`, time.Hour, time.Minute)
	if err == nil || !strings.Contains(err.Error(), "more than one series") {
		t.Fatalf("err = %v", err)
	}
	src.q, src.factory = nil, func(string, time.Duration) (prom.Querier, error) { return nil, errors.New("down") }
	if _, err := src.QueryRange(f.ctx, "up", time.Hour, time.Minute); err == nil {
		t.Fatal("factory error must surface")
	}
	src.q, src.factory = nil, func(string, time.Duration) (prom.Querier, error) { return loadQuerier{}, nil }
	s, err := src.QueryRange(f.ctx, "up", time.Hour, time.Minute)
	if err != nil || len(s.Points) == 0 {
		t.Fatalf("ok query: %v %d", err, len(s.Points))
	}
}

func TestSource_StateForecastsRolloutsEvents(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Manual", "src-w", "src-other")
	inc := f.incident("src-w", "src-other")
	var pol v1.TidePolicy
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: f.pol}, &pol); err != nil {
		t.Fatal(err)
	}
	src := f.iv.source(&pol, inc)
	w := tools.WorkloadRef{Namespace: testNS, Name: "src-w"}

	st, err := src.State(f.ctx, w)
	if err != nil || st.Guardrails["maxReplicas"] != "20" || st.Replicas == 0 {
		t.Fatalf("state = %+v, %v", st, err)
	}
	fs, err := src.Forecasts(f.ctx, w)
	if err != nil || len(fs.Models) == 0 || fs.Models[0].Peak <= 0 {
		t.Fatalf("forecasts = %+v, %v", fs, err)
	}
	rel, err := src.Related(f.ctx, w)
	if err != nil || len(rel) != 1 || rel[0].Workload != "src-other" {
		t.Fatalf("related = %+v, %v", rel, err)
	}
	if _, err := src.State(f.ctx, tools.WorkloadRef{Namespace: testNS, Name: "nope"}); err == nil {
		t.Fatal("out-of-incident workload must be rejected")
	}

	var d appsv1.Deployment
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: "src-w"}, &d); err != nil {
		t.Fatal(err)
	}
	for i := range 7 {
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: testNS, Name: fmt.Sprintf("src-w-rs%d", i),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: appsAPI, Kind: kindDeployment, Name: d.Name, UID: d.UID}},
			},
			Spec: appsv1.ReplicaSetSpec{Replicas: new(int32(1)), Selector: d.Spec.Selector, Template: d.Spec.Template},
		}
		if err := f.c.Create(f.ctx, rs); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.c.Delete(f.ctx, rs) })
	}
	ro, err := src.Rollouts(f.ctx, w)
	if err != nil || len(ro) != 5 || len(ro[0].Images) != 1 || ro[0].Images[0] != "busybox" {
		t.Fatalf("rollouts = %+v, %v", ro, err)
	}

	for i := range 25 {
		ev := &corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Namespace: testNS, Name: fmt.Sprintf("src-w-ev%d", i)},
			InvolvedObject: corev1.ObjectReference{Kind: "Pod", Namespace: testNS, Name: fmt.Sprintf("src-w-abc%d", i)},
			Reason:         "BackOff", Type: "Warning", Message: "restarting", Count: 1,
			LastTimestamp: metav1.NewTime(f.clk.Now().Add(-time.Duration(i) * time.Second)),
		}
		if i == 24 {
			ev.InvolvedObject = corev1.ObjectReference{Kind: "Pod", Namespace: testNS, Name: "src-other-xyz"}
		}
		if err := f.c.Create(f.ctx, ev); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = f.c.Delete(f.ctx, ev) })
	}
	evs, err := src.Events(f.ctx, w, time.Hour)
	if err != nil || len(evs) != 20 {
		t.Fatalf("events = %d, %v", len(evs), err)
	}
	if !evs[len(evs)-1].LastSeen.After(evs[0].LastSeen) {
		t.Error("events should be chronological, newest kept")
	}
}
