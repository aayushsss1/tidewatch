package baselines

import (
	"context"
	"time"

	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// KEDA creates an HPA over an external metric with an AverageValue target, so the HPA
// math is desired = ceil(metric / target) with target = capacity x 0.8 per pod. This
// models the Prometheus scaler on request rate, optionally combined with a Cron scaler
// that a human configures from the ops note.
//
// Simplifications:
//   - KEDA polls every 30s. The scenario series value at t is already a trailing 1m
//     average (the lag of rate(...[1m])), and the metric is refreshed only at 30s
//     boundaries (absolute time), so with a step shorter than 30s it goes stale.
//   - The HPA synchronizes every 15s with the same tolerance (0.1), scale-up policy
//     (max(100%, 4 pods) per 15s), scale-down policy and 300s stabilization as HPA.
//   - minReplicas >= 1 in every scenario, so KEDA's cooldownPeriod (scale to zero only)
//     is irrelevant and not modelled.
//   - Cron (KEDA Cron + Prometheus): each ops note published at or before now that parses
//     as "expect ~Nx" with a time adds a window [event-15m, event+60m] with
//     desiredReplicas = ceil(N x pre-event daily peak / (capacity x 0.8)). The pre-event
//     peak is the max load over the 24h before the note was published. The note is
//     followed literally, wrong or not; an unparseable note adds nothing. KEDA takes the
//     max of the triggers, applied here to the raw recommendation, before the HPA
//     stabilization and policies.
type KEDA struct {
	name  string
	cron  bool
	specs map[string]scenario.Workload
	state map[string]*hpaState
	// metric is the last polled value per workload.
	metric map[string]float64
	polled map[string]bool
	// windows are the cron windows per workload, created once per published note.
	windows  map[string][]cronWindow
	seenNote map[int]bool
	cache    seriesCache
}

type cronWindow struct {
	start, end time.Time
	desired    int32
}

func newKEDA(name string, cron bool, ws []scenario.Workload) *KEDA {
	k := &KEDA{
		name: name, cron: cron, specs: map[string]scenario.Workload{}, state: map[string]*hpaState{},
		metric: map[string]float64{}, polled: map[string]bool{},
		windows: map[string][]cronWindow{}, seenNote: map[int]bool{}, cache: seriesCache{},
	}
	for _, w := range ws {
		k.specs[w.Name] = w
		k.state[w.Name] = &hpaState{}
	}
	return k
}

// NewKEDAPrometheus builds KEDA's Prometheus scaler driving an HPA.
func NewKEDAPrometheus(ws []scenario.Workload) *KEDA { return newKEDA(NameKEDAProm, false, ws) }

// NewKEDACron builds KEDA's Cron scaler combined with the Prometheus scaler.
func NewKEDACron(ws []scenario.Workload) *KEDA { return newKEDA(NameKEDACron, true, ws) }

// Name implements harness.Controller.
func (k *KEDA) Name() string { return k.name }

const (
	cronLead = 15 * time.Minute
	cronHold = 60 * time.Minute
	dayPeak  = 24 * time.Hour
)

// addWindows creates cron windows for notes newly published at or before now.
func (k *KEDA) addWindows(now time.Time, sc *scenario.Scenario) {
	for i, n := range sc.Notes() {
		if n.PublishAt.After(now) || k.seenNote[i] {
			continue
		}
		k.seenNote[i] = true
		ev, ok := ParseNote(n.Text, n.PublishAt)
		if !ok {
			continue
		}
		for _, w := range sc.Workloads {
			peak := 0.0
			for _, p := range k.cache.get(sc, w.Name).Points {
				if p.T.After(n.PublishAt.Add(-dayPeak)) && !p.T.After(n.PublishAt) {
					peak = max(peak, p.V)
				}
			}
			k.windows[w.Name] = append(k.windows[w.Name], cronWindow{
				start:   ev.EventTime.Add(-cronLead),
				end:     ev.EventTime.Add(cronHold),
				desired: ceilEps(ev.Multiplier * peak / (w.Capacity * hpaTarget)),
			})
		}
	}
}

// Step implements harness.Controller.
func (k *KEDA) Step(_ context.Context, now time.Time, sc *scenario.Scenario, cl *cluster.SimCluster) error {
	if k.cron {
		k.addWindows(now, sc)
	}
	for _, w := range sc.Workloads {
		w = k.specs[w.Name]
		value := seriesAt(k.cache.get(sc, w.Name), now)
		st := k.state[w.Name]
		for i := range syncsPerStep(sc.Step) {
			sub := now.Add(time.Duration(i) * hpaSync)
			if !k.polled[w.Name] || sub.Unix()%int64(kedaPollPeriod/time.Second) == 0 {
				k.metric[w.Name], k.polled[w.Name] = value, true
			}
			s, err := cl.State(harness.ControllerNamespace, w.Name)
			if err != nil {
				return err
			}
			desired := s.Replicas
			target := w.Capacity * hpaTarget
			if s.Replicas > 0 && !withinTolerance(k.metric[w.Name]/(target*float64(s.Replicas))) {
				desired = ceilEps(k.metric[w.Name] / target)
			}
			for _, cw := range k.windows[w.Name] {
				if !sub.Before(cw.start) && !sub.After(cw.end) {
					desired = max(desired, cw.desired)
				}
			}
			lo, hi := int32(w.MinReplicas), int32(w.MaxReplicas) //nolint:gosec // small
			n := st.sync(sub, s.Replicas, desired, lo, hi)
			if n != s.Replicas {
				if err := cl.SetReplicas(harness.ControllerNamespace, w.Name, n); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
