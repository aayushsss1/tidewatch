package baselines

import (
	"context"
	"time"

	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// HPA models the Kubernetes Horizontal Pod Autoscaler on CPU-like utilization with the
// default behavior.
//
// Simplifications:
//   - Utilization is load / (ready x capacity), a stand-in for CPU/requests. Real CPU
//     utilization is not linear in request rate near saturation.
//   - Metric lag: metrics-server reports a trailing 60s average. The scenario series
//     value at t covers (t-step, t], so using it at t models that lag. Every sync within
//     a scenario step (step/15s of them) sees the same metric value.
//   - Each sync: desired = ceil(current x util/0.8), skipped when |util/0.8 - 1| <= 0.1.
//     Utilization averages the ready pods only; when scaling up with unready pods the
//     ratio is recomputed counting them at 0%, as the real controller does, so a pod that
//     is still starting does not cause the target to compound across syncs.
//   - Scale-up policy max(100%, 4 pods) per 15s; scale-down policy 100% per 15s with a
//     300s stabilization window (max recommendation over the window).
//   - Result clamped to the workload's min and max. No readiness-delay, pod-age or
//     missing-metric handling beyond the above.
type HPA struct {
	specs map[string]scenario.Workload
	state map[string]*hpaState
	cache seriesCache
}

// NewHPA builds an HPA controller for the scenario's workloads.
func NewHPA(ws []scenario.Workload) *HPA {
	h := &HPA{specs: map[string]scenario.Workload{}, state: map[string]*hpaState{}, cache: seriesCache{}}
	for _, w := range ws {
		h.specs[w.Name] = w
		h.state[w.Name] = &hpaState{}
	}
	return h
}

// Name implements harness.Controller.
func (*HPA) Name() string { return NameHPA }

// Step implements harness.Controller.
func (h *HPA) Step(_ context.Context, now time.Time, sc *scenario.Scenario, cl *cluster.SimCluster) error {
	for _, w := range sc.Workloads {
		w = h.specs[w.Name]
		load := seriesAt(h.cache.get(sc, w.Name), now)
		st := h.state[w.Name]
		for i := range syncsPerStep(sc.Step) {
			s, err := cl.State(harness.ControllerNamespace, w.Name)
			if err != nil {
				return err
			}
			desired := hpaDesired(load, w.Capacity, s.Replicas, s.Ready)
			lo, hi := int32(w.MinReplicas), int32(w.MaxReplicas) //nolint:gosec // small
			n := st.sync(now.Add(time.Duration(i)*hpaSync), s.Replicas, desired, lo, hi)
			if n != s.Replicas {
				if err := cl.SetReplicas(harness.ControllerNamespace, w.Name, n); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func syncsPerStep(step time.Duration) int { return max(1, int(step/hpaSync)) }

// hpaDesired is the HPA's raw recommendation for a utilization metric.
func hpaDesired(load, capacity float64, current, ready int32) int32 {
	if ready == 0 || current == 0 {
		return current
	}
	ratio := load / (float64(ready) * capacity) / hpaTarget
	if withinTolerance(ratio) {
		return current
	}
	if current > ready && ratio > 1 {
		// Unready pods count as 0% utilized when scaling up.
		newRatio := load / (float64(current) * capacity) / hpaTarget
		if newRatio <= 1 || withinTolerance(newRatio) {
			return current
		}
		ratio = newRatio
	}
	if ratio < 1 {
		// Scaling down ignores unready pods: the ratio applies to the ready count.
		return ceilEps(float64(ready) * ratio)
	}
	return ceilEps(float64(current) * ratio)
}
