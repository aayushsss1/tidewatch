package baselines

import (
	"context"
	"time"

	"github.com/aayushsss1/tidewatch/sim/cluster"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// Oracle has perfect foresight: it reads future values of the scenario series, so it is
// an upper bound that no real autoscaler can match, not a baseline to beat.
//
//	desired(t) = ceil(max load over [t, t + startupDelayMean + step] / (capacity x headroom))
//
// clamped to the workload's min and max. It sets the replica count every step, ignoring
// cooldowns, stabilization and rate limits. Series point T is the load over (T-step, T],
// so the window includes the point at t+step, the slot about to be accounted. The
// lookahead covers the slowest startup the cluster's default +-20% jitter can draw.
type Oracle struct {
	specs map[string]scenario.Workload
	cache seriesCache
}

// NewOracle builds the oracle for the scenario's workloads.
func NewOracle(ws []scenario.Workload) *Oracle {
	o := &Oracle{specs: map[string]scenario.Workload{}, cache: seriesCache{}}
	for _, w := range ws {
		o.specs[w.Name] = w
	}
	return o
}

// Name implements harness.Controller.
func (*Oracle) Name() string { return NameOracle }

// Step implements harness.Controller.
func (o *Oracle) Step(_ context.Context, now time.Time, sc *scenario.Scenario, cl *cluster.SimCluster) error {
	for _, w := range sc.Workloads {
		w = o.specs[w.Name]
		series := o.cache.get(sc, w.Name)
		peak := 0.0
		end := now.Add(w.StartupDelay + sc.Step)
		for t := now; !t.After(end); t = t.Add(sc.Step) {
			peak = max(peak, seriesAt(series, t))
		}
		n := cluster.Needed(peak, w.Capacity, cluster.DefaultHeadroom)
		n = min(max(n, int32(w.MinReplicas)), int32(w.MaxReplicas)) //nolint:gosec // small
		if err := cl.SetReplicas(harness.ControllerNamespace, w.Name, n); err != nil {
			return err
		}
	}
	return nil
}
