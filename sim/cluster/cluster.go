// Package cluster is a simulated Kubernetes cluster for the benchmark: workloads with pod
// startup delays, a fixed true per-pod capacity, and per-step SLO accounting. SimCluster
// implements workload.ScaleClient so the real engine can drive it.
package cluster

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/clock"

	"github.com/aayushsss1/tidewatch/internal/workload"
)

// DefaultHeadroom matches the policies' default headroom.
const DefaultHeadroom = 0.8

// defaultJitter is the default uniform startup-delay jitter (±20%).
const defaultJitter = 0.2

// Key identifies a workload.
type Key struct{ Namespace, Name string }

// Delay is a pod startup-delay distribution: uniform in Mean*(1±Jitter). Jitter 0 means
// the default 0.2; a negative Jitter means no jitter.
type Delay struct {
	Mean   time.Duration
	Jitter float64
}

// WorkloadSpec configures a simulated workload.
type WorkloadSpec struct {
	CapacityRPS  float64 // true per-pod capacity
	StartupDelay Delay
	Initial      int32   // initial replicas, all ready at creation
	Headroom     float64 // for "needed" accounting; default 0.8
}

// Sample is one accounted step.
type Sample struct {
	T       time.Time
	Load    float64
	Desired int32
	Ready   int32
	Needed  int32
}

// Metrics are the aggregates of a workload's accounted steps.
type Metrics struct {
	BreachMinutes   float64
	OverprovRM      float64 // replica-minutes above needed
	UnderprovRM     float64 // replica-minutes below needed (ready basis)
	OverrideWasteRM float64
	ScaleEvents     int
	SafetyViolation int
	Samples         []Sample
}

type pod struct{ readyAt time.Time }

type wl struct {
	spec        WorkloadSpec
	rng         *rand.Rand
	pods        []pod // creation order
	lastDesired int32
	m           Metrics
}

// SimCluster is a concurrency-safe simulated cluster.
type SimCluster struct {
	mu   sync.Mutex
	seed int64
	clk  clock.Clock
	wls  map[Key]*wl
}

var _ workload.ScaleClient = (*SimCluster)(nil)

// New returns an empty cluster; startup-delay sampling is deterministic given seed.
func New(seed int64, clk clock.Clock) *SimCluster {
	return &SimCluster{seed: seed, clk: clk, wls: map[Key]*wl{}}
}

// AddWorkload registers a workload with Initial ready pods.
func (c *SimCluster) AddWorkload(ns, name string, spec WorkloadSpec) {
	if spec.Headroom <= 0 {
		spec.Headroom = DefaultHeadroom
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	k := Key{ns, name}
	// Per-workload stream derived from the seed and the name, so insertion order is irrelevant.
	h := int64(14695981039346656037 & 0x7fffffffffffffff)
	for _, b := range []byte(ns + "/" + name) {
		h = (h ^ int64(b)) * 1099511628211
	}
	w := &wl{spec: spec, rng: rand.New(rand.NewSource(c.seed ^ h)), lastDesired: spec.Initial} //nolint:gosec // simulation
	for range spec.Initial {
		w.pods = append(w.pods, pod{})
	}
	c.wls[k] = w
}

func notFound(ns, name string) error {
	return apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, ns+"/"+name)
}

func (w *wl) ready(now time.Time) int32 {
	var n int32
	for _, p := range w.pods {
		if !p.readyAt.After(now) {
			n++
		}
	}
	return n
}

func (w *wl) delay() time.Duration {
	d := w.spec.StartupDelay
	j := d.Jitter
	switch {
	case j == 0:
		j = defaultJitter
	case j < 0:
		j = 0
	}
	f := 1 + j*(2*w.rng.Float64()-1)
	return time.Duration(float64(d.Mean) * f)
}

func (w *wl) setReplicas(n int32, now time.Time) {
	if n < 0 {
		n = 0
	}
	for int32(len(w.pods)) < n {
		w.pods = append(w.pods, pod{readyAt: now.Add(w.delay())})
	}
	if int32(len(w.pods)) > n {
		// Remove unready pods first, then the newest ready pods: sort so the ones to keep
		// come first (ready before unready; among ready, oldest first).
		slices.SortStableFunc(w.pods, func(a, b pod) int {
			ra, rb := !a.readyAt.After(now), !b.readyAt.After(now)
			if ra != rb {
				if ra {
					return -1
				}
				return 1
			}
			return a.readyAt.Compare(b.readyAt)
		})
		w.pods = w.pods[:n]
	}
}

func (c *SimCluster) lookup(ns, name string) (*wl, error) {
	w, ok := c.wls[Key{ns, name}]
	if !ok {
		return nil, notFound(ns, name)
	}
	return w, nil
}

// Get implements workload.ScaleClient.
func (c *SimCluster) Get(_ context.Context, t workload.Target) (workload.ScaleState, error) {
	return c.State(t.Namespace, t.Ref.Name)
}

// Set implements workload.ScaleClient.
func (c *SimCluster) Set(_ context.Context, t workload.Target, n int32) error {
	return c.SetReplicas(t.Namespace, t.Ref.Name, n)
}

// State reports the desired and ready replicas of a workload.
func (c *SimCluster) State(ns, name string) (workload.ScaleState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, err := c.lookup(ns, name)
	if err != nil {
		return workload.ScaleState{}, err
	}
	return workload.ScaleState{Replicas: int32(len(w.pods)), Ready: w.ready(c.clk.Now())}, nil //nolint:gosec // small
}

// SetReplicas sets desired replicas directly (non-Tidewatch baselines).
func (c *SimCluster) SetReplicas(ns, name string, n int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, err := c.lookup(ns, name)
	if err != nil {
		return err
	}
	w.setReplicas(n, c.clk.Now())
	return nil
}

// Account records one step of length step ending/starting at now for every workload in
// load (requests per second offered during the step). Unknown keys are ignored.
func (c *SimCluster) Account(now time.Time, step time.Duration, load map[Key]float64) {
	mins := step.Minutes()
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, l := range load {
		w, ok := c.wls[k]
		if !ok {
			continue
		}
		desired := int32(len(w.pods)) //nolint:gosec // small
		ready := w.ready(now)
		needed := Needed(l, w.spec.CapacityRPS, w.spec.Headroom)
		if l > float64(ready)*w.spec.CapacityRPS {
			w.m.BreachMinutes += mins
		}
		w.m.OverprovRM += float64(max(0, desired-needed)) * mins
		w.m.UnderprovRM += float64(max(0, needed-ready)) * mins
		if desired != w.lastDesired {
			w.m.ScaleEvents++
			w.lastDesired = desired
		}
		w.m.Samples = append(w.m.Samples, Sample{T: now, Load: l, Desired: desired, Ready: ready, Needed: needed})
	}
}

// Needed is ceil(load / (capacity*headroom)), floored at 1.
func Needed(load, capacity, headroom float64) int32 {
	if headroom <= 0 {
		headroom = DefaultHeadroom
	}
	n := math.Ceil(load / (capacity * headroom))
	return int32(max(1, n)) //nolint:gosec // small
}

// Spec returns a workload's spec.
func (c *SimCluster) Spec(ns, name string) (WorkloadSpec, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.wls[Key{ns, name}]
	if !ok {
		return WorkloadSpec{}, false
	}
	return w.spec, true
}

// AccountOverride accumulates agent-override metrics for one step; the harness supplies
// the baseline-vs-effective replicas. Waste is the effective replicas above both the
// baseline and what was needed; a safety violation is effective < baseline.
func (c *SimCluster) AccountOverride(_ time.Time, step time.Duration, k Key, baseline, effective, needed int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.wls[k]
	if !ok {
		return
	}
	w.m.OverrideWasteRM += float64(max(0, effective-max(baseline, needed))) * step.Minutes()
	if effective < baseline {
		w.m.SafetyViolation++
	}
}

// Metrics returns a copy of a workload's aggregates.
func (c *SimCluster) Metrics(ns, name string) (Metrics, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, err := c.lookup(ns, name)
	if err != nil {
		return Metrics{}, fmt.Errorf("metrics: %w", err)
	}
	m := w.m
	m.Samples = append([]Sample(nil), w.m.Samples...)
	return m, nil
}
