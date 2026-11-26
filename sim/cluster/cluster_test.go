package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func target(name string) workload.Target {
	return workload.Target{Namespace: "ns", Ref: v1.TargetRef{Name: name}}
}

func newC(seed int64, jitter float64) (*SimCluster, *clocktesting.FakeClock) {
	clk := clocktesting.NewFakeClock(t0)
	c := New(seed, clk)
	c.AddWorkload("ns", "a", WorkloadSpec{CapacityRPS: 10, StartupDelay: Delay{Mean: time.Minute, Jitter: jitter}, Initial: 2})
	return c, clk
}

func TestReadyAfterDelay(t *testing.T) {
	c, clk := newC(1, -1)
	ctx := context.Background()
	if err := c.Set(ctx, target("a"), 5); err != nil {
		t.Fatal(err)
	}
	st, _ := c.Get(ctx, target("a"))
	if st.Replicas != 5 || st.Ready != 2 {
		t.Fatalf("got %+v", st)
	}
	clk.Step(59 * time.Second)
	if st, _ = c.Get(ctx, target("a")); st.Ready != 2 {
		t.Fatalf("early ready %+v", st)
	}
	clk.Step(time.Second)
	if st, _ = c.Get(ctx, target("a")); st.Ready != 5 {
		t.Fatalf("want ready 5 got %+v", st)
	}
}

func TestScaleDownRemovesUnreadyFirst(t *testing.T) {
	c, clk := newC(1, -1)
	ctx := context.Background()
	_ = c.Set(ctx, target("a"), 5)
	clk.Step(30 * time.Second)
	_ = c.Set(ctx, target("a"), 3) // drops 2 unready; the 2 original stay ready
	st, _ := c.Get(ctx, target("a"))
	if st.Replicas != 3 || st.Ready != 2 {
		t.Fatalf("got %+v", st)
	}
	clk.Step(31 * time.Second)
	st, _ = c.Get(ctx, target("a"))
	if st.Ready != 3 {
		t.Fatalf("got %+v", st)
	}
	// All ready: scale down drops newest ready; the remaining still serve.
	_ = c.Set(ctx, target("a"), 2)
	if st, _ = c.Get(ctx, target("a")); st.Replicas != 2 || st.Ready != 2 {
		t.Fatalf("got %+v", st)
	}
}

func TestUnknownWorkload(t *testing.T) {
	c, _ := newC(1, 0)
	if _, err := c.Get(context.Background(), target("zz")); !apierrors.IsNotFound(err) {
		t.Fatalf("want NotFound, got %v", err)
	}
	if err := c.Set(context.Background(), target("zz"), 1); !apierrors.IsNotFound(err) {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestAccounting(t *testing.T) {
	c, clk := newC(1, -1)
	k := Key{"ns", "a"}
	step := time.Minute
	// capacity 10, headroom 0.8 => 8 rps/pod target.
	// step1: load 15, 2 ready (cap 20): no breach; needed ceil(15/8)=2; no over/under.
	c.Account(clk.Now(), step, map[Key]float64{k: 15})
	// scale to 4 (new pods ready in 60s)
	_ = c.SetReplicas("ns", "a", 4)
	// step2: load 30, ready 2 (cap 20): breach; needed ceil(30/8)=4; under 2; over 0.
	clk.Step(step)
	c.Account(clk.Now().Add(-time.Second), step, map[Key]float64{k: 30}) // pods ready at t0+60s not yet
	// step3: load 5, ready 4; needed 1; over 3; under 0; no breach.
	c.Account(clk.Now(), step, map[Key]float64{k: 5})
	m, err := c.Metrics("ns", "a")
	if err != nil {
		t.Fatal(err)
	}
	if m.BreachMinutes != 1 || m.OverprovRM != 3 || m.UnderprovRM != 2 || m.ScaleEvents != 1 || len(m.Samples) != 3 {
		t.Fatalf("got %+v", m)
	}
	if m.Samples[1].Needed != 4 || m.Samples[1].Ready != 2 || m.Samples[1].Desired != 4 {
		t.Fatalf("sample %+v", m.Samples[1])
	}
}

func TestNeededFloor(t *testing.T) {
	if Needed(0, 10, 0.8) != 1 {
		t.Fatal("floor")
	}
}

func TestAccountOverride(t *testing.T) {
	c, clk := newC(1, 0)
	k := Key{"ns", "a"}
	c.AccountOverride(clk.Now(), 2*time.Minute, k, 3, 8, 5) // waste (8-5)*2 = 6
	c.AccountOverride(clk.Now(), time.Minute, k, 3, 4, 6)   // 0
	c.AccountOverride(clk.Now(), time.Minute, k, 3, 2, 1)   // violation
	m, _ := c.Metrics("ns", "a")
	if m.OverrideWasteRM != 6 || m.SafetyViolation != 1 {
		t.Fatalf("got %+v", m)
	}
}

func run(seed int64) []time.Time {
	c, clk := newC(seed, 0)
	_ = c.SetReplicas("ns", "a", 12)
	var out []time.Time
	for range 120 {
		st, _ := c.State("ns", "a")
		_ = st
		clk.Step(time.Second)
		out = append(out, clk.Now().Add(time.Duration(func() int32 { s, _ := c.State("ns", "a"); return s.Ready }())))
	}
	return out
}

func TestDeterministic(t *testing.T) {
	a, b := run(7), run(7)
	for i := range a {
		if !a[i].Equal(b[i]) {
			t.Fatalf("diverged at %d", i)
		}
	}
	// Different seed should differ somewhere (jitter active).
	d := run(8)
	same := true
	for i := range a {
		if !a[i].Equal(d[i]) {
			same = false
		}
	}
	if same {
		t.Fatal("different seeds produced identical runs")
	}
}

func TestConcurrent(t *testing.T) {
	c, clk := newC(1, 0)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 200 {
				_ = c.Set(context.Background(), target("a"), int32(1+(i+j)%10))
				_, _ = c.Get(context.Background(), target("a"))
				c.Account(clk.Now(), time.Minute, map[Key]float64{{"ns", "a"}: 10})
			}
		}()
	}
	wg.Wait()
}
