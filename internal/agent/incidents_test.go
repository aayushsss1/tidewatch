package agent

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

func trig(pol, uid string, at time.Time) signal.Trigger {
	return signal.Trigger{
		Type: signal.Anomaly, UID: types.UID(uid), At: at,
		Workload: types.NamespacedName{Namespace: "ns", Name: uid},
		Policy:   v1.PolicyRef{Namespace: "ns", Name: pol},
	}
}

func newMgr() (*Manager, *clocktesting.FakeClock) {
	clk := clocktesting.NewFakeClock(t0)
	return NewManager(clk, nil, Options{}), clk
}

func TestManager_GroupsWithinWindow(t *testing.T) {
	m, clk := newMgr()
	a, _ := m.Add(trig("shop", "a", clk.Now()))
	clk.Step(2 * time.Minute)
	b, _ := m.Add(trig("shop", "b", clk.Now()))
	if a.ID != "shop-"+m.nonce+"-1" || b.ID != a.ID || len(b.Members) != 2 {
		t.Fatalf("a=%s b=%s members=%d", a.ID, b.ID, len(b.Members))
	}
	clk.Step(2 * time.Minute) // 4m since open, no correlation data
	c, _ := m.Add(trig("shop", "c", clk.Now()))
	if c.ID == a.ID {
		t.Fatal("beyond window without correlation must open a new incident")
	}
}

func TestManager_GroupsByCorrelationBeyondWindow(t *testing.T) {
	clk := clocktesting.NewFakeClock(t0)
	w := wave(40)
	src := &fakeSrc{m: map[types.UID]forecast.Series{
		"a": mkSeries(time.Minute, w), "b": mkSeries(time.Minute, w), "c": mkSeries(time.Minute, make([]float64, 40)),
	}}
	m := NewManager(clk, NewCorrelator(src, clk), Options{})
	a, _ := m.Add(trig("shop", "a", clk.Now()))
	clk.Step(5 * time.Minute)
	b, _ := m.Add(trig("shop", "b", clk.Now()))
	if b.ID != a.ID {
		t.Fatal("correlated workload should join")
	}
	if _, ok := b.Lags["b"]; !ok {
		t.Fatal("lag should be recorded")
	}
	c, _ := m.Add(trig("shop", "c", clk.Now()))
	if c.ID == a.ID {
		t.Fatal("uncorrelated workload must not join")
	}
}

func TestManager_SeparatePoliciesSeparateIncidents(t *testing.T) {
	m, clk := newMgr()
	a, _ := m.Add(trig("shop", "a", clk.Now()))
	b, _ := m.Add(trig("pay", "b", clk.Now()))
	if a.ID == b.ID || b.ID != "pay-"+m.nonce+"-2" {
		t.Fatalf("a=%s b=%s", a.ID, b.ID)
	}
}

func TestManager_LeadIsEarliestTrigger(t *testing.T) {
	m, clk := newMgr()
	m.Add(trig("shop", "a", clk.Now()))
	inc, _ := m.Add(trig("shop", "b", clk.Now().Add(-time.Minute)))
	if inc.Lead != "b" {
		t.Fatalf("lead=%s", inc.Lead)
	}
}

func TestManager_ClosesAfterQuiet(t *testing.T) {
	m, clk := newMgr()
	m.Add(trig("shop", "a", clk.Now()))
	if r := m.TakeReady(); len(r) != 1 {
		t.Fatalf("ready=%d", len(r))
	}
	if r := m.TakeReady(); len(r) != 0 {
		t.Fatal("must not be ready twice")
	}
	clk.Step(29 * time.Minute)
	if c := m.Tick(clk.Now()); len(c) != 0 {
		t.Fatal("closed too early")
	}
	clk.Step(time.Minute)
	if c := m.Tick(clk.Now()); len(c) != 1 || c[0].Status != StatusClosed {
		t.Fatalf("closed=%v", c)
	}
}

func TestManager_InvestigatingNotReadyUntilDecided(t *testing.T) {
	m, clk := newMgr()
	inc, _ := m.Add(trig("shop", "a", clk.Now()))
	m.TakeReady()
	m.SetStatus(inc.ID, StatusInvestigating)
	m.Add(trig("shop", "b", clk.Now()))
	if len(m.TakeReady()) != 0 {
		t.Fatal("investigating incident must not be re-queued")
	}
	m.SetStatus(inc.ID, StatusDecided)
	if r := m.TakeReady(); len(r) != 1 || len(r[0].Members) != 2 {
		t.Fatalf("ready=%v", r)
	}
}

func TestManager_StormCollapsesToOneIncident(t *testing.T) {
	m, clk := newMgr()
	started := 0
	var last *Incident
	for i := range 30 {
		inc, s := m.Add(trig(fmt.Sprintf("p%d", i%5), fmt.Sprintf("w%d", i), clk.Now()))
		if s {
			started++
		}
		last = inc
		clk.Step(2 * time.Second)
	}
	if started != 1 || !m.InStorm() || !last.Storm {
		t.Fatalf("started=%d storm=%v", started, m.InStorm())
	}
	if r := m.TakeReady(); len(r) != 1 || !r[0].Storm {
		t.Fatalf("ready=%d", len(r))
	}
}

func TestManager_StormEndsAfterQuiet(t *testing.T) {
	m, clk := newMgr()
	for i := range 25 {
		m.Add(trig("shop", fmt.Sprintf("w%d", i), clk.Now()))
	}
	clk.Step(9 * time.Minute)
	m.Tick(clk.Now())
	if !m.InStorm() {
		t.Fatal("storm ended early")
	}
	clk.Step(time.Minute)
	closed := m.Tick(clk.Now())
	if m.InStorm() {
		t.Fatal("storm should have ended")
	}
	foundStorm := false
	for _, c := range closed {
		foundStorm = foundStorm || c.Storm
	}
	if !foundStorm {
		t.Fatal("storm incident should close")
	}
	if inc, s := m.Add(trig("shop", "x", clk.Now())); s || inc.Storm {
		t.Fatal("new trigger after storm should be normal")
	}
}

func TestManager_StormInvestigatedOnce(t *testing.T) {
	m, clk := newMgr()
	var storm *Incident
	for i := range 25 {
		storm, _ = m.Add(trig("shop", fmt.Sprintf("w%d", i), clk.Now()))
	}
	if r := m.TakeReady(); len(r) != 1 {
		t.Fatalf("ready=%d", len(r))
	}
	m.SetStatus(storm.ID, StatusDecided)
	inc, _ := m.Add(trig("shop", "late", clk.Now()))
	if !inc.Storm || len(inc.Members) != 6 {
		t.Fatal("later triggers must still attach")
	}
	if r := m.TakeReady(); len(r) != 0 {
		t.Fatalf("storm re-queued: %d", len(r))
	}
}

func TestManager_IDsUniqueAcrossRestarts(t *testing.T) {
	m1, clk := newMgr()
	m2, _ := newMgr()
	a, _ := m1.Add(trig("shop", "a", clk.Now()))
	b, _ := m2.Add(trig("shop", "a", clk.Now()))
	if a.ID == b.ID || len(m1.nonce) != 6 {
		t.Fatalf("a=%s b=%s nonce=%q", a.ID, b.ID, m1.nonce)
	}
}

func TestManager_DecidedRequeueRules(t *testing.T) {
	m, clk := newMgr()
	mk := func(typ signal.Type, shortfall float64) signal.Trigger {
		tr := trig("shop", "a", clk.Now())
		tr.Type, tr.Shortfall = typ, shortfall
		return tr
	}
	inc, _ := m.Add(mk(signal.Anomaly, 1))
	m.TakeReady()
	m.SetStatus(inc.ID, StatusDecided)

	// Same type, similar shortfall: attaches without re-queueing.
	clk.Step(6 * time.Minute)
	m.Add(mk(signal.Anomaly, 1.5))
	m.Add(mk(signal.Anomaly, 1))
	if len(m.TakeReady()) != 0 {
		t.Fatal("repeated same-type triggers must not re-queue a decided incident")
	}

	// A type not yet in the incident re-queues.
	m.Add(mk(signal.ForecastBreach, 0.3))
	r := m.TakeReady()
	if len(r) != 1 || len(r[0].Members["a"].Triggers) != 4 {
		t.Fatalf("new type must re-queue and attach, got %v", r)
	}
	m.SetStatus(inc.ID, StatusDecided)

	// Within 5 minutes of the last queue, even a new type only attaches.
	clk.Step(2 * time.Minute)
	m.Add(mk(signal.ModelDisagreement, 0.5))
	if len(m.TakeReady()) != 0 {
		t.Fatal("5-minute floor must hold")
	}

	// After the floor, a >=2x shortfall of a known type re-queues.
	clk.Step(4 * time.Minute)
	m.Add(mk(signal.Anomaly, 2.9)) // max so far is 1.5: not 2x
	if len(m.TakeReady()) != 0 {
		t.Fatal("under 2x the max shortfall must not re-queue")
	}
	m.Add(mk(signal.Anomaly, 6))
	if len(m.TakeReady()) != 1 {
		t.Fatal("2x the max shortfall must re-queue")
	}
}
