package agent

import (
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/internal/signal"
)

func TestUrgency_ForecastBreachAndContextDue(t *testing.T) {
	u, ttb := Urgency([]signal.Trigger{
		{Type: signal.ForecastBreach, Shortfall: 0.6, TimeToBreach: new(10 * time.Minute)},
		{Type: signal.ContextDue, Shortfall: 3, TimeToBreach: new(30 * time.Second)},
	})
	// context: 3/max(0.5,1)=3 beats breach 0.06
	if u != 3 || ttb == nil || *ttb != 30*time.Second {
		t.Fatalf("u=%v ttb=%v", u, ttb)
	}
	u, _ = Urgency([]signal.Trigger{{Type: signal.ForecastBreach, Shortfall: 0.6, TimeToBreach: new(30 * time.Minute)}})
	if u < 0.0199 || u > 0.0201 {
		t.Fatalf("u=%v", u)
	}
}

func TestUrgency_NonBreachUsesShortfall(t *testing.T) {
	cases := []struct {
		tr   signal.Trigger
		want float64
	}{
		{signal.Trigger{Type: signal.ModelDisagreement, Shortfall: 0.5}, 0.3},
		{signal.Trigger{Type: signal.WorkloadChange}, 0.5},
		{signal.Trigger{Type: signal.OverProvisioned}, 0.01},
		{signal.Trigger{Type: signal.Anomaly, Shortfall: 0.8}, 0.8},
	}
	for _, c := range cases {
		u, ttb := Urgency([]signal.Trigger{c.tr})
		if u != c.want || ttb != nil {
			t.Errorf("%s: u=%v ttb=%v want %v", c.tr.Type, u, ttb, c.want)
		}
	}
	if u, _ := Urgency(nil); u != 0 {
		t.Fatal("empty should be 0")
	}
}

func TestDeadline_TooLate(t *testing.T) {
	now := t0
	if _, ok := Deadline(now, new(40*time.Second), 45*time.Second, time.Minute); ok {
		t.Fatal("expected too late")
	}
	d, ok := Deadline(now, new(10*time.Minute), 45*time.Second, time.Minute)
	if !ok || !d.Equal(now.Add(time.Minute)) {
		t.Fatalf("d=%v ok=%v", d, ok)
	}
	d, ok = Deadline(now, new(100*time.Second), 45*time.Second, time.Minute)
	if !ok || !d.Equal(now.Add(25*time.Second)) {
		t.Fatalf("d=%v ok=%v", d, ok)
	}
	d, ok = Deadline(now, nil, 45*time.Second, time.Minute)
	if !ok || !d.Equal(now.Add(time.Minute)) {
		t.Fatalf("nil ttb: d=%v ok=%v", d, ok)
	}
}

func TestPriorityOf_AnnotationWinsAndMapping(t *testing.T) {
	p := func(v int32) *int32 { return &v }
	cases := []struct {
		pp   *int32
		ann  string
		want int
	}{
		{nil, "", 5}, {p(0), "", 3}, {p(-5), "", 3}, {p(1_000_000_000), "", 10}, {p(2_000_000_000), "", 10},
		{p(1000), "", 6}, {p(1_000_000), "", 8}, {p(1), "", 4},
		{p(0), "9", 9}, {nil, "1", 1}, {nil, "10", 10},
		{nil, "11", 5}, {nil, "0", 5}, {nil, "x", 5}, {p(0), "99", 3},
	}
	for _, c := range cases {
		if got := PriorityOf(c.pp, c.ann); got != c.want {
			t.Errorf("PriorityOf(%v,%q)=%d want %d", c.pp, c.ann, got, c.want)
		}
	}
}
