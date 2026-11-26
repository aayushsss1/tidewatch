package promsim

import (
	"context"
	"math"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"

	"github.com/aayushsss1/tidewatch/sim/scenario"
)

const yml = `
name: t
seed: 1
trace:
  type: synthetic:diurnal
  base: 20
  amp: 10
start: 6h
duration: 2h
step: 1m
history: 1h
workloads:
  - name: frontend
    capacity: 5
  - name: api
    capacity: 5
    lag: 90s
    scale: 2
`

func setup(t *testing.T) (*Sim, *scenario.Scenario, *clocktesting.FakeClock) {
	t.Helper()
	sc, err := scenario.Parse([]byte(yml), "")
	if err != nil {
		t.Fatal(err)
	}
	epoch := time.Date(2030, 5, 1, 0, 0, 0, 0, time.UTC)
	clk := clocktesting.NewFakeClock(epoch.Add(6*time.Hour + 30*time.Minute))
	return New(sc, clk, epoch, nil), sc, clk
}

func TestAlignedValuesMatchScenario(t *testing.T) {
	s, sc, clk := setup(t)
	want := sc.Series("frontend") // default epoch; values depend only on offset
	end := clk.Now()
	q := `sum(rate(http_requests_total{namespace="shop",app="frontend"}[1m]))`
	got, err := s.RangeSeries(context.Background(), q, end.Add(-10*time.Minute), end, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Points) != 11 {
		t.Fatalf("len %d", len(got.Points))
	}
	for _, p := range got.Points {
		off := p.T.Sub(time.Date(2030, 5, 1, 0, 0, 0, 0, time.UTC))
		w := want.Points[int((off-(sc.Start-sc.History))/time.Minute)].V
		if math.IsNaN(p.V) || math.Abs(p.V-w) > 1e-9 {
			t.Fatalf("at %v got %v want %v", p.T, p.V, w)
		}
	}
}

func TestFutureIsNaN(t *testing.T) {
	s, _, clk := setup(t)
	now := clk.Now()
	got, _ := s.RangeSeries(
		context.Background(), `x{app="frontend"}`, now.Add(-2*time.Minute), now.Add(3*time.Minute), time.Minute)
	for i, p := range got.Points {
		if p.T.After(now) != math.IsNaN(p.V) {
			t.Fatalf("point %d (%v) v=%v", i, p.T, p.V)
		}
	}
}

func TestAppMatcherSelectsWorkload(t *testing.T) {
	s, _, clk := setup(t)
	now := clk.Now()
	a, _ := s.RangeSeries(context.Background(), `rate(r{app="frontend"}[5m])`, now, now, time.Minute)
	b, _ := s.RangeSeries(context.Background(), `r{namespace="n", app="api"}`, now, now, time.Minute)
	if math.IsNaN(a.Points[0].V) || math.IsNaN(b.Points[0].V) || a.Points[0].V == b.Points[0].V {
		t.Fatalf("a=%v b=%v", a.Points[0].V, b.Points[0].V)
	}
}

func TestUnknownQueryAllNaN(t *testing.T) {
	s, _, clk := setup(t)
	now := clk.Now()
	for _, q := range []string{`up`, `r{app="nope"}`, `not promql((`} {
		got, err := s.RangeSeries(context.Background(), q, now.Add(-5*time.Minute), now, time.Minute)
		if err != nil || len(got.Points) != 6 {
			t.Fatalf("%q: %v %d", q, err, len(got.Points))
		}
		for _, p := range got.Points {
			if !math.IsNaN(p.V) {
				t.Fatalf("%q: want NaN", q)
			}
		}
	}
}

func TestFactory(t *testing.T) {
	s, _, _ := setup(t)
	q, err := s.Factory()("http://ignored", time.Second)
	if err != nil || q == nil {
		t.Fatal(err)
	}
	if err := q.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
}
