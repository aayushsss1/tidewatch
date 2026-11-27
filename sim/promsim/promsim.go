// Package promsim is a simulated Prometheus (prom.Querier) that serves each workload's
// scenario load series. It never returns data after the simulation clock.
package promsim

import (
	"context"
	"math"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"k8s.io/utils/clock"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// KeyFunc maps a PromQL query to a scenario workload name.
type KeyFunc func(promql string) (workload string, ok bool)

// Sim implements prom.Querier over scenario load series.
type Sim struct {
	clk    clock.Clock
	epoch  time.Time
	keyOf  KeyFunc
	series map[string]forecast.Series
}

var _ prom.Querier = (*Sim)(nil)

// New returns a simulated Prometheus. keyOf may be nil to use AppLabelKey. Series are
// composed once, with the scenario's epoch overridden by the epoch argument.
func New(sc *scenario.Scenario, clk clock.Clock, epoch time.Time, keyOf KeyFunc) *Sim {
	if keyOf == nil {
		keyOf = AppLabelKey
	}
	s := &Sim{clk: clk, epoch: epoch, keyOf: keyOf, series: map[string]forecast.Series{}}
	cp := *sc // shallow copy; Series only reads shared state
	cp.Epoch = epoch
	for _, w := range cp.Workloads {
		s.series[w.Name] = cp.Series(w.Name)
	}
	return s
}

// AppLabelKey returns the value of the first vector selector's app label matcher.
func AppLabelKey(q string) (string, bool) {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(q)
	if err != nil {
		return "", false
	}
	var name string
	var found bool
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		if found {
			return nil
		}
		if vs, ok := n.(*parser.VectorSelector); ok {
			for _, m := range vs.LabelMatchers {
				if m.Name == "app" && m.Type == labels.MatchEqual {
					name, found = m.Value, true
					break
				}
			}
			// Only the first vector selector counts.
			if !found {
				found = true
			}
		}
		return nil
	})
	return name, name != ""
}

// loadMetric is the only metric the simulator holds: each workload's request rate.
const loadMetric = "http_requests_total"

// isLoadQuery reports whether q reads only the request-rate series: every vector selector is
// loadMetric and the only functions and aggregations are rate, irate, sum and sum by.
func isLoadQuery(q string) bool {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(q)
	if err != nil {
		return false
	}
	ok, selectors := true, 0
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		switch v := n.(type) {
		case *parser.VectorSelector:
			selectors++
			ok = ok && v.Name == loadMetric
		case *parser.Call:
			ok = ok && (v.Func.Name == "rate" || v.Func.Name == "irate")
		case *parser.AggregateExpr:
			ok = ok && v.Op == parser.SUM
		case nil, *parser.MatrixSelector, *parser.ParenExpr: // nil ends a subtree walk
		default:
			ok = false
		}
		return nil
	})
	return ok && selectors > 0
}

// RangeSeries implements prom.Querier. Slot semantics: the point at t is the average load
// over (t-step, t], so serving every t <= now never leaks future data. Only equality
// app matchers resolve; a regex app=~ matcher yields an all-NaN series. Only the request-rate
// series exists: any query that is not a rate or sum of it (a histogram, increase(), another
// metric) yields an all-NaN series, which the agent sees as no data.
func (s *Sim) RangeSeries(
	_ context.Context, q string, start, end time.Time, step time.Duration,
) (forecast.Series, error) {
	n := 0
	if step > 0 {
		n = max(int(end.Sub(start)/step)+1, 0)
	}
	out := forecast.Series{Step: step, Points: make([]forecast.Point, n)}
	for i := range out.Points {
		out.Points[i] = forecast.Point{T: start.Add(time.Duration(i) * step), V: math.NaN()}
	}
	if !isLoadQuery(q) {
		return out, nil
	}
	name, ok := s.keyOf(q)
	if !ok {
		return out, nil
	}
	src, ok := s.series[name]
	if !ok || len(src.Points) == 0 {
		return out, nil
	}
	now := s.clk.Now()
	first := src.Points[0].T
	for i := range out.Points {
		t := out.Points[i].T
		if t.After(now) {
			continue
		}
		j := int(t.Sub(first) / src.Step)
		if t.Before(first) || j >= len(src.Points) {
			continue
		}
		out.Points[i].V = src.Points[j].V
	}
	return out, nil
}

// Ready implements prom.Querier.
func (s *Sim) Ready(context.Context) error { return nil }

// Factory returns a function usable as engine.Options.PromFactory.
func (s *Sim) Factory() func(url string, timeout time.Duration) (prom.Querier, error) {
	return func(string, time.Duration) (prom.Querier, error) { return s, nil }
}
