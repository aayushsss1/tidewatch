package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

const (
	maxRange  = 24 * time.Hour
	minStep   = 15 * time.Second
	maxPoints = 200
)

// checkNamespaces requires every selector in the expression to carry an
// equality namespace matcher whose value is in the incident's namespaces.
func checkNamespaces(q string, allowed []string) error {
	expr, err := parser.NewParser(parser.Options{}).ParseExpr(q)
	if err != nil {
		return fmt.Errorf("invalid promql: %w", err)
	}
	return parser.Walk(nsVisitor{allowed}, expr, nil)
}

type nsVisitor struct{ allowed []string }

func (v nsVisitor) Visit(n parser.Node, _ []parser.Node) (parser.Visitor, error) {
	var vs *parser.VectorSelector
	switch s := n.(type) {
	case *parser.SubqueryExpr:
		if s.Range > maxRange || absDur(s.OriginalOffset) > maxRange || absDur(s.Offset) > maxRange {
			return nil, errors.New("subquery range/offset must be at most 24h")
		}
		return v, nil
	case *parser.VectorSelector:
		vs = s
	case *parser.MatrixSelector:
		if s.Range > maxRange {
			return nil, errors.New("selector range must be at most 24h")
		}
		var ok bool
		if vs, ok = s.VectorSelector.(*parser.VectorSelector); !ok {
			return nil, errors.New("unsupported selector")
		}
	default:
		return v, nil
	}
	if absDur(vs.OriginalOffset) > maxRange || absDur(vs.Offset) > maxRange {
		return nil, errors.New("offset must be at most 24h")
	}
	if vs.Timestamp != nil || vs.StartOrEnd != 0 {
		return nil, errors.New("@ modifier is not allowed")
	}
	for _, m := range vs.LabelMatchers {
		if m.Name == "namespace" && m.Type == labels.MatchEqual && slices.Contains(v.allowed, m.Value) {
			return v, nil
		}
	}
	return nil, errors.New(`every selector needs an equality namespace matcher for an incident namespace`)
}

func absDur(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

func parseDur(name, s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q", name, s)
	}
	return d, nil
}

func downsample(p []Point, n int) []Point {
	if len(p) <= n {
		return p
	}
	size := (len(p) + n - 1) / n
	var out []Point
	for i := 0; i < len(p); i += size {
		end := min(i+size, len(p))
		var t int64
		var v float64
		for _, x := range p[i:end] {
			t += x.T
			v += x.V
		}
		out = append(out, Point{T: t / int64(end-i), V: v / float64(end-i)})
	}
	return out
}

func queryMetrics(src Source, sc Scope) llm.Tool {
	return &tool{
		def: llm.ToolDef{
			Name: "query_metrics",
			Description: "Run a PromQL range query (range <=24h, step >=15s). Every selector must have an " +
				"equality namespace matcher for an incident namespace. Returns <=200 points plus " +
				"min/max/mean/last. " + dataNote,
			InputSchema: schema(map[string]any{
				"promql": str("PromQL expression; every selector needs namespace=\"<incident namespace>\""),
				"range":  str("Go duration, at most 24h, e.g. 1h"),
				"step":   str("Go duration, at least 15s, e.g. 1m"),
			}, "promql", "range", "step"),
		},
		run: func(ctx context.Context, in json.RawMessage) (string, error) {
			var a struct{ Promql, Range, Step string }
			if err := decode(in, &a); err != nil {
				return "", err
			}
			if err := checkNamespaces(a.Promql, sc.Namespaces); err != nil {
				return "", err
			}
			rng, err := parseDur("range", a.Range)
			if err != nil {
				return "", err
			}
			step, err := parseDur("step", a.Step)
			if err != nil {
				return "", err
			}
			if rng <= 0 || rng > maxRange {
				return "", errors.New("range must be in (0, 24h]")
			}
			if step < minStep {
				return "", errors.New("step must be at least 15s")
			}
			s, err := src.QueryRange(ctx, a.Promql, rng, step)
			if err != nil {
				return "", err
			}
			var clean []Point
			for _, p := range s.Points {
				if !math.IsNaN(p.V) && !math.IsInf(p.V, 0) {
					clean = append(clean, p)
				}
			}
			if len(clean) > 0 {
				s.Min, s.Max, s.Last = clean[0].V, clean[0].V, clean[len(clean)-1].V
				sum := 0.0
				for _, p := range clean {
					s.Min, s.Max = math.Min(s.Min, p.V), math.Max(s.Max, p.V)
					sum += p.V
				}
				s.Mean = sum / float64(len(clean))
			}
			return render("query_metrics", map[string]any{
				"points": downsample(clean, maxPoints),
				"min":    s.Min, "max": s.Max, "mean": s.Mean, "last": s.Last,
			}), nil
		},
	}
}
