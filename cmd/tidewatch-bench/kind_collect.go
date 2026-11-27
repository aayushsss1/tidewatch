package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// The kind-collect command reads one real-cluster run (hack/bench-kind.sh) back from Prometheus
// and appends its rows to results.csv, in the simulator's schema plus env, p95_ms and p99_ms.
//
// Load and readiness come from two recording rules (demo/bench/rules.yaml):
//
//	bench:load_rps{app}         requests/s, the same expression as the Tidewatch load profile
//	bench:ready_replicas{app}   ready replicas of the Deployment
//
// Both are sampled once a minute over [start, end]. A minute is a breach when load exceeds
// ready x capacity; needed replicas are ceil(load / (capacity x headroom)); over- and
// under-provisioning are the replica-minutes above and below needed. A minute where either
// series has no sample is skipped (and counted in skipped_steps) rather than assumed.
//
// Columns the cluster run cannot measure (lift_rm, safety_violations, llm_calls) are left empty
// rather than written as 0, so a reader cannot mistake "not measured" for "none".

var kindHeader = append(append([]string{}, csvHeader...), "env", "p95_ms", "p99_ms")

const (
	paramQuery = "query"
	trueStr    = "true"
	kindEnv    = "kind"
	loadRule   = "bench:load_rps"
	readyRule  = "bench:ready_replicas"
	kindStep   = time.Minute
	guardMatch = "matched"
	guardNA    = "n/a"
)

type kindConfig struct {
	Prom, Out, Scenario, Config, Guardrails, Model string
	Namespace                                      string
	Workloads                                      []string
	Start, End                                     time.Time
	Capacity, Headroom                             float64
	Seed                                           int64
	Agent                                          bool
}

func kindCollectCmd(ctx context.Context, args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("kind-collect", flag.ContinueOnError)
	var kc kindConfig
	var start, end, wls string
	fs.StringVar(&kc.Prom, "prom", "http://localhost:9090", "Prometheus base URL")
	fs.StringVar(&kc.Out, "out", "", "results.csv to append to (required)")
	fs.StringVar(&kc.Scenario, "scenario", "flash-sale-cascade", "scenario name")
	fs.StringVar(&kc.Config, "config", "", "config name: hpa, keda, tidewatch-baseline, tidewatch-agent (required)")
	fs.StringVar(&kc.Guardrails, "guardrails", "", "guardrail profile; default matched for tidewatch-*, n/a otherwise")
	fs.StringVar(&kc.Model, "model", "", "agent model, for the usd estimate")
	fs.StringVar(&kc.Namespace, "namespace", "shop", "namespace of the workloads")
	fs.StringVar(&wls, "workloads", "frontend,api,worker", "workloads, lead first")
	fs.StringVar(&start, "start", "", "window start, RFC3339 (required)")
	fs.StringVar(&end, "end", "", "window end, RFC3339 (required)")
	fs.Float64Var(&kc.Capacity, "capacity", 50, "per-pod capacity, requests/s")
	fs.Float64Var(&kc.Headroom, "headroom", 0.8, "target utilization used for needed replicas")
	fs.Int64Var(&kc.Seed, "seed", 1, "traffic seed, recorded in the CSV")
	fs.BoolVar(&kc.Agent, "agent", false, "read agent tokens and investigations from the operator's metrics")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if kc.Out == "" || kc.Config == "" || start == "" || end == "" {
		return errors.New("kind-collect needs --out, --config, --start and --end")
	}
	var err error
	if kc.Start, err = time.Parse(time.RFC3339, start); err != nil {
		return fmt.Errorf("--start: %w", err)
	}
	if kc.End, err = time.Parse(time.RFC3339, end); err != nil {
		return fmt.Errorf("--end: %w", err)
	}
	if !kc.End.After(kc.Start) {
		return errors.New("--end must be after --start")
	}
	if kc.Capacity <= 0 || kc.Headroom <= 0 {
		return errors.New("--capacity and --headroom must be positive")
	}
	kc.Workloads = strings.Split(wls, ",")
	if kc.Guardrails == "" {
		kc.Guardrails = guardNA
		if strings.HasPrefix(kc.Config, "tidewatch-") {
			kc.Guardrails = guardMatch
		}
	}
	pc := &promClient{base: strings.TrimRight(kc.Prom, "/"), hc: &http.Client{Timeout: 30 * time.Second}}
	rows, err := collectKind(ctx, pc, kc)
	if err != nil {
		return err
	}
	out, err := openCSV(kc.Out, kindHeader)
	if err != nil {
		return err
	}
	defer func() { _ = out.close() }()
	if err := out.write(rows); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(stdout, "kind-collect: %d rows for %s appended to %s\n", len(rows), kc.Config, kc.Out)
	return nil
}

// point is one sample of a range query.
type point struct {
	T int64 // unix seconds
	V float64
}

type promClient struct {
	base string
	hc   *http.Client
}

type promResponse struct {
	Status string `json:"status"`
	Error  string `json:"error"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  [2]any            `json:"value"`
			Values [][2]any          `json:"values"`
		} `json:"result"`
	} `json:"data"`
}

func (c *promClient) get(ctx context.Context, path string, q url.Values) (*promResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+path+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var pr promResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, fmt.Errorf("prometheus %s: %w", path, err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("prometheus %s: %s", path, pr.Error)
	}
	return &pr, nil
}

func parseSample(v [2]any) (point, bool) {
	ts, ok := v[0].(float64)
	s, ok2 := v[1].(string)
	if !ok || !ok2 {
		return point{}, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return point{}, false
	}
	return point{T: int64(math.Round(ts)), V: f}, true
}

// rangeSeries returns the single series of a range query, or nil when it matched nothing.
func (c *promClient) rangeSeries(ctx context.Context, query string, kc kindConfig) ([]point, error) {
	pr, err := c.get(ctx, "/api/v1/query_range", url.Values{
		paramQuery: {query}, "step": {strconv.Itoa(int(kindStep.Seconds()))},
		"start": {strconv.FormatInt(kc.Start.Unix(), 10)}, "end": {strconv.FormatInt(kc.End.Unix(), 10)},
	})
	if err != nil {
		return nil, err
	}
	if len(pr.Data.Result) == 0 {
		return nil, nil
	}
	var out []point
	for _, v := range pr.Data.Result[0].Values {
		if p, ok := parseSample(v); ok {
			out = append(out, p)
		}
	}
	return out, nil
}

// scalar returns an instant query's value at t; ok is false when nothing matched or it is NaN.
func (c *promClient) scalar(ctx context.Context, query string, t time.Time) (float64, bool, error) {
	pr, err := c.get(ctx, "/api/v1/query", url.Values{paramQuery: {query}, "time": {strconv.FormatInt(t.Unix(), 10)}})
	if err != nil {
		return 0, false, err
	}
	if len(pr.Data.Result) == 0 {
		return 0, false, nil
	}
	p, ok := parseSample(pr.Data.Result[0].Value)
	return p.V, ok, nil
}

// vector returns an instant query's values keyed by one label.
func (c *promClient) vector(ctx context.Context, query, label string, t time.Time) (map[string]float64, error) {
	pr, err := c.get(ctx, "/api/v1/query", url.Values{paramQuery: {query}, "time": {strconv.FormatInt(t.Unix(), 10)}})
	if err != nil {
		return nil, err
	}
	out := map[string]float64{}
	for _, r := range pr.Data.Result {
		if p, ok := parseSample(r.Value); ok {
			out[r.Metric[label]] += p.V
		}
	}
	return out, nil
}

// provisioning is the minute-by-minute accounting of one workload.
type provisioning struct {
	BreachMin, OverRM, UnderRM float64
	Skipped                    int
}

// accountMinutes joins load and ready on timestamp, one sample per minute.
func accountMinutes(load, ready []point, capacity, headroom float64) provisioning {
	rd := make(map[int64]float64, len(ready))
	for _, p := range ready {
		rd[p.T] = p.V
	}
	var out provisioning
	for _, l := range load {
		r, ok := rd[l.T]
		if !ok {
			out.Skipped++
			continue
		}
		if l.V > r*capacity {
			out.BreachMin++
		}
		needed := math.Ceil(l.V/(capacity*headroom) - 1e-9)
		out.OverRM += math.Max(r-needed, 0)
		out.UnderRM += math.Max(needed-r, 0)
	}
	// A ready sample with no load sample is also a minute we could not account.
	ld := make(map[int64]bool, len(load))
	for _, p := range load {
		ld[p.T] = true
	}
	for _, p := range ready {
		if !ld[p.T] {
			out.Skipped++
		}
	}
	return out
}

func collectKind(ctx context.Context, pc *promClient, kc kindConfig) ([][]string, error) {
	f := func(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }
	window := fmt.Sprintf("%ds", int(kc.End.Sub(kc.Start).Seconds()))
	wall := kc.End.Sub(kc.Start).Seconds()

	var tokIn, tokOut, invest float64
	if kc.Agent {
		tok, err := pc.vector(ctx, `sum by (direction) (tidewatch_agent_tokens_total)`, "direction", kc.End)
		if err != nil {
			return nil, err
		}
		tokIn, tokOut = tok["input"], tok["output"]
		if invest, _, err = pc.scalar(ctx, `sum(tidewatch_agent_investigations_total)`, kc.End); err != nil {
			return nil, err
		}
	}

	rows := make([][]string, 0, len(kc.Workloads))
	for i, w := range kc.Workloads {
		sel := fmt.Sprintf(`app=%q`, w)
		load, err := pc.rangeSeries(ctx, fmt.Sprintf(`%s{%s}`, loadRule, sel), kc)
		if err != nil {
			return nil, err
		}
		ready, err := pc.rangeSeries(ctx, fmt.Sprintf(`%s{%s}`, readyRule, sel), kc)
		if err != nil {
			return nil, err
		}
		if len(load) == 0 || len(ready) == 0 {
			return nil, fmt.Errorf("workload %s: no %s or %s samples in the window; is the PrometheusRule loaded?",
				w, loadRule, readyRule)
		}
		acct := accountMinutes(load, ready, kc.Capacity, kc.Headroom)
		events, _, err := pc.scalar(ctx, fmt.Sprintf(
			`changes(kube_deployment_spec_replicas{namespace=%q,deployment=%q}[%s])`, kc.Namespace, w, window), kc.End)
		if err != nil {
			return nil, err
		}
		quant := func(q string) (string, error) {
			v, ok, err := pc.scalar(ctx, fmt.Sprintf(
				`histogram_quantile(%s, sum by (le) (increase(http_request_duration_seconds_bucket{namespace=%q,app=%q}[%s])))`,
				q, kc.Namespace, w, window), kc.End)
			if err != nil || !ok {
				return "", err
			}
			return f(math.Round(v*1000*100) / 100), nil
		}
		p95, err := quant("0.95")
		if err != nil {
			return nil, err
		}
		p99, err := quant("0.99")
		if err != nil {
			return nil, err
		}
		row := []string{
			kc.Scenario, kc.Config, kc.Guardrails, "0", strconv.FormatInt(kc.Seed, 10), "", w,
			f(acct.BreachMin), f(acct.OverRM), f(acct.UnderRM), strconv.Itoa(int(events)), "", "", "0",
			"0", "", "0", "0", "0", "0", "false",
			kindEnv, p95, p99,
		}
		if i == 0 {
			if kc.Agent {
				row[5] = kc.Model
				row[14] = strconv.Itoa(int(invest))
				row[16], row[17] = strconv.Itoa(int(tokIn)), strconv.Itoa(int(tokOut))
				if cost, ok := usd(kc.Model, int(tokIn), int(tokOut)); ok {
					row[18] = strconv.FormatFloat(cost, 'f', 6, 64)
				}
			}
			row[13] = strconv.Itoa(acct.Skipped)
			row[19] = strconv.FormatFloat(wall, 'f', 2, 64)
			row[20] = trueStr
		} else {
			// Run-level columns live on the lead row only; fold this workload's skips into it.
			rows[0][13] = strconv.Itoa(atoiZero(rows[0][13]) + acct.Skipped)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func atoiZero(s string) int { n, _ := strconv.Atoi(s); return n }
