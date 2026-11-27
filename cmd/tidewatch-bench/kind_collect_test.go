package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"sigs.k8s.io/yaml"
)

func TestAccountMinutes(t *testing.T) {
	// capacity 50, headroom 0.8: one replica serves 40 rps comfortably, 50 at the limit.
	load := []point{{0, 30}, {60, 60}, {120, 160}, {180, 160}, {240, 40}, {300, 10}}
	ready := []point{{0, 2}, {60, 1}, {120, 2}, {180, 4}, {240, 1} /* 300 missing */}
	got := accountMinutes(load, ready, 50, 0.8)
	// t0: need 1, ready 2 -> over 1. t60: 60>50 breach, need 2, ready 1 -> under 1.
	// t120: 160>100 breach, need 4, ready 2 -> under 2. t180: 160<=200, need 4 -> 0.
	// t240: 40<=50, need exactly 1 (40/40, not 2), ready 1 -> 0. t300: no ready -> skipped.
	want := provisioning{BreachMin: 2, OverRM: 1, UnderRM: 3, Skipped: 1}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestAccountMinutesCountsReadyOnlySamplesAsSkipped(t *testing.T) {
	got := accountMinutes([]point{{0, 10}}, []point{{0, 1}, {60, 1}}, 50, 0.8)
	if got.Skipped != 1 {
		t.Fatalf("skipped = %d, want 1", got.Skipped)
	}
}

// fakeProm answers the queries collectKind makes from canned series.
func fakeProm(t *testing.T, seen *[]url.Values) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	series := func(vals ...string) string {
		var b strings.Builder
		for i, v := range vals {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `[%d,%q]`, 1700000000+60*i, v)
		}
		return `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{},"values":[` + b.String() + `]}]}}`
	}
	inst := func(v string) string {
		return `{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1700000300,"` + v + `"]}]}}`
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		mu.Lock()
		v := r.URL.Query()
		v.Set("path", r.URL.Path)
		*seen = append(*seen, v)
		mu.Unlock()
		var body string
		switch {
		case r.URL.Path == pathRange && strings.Contains(q, loadRule):
			body = series("30", "60", "160", "160", "40")
		case r.URL.Path == pathRange && strings.Contains(q, readyRule):
			body = series("2", "1", "2", "4", "1")
		case strings.HasPrefix(q, "changes("):
			body = inst("3")
		case strings.Contains(q, "0.95"):
			body = inst("0.0125")
		case strings.Contains(q, "0.99"):
			body = inst("NaN")
		case strings.Contains(q, "tidewatch_agent_tokens_total"):
			body = `{"status":"success","data":{"resultType":"vector","result":[` +
				`{"metric":{"direction":"input"},"value":[1,"2000000"]},{"metric":{"direction":"output"},"value":[1,"400000"]}]}}`
		case strings.Contains(q, "tidewatch_agent_investigations_total"):
			body = inst("7")
		default:
			t.Errorf("unexpected query %s %q", r.URL.Path, q)
			body = `{"status":"error","error":"unexpected"}`
		}
		_, _ = w.Write([]byte(body))
	}))
}

func TestKindCollectWritesRows(t *testing.T) {
	var seen []url.Values
	srv := fakeProm(t, &seen)
	defer srv.Close()
	out := filepath.Join(t.TempDir(), "results.csv")
	var stdout bytes.Buffer
	args := []string{"--prom", srv.URL, "--out", out, "--config", "tidewatch-agent", "--model", defaultModel,
		"--agent", "--workloads", "frontend,api", "--start", "2023-11-14T22:13:20Z", "--end", "2023-11-14T22:18:20Z"}
	if err := kindCollectCmd(context.Background(), args, &stdout); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(out) //nolint:gosec // test path
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("rows = %d, want header + 2", len(recs))
	}
	col := map[string]int{}
	for i, h := range recs[0] {
		col[h] = i
	}
	for _, h := range []string{"env", "p95_ms", "p99_ms", "breach_min", "lead"} {
		if _, ok := col[h]; !ok {
			t.Fatalf("missing column %s in %v", h, recs[0])
		}
	}
	lead, other := recs[1], recs[2]
	check := func(row []string, name, want string) {
		t.Helper()
		if row[col[name]] != want {
			t.Errorf("%s/%s = %q, want %q", row[col["workload"]], name, row[col[name]], want)
		}
	}
	check(lead, "env", "kind")
	check(lead, "guardrails", "matched")
	check(lead, "breach_min", "2")
	check(lead, "overprov_rm", "1")
	check(lead, "underprov_rm", "3")
	check(lead, "scale_events", "3")
	check(lead, "p95_ms", "12.5")
	check(lead, "p99_ms", "") // NaN is not a number
	check(lead, "input_tokens", "2000000")
	check(lead, "output_tokens", "400000")
	check(lead, "investigations", "7")
	check(lead, "usd", "0.400000") // 2M x 0.10 + 0.4M x 0.50 per million
	check(lead, "wall_seconds", "300.00")
	check(lead, "lead", "true")
	check(lead, "lift_rm", "")
	check(other, "lead", "false")
	check(other, "input_tokens", "0")

	assertQueries(t, seen)
}

// assertQueries checks what was asked of Prometheus, not just what came back.
func assertQueries(t *testing.T, seen []url.Values) {
	t.Helper()
	const window = "[300s]" // 22:13:20 to 22:18:20
	var hist, changes, ranges int
	for _, v := range seen {
		q := v.Get("query")
		switch {
		case v.Get("path") == pathRange:
			ranges++
			if v.Get("step") != "60" || v.Get("start") != "1700000000" || v.Get("end") != "1700000300" {
				t.Errorf("range params step/start/end = %s/%s/%s for %q", v.Get("step"), v.Get("start"), v.Get("end"), q)
			}
		case strings.Contains(q, "histogram_quantile"):
			hist++
			for _, want := range []string{"http_request_duration_seconds_bucket", "increase(", window, "by (le)", `app="`} {
				if !strings.Contains(q, want) {
					t.Errorf("histogram query %q lacks %q", q, want)
				}
			}
			if v.Get("time") != "1700000300" {
				t.Errorf("histogram evaluated at %s, want the window end", v.Get("time"))
			}
		case strings.HasPrefix(q, "changes("):
			changes++
			if !strings.Contains(q, "kube_deployment_spec_replicas") || !strings.Contains(q, window) {
				t.Errorf("changes query %q", q)
			}
		}
	}
	// Two workloads: load and ready ranges each; p95 and p99 each; one changes() each.
	if ranges != 4 || hist != 4 || changes != 2 {
		t.Errorf("range/histogram/changes queries = %d/%d/%d, want 4/4/2", ranges, hist, changes)
	}
}

func TestRulesFileMatchesCollector(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "demo", "bench", "rules.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Spec struct {
			Groups []struct {
				Rules []struct{ Record, Expr string } `yaml:"rules"`
			} `yaml:"groups"`
		} `yaml:"spec"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	exprs := map[string]string{}
	for _, g := range doc.Spec.Groups {
		for _, r := range g.Rules {
			exprs[r.Record] = r.Expr
		}
	}
	if e := exprs[readyRule]; !strings.Contains(e, "kube_deployment_status_replicas_ready") {
		t.Errorf("%s expr = %q, want kube_deployment_status_replicas_ready", readyRule, e)
	}
	if e := exprs[loadRule]; !strings.Contains(e, "http_requests_total") {
		t.Errorf("%s expr = %q", loadRule, e)
	}
}

func TestRangeSeriesRejectsSeveralSeries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"result":[` +
			`{"metric":{"a":"1"},"values":[]},{"metric":{"a":"2"},"values":[]}]}}`))
	}))
	defer srv.Close()
	pc := &promClient{base: srv.URL, hc: srv.Client()}
	if _, err := pc.rangeSeries(context.Background(), "x", kindConfig{}); err == nil {
		t.Fatal("two series should be an error")
	}
}
