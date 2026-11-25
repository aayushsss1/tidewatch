package tools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/llm"
)

type fakeSource struct {
	events int
	points int
}

func (f fakeSource) State(context.Context, WorkloadRef) (WorkloadState, error) {
	return WorkloadState{Replicas: 3, Ready: 3}, nil
}
func (f fakeSource) Forecasts(context.Context, WorkloadRef) (ForecastSet, error) {
	return ForecastSet{Models: []ModelForecast{{Name: "hw", Peak: 10}}}, nil
}
func (f fakeSource) Related(context.Context, WorkloadRef) ([]Related, error)  { return nil, nil }
func (f fakeSource) Outcomes(context.Context, WorkloadRef) ([]Outcome, error) { return nil, nil }
func (f fakeSource) Events(context.Context, WorkloadRef, time.Duration) ([]Event, error) {
	out := make([]Event, f.events)
	for i := range out {
		out[i] = Event{
			Reason: "BackOff", Type: "Warning", Count: int32(i), LastSeen: time.Unix(int64(i), 0).UTC(),
			Message: strings.Repeat("m", 300),
		}
	}
	return out, nil
}
func (f fakeSource) Rollouts(context.Context, WorkloadRef) ([]Rollout, error) { return nil, nil }
func (f fakeSource) OpsNotes(context.Context) (string, error)                 { return "notes", nil }
func (f fakeSource) SimilarEpisodes(context.Context, WorkloadRef) ([]EpisodeSummary, error) {
	return nil, nil
}
func (f fakeSource) QueryRange(_ context.Context, _ string, _, _ time.Duration) (Series, error) {
	s := Series{}
	for i := 0; i < f.points; i++ {
		s.Points = append(s.Points, Point{T: int64(i), V: float64(i)})
	}
	return s, nil
}

const qm1h = `{"promql":"x{namespace=\"shop\"}","range":"1h","step":"15s"}`

var webArg = json.RawMessage(`{"workload":"web"}`)

var scope = Scope{Policy: "shop.p", Namespaces: []string{"shop"}, Workloads: []WorkloadRef{{"shop", "web"}}}

func find(t *testing.T, ts []llm.Tool, name string) llm.Tool {
	t.Helper()
	for _, x := range ts {
		if x.Def().Name == name {
			return x
		}
	}
	t.Fatalf("no tool %s", name)
	return nil
}

func TestTools_TwelveDefinitionsWithSchemas(t *testing.T) {
	ts := For(fakeSource{}, scope)
	if len(ts) != 12 {
		t.Fatalf("got %d", len(ts))
	}
	term := 0
	for _, x := range ts {
		d := x.Def()
		var s map[string]any
		if err := json.Unmarshal(d.InputSchema, &s); err != nil || s["type"] != "object" {
			t.Errorf("%s schema bad: %v", d.Name, err)
		}
		if _, ok := s["properties"]; !ok {
			t.Errorf("%s no properties", d.Name)
		}
		if x.Terminal() {
			term++
		}
	}
	if term != 2 {
		t.Fatalf("terminal=%d", term)
	}
	for _, n := range []string{"propose_scaling", "no_action"} {
		if out, err := find(t, ts, n).Run(context.Background(), nil); err != nil || out != "ok" {
			t.Errorf("%s run: %q %v", n, out, err)
		}
	}
}

func TestTools_RejectsOutOfScopeWorkload(t *testing.T) {
	ts := For(fakeSource{}, scope)
	_, err := find(t, ts, "get_workload_state").Run(context.Background(), json.RawMessage(`{"workload":"other"}`))
	if err == nil || err.Error() != "workload not in incident scope" {
		t.Fatalf("err=%v", err)
	}
	out, err := find(t, ts, "get_workload_state").Run(context.Background(), webArg)
	if err != nil || !json.Valid([]byte(out)) {
		t.Fatalf("%q %v", out, err)
	}
}

func TestQueryMetrics_RequiresNamespaceMatcher(t *testing.T) {
	tl := find(t, For(fakeSource{points: 3}, scope), "query_metrics")
	run := func(q string) error {
		in, _ := json.Marshal(map[string]any{"promql": q, "range": "10m", "step": "1m"})
		_, err := tl.Run(context.Background(), in)
		return err
	}
	if run(`sum(rate(x[1m]))`) == nil {
		t.Error("missing ns accepted")
	}
	if err := run(`sum(rate(x{namespace="shop"}[1m]))`); err != nil {
		t.Errorf("ok query rejected: %v", err)
	}
	for _, q := range []string{
		`x{namespace="kube-system"}`, `x{namespace=~"shop|kube-system"}`, `x{namespace!="shop"}`,
		`x{namespace="shop"} + y`, `x{`,
	} {
		if run(q) == nil {
			t.Errorf("accepted %s", q)
		}
	}
	in := `{"promql":"x{namespace=\"shop\"}","range":"48h","step":"1m"}`
	if _, err := tl.Run(context.Background(), json.RawMessage(in)); err == nil {
		t.Error("48h accepted")
	}
	in = `{"promql":"x{namespace=\"shop\"}","range":"1h","step":"5s"}`
	if _, err := tl.Run(context.Background(), json.RawMessage(in)); err == nil {
		t.Error("5s step accepted")
	}
}

func TestQueryMetrics_DownsamplesTo200(t *testing.T) {
	tl := find(t, For(fakeSource{points: 1000}, scope), "query_metrics")
	out, err := tl.Run(context.Background(), json.RawMessage(qm1h))
	if err != nil {
		t.Fatal(err)
	}
	var env struct {
		Result struct {
			Points []Point `json:"points"`
			Max    float64 `json:"max"`
			Last   float64 `json:"last"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &env); err != nil {
		t.Fatalf("%v: %s", err, out)
	}
	s := env.Result
	if len(s.Points) > 200 || len(s.Points) == 0 {
		t.Fatalf("points=%d", len(s.Points))
	}
	if s.Max != 999 || s.Last != 999 {
		t.Fatalf("stats %+v", s)
	}
}

func TestTools_OutputsCappedAt4KB(t *testing.T) {
	ts := For(fakeSource{events: 500}, scope)
	out, err := find(t, ts, "get_events").Run(context.Background(), webArg)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) > 4096 || !json.Valid([]byte(out)) {
		t.Fatalf("len=%d valid=%v", len(out), json.Valid([]byte(out)))
	}
	if !strings.Contains(out, "data") {
		t.Error("not labelled as data")
	}
}

func TestEvents_RedactsMessages(t *testing.T) {
	sc := scope
	sc.RedactEvents = true
	out, _ := find(t, For(fakeSource{events: 2}, sc), "get_events").Run(context.Background(), webArg)
	if strings.Contains(out, "mmmm") || strings.Contains(out, "message") {
		t.Fatalf("message leaked: %s", out)
	}
	if !strings.Contains(out, "BackOff") {
		t.Fatal(out)
	}
	out, _ = find(t, For(fakeSource{events: 1}, scope), "get_events").Run(context.Background(), webArg)
	if !strings.Contains(out, "mmmm") || strings.Contains(out, strings.Repeat("m", 201)) {
		t.Fatalf("unredacted should truncate to 200: %s", out)
	}
}

func call(name, in string) llm.ToolCall {
	return llm.ToolCall{ID: "1", Name: name, Input: json.RawMessage(in)}
}

const proposal = `{"overrides":[` +
	`{"workload":"web","minReplicas":5,"start":"2026-01-01T10:00:00Z","end":"2026-01-01T11:00:00Z"},` +
	`{"workload":"api","minReplicas":2,"end":"2026-01-01T11:00:00Z"}],` +
	`"reason":"r","confidence":0.8,"trustedModel":"hw","episodeRefs":["e1"]}`

func TestParseFinal_ProposalAndNoAction(t *testing.T) {
	p, na, err := ParseFinal(call("propose_scaling", proposal))
	if err != nil || na != nil || p == nil {
		t.Fatalf("%v %v %v", p, na, err)
	}
	if len(p.Overrides) != 2 || p.Overrides[0].MinReplicas != 5 || p.TrustedModel != "hw" ||
		p.EpisodeRefs[0] != "e1" || !p.Overrides[1].Start.IsZero() {
		t.Fatalf("%+v", p)
	}
	p, na, err = ParseFinal(call("no_action", `{"reason":"fine","confidence":0.9}`))
	if err != nil || p != nil || na == nil || na.Reason != "fine" || na.Confidence != 0.9 {
		t.Fatalf("%v %v %v", p, na, err)
	}
}

func TestParseFinal_RejectsMalformed(t *testing.T) {
	cases := map[string]llm.ToolCall{
		"no reason": call("no_action", `{"confidence":0.5}`),
		"conf high": call("no_action", `{"reason":"x","confidence":1.5}`),
		"conf neg":  call("propose_scaling", `{"overrides":[],"reason":"x","confidence":-0.1}`),
		"neg min": call("propose_scaling",
			`{"overrides":[{"workload":"w","minReplicas":-1}],"reason":"x","confidence":0.5}`),
		"end<start": call("propose_scaling", `{"overrides":[{"workload":"w","minReplicas":1,"start":"2026-01-01T11:00:00Z",`+
			`"end":"2026-01-01T10:00:00Z"}],"reason":"x","confidence":0.5}`),
		"bad json":     call("no_action", `{`),
		"bad type":     call("no_action", `{"reason":5,"confidence":0.5}`),
		"unknown tool": call("get_events", `{}`),
	}
	for n, c := range cases {
		if _, _, err := ParseFinal(c); err == nil {
			t.Errorf("%s accepted", n)
		}
	}
}
