package scenario

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const flat = `
name: t
seed: 1
trace:
  type: synthetic:diurnal
  base: 10
  amp: 0
start: 6h
duration: 2h
step: 1m
history: 1h
workloads:
  - name: frontend
    capacity: 5
    startupDelay: 30s
    minReplicas: 1
    maxReplicas: 50
  - name: api
    capacity: 5
    lag: 90s
    scale: 2
  - name: other
    capacity: 5
    trace:
      type: synthetic:diurnal
      base: 3
events:
  - kind: surge
    start: 7h
    ramp: 10m
    hold: 20m
    mult: 5
notes:
  - publishAt: 7h30m
    text: second
  - publishAt: 6h30m
    text: first
`

func parse(t *testing.T, y string) *Scenario {
	t.Helper()
	s, err := Parse([]byte(y), "")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func at(t *testing.T, s *Scenario, w string, off time.Duration) float64 {
	t.Helper()
	ser := s.Series(w)
	for _, p := range ser.Points {
		if p.T.Equal(s.Epoch.Add(off)) {
			return p.V
		}
	}
	t.Fatalf("no point at %v for %s", off, w)
	return 0
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestSeriesRangeAndStep(t *testing.T) {
	s := parse(t, flat)
	ser := s.Series("frontend")
	if ser.Step != time.Minute {
		t.Fatalf("step %v", ser.Step)
	}
	// [start-history, start+duration] = [5h, 8h] inclusive at 1m.
	if len(ser.Points) != 181 {
		t.Fatalf("len %d, want 181", len(ser.Points))
	}
	if !ser.Points[0].T.Equal(s.Epoch.Add(5*time.Hour)) || !ser.Points[180].T.Equal(s.Epoch.Add(8*time.Hour)) {
		t.Errorf("bounds %v %v", ser.Points[0].T, ser.Points[180].T)
	}
	if !s.StartTime().Equal(s.Epoch.Add(6*time.Hour)) || !s.EndTime().Equal(s.Epoch.Add(8*time.Hour)) {
		t.Errorf("StartTime/EndTime %v %v", s.StartTime(), s.EndTime())
	}
}

func TestSurgeShape(t *testing.T) {
	s := parse(t, flat)
	cases := []struct {
		off  time.Duration
		want float64
	}{
		{6*time.Hour + 59*time.Minute, 10},
		{7 * time.Hour, 10},
		{7*time.Hour + 5*time.Minute, 10 * 3}, // halfway through ramp: 1+(5-1)/2 = 3
		{7*time.Hour + 10*time.Minute, 50},
		{7*time.Hour + 29*time.Minute, 50},
		{7*time.Hour + 30*time.Minute, 10}, // dropped back
	}
	for _, c := range cases {
		if got := at(t, s, "frontend", c.off); !near(got, c.want) {
			t.Errorf("frontend at %v = %v, want %v", c.off, got, c.want)
		}
	}
}

func TestChainLagAndScale(t *testing.T) {
	s := parse(t, flat)
	// api(t) = frontend(t-90s)*2; sampled on the 1m grid, so use 2m-aligned check via
	// a lag that is a whole number of steps below.
	s2 := parse(t, strings.Replace(flat, "lag: 90s", "lag: 2m", 1))
	got := at(t, s2, "api", 7*time.Hour+7*time.Minute)
	want := 2 * at(t, s2, "frontend", 7*time.Hour+5*time.Minute)
	if !near(got, want) {
		t.Errorf("api = %v, want %v", got, want)
	}
	// Off-grid lag is evaluated exactly, not snapped: ramp at 7h06m-90s=7h04m30s -> 1+4*0.45.
	got = at(t, s, "api", 7*time.Hour+6*time.Minute)
	if want := 2 * 10 * (1 + 4*0.45); !near(got, want) {
		t.Errorf("api off-grid = %v, want %v", got, want)
	}
}

func TestIndependentTraceIgnoresEvents(t *testing.T) {
	s := parse(t, flat)
	for _, off := range []time.Duration{6 * time.Hour, 7*time.Hour + 15*time.Minute} {
		if got := at(t, s, "other", off); !near(got, 3) {
			t.Errorf("other at %v = %v, want 3", off, got)
		}
	}
}

func TestNotesSorted(t *testing.T) {
	s := parse(t, flat)
	n := s.Notes()
	if len(n) != 2 || n[0].Text != "first" || n[1].Text != "second" {
		t.Fatalf("%+v", n)
	}
	if !n[0].PublishAt.Equal(s.Epoch.Add(6*time.Hour + 30*time.Minute)) {
		t.Errorf("publishAt %v", n[0].PublishAt)
	}
	n[0].Text = "mutated"
	if s.Notes()[0].Text != "first" {
		t.Error("Notes returned internal slice")
	}
}

func TestDefaults(t *testing.T) {
	s := parse(t, `
name: d
trace: {type: "synthetic:diurnal", base: 1, amp: 1}
duration: 1h
workloads: [{name: a, capacity: 1}]
`)
	if s.Step != time.Minute || s.History != 72*time.Hour {
		t.Errorf("step %v history %v", s.Step, s.History)
	}
	if s.Workloads[0].MinReplicas != 1 || s.Workloads[0].MaxReplicas != 100 {
		t.Errorf("bounds %+v", s.Workloads[0])
	}
}

func TestValidationErrors(t *testing.T) {
	rep := func(old, repl string) string { return strings.Replace(flat, old, repl, 1) }
	base := func(mut func(string) string) string { return mut(flat) }
	cases := map[string]struct{ y, want string }{
		"no name":       {base(func(s string) string { return strings.Replace(s, "name: t\n", "", 1) }), "name"},
		"zero duration": {rep("duration: 2h", "duration: 0s"), "duration"},
		"bad capacity": {base(func(s string) string {
			return strings.Replace(s, "capacity: 5\n    startupDelay", "capacity: 0\n    startupDelay", 1)
		}), "capacity"},
		"min>max":        {rep("minReplicas: 1\n", "minReplicas: 60\n"), "maxReplicas"},
		"dup workload":   {rep("name: api", "name: frontend"), "duplicate"},
		"bad event kind": {base(func(s string) string { return strings.Replace(s, "kind: surge", "kind: dip", 1) }), "kind"},
		"bad mult":       {base(func(s string) string { return strings.Replace(s, "mult: 5", "mult: 0", 1) }), "mult"},
		"unknown field":  {base(func(s string) string { return s + "bogus: 1\n" }), "bogus"},
		"bad trace type": {base(func(s string) string {
			return strings.Replace(s, "type: synthetic:diurnal\n  base: 10", "type: csv\n  base: 10", 1)
		}), "trace"},
		"empty note": {base(func(s string) string { return strings.Replace(s, "text: first", "text: ''", 1) }), "text"},
		"lead lag": {base(func(s string) string {
			return strings.Replace(s, "capacity: 5\n    startupDelay", "capacity: 5\n    lag: 1m\n    startupDelay", 1)
		}), "lead"},
		"too many": {rep("  - name: other", "  - name: o2\n    capacity: 1\n  - name: o3\n    capacity: 1\n"+
			"  - name: other"), "1-3 workloads"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(c.y), "")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("err = %v, want containing %q", err, c.want)
			}
		})
	}
}

func writeAzureDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	hdr := "HashOwner,HashApp,HashFunction,Trigger"
	for i := 1; i <= 1440; i++ {
		hdr += "," + strconv.Itoa(i)
	}
	rowStr := "o,a,fnabc,http" + strings.Repeat(",60", 1440) + "\n"
	for d := 1; d <= 14; d++ {
		name := filepath.Join(dir, fmt.Sprintf("invocations_per_function_md.anon.d%02d.csv", d))
		if err := os.WriteFile(name, []byte(hdr+"\n"+rowStr), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const azureYAML = `
name: az
trace: {type: azure2019, function: fnab, peakRPS: 40}
start: 2h
history: 1h
duration: 1h
workloads: [{name: a, capacity: 1}]
`

func TestAzureTraceResolves(t *testing.T) {
	dir := writeAzureDir(t)
	s, err := Parse([]byte(azureYAML), dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := at(t, s, "a", 2*time.Hour+10*time.Minute); !near(got, 40) {
		t.Errorf("rescaled rps = %v, want 40", got)
	}
	if _, err := Parse([]byte(azureYAML), t.TempDir()); err == nil || !strings.Contains(err.Error(), "fetch-azure2019") {
		t.Errorf("missing data err = %v", err)
	}
}

func TestAzureWindowMustFitTrace(t *testing.T) {
	dir := writeAzureDir(t)
	cases := map[string]string{
		"before origin": strings.Replace(azureYAML, "start: 2h", "start: 30m", 1),
		"past end":      strings.Replace(azureYAML, "duration: 1h", "duration: 400h", 1),
	}
	for name, y := range cases {
		if _, err := Parse([]byte(y), dir); err == nil || !strings.Contains(err.Error(), "wrap") {
			t.Errorf("%s: err = %v, want window error mentioning wrap", name, err)
		}
		wrapped := strings.Replace(y, "peakRPS: 40", "peakRPS: 40, wrap: true", 1)
		if _, err := Parse([]byte(wrapped), dir); err != nil {
			t.Errorf("%s with wrap: %v", name, err)
		}
	}
}

func TestAzureLagBeforeOriginRejected(t *testing.T) {
	y := strings.Replace(azureYAML, "workloads: [{name: a, capacity: 1}]",
		"workloads: [{name: a, capacity: 1}, {name: b, capacity: 1, lag: 90m}]", 1)
	if _, err := Parse([]byte(y), writeAzureDir(t)); err == nil || !strings.Contains(err.Error(), "lag") {
		t.Errorf("err = %v", err)
	}
}

func TestTopLevelTraceNotLoadedWhenAllOverride(t *testing.T) {
	y := `
name: ov
trace: {type: azure2019, function: nothere}
start: 1h
duration: 1h
history: 0s
workloads:
  - name: a
    capacity: 1
    trace: {type: synthetic:diurnal, base: 4}
`
	s, err := Parse([]byte(y), t.TempDir()) // empty azure dir: would fail if loaded
	if err != nil {
		t.Fatal(err)
	}
	if got := at(t, s, "a", time.Hour); !near(got, 4) {
		t.Errorf("got %v", got)
	}
}

func TestEventAndNoteOutsideWindow(t *testing.T) {
	cases := map[string]struct{ old, repl string }{
		"event before": {"start: 7h\n    ramp", "start: 1h\n    ramp"},
		"event after":  {"start: 7h\n    ramp", "start: 9h\n    ramp"},
		"note after":   {"publishAt: 7h30m", "publishAt: 20h"},
		"note before":  {"publishAt: 6h30m", "publishAt: 1h"},
	}
	for name, c := range cases {
		_, err := Parse([]byte(strings.Replace(flat, c.old, c.repl, 1)), "")
		if err == nil || !strings.Contains(err.Error(), "outside the scenario window") {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestOverlappingEventsMultiply(t *testing.T) {
	y := strings.Replace(flat, "notes:", `  - kind: surge
    start: 7h
    ramp: 0s
    hold: 30m
    mult: 2
notes:`, 1)
	s := parse(t, y)
	if got := at(t, s, "frontend", 7*time.Hour+15*time.Minute); !near(got, 10*5*2) {
		t.Errorf("got %v, want 100", got)
	}
}

func TestLoadExampleScenarios(t *testing.T) {
	files, _ := filepath.Glob("../scenarios/*.yaml")
	if len(files) == 0 {
		t.Fatal("no example scenarios")
	}
	for _, f := range files {
		if filepath.Base(f) == "azure-workloads.yaml" {
			continue // the generated workload selection, not a scenario
		}
		s, err := Load(f)
		if err != nil {
			t.Errorf("%s: %v", f, err)
			continue
		}
		for _, w := range s.Workloads {
			if len(s.Series(w.Name).Points) == 0 {
				t.Errorf("%s/%s: empty series", f, w.Name)
			}
		}
	}
}
