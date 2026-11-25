package episodes

import (
	"math"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

var t0 = time.Date(2026, 11, 20, 9, 0, 0, 0, time.UTC) // a Friday

func rampSeries(n int, f func(i int) float64) forecast.Series {
	s := forecast.Series{Step: time.Minute}
	for i := range n {
		s.Points = append(s.Points, forecast.Point{T: t0.Add(time.Duration(i) * time.Minute), V: f(i)})
	}
	return s
}

func TestShape_ZNormalized32(t *testing.T) {
	s := rampSeries(60, func(i int) float64 { return float64(10 + i*3) })
	sh := Shape(s, t0, t0.Add(59*time.Minute))
	if len(sh) != 32 {
		t.Fatalf("len=%d", len(sh))
	}
	var mean, ss float64
	for _, v := range sh {
		mean += v
	}
	mean /= 32
	for _, v := range sh {
		ss += (v - mean) * (v - mean)
	}
	if math.Abs(mean) > 1e-9 || math.Abs(math.Sqrt(ss/32)-1) > 1e-9 {
		t.Fatalf("mean=%v std=%v", mean, math.Sqrt(ss/32))
	}
	few := rampSeries(5, func(i int) float64 { return float64(i) })
	if Shape(few, t0, t0.Add(4*time.Minute)) != nil {
		t.Fatal("want nil for <8 points")
	}
	nan := rampSeries(60, func(i int) float64 { return math.NaN() })
	if Shape(nan, t0, t0.Add(59*time.Minute)) != nil {
		t.Fatal("want nil for NaN")
	}
}

func spec(shape []float64, note string, deploy bool) v1.TideEpisodeSpec {
	return v1.TideEpisodeSpec{
		Shape:    EncodeShape(shape),
		Calendar: v1.EpisodeCalendar{Weekday: fri, Hour: 9},
		Context:  v1.EpisodeContext{OpsNote: note, Deploy: deploy},
		Start:    metav1.NewTime(t0),
	}
}

func TestSimilarity_IdenticalNearOne(t *testing.T) {
	sh := Shape(rampSeries(60, func(i int) float64 { return float64(i * i) }), t0, t0.Add(59*time.Minute))
	q := Query{Shape: sh, Weekday: time.Friday, Hour: 9, OpsNote: "marketing push launch", Deploy: true}
	got := Similarity(q, spec(sh, "marketing push launch", true))
	if got < 0.99 {
		t.Fatalf("got %v", got)
	}
	// opposite shape scores lower
	rev := make([]float64, 32)
	for i := range rev {
		rev[i] = -sh[i]
	}
	if Similarity(q, spec(rev, "marketing push launch", true)) >= got-0.2 {
		t.Fatal("reversed shape should score clearly lower")
	}
}

func TestSimilarity_ManualWithoutShapeRedistributes(t *testing.T) {
	sh := Shape(rampSeries(60, func(i int) float64 { return float64(i) }), t0, t0.Add(59*time.Minute))
	q := Query{Shape: sh, Weekday: time.Friday, Hour: 9, OpsNote: "sale", Deploy: false}
	ep := spec(nil, "sale", false)
	if got := Similarity(q, ep); math.Abs(got-1) > 1e-9 {
		t.Fatalf("perfect calendar+context without shape should be 1, got %v", got)
	}
	ep = spec(nil, "", true) // calendar 1.0, context 0.0
	want := 0.25 / 0.4
	if got := Similarity(q, ep); math.Abs(got-want) > 1e-9 {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestSimilarity_CalendarAndContext(t *testing.T) {
	if got := calendarSim(time.Friday, 9, "Fri", 9); got != 1 {
		t.Fatal(got)
	}
	if got := calendarSim(time.Friday, 9, "Mon", 9); got != 0.5 {
		t.Fatal(got)
	}
	if got := calendarSim(time.Friday, 23, "Fri", 1); math.Abs(got-(0.5+0.5*(1-2.0/6))) > 1e-9 {
		t.Fatalf("wrap: %v", got)
	}
	if got := calendarSim(time.Friday, 0, "Mon", 12); got != 0 {
		t.Fatal(got)
	}
	// "to" and "an" are shorter than 3 chars and ignored: jaccard of {big,sale} vs {big,launch}=1/3
	if got := contextSim("Big sale to an", false, "big launch", false); math.Abs(got-(0.7/3+0.3)) > 1e-9 {
		t.Fatal(got)
	}
	if got := contextSim("", true, "", false); got != 0 {
		t.Fatal(got)
	}
}

func TestSummary_Format(t *testing.T) {
	timely := true
	ep := v1.TideEpisodeSpec{
		Lead: "web", PeakRatio: "4.8", Action: "prescaled api ≥8 at −8m",
		Members:  []v1.EpisodeMember{{Workload: "api", LagSeconds: 90}},
		Calendar: v1.EpisodeCalendar{Weekday: "Fri", Hour: 9, Timezone: "UTC"},
		Outcome:  &v1.Outcome{Timely: &timely, Needed: true, WastedReplicaMinutes: "14"},
	}
	want := "ep1 (0.83): Fri 09:00 UTC, 4.8× peak, api +90s; action: prescaled api ≥8 at −8m; outcome: timely, needed, 14 wasted rep-min"
	if got := Summary("ep1", 0.83, ep); got != want {
		t.Fatalf("\n got %q\nwant %q", got, want)
	}
	ep.Action = string(make([]rune, 1000))
	ep.Action = ""
	for range 1000 {
		ep.Action += "x"
	}
	if n := len([]rune(Summary("ep1", 0.5, ep))); n > 400 {
		t.Fatalf("len=%d", n)
	}
}

func TestEpisodeName_EmptyAndTrailingLead(t *testing.T) {
	r := &Recorder{}
	ep := r.Build(&IncidentView{ID: "x", Lead: "___", Policy: v1.PolicyRef{Namespace: "n", Name: "p"}, Start: t0})
	if !strings.HasPrefix(ep.Name, "episode-") {
		t.Fatal(ep.Name)
	}
	if got := ep.Labels[LabelLead]; got != "" {
		t.Fatalf("label %q", got)
	}
	if got := leadLabel("web-front_"); got != "web-front" {
		t.Fatal(got)
	}
}

func TestShape_NegativeValuesRoundTrip(t *testing.T) {
	in := make([]float64, 32)
	for i := range in {
		in[i] = float64(i-16) / 4
	}
	got := decodeShape(EncodeShape(in))
	for i := range in {
		if math.Abs(got[i]-in[i]) > 1e-4 {
			t.Fatalf("%d: %v vs %v", i, got[i], in[i])
		}
	}
}
