package signal

import (
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

var (
	t0  = time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	wl  = types.NamespacedName{Namespace: "ns", Name: "web"}
	pol = v1.PolicyRef{Namespace: "ns", Name: "p"}
)

// fc builds a forecast with one point per step starting one step after t0.
func fc(model string, points, upper []float64) forecast.Forecast {
	f := forecast.Forecast{Model: model}
	for i, p := range points {
		f.Points = append(f.Points, forecast.ForecastPoint{
			T: t0.Add(time.Duration(i+1) * time.Minute), Point: p, Lower: p / 2, Upper: upper[i],
		})
	}
	return f
}

func flat(v float64) []float64 {
	s := make([]float64, 30)
	for i := range s {
		s[i] = v
	}
	return s
}

func base() Input {
	return Input{
		Now: t0, Step: time.Minute, Horizon: 30 * time.Minute, TimeToReady: time.Minute,
		Capacity: 100, Healthy: true,
	}
}

func types_(ts []Trigger) []Type {
	o := make([]Type, 0, len(ts))
	for _, t := range ts {
		o = append(o, t.Type)
	}
	return o
}

func has(ts []Trigger, typ Type) *Trigger {
	for i := range ts {
		if ts[i].Type == typ {
			return &ts[i]
		}
	}
	return nil
}

func detect(in Input) []Trigger { return Detect(in, "uid", wl, pol, NewDebouncer()) }

func TestDetect_ForecastBreach(t *testing.T) {
	up := flat(50)
	for i := 19; i < 30; i++ { // exceeds capacity from minute 20
		up[i] = 150
	}
	in := base()
	f := fc("m", flat(50), up)
	in.Trusted = &f
	tr := has(detect(in), ForecastBreach)
	if tr == nil {
		t.Fatal("expected ForecastBreach")
	}
	if tr.TimeToBreach == nil || *tr.TimeToBreach != 20*time.Minute {
		t.Fatalf("time to breach = %v", tr.TimeToBreach)
	}
	if tr.Shortfall < 0.49 || tr.Shortfall > 0.51 || tr.Workload != wl || tr.UID != "uid" || tr.Policy != pol {
		t.Fatalf("bad trigger %+v", tr)
	}
}

func TestDetect_ForecastBreachNotFiring(t *testing.T) {
	cases := map[string]func(*Input){
		"within 20%": func(in *Input) {
			f := fc("m", flat(50), flat(115))
			in.Trusted = &f
		},
		"baseline already covers it": func(in *Input) {
			up := flat(50)
			up[3] = 200 // minute 4 <= now + ttr(1m) + 4 steps
			f := fc("m", flat(50), up)
			in.Trusted = &f
		},
		"no trusted model": func(in *Input) {},
		"zero capacity": func(in *Input) {
			f := fc("m", flat(50), flat(500))
			in.Trusted, in.Capacity = &f, 0
		},
	}
	for name, mod := range cases {
		t.Run(name, func(t *testing.T) {
			in := base()
			mod(&in)
			if has(detect(in), ForecastBreach) != nil {
				t.Fatal("unexpected ForecastBreach")
			}
		})
	}
}

func TestDetect_Anomaly(t *testing.T) {
	in := base()
	in.OutsideSteps = 2
	if has(detect(in), Anomaly) != nil {
		t.Fatal("2 steps must not fire")
	}
	in.OutsideSteps = 3
	if has(detect(in), Anomaly) == nil {
		t.Fatal("3 steps must fire")
	}
}

func TestDetect_AnomalyShortfall(t *testing.T) {
	in := base()
	in.OutsideSteps = 3
	in.Actual, in.AnomalyForecast = 150, 100
	if tr := has(detect(in), Anomaly); tr == nil || tr.Shortfall != 0.5 {
		t.Fatalf("shortfall = %+v, want 0.5", tr)
	}
	in = base()
	in.OutsideSteps = 3
	in.Actual, in.AnomalyForecast = 40, 100 // below the interval: symmetric
	if tr := has(detect(in), Anomaly); tr == nil || tr.Shortfall != 0.6 {
		t.Fatalf("shortfall = %+v, want 0.6", tr)
	}
	in = base()
	in.OutsideSteps = 3
	in.Actual, in.AnomalyForecast = 150, 0 // no usable forecast
	if tr := has(detect(in), Anomaly); tr == nil || tr.Shortfall != 0 {
		t.Fatalf("shortfall = %+v, want 0", tr)
	}
}

func TestDetect_ModelDisagreement(t *testing.T) {
	in := base()
	a, b := fc("a", flat(60), flat(70)), fc("b", flat(150), flat(160))
	in.Forecasts = []forecast.Forecast{a, b}
	if has(detect(in), ModelDisagreement) == nil {
		t.Fatal("expected ModelDisagreement")
	}
	// Differ by >40% but both need scaling: the decision does not change.
	c, d := fc("c", flat(150), flat(160)), fc("d", flat(300), flat(310))
	in.Forecasts = []forecast.Forecast{c, d}
	if has(detect(in), ModelDisagreement) != nil {
		t.Fatal("same decision must not fire")
	}
	// Close peaks.
	e, g := fc("e", flat(95), flat(100)), fc("g", flat(110), flat(120))
	in.Forecasts = []forecast.Forecast{e, g}
	if has(detect(in), ModelDisagreement) != nil {
		t.Fatal("close peaks must not fire")
	}
}

func TestDetect_WorkloadChange(t *testing.T) {
	if has(detect(base()), WorkloadChange) != nil {
		t.Fatal("quiet workload must not fire")
	}
	in := base()
	in.Restarts5m = 2
	if has(detect(in), WorkloadChange) != nil {
		t.Fatal("2 restarts must not fire")
	}
	for name, mod := range map[string]func(*Input){
		"template": func(in *Input) { in.TemplateChanged = true },
		"restarts": func(in *Input) { in.Restarts5m = 3 },
		"oom":      func(in *Input) { in.OOMKilled = true },
		"external": func(in *Input) { in.ExternalReplicaChange = true },
	} {
		t.Run(name, func(t *testing.T) {
			in := base()
			mod(&in)
			if has(detect(in), WorkloadChange) == nil {
				t.Fatal("expected WorkloadChange")
			}
		})
	}
}

func TestDetect_ContextDue(t *testing.T) {
	in := base()
	in.Notes = "flash sale at 2026-11-25T10:20:00Z, expect ~4× on frontend"
	tr := has(detect(in), ContextDue)
	if tr == nil {
		t.Fatal("expected ContextDue")
	}
	if *tr.TimeToBreach != 20*time.Minute || tr.Shortfall != 3 {
		t.Fatalf("bad trigger %+v", tr)
	}
	in.Notes = "no multiplier at 2026-11-25T10:05:00Z"
	if tr := has(detect(in), ContextDue); tr == nil || tr.Shortfall != 1 {
		t.Fatalf("want shortfall 1, got %+v", tr)
	}
	in.Notes = "sale at 2026-11-25T11:00:00Z" // beyond horizon
	if has(detect(in), ContextDue) != nil {
		t.Fatal("beyond horizon must not fire")
	}
	in.Notes = "sale at 2026-11-25T09:00:00Z" // past
	if has(detect(in), ContextDue) != nil {
		t.Fatal("past must not fire")
	}
	in.Notes = "x " + strings.Repeat("y", 400) + " 2026-11-25T10:05:00Z"
	if tr := has(detect(in), ContextDue); tr == nil || len([]rune(tr.Detail)) != 200 {
		t.Fatalf("detail not truncated: %+v", tr)
	}
}

func TestDetect_OverProvisioned(t *testing.T) {
	in := base()
	a, b := fc("a", flat(20), flat(30)), fc("b", flat(40), flat(45))
	in.Forecasts = []forecast.Forecast{a, b}
	if has(detect(in), OverProvisioned) == nil {
		t.Fatal("expected OverProvisioned")
	}
	c := fc("c", flat(60), flat(70))
	in.Forecasts = []forecast.Forecast{a, c}
	if has(detect(in), OverProvisioned) != nil {
		t.Fatal("one busy model must not fire")
	}
	in.Forecasts = nil
	if has(detect(in), OverProvisioned) != nil {
		t.Fatal("no forecasts must not fire")
	}
}

func TestDetect_OverProvisionedAtMostHourly(t *testing.T) {
	d := NewDebouncer()
	in := base()
	a := fc("a", flat(20), flat(30))
	in.Forecasts = []forecast.Forecast{a}
	if has(Detect(in, "u", wl, pol, d), OverProvisioned) == nil {
		t.Fatal("first must fire")
	}
	in.Now = t0.Add(30 * time.Minute)
	if has(Detect(in, "u", wl, pol, d), OverProvisioned) != nil {
		t.Fatal("within an hour must be suppressed")
	}
	in.Now = t0.Add(61 * time.Minute)
	if has(Detect(in, "u", wl, pol, d), OverProvisioned) == nil {
		t.Fatal("after an hour must fire")
	}
}

func TestDetect_DebounceSuppressesRepeat(t *testing.T) {
	d := NewDebouncer()
	in := base()
	in.OutsideSteps = 3
	if len(Detect(in, "u", wl, pol, d)) != 1 {
		t.Fatal("first must fire")
	}
	in.Now = t0.Add(5 * time.Minute)
	if got := Detect(in, "u", wl, pol, d); len(got) != 0 {
		t.Fatalf("repeat within 10m must be suppressed, got %v", types_(got))
	}
	if len(Detect(in, "other", wl, pol, d)) != 1 {
		t.Fatal("other workload is independent")
	}
	in.Now = t0.Add(11 * time.Minute)
	if len(Detect(in, "u", wl, pol, d)) != 1 {
		t.Fatal("after 10m must fire again")
	}
	d.Forget("u")
	in.Now = t0.Add(12 * time.Minute)
	if len(Detect(in, "u", wl, pol, d)) != 1 {
		t.Fatal("Forget must clear the debounce")
	}
}

func TestDetect_DebounceAllowsWorsening(t *testing.T) {
	d := NewDebouncer()
	in := base()
	mk := func(upper float64) *forecast.Forecast {
		up := flat(50)
		for i := 19; i < 30; i++ {
			up[i] = upper
		}
		f := fc("m", flat(50), up)
		return &f
	}
	in.Trusted = mk(150) // shortfall 0.5
	if has(Detect(in, "u", wl, pol, d), ForecastBreach) == nil {
		t.Fatal("first must fire")
	}
	in.Now = t0.Add(time.Minute)
	in.Trusted = mk(160) // 0.6: +20%, not enough
	// Re-anchor forecast times to the new now.
	shift := func(f *forecast.Forecast) {
		for i := range f.Points {
			f.Points[i].T = f.Points[i].T.Add(time.Minute)
		}
	}
	shift(in.Trusted)
	if has(Detect(in, "u", wl, pol, d), ForecastBreach) != nil {
		t.Fatal("mild growth must be suppressed")
	}
	in.Now = t0.Add(4 * time.Minute)
	in.Trusted = mk(250) // 1.5: tripled
	shift(in.Trusted)
	shift(in.Trusted)
	shift(in.Trusted)
	shift(in.Trusted)
	if has(Detect(in, "u", wl, pol, d), ForecastBreach) == nil {
		t.Fatal("shortfall growth of >=2x after 3m must fire")
	}
}

func TestDetect_UnhealthyDataNoTriggers(t *testing.T) {
	in := base()
	in.Healthy = false
	in.OutsideSteps, in.OOMKilled, in.TemplateChanged = 9, true, true
	if got := detect(in); len(got) != 0 {
		t.Fatalf("got %v", types_(got))
	}
}

func TestDetect_NilDebouncer(t *testing.T) {
	in := base()
	in.OOMKilled = true
	if len(Detect(in, "u", wl, pol, nil)) != 1 {
		t.Fatal("nil debouncer must not suppress")
	}
}

func TestDetect_AnomalyShortfallCappedAtTen(t *testing.T) {
	in := base()
	in.OutsideSteps = 3
	in.Actual, in.AnomalyForecast = 5000, 100 // ratio 49
	if tr := has(detect(in), Anomaly); tr == nil || tr.Shortfall != 10 {
		t.Fatalf("shortfall = %+v, want capped at 10", tr)
	}
}

func TestDetect_DebounceWorseningNeedsDoubleAndThreeMinutes(t *testing.T) {
	d := NewDebouncer()
	in := base()
	mk := func(upper float64, shift time.Duration) *forecast.Forecast {
		up := flat(50)
		for i := 19; i < 30; i++ {
			up[i] = upper
		}
		f := fc("m", flat(50), up)
		for i := range f.Points {
			f.Points[i].T = f.Points[i].T.Add(shift)
		}
		return &f
	}
	fire := func(at time.Duration, upper float64) bool {
		in.Now = t0.Add(at)
		in.Trusted = mk(upper, at)
		return has(Detect(in, "u", wl, pol, d), ForecastBreach) != nil
	}
	if !fire(0, 150) { // shortfall 0.5
		t.Fatal("first must fire")
	}
	if fire(time.Minute, 260) { // 1.6: >2x but before 3m
		t.Fatal("2x growth within 3m must be suppressed")
	}
	if fire(4*time.Minute, 190) { // 0.9: <2x
		t.Fatal("1.8x growth after 3m must be suppressed")
	}
	if !fire(4*time.Minute, 260) { // 1.6 >= 1.0
		t.Fatal("2x growth after 3m must fire")
	}
}
