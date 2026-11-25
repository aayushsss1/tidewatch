package engine

import (
	"math"
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

func baseIn(actual float64, f *forecast.Forecast) BaselineInput {
	return BaselineInput{
		Now:         t0,
		Actual:      actual,
		Trusted:     f,
		TimeToReady: 2 * time.Minute,
		Margin:      time.Minute,
		Config:      workload.EffectiveConfig{TargetPerReplica: 50, Headroom: 0.8}, // capacity 40
	}
}

func fc(pts ...forecast.ForecastPoint) *forecast.Forecast { return &forecast.Forecast{Points: pts} }

func TestBaseline_ForecastLeadsReactive(t *testing.T) {
	f := fc(forecast.ForecastPoint{T: t0.Add(2 * time.Minute), Upper: 300})
	r := Baseline(baseIn(80, f))
	if !r.OK || r.Desired != 8 || r.ReactiveFloor != 2 || r.ForecastDesired != 8 {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_IgnoresForecastBeyondLookahead(t *testing.T) {
	f := fc(
		forecast.ForecastPoint{T: t0.Add(time.Minute), Upper: 60},
		forecast.ForecastPoint{T: t0.Add(4 * time.Minute), Upper: 900},
	)
	r := Baseline(baseIn(80, f))
	if r.Desired != 2 || r.ForecastDesired != 2 {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_ReactiveFloorWhenForecastLow(t *testing.T) {
	f := fc(forecast.ForecastPoint{T: t0.Add(time.Minute), Upper: 10})
	r := Baseline(baseIn(200, f))
	if r.Desired != 5 || r.ForecastDesired != 1 || r.ReactiveFloor != 5 {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_NoForecastUsesReactive(t *testing.T) {
	r := Baseline(baseIn(81, nil))
	if !r.OK || r.Desired != 3 {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_NothingKnown(t *testing.T) {
	if r := Baseline(baseIn(math.NaN(), nil)); r.OK {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_ForecastOnlyWhenActualNaN(t *testing.T) {
	f := fc(forecast.ForecastPoint{T: t0.Add(time.Minute), Upper: 100})
	r := Baseline(baseIn(math.NaN(), f))
	if !r.OK || r.Desired != 3 {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_InfActualNotOK(t *testing.T) {
	r := Baseline(baseIn(math.Inf(1), nil))
	if r.OK {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_HugeActualSaturates(t *testing.T) {
	r := Baseline(baseIn(1e300, nil))
	if !r.OK || r.Desired != math.MaxInt32 {
		t.Fatalf("%+v", r)
	}
}

func TestBaseline_InfForecastIgnored(t *testing.T) {
	f := fc(forecast.ForecastPoint{T: t0.Add(2 * time.Minute), Upper: math.Inf(1)})
	r := Baseline(baseIn(80, f))
	if r.Desired != 2 {
		t.Fatalf("%+v", r)
	}
}
