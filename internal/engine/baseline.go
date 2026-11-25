package engine

import (
	"math"
	"time"

	"github.com/aayushsss1/tidewatch/internal/workload"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// BaselineInput is everything the baseline needs for one workload tick.
type BaselineInput struct {
	Now         time.Time
	Actual      float64            // latest load; NaN if unknown
	Trusted     *forecast.Forecast // nil if no usable model
	TimeToReady time.Duration      // p90
	Margin      time.Duration      // 2 × step
	Config      workload.EffectiveConfig
}

// BaselineResult is the deterministic replica recommendation.
type BaselineResult struct {
	Desired                        int32
	ForecastDesired, ReactiveFloor int32
	OK                             bool // false when Actual is NaN and no forecast
}

// Baseline returns max(forecast-driven, reactive) replicas needed for the load.
func Baseline(in BaselineInput) BaselineResult {
	capacity := in.Config.TargetPerReplica * in.Config.Headroom
	if capacity <= 0 {
		return BaselineResult{}
	}
	var res BaselineResult
	if in.Trusted != nil {
		if upper, ok := in.Trusted.MaxUpper(in.Now.Add(in.TimeToReady + in.Margin)); ok && !math.IsNaN(upper) {
			res.ForecastDesired = ceilDiv(upper, capacity)
			res.OK = true
		}
	}
	if !math.IsNaN(in.Actual) {
		res.ReactiveFloor = ceilDiv(in.Actual, capacity)
		res.OK = true
	}
	res.Desired = max(res.ForecastDesired, res.ReactiveFloor)
	return res
}

func ceilDiv(load, capacity float64) int32 {
	return int32(math.Ceil(max(load, 0) / capacity))
}
