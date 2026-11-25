package forecast

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ForecastPoint is one predicted value with its prediction interval.
type ForecastPoint struct {
	T                   time.Time
	Point, Lower, Upper float64
}

// Forecast is the output of a Forecaster.
type Forecast struct {
	Model  string
	Points []ForecastPoint
}

// Peak returns the point with the highest Point value among those with T <= until.
func (f Forecast) Peak(until time.Time) (ForecastPoint, bool) {
	var best ForecastPoint
	found := false
	for _, p := range f.Points {
		if p.T.After(until) {
			continue
		}
		if !found || p.Point > best.Point {
			best, found = p, true
		}
	}
	return best, found
}

// MaxUpper returns the highest Upper value among points with T <= until.
func (f Forecast) MaxUpper(until time.Time) (float64, bool) {
	var best float64
	found := false
	for _, p := range f.Points {
		if p.T.After(until) {
			continue
		}
		if !found || p.Upper > best {
			best, found = p.Upper, true
		}
	}
	return best, found
}

// ErrInsufficientHistory is returned when a series is too short for a model.
var ErrInsufficientHistory = errors.New("insufficient history")

// ErrModelFailed is returned when a model produces NaN or Inf values.
var ErrModelFailed = errors.New("model failed")

// Forecaster produces a forecast from a series.
type Forecaster interface {
	Name() string
	Forecast(ctx context.Context, s Series, horizon, season time.Duration) (Forecast, error)
}

// Registry looks up forecasters by name.
type Registry struct {
	byName map[string]Forecaster
}

// NewRegistry builds a registry of the given forecasters.
func NewRegistry(fs ...Forecaster) *Registry {
	r := &Registry{byName: make(map[string]Forecaster, len(fs))}
	for _, f := range fs {
		r.byName[f.Name()] = f
	}
	return r
}

// Get returns the forecasters for names, in order. An unknown name is an error.
func (r *Registry) Get(names []string) ([]Forecaster, error) {
	out := make([]Forecaster, 0, len(names))
	for _, n := range names {
		f, ok := r.byName[n]
		if !ok {
			return nil, fmt.Errorf("unknown forecast model %q", n)
		}
		out = append(out, f)
	}
	return out, nil
}
