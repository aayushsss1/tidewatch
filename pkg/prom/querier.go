// Package prom queries Prometheus for load history.
package prom

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// ErrMultipleSeries is returned when a load query yields more than one series.
var ErrMultipleSeries = errors.New("load query must return exactly one series")

// Querier reads load series from Prometheus.
type Querier interface {
	// RangeSeries runs a range query that must yield ≤1 series; samples are aligned to step, missing steps are NaN.
	RangeSeries(ctx context.Context, query string, start, end time.Time, step time.Duration) (forecast.Series, error)
	Ready(ctx context.Context) error
}

type querier struct {
	client  api.Client
	api     promv1.API
	timeout time.Duration
}

// New returns a Querier for the Prometheus at url; each call is bounded by timeout.
func New(url string, timeout time.Duration) (Querier, error) {
	c, err := api.NewClient(api.Config{Address: url})
	if err != nil {
		return nil, fmt.Errorf("creating prometheus client: %w", err)
	}
	return &querier{client: c, api: promv1.NewAPI(c), timeout: timeout}, nil
}

func (q *querier) RangeSeries(
	ctx context.Context, query string, start, end time.Time, step time.Duration,
) (forecast.Series, error) {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	val, _, err := q.api.QueryRange(ctx, query, promv1.Range{Start: start, End: end, Step: step})
	if err != nil {
		return forecast.Series{}, fmt.Errorf("prometheus range query: %w", err)
	}
	m, ok := val.(model.Matrix)
	if !ok {
		return forecast.Series{}, fmt.Errorf("unexpected prometheus result type %s", val.Type())
	}
	if len(m) > 1 {
		return forecast.Series{}, fmt.Errorf("%w, got %d", ErrMultipleSeries, len(m))
	}

	n := int(end.Sub(start)/step) + 1
	out := forecast.Series{Step: step, Points: make([]forecast.Point, max(n, 0))}
	for i := range out.Points {
		out.Points[i] = forecast.Point{T: start.Add(time.Duration(i) * step), V: math.NaN()}
	}
	if len(m) == 0 {
		return out, nil
	}
	for _, s := range m[0].Values {
		off := s.Timestamp.Time().Sub(start)
		i := int((off + step/2).Truncate(step) / step)
		if off+step/2 < 0 || i >= n {
			continue
		}
		out.Points[i].V = float64(s.Value)
	}
	return out, nil
}

func (q *querier) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, q.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, q.client.URL("/-/ready", nil).String(), nil)
	if err != nil {
		return err
	}
	resp, _, err := q.client.Do(ctx, req)
	if err != nil {
		return fmt.Errorf("prometheus not ready: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("prometheus not ready: status %d", resp.StatusCode)
	}
	return nil
}
