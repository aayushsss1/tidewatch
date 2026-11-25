// Package engine holds the pure decision pipeline: collection, baseline and enforcement.
package engine

import (
	"context"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
)

const (
	maxRecentMissing  = 3
	maxSeasonMissFrac = 0.10
)

// GapStats summarises missing samples in a collected series.
type GapStats struct {
	RecentMissing     int     // trailing NaN count
	SeasonMissingFrac float64 // fraction of NaN over the whole window
}

// Healthy reports whether the series is complete enough to forecast from.
func (g GapStats) Healthy() bool {
	return g.RecentMissing < maxRecentMissing && g.SeasonMissingFrac <= maxSeasonMissFrac
}

type buffer struct {
	query  string
	step   time.Duration
	points []forecast.Point // capped at capacity; oldest dropped first
}

// Collector keeps a bounded load history per workload key, fetched incrementally from Prometheus.
type Collector struct {
	q    prom.Querier
	mu   sync.Mutex
	bufs map[string]*buffer
}

// NewCollector returns a Collector reading from q.
func NewCollector(q prom.Querier) *Collector {
	return &Collector{q: q, bufs: map[string]*buffer{}}
}

// Update backfills on first call (history window), then fetches (lastT, now] incrementally; trims to history.
// On a querier error the previous buffer is kept and returned alongside the error.
//
// The mutex is not held across the Prometheus query, so different keys update in
// parallel. Invariant: each key is updated by at most one worker per tick, so the
// buffer's last timestamp cannot move between the unlocked query and the re-lock.
func (c *Collector) Update(
	ctx context.Context, key, query string, history, step time.Duration, now time.Time,
) (forecast.Series, GapStats, error) {
	c.mu.Lock()
	b := c.bufs[key]
	if b == nil || b.query != query || b.step != step {
		b = &buffer{query: query, step: step}
		c.bufs[key] = b
	}
	capacity := max(int(history/step), 1)

	start := now.Add(-history)
	if n := len(b.points); n > 0 {
		start = b.points[n-1].T.Add(step)
	}
	if start.After(now) {
		defer c.mu.Unlock()
		return c.snapshot(b, capacity)
	}
	c.mu.Unlock()

	fetched, qerr := c.q.RangeSeries(ctx, query, start, now, step)

	c.mu.Lock()
	defer c.mu.Unlock()
	if qerr == nil {
		for _, p := range fetched.Points {
			if n := len(b.points); n > 0 && !p.T.After(b.points[n-1].T) {
				continue
			}
			b.points = append(b.points, p)
		}
	}
	s, g, _ := c.snapshot(b, capacity)
	return s, g, qerr
}

func (c *Collector) snapshot(b *buffer, capacity int) (forecast.Series, GapStats, error) {
	if over := len(b.points) - capacity; over > 0 {
		n := copy(b.points, b.points[over:])
		b.points = b.points[:n]
	}
	s := forecast.Series{Step: b.step, Points: make([]forecast.Point, len(b.points))}
	copy(s.Points, b.points)
	return s, gapStats(s.Points), nil
}

func gapStats(pts []forecast.Point) GapStats {
	// Leading NaNs predate the series and are not gaps: count from the first
	// valid sample. Models signal too-short history via ErrInsufficientHistory.
	first := slices.IndexFunc(pts, func(p forecast.Point) bool { return !math.IsNaN(p.V) })
	if first < 0 { // no valid samples at all: unhealthy
		return GapStats{RecentMissing: len(pts), SeasonMissingFrac: 1}
	}
	pts = pts[first:]
	var g GapStats
	missing := 0
	for _, p := range pts {
		if math.IsNaN(p.V) {
			missing++
		}
	}
	for i := len(pts) - 1; i >= 0 && math.IsNaN(pts[i].V); i-- {
		g.RecentMissing++
	}
	g.SeasonMissingFrac = float64(missing) / float64(len(pts))
	return g
}

// Forget drops the buffer for key.
func (c *Collector) Forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.bufs, key)
}
