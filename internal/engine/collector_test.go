package engine

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

type call struct {
	query      string
	start, end time.Time
}

type fakeQuerier struct {
	calls []call
	err   error
	nan   func(t time.Time) bool
}

func (f *fakeQuerier) RangeSeries(
	_ context.Context, query string, start, end time.Time, step time.Duration,
) (forecast.Series, error) {
	f.calls = append(f.calls, call{query, start, end})
	if f.err != nil {
		return forecast.Series{}, f.err
	}
	s := forecast.Series{Step: step}
	for t := start; !t.After(end); t = t.Add(step) {
		v := 1.0
		if f.nan != nil && f.nan(t) {
			v = math.NaN()
		}
		s.Points = append(s.Points, forecast.Point{T: t, V: v})
	}
	return s, nil
}

func (f *fakeQuerier) Ready(context.Context) error { return nil }

var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

const (
	step    = time.Minute
	history = 10 * time.Minute
)

func TestCollector_BackfillsOnFirstUpdate(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	s, _, err := c.Update(context.Background(), "k", "q", history, step, t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.calls) != 1 || !q.calls[0].start.Equal(t0.Add(-history)) {
		t.Fatalf("calls = %+v", q.calls)
	}
	if len(s.Points) == 0 {
		t.Fatal("empty series")
	}
}

func TestCollector_IncrementalAfterBackfill(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	_, _, _ = c.Update(context.Background(), "k", "q", history, step, t0)
	now := t0.Add(3 * step)
	s, _, err := c.Update(context.Background(), "k", "q", history, step, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(q.calls) != 2 || !q.calls[1].start.Equal(t0.Add(step)) {
		t.Fatalf("calls = %+v", q.calls)
	}
	if last := s.Points[len(s.Points)-1].T; !last.Equal(now) {
		t.Fatalf("last = %v", last)
	}
}

func TestCollector_SkipsQueryWhenNothingNew(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	_, _, _ = c.Update(context.Background(), "k", "q", history, step, t0)
	_, _, err := c.Update(context.Background(), "k", "q", history, step, t0.Add(30*time.Second))
	if err != nil || len(q.calls) != 1 {
		t.Fatalf("err=%v calls=%d", err, len(q.calls))
	}
}

func TestCollector_TrimsToHistory(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	max := int(history / step)
	for i := range 30 {
		s, _, err := c.Update(context.Background(), "k", "q", history, step, t0.Add(time.Duration(i)*step))
		if err != nil {
			t.Fatal(err)
		}
		if len(s.Points) > max {
			t.Fatalf("len %d > %d", len(s.Points), max)
		}
	}
}

func TestCollector_ResetsOnQueryChange(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	_, _, _ = c.Update(context.Background(), "k", "q1", history, step, t0)
	_, _, _ = c.Update(context.Background(), "k", "q2", history, step, t0.Add(step))
	if len(q.calls) != 2 || !q.calls[1].start.Equal(t0.Add(step).Add(-history)) || q.calls[1].query != "q2" {
		t.Fatalf("calls = %+v", q.calls)
	}
}

func TestCollector_GapStats(t *testing.T) {
	q := &fakeQuerier{nan: func(ts time.Time) bool { return ts.After(t0.Add(-3 * step)) }}
	c := NewCollector(q)
	_, g, err := c.Update(context.Background(), "k", "q", history, step, t0)
	if err != nil {
		t.Fatal(err)
	}
	if g.RecentMissing != 3 || g.Healthy() {
		t.Fatalf("gaps = %+v", g)
	}
	if !(GapStats{RecentMissing: 2, SeasonMissingFrac: 0.1}).Healthy() {
		t.Fatal("expected healthy")
	}
	if (GapStats{SeasonMissingFrac: 0.11}).Healthy() {
		t.Fatal("expected unhealthy")
	}
}

func TestCollector_QuerierErrorKeepsBuffer(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	first, _, _ := c.Update(context.Background(), "k", "q", history, step, t0)
	q.err = errors.New("boom")
	s, _, err := c.Update(context.Background(), "k", "q", history, step, t0.Add(2*step))
	if err == nil {
		t.Fatal("expected error")
	}
	if len(s.Points) != len(first.Points) {
		t.Fatalf("buffer changed: %d vs %d", len(s.Points), len(first.Points))
	}
}

func TestCollector_Forget(t *testing.T) {
	q := &fakeQuerier{}
	c := NewCollector(q)
	_, _, _ = c.Update(context.Background(), "k", "q", history, step, t0)
	c.Forget("k")
	_, _, _ = c.Update(context.Background(), "k", "q", history, step, t0.Add(step))
	if !q.calls[1].start.Equal(t0.Add(step).Add(-history)) {
		t.Fatalf("calls = %+v", q.calls)
	}
}

func TestCollector_NewWorkloadWithShortHistoryIsHealthy(t *testing.T) {
	// 2 "seasons" (20 min) of data inside a 70 min history window: the leading NaNs
	// predate the series and are not gaps.
	q := &fakeQuerier{nan: func(ts time.Time) bool { return ts.Before(t0.Add(-20 * step)) }}
	c := NewCollector(q)
	_, g, err := c.Update(context.Background(), "k", "q", 70*step, step, t0)
	if err != nil {
		t.Fatal(err)
	}
	if !g.Healthy() {
		t.Fatalf("gaps = %+v, want healthy", g)
	}
}

func TestCollector_InteriorGapsAfterFirstSampleStillCount(t *testing.T) {
	q := &fakeQuerier{nan: func(ts time.Time) bool {
		return ts.Before(t0.Add(-40*step)) || (ts.After(t0.Add(-30*step)) && ts.Before(t0.Add(-20*step)))
	}}
	c := NewCollector(q)
	_, g, _ := c.Update(context.Background(), "k", "q", 70*step, step, t0)
	if g.Healthy() {
		t.Fatalf("gaps = %+v, want unhealthy", g)
	}
}

func TestCollector_NoValidSamplesIsUnhealthy(t *testing.T) {
	q := &fakeQuerier{nan: func(time.Time) bool { return true }}
	c := NewCollector(q)
	_, g, _ := c.Update(context.Background(), "k", "q", history, step, t0)
	if g.Healthy() {
		t.Fatalf("gaps = %+v, want unhealthy", g)
	}
}
