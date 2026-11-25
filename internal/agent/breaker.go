package agent

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

// Breaker is a consecutive-failure circuit breaker with a single half-open trial.
type Breaker struct {
	threshold int
	openFor   time.Duration
	clk       clock.Clock
	mu        sync.Mutex
	state     breakerState
	failures  int
	openedAt  time.Time
	trial     bool // half-open trial handed out
}

// NewBreaker opens after threshold consecutive failures, for the open duration.
func NewBreaker(threshold int, open time.Duration, clk clock.Clock) *Breaker {
	return &Breaker{threshold: threshold, openFor: open, clk: clk}
}

// Allow reports whether a call may proceed. After the open duration it admits
// exactly one trial call until Success or Failure is reported.
func (b *Breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stateOpen:
		if b.clk.Since(b.openedAt) < b.openFor {
			return false
		}
		b.state, b.trial = stateHalfOpen, true
		return true
	case stateHalfOpen:
		if b.trial {
			return false
		}
		b.trial = true
		return true
	}
	return true
}

// Success closes the breaker and resets the failure streak.
func (b *Breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.failures, b.trial = stateClosed, 0, false
}

// Failure records a failed call; it re-opens a half-open breaker immediately.
func (b *Breaker) Failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.state == stateHalfOpen || b.failures >= b.threshold {
		b.state, b.openedAt, b.trial = stateOpen, b.clk.Now(), false
	}
}

// Open reports whether the breaker is not closed (open or awaiting its trial).
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state != stateClosed
}
