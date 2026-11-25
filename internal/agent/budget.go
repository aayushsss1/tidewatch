package agent

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// Budget tracks LLM token spend per clock hour.
type Budget struct {
	perHour int
	clk     clock.Clock
	mu      sync.Mutex
	hour    time.Time
	spent   int
}

// NewBudget returns a Budget allowing perHour tokens per clock hour.
func NewBudget(perHour int, clk clock.Clock) *Budget {
	return &Budget{perHour: perHour, clk: clk}
}

// roll resets spend on a new clock hour. Callers hold b.mu.
func (b *Budget) roll() {
	if h := b.clk.Now().Truncate(time.Hour); !h.Equal(b.hour) {
		b.hour, b.spent = h, 0
	}
}

// Allow reports whether estimate tokens fit in the remaining budget.
func (b *Budget) Allow(estimate int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	return b.perHour-b.spent >= estimate
}

// Spend records tokens used.
func (b *Budget) Spend(tokens int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	b.spent += tokens
}

// Remaining returns the tokens left this hour, never negative.
func (b *Budget) Remaining() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.roll()
	return max(0, b.perHour-b.spent)
}
