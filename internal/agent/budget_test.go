package agent

import (
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

func TestBudget_ResetsHourly(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Date(2026, 1, 1, 12, 10, 0, 0, time.UTC))
	b := NewBudget(1000, clk)
	if !b.Allow(1000) || b.Allow(1001) {
		t.Fatal("allow bounds")
	}
	b.Spend(900)
	if b.Remaining() != 100 || b.Allow(101) || !b.Allow(100) {
		t.Fatalf("remaining=%d", b.Remaining())
	}
	b.Spend(500) // overspend clamps at 0
	if b.Remaining() != 0 {
		t.Fatal(b.Remaining())
	}
	clk.Step(49 * time.Minute) // 12:59
	if b.Remaining() != 0 {
		t.Fatal("same hour")
	}
	clk.Step(time.Minute) // 13:00
	if b.Remaining() != 1000 {
		t.Fatalf("remaining=%d", b.Remaining())
	}
}
