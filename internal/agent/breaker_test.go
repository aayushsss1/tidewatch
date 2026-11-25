package agent

import (
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

func TestBreaker_OpensAfterFiveAndHalfOpens(t *testing.T) {
	clk := clocktesting.NewFakeClock(t0)
	b := NewBreaker(5, 2*time.Minute, clk)
	for range 4 {
		b.Failure()
	}
	if b.Open() || !b.Allow() {
		t.Fatal("should still be closed")
	}
	b.Success() // resets the streak
	for range 4 {
		b.Failure()
	}
	if b.Open() {
		t.Fatal("streak must reset on success")
	}
	b.Failure()
	if !b.Open() || b.Allow() {
		t.Fatal("should be open")
	}
	clk.Step(119 * time.Second)
	if b.Allow() {
		t.Fatal("still open")
	}
	clk.Step(time.Second)
	if !b.Allow() {
		t.Fatal("half-open should allow one trial")
	}
	if b.Allow() {
		t.Fatal("only one trial")
	}
	b.Failure() // trial fails: re-open
	if b.Allow() {
		t.Fatal("re-opened")
	}
	clk.Step(2 * time.Minute)
	if !b.Allow() {
		t.Fatal("second trial")
	}
	b.Success()
	if b.Open() || !b.Allow() || !b.Allow() {
		t.Fatal("success should close")
	}
}
