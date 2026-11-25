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

func openBreaker(clk *clocktesting.FakeClock) *Breaker {
	b := NewBreaker(1, 2*time.Minute, clk)
	b.Failure()
	clk.Step(2 * time.Minute)
	return b
}

func TestBreaker_ReleaseClearsTrial(t *testing.T) {
	clk := clocktesting.NewFakeClock(t0)
	b := openBreaker(clk)
	if !b.Allow() || b.Allow() {
		t.Fatal("one trial expected")
	}
	b.Release()
	if !b.Open() {
		t.Fatal("release must not close the breaker")
	}
	if !b.Allow() {
		t.Fatal("trial should be re-admitted after Release")
	}
}

func TestBreaker_UnreportedTrialReadmittedAfterOpenDuration(t *testing.T) {
	clk := clocktesting.NewFakeClock(t0)
	b := openBreaker(clk)
	b.Allow()
	clk.Step(119 * time.Second)
	if b.Allow() {
		t.Fatal("trial still outstanding")
	}
	clk.Step(time.Second)
	if !b.Allow() {
		t.Fatal("safety net should re-admit")
	}
}
