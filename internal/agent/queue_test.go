package agent

import (
	"context"
	"testing"
	"time"
)

func inc(id string) *Incident { return &Incident{ID: id} }

func TestQueue_PopsHighestScore(t *testing.T) {
	q := NewQueue()
	dl := time.Now().Add(time.Hour)
	q.Push(inc("lo"), 1, dl)
	q.Push(inc("hi"), 9, dl)
	q.Push(inc("mid"), 5, dl)
	if q.Len() != 3 {
		t.Fatal(q.Len())
	}
	for _, want := range []string{"hi", "mid", "lo"} {
		got, _, ok := q.Pop(context.Background())
		if !ok || got.ID != want {
			t.Fatalf("got %v want %s", got, want)
		}
	}
}

func TestQueue_DropsExpired(t *testing.T) {
	q := NewQueue()
	q.Push(inc("dead"), 9, time.Now().Add(-time.Second))
	q.Push(inc("live"), 1, time.Now().Add(time.Hour))
	got, _, ok := q.Pop(context.Background())
	if !ok || got.ID != "live" {
		t.Fatalf("got %v", got)
	}
	d := q.Dropped()
	if len(d) != 1 || d[0].ID != "dead" || len(q.Dropped()) != 0 {
		t.Fatalf("dropped=%v", d)
	}
}

func TestQueue_PopUnblocksOnCancel(t *testing.T) {
	q := NewQueue()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan bool)
	go func() { _, _, ok := q.Pop(ctx); done <- ok }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("expected ok=false")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop did not unblock")
	}
}

func TestQueue_PopWakesOnPush(t *testing.T) {
	q := NewQueue()
	done := make(chan string)
	go func() {
		i, _, _ := q.Pop(context.Background())
		done <- i.ID
	}()
	time.Sleep(20 * time.Millisecond)
	q.Push(inc("x"), 1, time.Now().Add(time.Hour))
	select {
	case id := <-done:
		if id != "x" {
			t.Fatal(id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pop did not wake")
	}
}
