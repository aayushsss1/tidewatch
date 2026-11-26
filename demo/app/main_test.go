package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestApp_NotReadyDuringStartupDelay(t *testing.T) {
	now := time.Unix(1000, 0)
	var mu sync.Mutex
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	a := newApp(config{CapacityRPS: 50, StartupDelay: 45 * time.Second, MaxWait: time.Second}, clock)
	h := a.handler()
	if c := get(h, "/readyz").Code; c != 503 {
		t.Fatalf("readyz before = %d", c)
	}
	if c := get(h, "/healthz").Code; c != 200 {
		t.Fatalf("healthz = %d", c)
	}
	mu.Lock()
	now = now.Add(46 * time.Second)
	mu.Unlock()
	if c := get(h, "/readyz").Code; c != 200 {
		t.Fatalf("readyz after = %d", c)
	}
}

func TestApp_ShedsBeyondCapacity(t *testing.T) {
	a := newApp(config{CapacityRPS: 5, MaxWait: 50 * time.Millisecond}, time.Now)
	h := a.handler()
	var wg sync.WaitGroup
	var shed atomic.Int32
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if get(h, "/work").Code == 503 {
				shed.Add(1)
			}
		}()
	}
	wg.Wait()
	if shed.Load() == 0 {
		t.Fatal("expected some 503s")
	}
	body := get(h, "/metrics").Body.String()
	if !strings.Contains(body, `http_requests_total{code="503"}`) {
		t.Fatalf("metrics missing 503 counter:\n%s", body)
	}
}

func TestApp_CallsDownstreamAfterDelay(t *testing.T) {
	got := make(chan time.Time, 1)
	ds := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case got <- time.Now():
		default:
		}
	}))
	defer ds.Close()
	delay := 150 * time.Millisecond
	a := newApp(config{CapacityRPS: 50, MaxWait: time.Second, DownstreamURL: ds.URL, DownstreamDelay: delay}, time.Now)
	start := time.Now()
	if c := get(a.handler(), "/work").Code; c != 200 {
		t.Fatalf("work = %d", c)
	}
	select {
	case at := <-got:
		if at.Sub(start) < delay {
			t.Fatalf("downstream called after %v, want >= %v", at.Sub(start), delay)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no downstream call")
	}
}

func TestApp_BurnCostsCPUOnlyWhenEnabled(t *testing.T) {
	timeWork := func(burn time.Duration) time.Duration {
		a := newApp(config{CapacityRPS: 1000, MaxWait: time.Second, BurnPerRequest: burn}, time.Now)
		h := a.handler()
		get(h, "/work")
		start := time.Now()
		for i := 0; i < 20; i++ {
			get(h, "/work")
		}
		return time.Since(start) / 20
	}
	off, on := timeWork(0), timeWork(4*time.Millisecond)
	if off > time.Millisecond {
		t.Errorf("burn disabled but a request took %v", off)
	}
	if on < 2*time.Millisecond || on > 20*time.Millisecond {
		t.Errorf("4ms burn took %v per request", on)
	}
}
