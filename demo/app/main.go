// Command app is a capacity-limited demo HTTP service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/time/rate"
)

type config struct {
	CapacityRPS     int
	StartupDelay    time.Duration
	DownstreamURL   string
	DownstreamDelay time.Duration
	MaxWait         time.Duration
	// BurnPerRequest is the CPU time each admitted request burns in a calibrated busy loop. Zero
	// disables it. Only the real-cluster benchmark sets it, so an HPA on CPU has a signal that is
	// proportional to requests served.
	BurnPerRequest time.Duration
}

type app struct {
	cfg      config
	clock    func() time.Time
	started  time.Time
	limiter  *rate.Limiter
	reg      *prometheus.Registry
	requests *prometheus.CounterVec
	duration prometheus.Histogram
	client   *http.Client
	burnIter int64 // busy-loop iterations per request; 0 when the burn is off
}

func newApp(cfg config, clock func() time.Time) *app {
	a := &app{
		cfg:     cfg,
		clock:   clock,
		started: clock(),
		limiter: rate.NewLimiter(rate.Limit(cfg.CapacityRPS), cfg.CapacityRPS),
		reg:     prometheus.NewRegistry(),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "Handled /work requests by status code.",
		}, []string{"code"}),
		duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "http_request_duration_seconds", Help: "Latency of /work requests.",
			Buckets: prometheus.DefBuckets,
		}),
		client: &http.Client{Timeout: 5 * time.Second},
	}
	a.reg.MustRegister(a.requests, a.duration)
	if cfg.BurnPerRequest > 0 {
		a.burnIter = int64(float64(calibrate()) * float64(cfg.BurnPerRequest) / float64(time.Millisecond))
	}
	return a
}

func (a *app) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if a.clock().Sub(a.started) < a.cfg.StartupDelay {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.HandlerFor(a.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/work", a.work)
	return mux
}

func (a *app) work(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), a.cfg.MaxWait)
	defer cancel()
	code := http.StatusOK
	if err := a.limiter.Wait(ctx); err != nil {
		code = http.StatusServiceUnavailable
	}
	if code == http.StatusOK && a.burnIter > 0 {
		burn(a.burnIter)
	}
	w.WriteHeader(code)
	a.requests.WithLabelValues(strconv.Itoa(code)).Inc()
	a.duration.Observe(time.Since(start).Seconds())
	if code == http.StatusOK && a.cfg.DownstreamURL != "" {
		go a.callDownstream()
	}
}

// burnSink keeps the compiler from removing the busy loop.
var burnSink atomic.Uint64

// burn runs n iterations of a cheap integer recurrence on one core.
func burn(n int64) {
	x := uint64(88172645463325252)
	for i := int64(0); i < n; i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
	}
	burnSink.Store(x)
}

// calibrate returns how many burn iterations fit in one millisecond of CPU on this machine.
func calibrate() int64 {
	const probe = 20_000_000
	burn(probe / 10) // warm up
	start := time.Now()
	burn(probe)
	per := float64(probe) / (float64(time.Since(start)) / float64(time.Millisecond))
	return int64(per)
}

func (a *app) callDownstream() {
	time.Sleep(a.cfg.DownstreamDelay)
	resp, err := a.client.Get(a.cfg.DownstreamURL)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

func envDuration(key, def string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		v = def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return d
}

func main() {
	capacity := 50
	if v := os.Getenv("CAPACITY_RPS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			log.Fatalf("invalid CAPACITY_RPS=%q", v)
		}
		capacity = n
	}
	cfg := config{
		CapacityRPS:     capacity,
		StartupDelay:    envDuration("STARTUP_DELAY", "45s"),
		DownstreamURL:   os.Getenv("DOWNSTREAM_URL"),
		DownstreamDelay: envDuration("DOWNSTREAM_DELAY", "0s"),
		MaxWait:         envDuration("MAX_WAIT", "2s"),
		BurnPerRequest:  envDuration("BURN_CPU_PER_REQUEST", "0s"),
	}
	srv := &http.Server{Addr: ":8080", Handler: newApp(cfg, time.Now).handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
