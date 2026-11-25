// Command app is a capacity-limited demo HTTP service.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
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
	w.WriteHeader(code)
	a.requests.WithLabelValues(strconv.Itoa(code)).Inc()
	a.duration.Observe(time.Since(start).Seconds())
	if code == http.StatusOK && a.cfg.DownstreamURL != "" {
		go a.callDownstream()
	}
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
	}
	srv := &http.Server{Addr: ":8080", Handler: newApp(cfg, time.Now).handler(), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}
