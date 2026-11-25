// Command trafficgen drives pattern-shaped load and seeds counter history.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
)

type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ";") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func patternFlags(fs *flag.FlagSet) func() (Pattern, error) {
	base := fs.Float64("base", 20, "baseline requests/s")
	amp := fs.Float64("amp", 80, "amplitude requests/s")
	period := fs.Duration("period", time.Hour, "pattern period")
	epoch := fs.String("epoch", "2026-01-01T00:00:00Z", "pattern epoch (RFC3339)")
	noise := fs.Float64("noise", 0.05, "multiplicative noise fraction")
	seed := fs.Int64("seed", 1, "noise seed")
	return func() (Pattern, error) {
		e, err := time.Parse(time.RFC3339, *epoch)
		if err != nil {
			return Pattern{}, fmt.Errorf("invalid --epoch: %w", err)
		}
		return Pattern{Base: *base, Amp: *amp, Period: *period, Epoch: e, Noise: *noise, Seed: *seed}, nil
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: trafficgen run|seed [flags]")
	}
	var err error
	switch os.Args[1] {
	case "run":
		err = runCmd(os.Args[2:])
	case "seed":
		err = seedCmd(os.Args[2:])
	default:
		err = fmt.Errorf("unknown subcommand %q", os.Args[1])
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	target := fs.String("target", "", "target URL, e.g. http://frontend.shop/work")
	getPattern := patternFlags(fs)
	_ = fs.Parse(args)
	if *target == "" {
		return fmt.Errorf("--target is required")
	}
	p, err := getPattern()
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	sem := make(chan struct{}, 500)
	var sent, failed atomic.Int64
	var wg sync.WaitGroup
	fire := func() {
		defer wg.Done()
		defer func() { <-sem }()
		resp, err := client.Get(*target)
		if err != nil {
			failed.Add(1)
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 500 {
			failed.Add(1)
		}
	}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for sec := 0; ; sec++ {
		n := int(math.Round(p.RPS(time.Now())))
		for i := 0; i < n; i++ {
			sem <- struct{}{}
			wg.Add(1)
			sent.Add(1)
			go fire()
			if n > 0 {
				time.Sleep(time.Second / time.Duration(n+1))
			}
		}
		<-tick.C
		if sec%10 == 0 {
			log.Printf("rps=%d sent=%d errors=%d", n, sent.Load(), failed.Load())
		}
	}
}

func seedCmd(args []string) error {
	fs := flag.NewFlagSet("seed", flag.ExitOnError)
	url := fs.String("remote-write", "", "Prometheus remote-write URL")
	from := fs.Duration("from", time.Hour, "history length")
	step := fs.Duration("step", 15*time.Second, "sample step")
	var series multiFlag
	fs.Var(&series, "series", "label set, e.g. namespace=shop,app=frontend (repeatable)")
	getPattern := patternFlags(fs)
	_ = fs.Parse(args)
	if *url == "" || len(series) == 0 {
		return fmt.Errorf("--remote-write and at least one --series are required")
	}
	p, err := getPattern()
	if err != nil {
		return err
	}
	var all []tsData
	for _, s := range series {
		labels, err := parseLabels(s)
		if err != nil {
			return err
		}
		all = append(all, tsData{labels: labels, samples: counterSamples(p, time.Now(), *from, *step)})
	}
	return writeAll(&http.Client{Timeout: 30 * time.Second}, *url, all, 5000)
}

func parseLabels(s string) (map[string]string, error) {
	m := map[string]string{"__name__": "http_requests_total", "instance": "seed", "code": "200"}
	for _, kv := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid series label %q", kv)
		}
		m[k] = v
	}
	return m, nil
}

type sample struct {
	value float64
	ts    int64 // milliseconds
}

type tsData struct {
	labels  map[string]string
	samples []sample
}

// counterSamples returns cumulative counts at step boundaries over [now-from, now),
// integrating RPS per second.
func counterSamples(p Pattern, now time.Time, from, step time.Duration) []sample {
	start := now.Add(-from).Truncate(step)
	var out []sample
	cum := 0.0
	cursor := start
	for t := start; t.Before(now); t = t.Add(step) {
		for ; cursor.Before(t); cursor = cursor.Add(time.Second) {
			cum += p.RPS(cursor)
		}
		out = append(out, sample{value: cum, ts: t.UnixMilli()})
	}
	return out
}

func writeAll(c *http.Client, url string, all []tsData, batch int) error {
	var cur []tsData
	n := 0
	flush := func() error {
		if len(cur) == 0 {
			return nil
		}
		err := post(c, url, encodeWriteRequest(cur))
		cur, n = nil, 0
		return err
	}
	for _, ts := range all {
		for i := 0; i < len(ts.samples); i += batch {
			end := min(i+batch, len(ts.samples))
			chunk := ts.samples[i:end]
			if n+len(chunk) > batch {
				if err := flush(); err != nil {
					return err
				}
			}
			cur = append(cur, tsData{labels: ts.labels, samples: chunk})
			n += len(chunk)
		}
	}
	return flush()
}

func post(c *http.Client, url string, raw []byte) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(snappy.Encode(nil, raw)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("remote write: %s", resp.Status)
	}
	return nil
}

// Minimal protobuf encoding of prometheus WriteRequest (prompb), avoiding the
// heavy prometheus module dependency.
func encodeWriteRequest(series []tsData) []byte {
	var out []byte
	for _, ts := range series {
		var t []byte
		names := make([]string, 0, len(ts.labels))
		for k := range ts.labels {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			var l []byte
			l = appendBytes(l, 1, []byte(k))
			l = appendBytes(l, 2, []byte(ts.labels[k]))
			t = appendBytes(t, 1, l)
		}
		for _, s := range ts.samples {
			var sm []byte
			sm = append(sm, 0x09) // field 1, fixed64
			bits := math.Float64bits(s.value)
			for i := 0; i < 8; i++ {
				sm = append(sm, byte(bits>>(8*i)))
			}
			sm = append(sm, 0x10) // field 2, varint
			sm = appendVarint(sm, uint64(s.ts))
			t = appendBytes(t, 2, sm)
		}
		out = appendBytes(out, 1, t)
	}
	return out
}

func appendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

func appendBytes(b []byte, field int, data []byte) []byte {
	b = appendVarint(b, uint64(field<<3|2))
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}
