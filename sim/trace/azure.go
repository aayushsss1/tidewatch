package trace

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// Workload is one function's per-minute invocation counts concatenated over all loaded days.
// Data: Azure Functions Trace 2019, CC-BY-4.0 (Shahrad et al., USENIX ATC 2020).
type Workload struct {
	ID      string // HashFunction
	Owner   string // HashOwner
	App     string // HashApp
	Trigger string
	Counts  []float64 // invocations per minute
}

// Selector chooses functions. Exactly one mode applies: Prefix (HashFunction prefix match)
// or TopN (auto-pick).
type Selector struct {
	Prefix string
	// TopN picks the N highest-volume functions that pass MinMeanPerMin and have a lag-1440
	// autocorrelation of at least MinACF. It needs at least two days of data.
	TopN int
	// MinMeanPerMin is the minimum mean invocations per minute for TopN. Default 5.
	MinMeanPerMin float64
	// MinACF is the minimum lag-1440 autocorrelation for TopN. Default 0.3.
	MinACF float64
}

const (
	minutesPerDay  = 1440
	defaultMinMean = 5.0
	defaultMinACF  = 0.3
	candidateMult  = 25 // volume candidates examined per requested function
	candidateMin   = 200
)

// At implements Source: the rate in requests per second during the minute containing off.
// The trace wraps around, so offsets beyond its length (or negative) are valid.
func (w Workload) At(off time.Duration) float64 {
	if len(w.Counts) == 0 {
		return 0
	}
	n := time.Duration(len(w.Counts)) * time.Minute
	return w.Counts[int(mod(off, n)/time.Minute)] / 60
}

// PeakRPS is the maximum per-minute rate in requests per second.
func (w Workload) PeakRPS() float64 {
	m := 0.0
	for _, c := range w.Counts {
		m = math.Max(m, c)
	}
	return m / 60
}

// Rescaled returns a copy scaled so that PeakRPS equals peak. The receiver is unchanged.
// A trace that is all zero is returned as is.
func (w Workload) Rescaled(peak float64) Workload {
	out := w
	out.Counts = append([]float64(nil), w.Counts...)
	cur := w.PeakRPS()
	if cur == 0 {
		return out
	}
	k := peak / cur
	for i := range out.Counts {
		out.Counts[i] *= k
	}
	return out
}

// Series converts the whole trace to a 1m series in requests per second, starting at epoch.
func (w Workload) Series(epoch time.Time) forecast.Series {
	s := forecast.Series{Step: time.Minute, Points: make([]forecast.Point, len(w.Counts))}
	for i, c := range w.Counts {
		s.Points[i] = forecast.Point{T: epoch.Add(time.Duration(i) * time.Minute), V: c / 60}
	}
	return s
}

// LoadAzure2019 reads the invocations_per_function_md.anon.dNN.csv files in dir and returns
// the functions chosen by sel, with the days concatenated in order. Run
// scripts/bench/fetch-azure2019.sh to populate dir.
func LoadAzure2019(dir string, sel Selector) ([]Workload, error) {
	if sel.Prefix == "" && sel.TopN <= 0 {
		return nil, errors.New("azure2019: selector needs a function prefix or topN > 0")
	}
	files, err := dayFiles(dir)
	if err != nil {
		return nil, err
	}
	if sel.Prefix != "" {
		return loadByPrefix(files, sel.Prefix)
	}
	return loadTopN(files, sel)
}

func dayFiles(dir string) ([]string, error) {
	files, _ := filepath.Glob(filepath.Join(dir, "invocations_per_function_md.anon.d*.csv"))
	sort.Strings(files)
	if len(files) == 0 {
		return nil, fmt.Errorf("azure2019: no invocations_per_function_md.anon.d*.csv in %s; run scripts/bench/fetch-azure2019.sh", dir)
	}
	return files, nil
}

type key struct{ owner, app, fn string }

type row struct {
	key     key
	trigger string
	counts  []float64
}

// scanFile streams rows of one day file to fn. Counts are parsed only when parse(key) is true.
func scanFile(path string, parse func(key) bool, fn func(row)) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	r := bufio.NewReaderSize(f, 1<<20)
	header, err := r.ReadString('\n')
	if err != nil {
		return fmt.Errorf("%s: reading header: %w", path, err)
	}
	if cols := strings.Count(header, ",") + 1; cols != 4+minutesPerDay {
		return fmt.Errorf("%s: header has %d columns, want %d", path, cols, 4+minutesPerDay)
	}
	for ln := 2; ; ln++ {
		line, err := r.ReadString('\n')
		if line = strings.TrimRight(line, "\r\n"); line != "" {
			parts := strings.Split(line, ",")
			if len(parts) != 4+minutesPerDay {
				return fmt.Errorf("%s:%d: %d columns, want %d", path, ln, len(parts), 4+minutesPerDay)
			}
			k := key{parts[0], parts[1], parts[2]}
			rw := row{key: k, trigger: parts[3]}
			if parse == nil || parse(k) {
				rw.counts = make([]float64, minutesPerDay)
				for i := range rw.counts {
					v, perr := strconv.ParseFloat(parts[4+i], 64)
					if perr != nil {
						return fmt.Errorf("%s:%d: column %d: %w", path, ln, i+1, perr)
					}
					rw.counts[i] = v
				}
			}
			fn(rw)
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// collect loads the full multi-day series of the wanted keys.
func collect(files []string, want func(key) bool) ([]Workload, error) {
	byKey := map[key]*Workload{}
	var order []key
	for d, path := range files {
		err := scanFile(path, want, func(r row) {
			if r.counts == nil {
				return
			}
			w := byKey[r.key]
			if w == nil {
				w = &Workload{ID: r.key.fn, Owner: r.key.owner, App: r.key.app, Trigger: r.trigger}
				byKey[r.key] = w
				order = append(order, r.key)
			}
			// Pad days where the function was absent.
			for len(w.Counts) < d*minutesPerDay {
				w.Counts = append(w.Counts, 0)
			}
			w.Counts = append(w.Counts, r.counts...)
		})
		if err != nil {
			return nil, err
		}
	}
	out := make([]Workload, 0, len(order))
	for _, k := range order {
		w := byKey[k]
		for len(w.Counts) < len(files)*minutesPerDay {
			w.Counts = append(w.Counts, 0)
		}
		out = append(out, *w)
	}
	return out, nil
}

func loadByPrefix(files []string, prefix string) ([]Workload, error) {
	out, err := collect(files, func(k key) bool { return strings.HasPrefix(k.fn, prefix) })
	if err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("azure2019: no function with HashFunction prefix %q", prefix)
	}
	return out, nil
}

func loadTopN(files []string, sel Selector) ([]Workload, error) {
	if len(files) < 2 {
		return nil, errors.New("azure2019: seasonality selection needs at least 2 day files")
	}
	minMean, minACF := sel.MinMeanPerMin, sel.MinACF
	if minMean <= 0 {
		minMean = defaultMinMean
	}
	if minACF <= 0 {
		minACF = defaultMinACF
	}
	// Pass 1: total volume per function.
	totals := map[key]float64{}
	for _, path := range files {
		err := scanFile(path, func(key) bool { return true }, func(r row) {
			s := 0.0
			for _, c := range r.counts {
				s += c
			}
			totals[r.key] += s
		})
		if err != nil {
			return nil, err
		}
	}
	minTotal := minMean * float64(len(files)*minutesPerDay)
	var cands []key
	for k, t := range totals {
		if t >= minTotal {
			cands = append(cands, k)
		}
	}
	sortKeys(cands, func(k key) float64 { return totals[k] })
	if lim := max(candidateMin, candidateMult*sel.TopN); len(cands) > lim {
		cands = cands[:lim]
	}
	set := make(map[key]bool, len(cands))
	for _, k := range cands {
		set[k] = true
	}
	// Pass 2: full series for the candidates, then the seasonality filter.
	ws, err := collect(files, func(k key) bool { return set[k] })
	if err != nil {
		return nil, err
	}
	vol := func(w Workload) float64 {
		return totals[key{w.Owner, w.App, w.ID}]
	}
	sort.SliceStable(ws, func(i, j int) bool { return vol(ws[i]) > vol(ws[j]) })
	var out []Workload
	for _, w := range ws {
		if autocorr(w.Counts, minutesPerDay) >= minACF {
			out = append(out, w)
			if len(out) == sel.TopN {
				break
			}
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("azure2019: no function with mean >= %g/min and lag-%d autocorrelation >= %g", minMean, minutesPerDay, minACF)
	}
	return out, nil
}

func sortKeys(ks []key, by func(key) float64) {
	sort.Slice(ks, func(i, j int) bool {
		a, b := by(ks[i]), by(ks[j])
		if a != b {
			return a > b
		}
		return ks[i].fn < ks[j].fn // deterministic ties
	})
}

// autocorr is the sample autocorrelation of x at the given lag. It returns 0 for series
// that are too short or constant.
func autocorr(x []float64, lag int) float64 {
	n := len(x)
	if lag <= 0 || n <= lag {
		return 0
	}
	mean := 0.0
	for _, v := range x {
		mean += v
	}
	mean /= float64(n)
	var num, den float64
	for i, v := range x {
		den += (v - mean) * (v - mean)
		if i+lag < n {
			num += (v - mean) * (x[i+lag] - mean)
		}
	}
	if den == 0 {
		return 0
	}
	return num / den
}
