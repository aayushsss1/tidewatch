package trace

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
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
	numDays        = 14
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

// Length is the extent of the trace; At wraps beyond it.
func (w Workload) Length() time.Duration { return time.Duration(len(w.Counts)) * time.Minute }

// PeakRPS is the maximum per-minute rate in requests per second.
func (w Workload) PeakRPS() float64 {
	m := 0.0
	for _, c := range w.Counts {
		m = math.Max(m, c)
	}
	return m / 60
}

// RobustPeakRPS is the 99th percentile of the per-minute rate in requests per second. It is
// the reference level for rescaling because single-minute outliers would otherwise dominate.
func (w Workload) RobustPeakRPS() float64 {
	if len(w.Counts) == 0 {
		return 0
	}
	c := slices.Clone(w.Counts)
	slices.Sort(c)
	idx := int(math.Ceil(0.99*float64(len(c)))) - 1
	return c[max(idx, 0)] / 60
}

// Rescaled returns a copy scaled so that RobustPeakRPS (p99) equals peak. The receiver is
// unchanged. A trace whose p99 is zero is returned as is.
func (w Workload) Rescaled(peak float64) Workload {
	out := w
	out.Counts = append([]float64(nil), w.Counts...)
	cur := w.RobustPeakRPS()
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

// LoadAzure2019 reads invocations_per_function_md.anon.d01..d14.csv in dir (all 14 are
// required) and returns the functions chosen by sel, with the days concatenated in order.
// The per-day files only list a function on days it was invoked, so a function absent from a
// day is treated as zero invocations that day (zero-filled). TopN selection only considers
// functions present in all 14 days. A Prefix selector must match exactly one function.
// Run scripts/bench/fetch-azure2019.sh to populate dir.
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
	files := make([]string, numDays)
	var missing []string
	for d := 1; d <= numDays; d++ {
		name := fmt.Sprintf("invocations_per_function_md.anon.d%02d.csv", d)
		files[d-1] = filepath.Join(dir, name)
		if st, err := os.Stat(files[d-1]); err != nil || st.IsDir() {
			missing = append(missing, fmt.Sprintf("d%02d", d))
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("azure2019: missing day files %s in %s; run scripts/bench/fetch-azure2019.sh",
			strings.Join(missing, ","), dir)
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
	if err := checkHeader(path, strings.TrimRight(header, "\r\n")); err != nil {
		return err
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

func checkHeader(path, header string) error {
	cols := strings.Split(header, ",")
	if len(cols) != 4+minutesPerDay {
		return fmt.Errorf("%s: header has %d columns, want %d", path, len(cols), 4+minutesPerDay)
	}
	for i, want := range []string{"HashOwner", "HashApp", "HashFunction", "Trigger"} {
		if cols[i] != want {
			return fmt.Errorf("%s: header column %d is %q, want %q", path, i+1, cols[i], want)
		}
	}
	for i := 1; i <= minutesPerDay; i++ {
		if cols[3+i] != strconv.Itoa(i) {
			return fmt.Errorf("%s: header column %d is %q, want %q", path, 4+i, cols[3+i], strconv.Itoa(i))
		}
	}
	return nil
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
			// Days where the function was absent mean zero invocations: zero-fill.
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
	switch len(out) {
	case 0:
		return nil, fmt.Errorf("azure2019: no function with HashFunction prefix %q", prefix)
	case 1:
		return out, nil
	}
	n := min(len(out), 5)
	ids := make([]string, n)
	for i := range n {
		ids[i] = out[i].ID
	}
	return nil, fmt.Errorf("azure2019: prefix %q is ambiguous (%d functions, e.g. %s); use a longer prefix",
		prefix, len(out), strings.Join(ids, ", "))
}

func loadTopN(files []string, sel Selector) ([]Workload, error) {
	minMean, minACF := sel.MinMeanPerMin, sel.MinACF
	if minMean <= 0 {
		minMean = defaultMinMean
	}
	if minACF <= 0 {
		minACF = defaultMinACF
	}
	// Pass 1: total volume per function.
	totals := map[key]float64{}
	present := map[key]int{}
	for _, path := range files {
		err := scanFile(path, func(key) bool { return true }, func(r row) {
			s := 0.0
			for _, c := range r.counts {
				s += c
			}
			totals[r.key] += s
			present[r.key]++
		})
		if err != nil {
			return nil, err
		}
	}
	minTotal := minMean * float64(len(files)*minutesPerDay)
	var cands []key
	for k, t := range totals {
		if t >= minTotal && present[k] == len(files) {
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
	slices.SortStableFunc(ws, func(a, b Workload) int { return cmp.Compare(vol(b), vol(a)) })
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
		return nil, fmt.Errorf("azure2019: no function with mean >= %g/min and lag-%d autocorrelation >= %g",
			minMean, minutesPerDay, minACF)
	}
	return out, nil
}

func sortKeys(ks []key, by func(key) float64) {
	slices.SortFunc(ks, func(a, b key) int {
		if c := cmp.Compare(by(b), by(a)); c != 0 {
			return c
		}
		return cmp.Compare(a.fn, b.fn) // deterministic ties
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
