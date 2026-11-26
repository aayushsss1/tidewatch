// Package scenario defines the benchmark scenario schema (YAML), validates it, and
// composes the offered-load series of each workload from a trace, injected events and
// chain lags.
//
// Convention: all offsets (start, events, notes, lags) are measured from the trace origin
// (offset 0). Start is where measurement begins; History of pre-seeded data precedes it, so
// Series covers [Start-History, Start+Duration]. Events and notes must fall inside that
// window. Overlapping events multiply. Non-periodic traces (azure2019) must cover the
// whole window (lags included) unless the trace sets wrap: true. The harness picks the
// absolute Epoch that offset 0 maps to.
package scenario

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/sim/trace"
)

// DefaultEpoch is the absolute time that offset 0 maps to unless the harness overrides
// Scenario.Epoch.
var DefaultEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

const (
	TraceSynthetic = "synthetic:diurnal"
	TraceAzure2019 = "azure2019"

	defaultHistory = 72 * time.Hour
	defaultStep    = time.Minute
	defaultMaxRepl = 100
	defaultMinRepl = 1
	maxWorkloads   = 3
	eventKindSurge = "surge"
	defaultPeriod  = 24 * time.Hour
)

// TraceSpec selects a load trace.
type TraceSpec struct {
	Type string `yaml:"type"`

	// synthetic:diurnal. Period defaults to 24h.
	Base   float64       `yaml:"base"`
	Amp    float64       `yaml:"amp"`
	Period time.Duration `yaml:"period"`
	Noise  float64       `yaml:"noise"`
	Seed   int64         `yaml:"seed"`

	// azure2019: either Function (HashFunction prefix) or Rank (index into the auto-picked
	// seasonal functions by volume, default 0). PeakRPS rescales the trace when > 0.
	Function      string  `yaml:"function"`
	Rank          int     `yaml:"rank"`
	MinMeanPerMin float64 `yaml:"minMeanPerMin"`
	PeakRPS       float64 `yaml:"peakRPS"` // rescales the trace's p99 per-minute rate
	// Wrap lets a non-periodic trace repeat when the scenario window exceeds its length.
	Wrap bool `yaml:"wrap"`
}

// Workload is one scaled target.
type Workload struct {
	Name         string        `yaml:"name"`
	Capacity     float64       `yaml:"capacity"` // requests per second per pod
	StartupDelay time.Duration `yaml:"startupDelay"`
	MinReplicas  int           `yaml:"minReplicas"`
	MaxReplicas  int           `yaml:"maxReplicas"`
	// Lag and Scale apply to non-lead workloads: the load is the source series shifted by
	// Lag and multiplied by Scale (default 1). The lead's Scale multiplies its trace.
	Lag   time.Duration `yaml:"lag"`
	Scale float64       `yaml:"scale"`
	// Trace, when set, makes the workload independent of the lead (events do not apply).
	Trace *TraceSpec `yaml:"trace"`
}

// Event is an injected load event applied to the lead workload.
type Event struct {
	Kind  string        `yaml:"kind"`
	Start time.Duration `yaml:"start"`
	Ramp  time.Duration `yaml:"ramp"`
	Hold  time.Duration `yaml:"hold"`
	Mult  float64       `yaml:"mult"`
}

// factor is the surge multiplier at offset t: 1 before Start, a linear ramp 1 -> Mult over
// Ramp, Mult for Hold, then 1 again.
func (e Event) factor(t time.Duration) float64 {
	d := t - e.Start
	switch {
	case d < 0 || d >= e.Ramp+e.Hold:
		return 1
	case d < e.Ramp:
		return 1 + (e.Mult-1)*float64(d)/float64(e.Ramp)
	default:
		return e.Mult
	}
}

// noteSpec is the YAML form of a Note.
type noteSpec struct {
	PublishAt time.Duration `yaml:"publishAt"`
	Text      string        `yaml:"text"`
}

// Note is an ops note published to the policy at PublishAt.
type Note struct {
	PublishAt time.Time
	Text      string
}

// Scenario is a validated scenario. Epoch may be set by the harness before calling Series
// or Notes.
type Scenario struct {
	Name      string        `yaml:"name"`
	Seed      int64         `yaml:"seed"`
	Trace     TraceSpec     `yaml:"trace"`
	Start     time.Duration `yaml:"start"`
	Duration  time.Duration `yaml:"duration"`
	Step      time.Duration `yaml:"step"`
	History   time.Duration `yaml:"history"`
	Workloads []Workload    `yaml:"workloads"`
	Events    []Event       `yaml:"events"`
	NoteSpecs []noteSpec    `yaml:"notes"`

	Epoch time.Time `yaml:"-"`

	sources map[string]trace.Source // workload name -> pre-lag source (own trace or lead composed)
}

// Load reads, validates and resolves a scenario file. Azure traces are read from the
// default bench cache (see AzureDir).
func Load(path string) (*Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	s, err := Parse(data, "")
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// AzureDir returns the default Azure 2019 cache directory.
func AzureDir() string {
	cache := os.Getenv("TIDEWATCH_BENCH_CACHE")
	if cache == "" {
		home, _ := os.UserHomeDir()
		cache = filepath.Join(home, ".cache", "tidewatch-bench")
	}
	return filepath.Join(cache, "azure2019")
}

// Parse decodes, validates and resolves a scenario. azureDir overrides the Azure cache
// location when non-empty.
func Parse(data []byte, azureDir string) (*Scenario, error) {
	var s Scenario
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("empty scenario")
		}
		return nil, fmt.Errorf("decode: %w", err)
	}
	s.applyDefaults()
	if err := s.validate(); err != nil {
		return nil, err
	}
	if azureDir == "" {
		azureDir = AzureDir()
	}
	if err := s.resolve(azureDir); err != nil {
		return nil, err
	}
	s.Epoch = DefaultEpoch
	return &s, nil
}

func (s *Scenario) applyDefaults() {
	if s.Step == 0 {
		s.Step = defaultStep
	}
	if s.History == 0 {
		s.History = defaultHistory
	}
	for i := range s.Workloads {
		w := &s.Workloads[i]
		if w.MinReplicas == 0 {
			w.MinReplicas = defaultMinRepl
		}
		if w.MaxReplicas == 0 {
			w.MaxReplicas = defaultMaxRepl
		}
		if w.Scale == 0 {
			w.Scale = 1
		}
	}
}

func (t *TraceSpec) validate(where string) error {
	switch t.Type {
	case TraceSynthetic:
		if t.Base < 0 || t.Amp < 0 || t.Period < 0 || t.Noise < 0 || t.Noise >= 1 {
			return fmt.Errorf("%s: synthetic trace needs base, amp, period >= 0 and 0 <= noise < 1", where)
		}
		if t.Base == 0 && t.Amp == 0 {
			return fmt.Errorf("%s: synthetic trace needs base or amp > 0", where)
		}
	case TraceAzure2019:
		if t.Rank < 0 || t.PeakRPS < 0 || t.MinMeanPerMin < 0 {
			return fmt.Errorf("%s: azure2019 rank, peakRPS and minMeanPerMin must be >= 0", where)
		}
		if t.Function != "" && t.Rank != 0 {
			return fmt.Errorf("%s: azure2019 takes function or rank, not both", where)
		}
	case "":
		return fmt.Errorf("%s: trace.type is required (%s or %s)", where, TraceSynthetic, TraceAzure2019)
	default:
		return fmt.Errorf("%s: unknown trace type %q (want %s or %s)", where, t.Type, TraceSynthetic, TraceAzure2019)
	}
	return nil
}

func (s *Scenario) validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return errors.New("name is required")
	}
	if s.Duration <= 0 {
		return errors.New("duration must be > 0")
	}
	if s.Step <= 0 {
		return errors.New("step must be > 0")
	}
	if s.Start < 0 || s.History < 0 {
		return errors.New("start and history must be >= 0")
	}
	if s.Duration%s.Step != 0 || s.History%s.Step != 0 {
		return fmt.Errorf("duration (%v) and history (%v) must be multiples of step (%v)", s.Duration, s.History, s.Step)
	}
	if n := len(s.Workloads); n < 1 || n > maxWorkloads {
		return fmt.Errorf("need 1-%d workloads, got %d", maxWorkloads, n)
	}
	// The top-level trace is only used when the lead workload has no trace of its own.
	if s.Workloads[0].Trace == nil || s.Trace.Type != "" {
		if err := s.Trace.validate("trace"); err != nil {
			return err
		}
	}
	if err := s.validateWorkloads(); err != nil {
		return err
	}
	return s.validateEventsAndNotes()
}

func (s *Scenario) validateWorkloads() error {
	seen := map[string]bool{}
	for i, w := range s.Workloads {
		where := fmt.Sprintf("workloads[%d] (%q)", i, w.Name)
		switch {
		case w.Name == "":
			return fmt.Errorf("workloads[%d]: name is required", i)
		case seen[w.Name]:
			return fmt.Errorf("%s: duplicate workload name", where)
		case w.Capacity <= 0:
			return fmt.Errorf("%s: capacity must be > 0", where)
		case w.StartupDelay < 0:
			return fmt.Errorf("%s: startupDelay must be >= 0", where)
		case w.MinReplicas < 0 || w.MaxReplicas < w.MinReplicas:
			return fmt.Errorf("%s: need 0 <= minReplicas <= maxReplicas (maxReplicas %d)", where, w.MaxReplicas)
		case w.Lag < 0 || w.Scale < 0:
			return fmt.Errorf("%s: lag and scale must be >= 0", where)
		case i == 0 && w.Lag != 0:
			return fmt.Errorf("%s: the lead workload cannot have a lag", where)
		}
		seen[w.Name] = true
		if w.Trace != nil {
			if err := w.Trace.validate(where + " trace"); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Scenario) validateEventsAndNotes() error {
	lo, hi := s.Start-s.History, s.Start+s.Duration
	for i, e := range s.Events {
		where := fmt.Sprintf("events[%d]", i)
		switch {
		case e.Kind != eventKindSurge:
			return fmt.Errorf("%s: unknown kind %q (want %s)", where, e.Kind, eventKindSurge)
		case e.Start < 0 || e.Ramp < 0 || e.Hold < 0:
			return fmt.Errorf("%s: start, ramp and hold must be >= 0", where)
		case e.Mult <= 0:
			return fmt.Errorf("%s: mult must be > 0", where)
		case e.Ramp+e.Hold == 0:
			return fmt.Errorf("%s: ramp + hold must be > 0", where)
		case e.Start < lo || e.Start > hi:
			return fmt.Errorf("%s: start %v is outside the scenario window [%v, %v]", where, e.Start, lo, hi)
		}
	}
	for i, n := range s.NoteSpecs {
		if n.PublishAt < 0 {
			return fmt.Errorf("notes[%d]: publishAt must be >= 0", i)
		}
		if n.PublishAt < lo || n.PublishAt > hi {
			return fmt.Errorf("notes[%d]: publishAt %v is outside the scenario window [%v, %v]", i, n.PublishAt, lo, hi)
		}
		if strings.TrimSpace(n.Text) == "" {
			return fmt.Errorf("notes[%d]: text is required", i)
		}
		if _, err := RenderNote(n.Text, DefaultEpoch); err != nil {
			return fmt.Errorf("notes[%d]: %w", i, err)
		}
	}
	return nil
}

// resolve builds the per-workload sources, loading Azure data where needed, and checks
// that non-periodic traces cover the window each workload reads.
func (s *Scenario) resolve(azureDir string) error {
	cache := map[string][]trace.Workload{}
	lo, hi := s.Start-s.History, s.Start+s.Duration
	// build returns the source and checks coverage of [lo-lag, hi-lag].
	build := func(spec TraceSpec, lag time.Duration, where string) (trace.Source, error) {
		src, length, err := buildTrace(spec, azureDir, cache)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		if length > 0 && !spec.Wrap && (lo-lag < 0 || hi-lag > length) {
			return nil, fmt.Errorf("%s: window [%v, %v] (lag %v) exceeds the trace length %v; "+
				"adjust start/history/duration or set wrap: true", where, lo-lag, hi-lag, lag, length)
		}
		return src, nil
	}
	lead := s.Workloads[0]
	spec := s.Trace
	where := "trace"
	if lead.Trace != nil {
		spec, where = *lead.Trace, "workloads[0] trace"
	}
	leadTrace, err := build(spec, 0, where)
	if err != nil {
		return err
	}
	events := s.Events
	leadSrc := sourceFunc(func(t time.Duration) float64 {
		v := lead.Scale * leadTrace.At(t)
		for _, e := range events {
			v *= e.factor(t)
		}
		return v
	})
	s.sources = map[string]trace.Source{lead.Name: leadSrc}
	for i := 1; i < len(s.Workloads); i++ {
		w := s.Workloads[i]
		var base trace.Source = leadSrc
		if w.Trace != nil {
			if base, err = build(*w.Trace, w.Lag, fmt.Sprintf("workloads[%d] (%q) trace", i, w.Name)); err != nil {
				return err
			}
		} else if spec.Type == TraceAzure2019 && !spec.Wrap && lo-w.Lag < 0 {
			return fmt.Errorf("workloads[%d] (%q): lag %v reads before the trace origin (window starts at %v)",
				i, w.Name, w.Lag, lo)
		}
		lag, scale := w.Lag, w.Scale
		s.sources[w.Name] = sourceFunc(func(t time.Duration) float64 { return scale * base.At(t-lag) })
	}
	return nil
}

type sourceFunc func(time.Duration) float64

func (f sourceFunc) At(t time.Duration) float64 { return f(t) }

// buildTrace returns the source and its length (0 for periodic sources).
func buildTrace(spec TraceSpec, azureDir string,
	cache map[string][]trace.Workload) (trace.Source, time.Duration, error) {
	if spec.Type == TraceSynthetic {
		period := spec.Period
		if period == 0 {
			period = defaultPeriod
		}
		return trace.Diurnal{Base: spec.Base, Amp: spec.Amp, Period: period, Noise: spec.Noise, Seed: spec.Seed}, 0, nil
	}
	sel := trace.Selector{Prefix: spec.Function, MinMeanPerMin: spec.MinMeanPerMin}
	if spec.Function == "" {
		sel.TopN = spec.Rank + 1
	}
	key := fmt.Sprintf("%+v", sel)
	ws, ok := cache[key]
	if !ok {
		var err error
		if ws, err = trace.LoadAzure2019(azureDir, sel); err != nil {
			return nil, 0, err
		}
		cache[key] = ws
	}
	idx := 0
	if spec.Function == "" {
		idx = spec.Rank
		if idx >= len(ws) {
			return nil, 0, fmt.Errorf("azure2019: rank %d requested but only %d seasonal functions qualify",
				spec.Rank, len(ws))
		}
	}
	w := ws[idx]
	if spec.PeakRPS > 0 {
		w = w.Rescaled(spec.PeakRPS)
	}
	return w, w.Length(), nil
}

// Workload returns the named workload spec.
func (s *Scenario) Workload(name string) (Workload, bool) {
	for _, w := range s.Workloads {
		if w.Name == name {
			return w, true
		}
	}
	return Workload{}, false
}

// StartTime is the start of the simulated run as an absolute time.
func (s *Scenario) StartTime() time.Time { return s.Epoch.Add(s.Start) }

// EndTime is the end of the simulated run as an absolute time.
func (s *Scenario) EndTime() time.Time { return s.Epoch.Add(s.Start + s.Duration) }

// Series returns the offered load (requests per second) of the workload over
// [Start-History, Start+Duration] inclusive at Step. Unknown names return an empty series.
func (s *Scenario) Series(workload string) forecast.Series {
	src, ok := s.sources[workload]
	if !ok {
		return forecast.Series{Step: s.Step}
	}
	return trace.Sample(src, s.Epoch, s.Start-s.History, s.Start+s.Duration, s.Step)
}

// Notes returns the ops notes, templates rendered against Epoch (see RenderNote), sorted by PublishAt (stable for equal times). The result is a copy.
func (s *Scenario) Notes() []Note {
	out := make([]Note, len(s.NoteSpecs))
	for i, n := range s.NoteSpecs {
		// Render once, here, so Tidewatch and the controller baselines get identical text.
		// validate has already proved the template renders, so the error cannot occur.
		text, err := RenderNote(n.Text, s.Epoch)
		if err != nil {
			text = n.Text
		}
		out[i] = Note{PublishAt: s.Epoch.Add(n.PublishAt), Text: text}
	}
	slices.SortStableFunc(out, func(a, b Note) int { return a.PublishAt.Compare(b.PublishAt) })
	return out
}
