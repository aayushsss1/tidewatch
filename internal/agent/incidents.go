package agent

import (
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/signal"
)

// Status is the lifecycle state of an incident.
type Status string

// Incident statuses, see spec §7.2.
const (
	StatusOpen          Status = "open"
	StatusInvestigating Status = "investigating"
	StatusDecided       Status = "decided"
	StatusClosed        Status = "closed"
)

// Member is a workload in an incident with the triggers it raised.
type Member struct {
	Workload types.NamespacedName
	UID      types.UID
	Triggers []signal.Trigger
}

// Incident groups related triggers of one policy (or a storm of all policies).
type Incident struct {
	ID          string
	Policy      v1.PolicyRef
	Storm       bool
	Members     map[types.UID]*Member
	Lead        types.UID
	Lags        map[types.UID]time.Duration
	Opened      time.Time
	LastTrigger time.Time
	Status      Status
	Pending     bool // new triggers since last queued
}

func (i *Incident) clone() *Incident {
	c := *i
	c.Members = make(map[types.UID]*Member, len(i.Members))
	for k, m := range i.Members {
		mc := *m
		mc.Triggers = slices.Clone(m.Triggers)
		c.Members[k] = &mc
	}
	c.Lags = maps.Clone(i.Lags)
	return &c
}

// Options tunes the Manager. Zero fields take the spec §7.2 defaults.
type Options struct {
	GroupWindow, CloseAfter, StormWindow, StormQuiet time.Duration
	StormTriggers                                    int
	CorrThreshold                                    float64
}

func (o Options) withDefaults() Options {
	if o.GroupWindow == 0 {
		o.GroupWindow = 3 * time.Minute
	}
	if o.CloseAfter == 0 {
		o.CloseAfter = 30 * time.Minute
	}
	if o.StormWindow == 0 {
		o.StormWindow = 2 * time.Minute
	}
	if o.StormQuiet == 0 {
		o.StormQuiet = 10 * time.Minute
	}
	if o.StormTriggers == 0 {
		o.StormTriggers = 20
	}
	if o.CorrThreshold == 0 {
		o.CorrThreshold = 0.7
	}
	return o
}

// Manager groups triggers into incidents and tracks storm mode.
type Manager struct {
	clk   clock.Clock
	corr  *Correlator
	o     Options
	mu    sync.Mutex
	seq   int
	incs  []*Incident // creation order
	times []time.Time // recent trigger times for storm counting
	storm *Incident   // non-nil while in storm mode
}

// NewManager returns a Manager. corr may be nil (time window only).
func NewManager(clk clock.Clock, corr *Correlator, o Options) *Manager {
	return &Manager{clk: clk, corr: corr, o: o.withDefaults()}
}

// Add records a trigger, returning a snapshot of the incident it joined and
// whether this trigger started a storm.
func (m *Manager) Add(t signal.Trigger) (inc *Incident, stormStarted bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clk.Now()

	m.times = append(m.times, now)
	cut := 0
	for cut < len(m.times) && now.Sub(m.times[cut]) > m.o.StormWindow {
		cut++
	}
	m.times = m.times[cut:]

	if m.storm == nil && len(m.times) > m.o.StormTriggers {
		m.seq++
		m.storm = &Incident{
			ID: fmt.Sprintf("storm-%d", m.seq), Storm: true, Opened: now, Status: StatusOpen,
			Members: map[types.UID]*Member{}, Lags: map[types.UID]time.Duration{},
		}
		m.incs = append(m.incs, m.storm)
		stormStarted = true
	}
	if m.storm != nil {
		m.attach(m.storm, t, now)
		return m.storm.clone(), stormStarted
	}

	target, lag, hasLag := m.find(t, now)
	if target == nil {
		m.seq++
		target = &Incident{
			ID: fmt.Sprintf("%s-%d", t.Policy.Name, m.seq), Policy: t.Policy, Opened: now, Status: StatusOpen,
			Members: map[types.UID]*Member{}, Lags: map[types.UID]time.Duration{},
		}
		m.incs = append(m.incs, target)
	} else if hasLag {
		target.Lags[t.UID] = lag
	}
	m.attach(target, t, now)
	return target.clone(), false
}

// find returns the open incident t joins, with the measured lag if it joined by correlation.
func (m *Manager) find(t signal.Trigger, now time.Time) (*Incident, time.Duration, bool) {
	for _, in := range m.incs {
		if in.Storm || in.Status == StatusClosed || in.Policy != t.Policy {
			continue
		}
		if now.Sub(in.Opened) < m.o.GroupWindow {
			return in, 0, false
		}
		for uid := range in.Members {
			if uid == t.UID {
				return in, 0, false
			}
			if c, lag, ok := m.corr.Related(uid, t.UID); ok && c >= m.o.CorrThreshold {
				return in, lag, true
			}
		}
	}
	return nil, 0, false
}

func (m *Manager) attach(in *Incident, t signal.Trigger, now time.Time) {
	mem := in.Members[t.UID]
	if mem == nil {
		mem = &Member{Workload: t.Workload, UID: t.UID}
		in.Members[t.UID] = mem
	}
	mem.Triggers = append(mem.Triggers, t)
	in.LastTrigger, in.Pending = now, true
	if lead := in.Members[in.Lead]; lead == nil || t.At.Before(firstAt(lead)) {
		in.Lead = t.UID
	}
}

func firstAt(m *Member) time.Time {
	first := m.Triggers[0].At
	for _, t := range m.Triggers[1:] {
		if t.At.Before(first) {
			first = t.At
		}
	}
	return first
}

// TakeReady returns snapshots of incidents with new triggers that are open or
// decided, and marks them queued. In storm mode only the storm incident is returned.
func (m *Manager) TakeReady() []*Incident {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*Incident
	for _, in := range m.incs {
		if m.storm != nil && in != m.storm {
			continue
		}
		if in.Pending && (in.Status == StatusOpen || in.Status == StatusDecided) {
			in.Pending = false
			out = append(out, in.clone())
		}
	}
	return out
}

// SetStatus updates the status of the incident with the given id.
func (m *Manager) SetStatus(id string, s Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, in := range m.incs {
		if in.ID == id {
			in.Status = s
		}
	}
}

// Tick closes quiet incidents and ends storm mode after StormQuiet.
func (m *Manager) Tick(now time.Time) (closed []*Incident) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.storm != nil && now.Sub(m.storm.LastTrigger) >= m.o.StormQuiet {
		m.storm.Status = StatusClosed
		m.storm = nil
		m.times = nil
	}
	keep := m.incs[:0]
	for _, in := range m.incs {
		if in.Status != StatusClosed && in != m.storm && now.Sub(in.LastTrigger) >= m.o.CloseAfter {
			in.Status = StatusClosed
		}
		if in.Status == StatusClosed {
			closed = append(closed, in.clone())
			continue
		}
		keep = append(keep, in)
	}
	m.incs = keep
	return closed
}

// InStorm reports whether storm mode is active.
func (m *Manager) InStorm() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.storm != nil
}
