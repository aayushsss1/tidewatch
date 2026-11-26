package engine

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/signal"
)

const (
	podWindow     = 5 * time.Minute
	notesTTL      = 60 * time.Second
	notesTimeout  = 2 * time.Second
	maxNotesBytes = 64 << 10
)

// TriggerSink receives the triggers detected in phase 1. Submit is called from
// the engine's worker goroutines, so implementations must be safe for concurrent use.
type TriggerSink interface{ Submit(signal.Trigger) }

type restartSample struct {
	at    time.Time
	total int
}

// detectTriggers is the phase-1 trigger hook. It does nothing when no sink is
// configured, and never fires on unhealthy data.
func (e *Engine) detectTriggers(ctx context.Context, it *item, healthy bool) {
	if e.opts.Triggers == nil {
		return
	}
	st := it.st
	outside := e.trackOutside(it, healthy)
	if !healthy || !it.pol.Spec.Agent.Enabled { // a disabled policy never feeds the agent
		return
	}
	wall := e.clock.Now()
	restarts, oom := e.podSignals(ctx, it, wall)
	in := signal.Input{
		Now: it.now, Step: it.step, Horizon: it.horizon,
		TimeToReady: st.ttr.P90(e.opts.DefaultTimeToReady),
		Actual:      it.actual,
		Capacity:    float64(it.scale.Replicas) * it.cfg.TargetPerReplica * it.cfg.Headroom,
		Trusted:     it.trusted, Forecasts: it.forecasts, OutsideSteps: outside,
		AnomalyForecast: st.anomalyPoint,
		TemplateChanged: e.templateChanged(it), OOMKilled: oom,
		ExternalReplicaChange: st.externalChange, Restarts5m: restarts,
		Notes: e.notes(ctx, it), Healthy: true,
	}
	wl := client.ObjectKeyFromObject(it.tw)
	// An active override means the agent already acted: the baseline plus the
	// override cover the workload, so only a workload change may wake it again.
	if e.ov != nil {
		_, o := e.ov.Active(it.tw.UID, it.now)
		in.Covered = o != nil
	}
	for _, t := range signal.Detect(in, it.tw.UID, wl, it.policyRef(), e.debounce) {
		e.opts.Triggers.Submit(t)
	}
}

// trackOutside updates and returns the number of consecutive steps the actual
// load fell outside the trusted model's interval. The interval for the current
// aligned time comes from the previous tick's forecast of the same model.
func (e *Engine) trackOutside(it *item, healthy bool) int {
	st := it.st
	prev := st.prevForecasts
	st.prevForecasts = it.forecasts
	if !healthy || it.trusted == nil || st.outsideAt.Equal(it.now) {
		return st.outsideSteps
	}
	st.outsideAt = it.now
	st.anomalyPoint = 0
	if math.IsNaN(it.actual) {
		st.outsideSteps = 0
		return 0
	}
	for _, f := range prev {
		if f.Model != it.trusted.Model {
			continue
		}
		for _, p := range f.Points {
			if p.T.Equal(it.now) {
				st.anomalyPoint = p.Point
				if it.actual < p.Lower || it.actual > p.Upper {
					st.outsideSteps++
				} else {
					st.outsideSteps = 0
				}
				return st.outsideSteps
			}
		}
	}
	st.outsideSteps = 0 // no interval to compare against
	return 0
}

// templateChanged compares the pod template hash with the last one seen. The
// first observation never counts. (metadata.generation would also bump on every
// replica change, so the template itself is hashed; generation is the fallback
// for kinds without spec.template.)
func (e *Engine) templateChanged(it *item) bool {
	st := it.st
	var sig uint64
	obj := it.target.Object
	if tpl, found, _ := unstructured.NestedMap(obj.Object, "spec", "template"); found {
		b, _ := json.Marshal(tpl) // map keys marshal sorted: stable
		h := fnv.New64a()
		_, _ = h.Write(b)
		sig = h.Sum64()
	} else {
		sig = uint64(obj.GetGeneration())
	}
	changed := st.tplSeen && st.tplSig != sig
	st.tplSig, st.tplSeen = sig, true
	return changed
}

// podSignals returns restarts over the last 5 minutes and whether a container
// was OOMKilled in that window. List errors are treated as zero.
func (e *Engine) podSignals(ctx context.Context, it *item, wall time.Time) (int, bool) {
	sel := it.scale.Selector
	if sel == "" || !it.pol.Spec.Agent.Enabled {
		return 0, false
	}
	log := logf.Log.WithName("engine").WithValues("workload", client.ObjectKeyFromObject(it.tw))
	ls, err := labels.Parse(sel)
	if err != nil {
		log.V(1).Info("Parsing scale selector", "error", err.Error())
		return 0, false
	}
	var pods corev1.PodList
	if err := e.c.List(ctx, &pods, client.InNamespace(it.tw.Namespace), client.MatchingLabelsSelector{Selector: ls}); err != nil {
		log.V(1).Info("Listing pods", "error", err.Error())
		return 0, false
	}
	total, oom := 0, false
	cutoff := wall.Add(-podWindow)
	for i := range pods.Items {
		for _, cs := range pods.Items[i].Status.ContainerStatuses {
			total += int(cs.RestartCount)
			if t := cs.LastTerminationState.Terminated; t != nil && t.Reason == "OOMKilled" && t.FinishedAt.After(cutoff) {
				oom = true
			}
		}
	}
	st := it.st
	st.restarts = slices.DeleteFunc(st.restarts, func(s restartSample) bool { return s.at.Before(cutoff) })
	delta := 0
	if len(st.restarts) > 0 {
		lowest := st.restarts[0].total
		for _, s := range st.restarts {
			lowest = min(lowest, s.total)
		}
		delta = max(total-lowest, 0)
	}
	st.restarts = append(st.restarts, restartSample{at: wall, total: total})
	return delta, oom
}

// notes returns the policy's ops notes plus the referenced ConfigMap's values.
// It is empty unless the agent is enabled. The ConfigMap is read through the
// uncached APIReader (nil skips it), at most once per policy per minute.
func (e *Engine) notes(ctx context.Context, it *item) string {
	a := it.pol.Spec.Agent
	if !a.Enabled {
		return ""
	}
	parts := []string{}
	if a.Context.Notes != "" {
		parts = append(parts, a.Context.Notes)
	}
	if ref := a.Context.ConfigMapRef; ref != nil && e.opts.APIReader != nil {
		if txt := e.configMapNotes(ctx, it.policyRef(), ref.Name); txt != "" {
			parts = append(parts, txt)
		}
	}
	s := strings.Join(parts, "\n")
	if len(s) > maxNotesBytes {
		n := maxNotesBytes
		for n > 0 && !utf8.RuneStart(s[n]) { // never split a UTF-8 sequence
			n--
		}
		s = s[:n]
	}
	return s
}

// notesEntry is one policy's cached notes. Its own mutex single-flights the
// read, so a slow API server blocks only workloads of that policy.
type notesEntry struct {
	mu   sync.Mutex
	at   time.Time
	name string
	text string
	ok   bool
}

func (e *Engine) notesEntryFor(pol v1.PolicyRef) *notesEntry {
	e.notesMu.Lock() // guards the map only; never held during a read
	defer e.notesMu.Unlock()
	en := e.notesCache[pol]
	if en == nil {
		en = &notesEntry{}
		e.notesCache[pol] = en
	}
	return en
}

func (e *Engine) configMapNotes(ctx context.Context, pol v1.PolicyRef, name string) string {
	en := e.notesEntryFor(pol)
	en.mu.Lock()
	defer en.mu.Unlock()
	now := e.clock.Now()
	if en.ok && en.name == name && now.Sub(en.at) < notesTTL {
		return en.text
	}
	rctx, cancel := context.WithTimeout(ctx, notesTimeout)
	defer cancel()
	var cm corev1.ConfigMap
	key := types.NamespacedName{Namespace: pol.Namespace, Name: name}
	text := ""
	if err := e.opts.APIReader.Get(rctx, key, &cm); err != nil {
		logf.Log.WithName("engine").V(1).Info("Getting notes ConfigMap", "configMap", key, "error", err.Error())
	} else {
		vals := make([]string, 0, len(cm.Data))
		for _, k := range slices.Sorted(maps.Keys(cm.Data)) {
			vals = append(vals, cm.Data[k])
		}
		text = strings.Join(vals, "\n")
	}
	en.at, en.name, en.text, en.ok = now, name, text, true
	return text
}
