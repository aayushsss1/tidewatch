package engine

import (
	"context"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/internal/workload"
)

// slowReader sleeps before serving a ConfigMap and counts reads.
type slowReader struct {
	client.Reader
	delay time.Duration
	n     atomic.Int32
}

func (r *slowReader) Get(_ context.Context, _ client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	r.n.Add(1)
	time.Sleep(r.delay)
	obj.(*corev1.ConfigMap).Data = map[string]string{"k": "note"}
	return nil
}

func TestConfigMapNotes_PoliciesDoNotBlockEachOther(t *testing.T) {
	r := &slowReader{delay: 300 * time.Millisecond}
	e := New(nil, Options{APIReader: r})
	pols := []v1.PolicyRef{{Namespace: "a", Name: "p1"}, {Namespace: "a", Name: "p2"}}

	start := time.Now()
	var wg sync.WaitGroup
	for _, p := range pols {
		wg.Go(func() { _ = e.configMapNotes(context.Background(), p, "cm") })
	}
	wg.Wait()
	if d := time.Since(start); d >= 500*time.Millisecond {
		t.Fatalf("reads were serialised: %v", d)
	}
	if r.n.Load() != 2 {
		t.Fatalf("reads = %d", r.n.Load())
	}

	// Concurrent callers of one policy share a single read.
	r.n.Store(0)
	e2 := New(nil, Options{APIReader: r})
	for range 4 {
		wg.Go(func() { _ = e2.configMapNotes(context.Background(), pols[0], "cm") })
	}
	wg.Wait()
	if r.n.Load() != 1 {
		t.Fatalf("same-policy reads = %d", r.n.Load())
	}
}

func TestNotes_TruncationIsRuneSafe(t *testing.T) {
	e := New(nil, Options{})
	it := &item{pol: &v1.TidePolicy{}}
	it.pol.Spec.Agent.Enabled = true
	// 3-byte runes straddle the 64 KiB boundary.
	it.pol.Spec.Agent.Context.Notes = strings.Repeat("あ", maxNotesBytes)
	got := e.notes(context.Background(), it)
	if len(got) > maxNotesBytes || !strings.HasSuffix(got, "あ") || strings.ContainsRune(got, '�') {
		t.Fatalf("bad truncation: len=%d", len(got))
	}
}

func TestDetectTriggers_ActiveOverrideSuppressesAllButWorkloadChange(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 45, 0, 0, time.UTC)
	store := overrides.NewStore()
	s := &sink{}
	e := New(nil, Options{Triggers: s, Overrides: store, Clock: clocktesting.NewFakeClock(now)})
	tw := &v1.TideWorkload{}
	tw.UID, tw.Namespace, tw.Name = "uid-1", "ns", "wl"
	pol := &v1.TidePolicy{}
	pol.Spec.Agent.Enabled = true
	newItem := func() *item {
		st := &wlState{outsideSteps: 5, outsideAt: now, externalChange: true}
		return &item{
			tw: tw, pol: pol, st: st, now: now, step: time.Minute, horizon: 30 * time.Minute, actual: 10,
			target: workload.Target{Object: &unstructured.Unstructured{Object: map[string]any{}}},
			scale:  workload.ScaleState{Replicas: 2},
			cfg:    workload.EffectiveConfig{TargetPerReplica: 10, Headroom: 0.8},
		}
	}

	e.detectTriggers(context.Background(), newItem(), true)
	if got := s.types(); !slices.Contains(got, signal.Anomaly) || !slices.Contains(got, signal.WorkloadChange) {
		t.Fatalf("without an override both must fire, got %v", got)
	}

	s.mu.Lock()
	s.got = nil
	s.mu.Unlock()
	e.debounce.Forget(tw.UID)
	if err := store.AddOverride(tw.UID, v1.Override{
		ID: "o1", IncidentID: "inc", MinReplicas: 5,
		Start: metav1.NewTime(now.Add(-time.Minute)), End: metav1.NewTime(now.Add(20 * time.Minute)),
	}); err != nil {
		t.Fatal(err)
	}
	e.detectTriggers(context.Background(), newItem(), true)
	if got := s.types(); !slices.Equal(got, []signal.Type{signal.WorkloadChange}) {
		t.Fatalf("with an active override only WorkloadChange may fire, got %v", got)
	}
}
