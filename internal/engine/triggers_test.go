package engine

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
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
