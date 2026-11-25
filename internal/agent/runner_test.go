package agent

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/signal"
)

func (f *fixture) runner(provider bool) (*Runner, *record.FakeRecorder) {
	rec := record.NewFakeRecorder(200)
	o := RunnerOptions{Workers: 1, Clock: f.clk}
	if provider {
		o.Provider = f.prov
	}
	return NewRunner(f.c, f.c, f.eng, f.store, rec, o), rec
}

func (f *fixture) trigger(name string, typ signal.Type, at time.Time) signal.Trigger {
	ttb := 10 * time.Minute
	return signal.Trigger{
		Type: typ, Workload: types.NamespacedName{Namespace: testNS, Name: name}, UID: f.uids[name],
		Policy: v1.PolicyRef{Namespace: testNS, Name: f.pol}, At: at, TimeToBreach: &ttb, Shortfall: 0.5, Detail: "d",
	}
}

// work runs the next queued incident like a worker would.
func (r *Runner) work(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	inc, dl, ok := r.queue.Pop(ctx)
	if !ok {
		t.Fatal("nothing queued")
	}
	r.process(ctx, inc, dl)
}

func (f *fixture) episodes() []v1.TideEpisode {
	var l v1.TideEpisodeList
	if err := f.c.List(f.ctx, &l, client.InNamespace(testNS), client.MatchingLabels{v1.LabelPolicy: testNS + "." + f.pol}); err != nil {
		f.t.Fatal(err)
	}
	return l.Items
}

func (f *fixture) cleanEpisodes() {
	f.t.Cleanup(func() {
		for _, e := range f.episodes() {
			_ = f.c.Delete(f.ctx, &e)
		}
	})
}

func countEvents(rec *record.FakeRecorder, reason string) int {
	n := 0
	for {
		select {
		case e := <-rec.Events:
			if strings.Contains(e, reason) {
				n++
			}
		default:
			return n
		}
	}
}

func TestRunner_EndToEnd(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-e2e")
	f.cleanEpisodes()
	r, _ := f.runner(true)
	f.propose(0.9, ovIn{Workload: "frontend-e2e", MinReplicas: 12})

	r.Submit(f.trigger("frontend-e2e", signal.ForecastBreach, f.clk.Now()))
	r.RunOnce(f.ctx)
	if r.queue.Len() != 1 {
		t.Fatalf("queue = %d, want 1", r.queue.Len())
	}
	r.work(t)

	if len(f.prov.Calls) == 0 {
		t.Fatal("provider not called")
	}
	if _, o := f.store.Active(f.uids["frontend-e2e"], f.clk.Now()); o == nil || o.MinReplicas != 12 {
		t.Fatalf("override not stored: %+v", o)
	}
	for range 3 { // the engine's scale-up rate limit may take more than one step
		f.clk.Step(time.Minute)
		f.eng.Step(f.ctx)
	}
	if got := f.replicas("frontend-e2e"); got != 12 {
		t.Fatalf("replicas = %d, want 12 (override applied by the engine)", got)
	}

	f.clk.Step(31 * time.Minute) // quiet for CloseAfter
	r.RunOnce(f.ctx)
	eps := f.episodes()
	if len(eps) != 1 {
		t.Fatalf("episodes = %d, want 1", len(eps))
	}
	if sp := eps[0].Spec; sp.Lead != "frontend-e2e" || sp.Source != "recorded" || !strings.Contains(sp.Action, "12") {
		t.Errorf("episode = %+v", sp)
	}
}

func TestRunner_DisabledAgentRecordsIncidentWithoutProvider(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-off")
	f.cleanEpisodes()
	var pol v1.TidePolicy
	if err := f.c.Get(f.ctx, types.NamespacedName{Namespace: testNS, Name: f.pol}, &pol); err != nil {
		t.Fatal(err)
	}
	pol.Spec.Agent.Enabled = false
	if err := f.c.Update(f.ctx, &pol); err != nil {
		t.Fatal(err)
	}
	r, rec := f.runner(true)

	now := f.clk.Now()
	r.Submit(f.trigger("frontend-off", signal.Anomaly, now))
	r.Submit(f.trigger("frontend-off", signal.Anomaly, now.Add(10*time.Minute)))
	r.RunOnce(f.ctx)
	r.work(t)

	if len(f.prov.Calls) != 0 {
		t.Fatalf("provider called %d times for a disabled policy", len(f.prov.Calls))
	}
	if n := len(rec.Events); n != 0 {
		t.Errorf("a silent skip emitted %d events", n)
	}
	r.mgr.mu.Lock()
	incs, status := len(r.mgr.incs), r.mgr.incs[0].Status
	r.mgr.mu.Unlock()
	if incs != 1 || status != StatusDecided {
		t.Errorf("incidents = %d status = %q, want 1 decided", incs, status)
	}
	f.clk.Step(31 * time.Minute)
	r.RunOnce(f.ctx)
	if n := len(f.episodes()); n != 1 {
		t.Errorf("episodes = %d, want 1 (a 13m anomaly is notable)", n)
	}
}

func TestRunner_NoProviderStillRunsIncidents(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-nollm")
	r, _ := f.runner(false)
	r.Submit(f.trigger("frontend-nollm", signal.ForecastBreach, f.clk.Now()))
	r.RunOnce(f.ctx)
	r.work(t)
	if len(f.prov.Calls) != 0 || r.mgr.InStorm() {
		t.Fatal("unexpected provider call")
	}
}

func TestRunner_StormInvestigatesOnceAndNotifiesOnce(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto", "frontend-storm")
	r, rec := f.runner(true)
	f.script("no_action", map[string]any{kReason: "unclear", kConf: 0.5}, 10)

	for range 30 {
		r.Submit(f.trigger("frontend-storm", signal.ForecastBreach, f.clk.Now()))
	}
	r.RunOnce(f.ctx)
	if !r.mgr.InStorm() {
		t.Fatal("storm mode not entered")
	}
	if r.queue.Len() != 1 {
		t.Fatalf("queue = %d, want exactly the storm incident", r.queue.Len())
	}
	r.work(t)
	r.Submit(f.trigger("frontend-storm", signal.ForecastBreach, f.clk.Now()))
	r.RunOnce(f.ctx)

	if r.queue.Len() != 0 || len(f.prov.Calls) != 1 {
		t.Errorf("queue = %d, provider calls = %d, want 0 and 1", r.queue.Len(), len(f.prov.Calls))
	}
	if n := countEvents(rec, "StormDetected"); n != 1 {
		t.Errorf("StormDetected events = %d, want 1", n)
	}
}

func TestRunner_SubmitNeverBlocksWhenFull(t *testing.T) {
	f := newFixture(t, v1.ModeActive, "Auto")
	r, _ := f.runner(true)
	done := make(chan struct{})
	go func() {
		for range triggerBuffer + 50 {
			r.Submit(signal.Trigger{Type: signal.Anomaly})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Submit blocked")
	}
	if len(r.in) != triggerBuffer {
		t.Errorf("buffered = %d, want %d", len(r.in), triggerBuffer)
	}
}
