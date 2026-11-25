package main

import (
	"context"
	"testing"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// deadlineReader records the deadline of the context its Get receives.
type deadlineReader struct {
	client.Reader
	deadline time.Time
	ok       bool
}

func (r *deadlineReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	r.deadline, r.ok = ctx.Deadline()
	return context.DeadlineExceeded
}

func TestWebhookURL_LookupHasTenSecondTimeout(t *testing.T) {
	t.Setenv("POD_NAMESPACE", "tidewatch-system")
	r := &deadlineReader{}
	start := time.Now()
	if got := webhookURL(r, "slack"); got != "" {
		t.Fatalf("url = %q after a failed read", got)
	}
	if !r.ok || r.deadline.Sub(start) > 10*time.Second+time.Second {
		t.Fatalf("lookup context deadline = %v (set=%v), want about 10s", r.deadline.Sub(start), r.ok)
	}
}
