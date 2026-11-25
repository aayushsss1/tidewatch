package agent

import (
	"context"

	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
)

// Notifier tells humans and the cluster what the agent did. Implementations
// must not block for long and must never fail the caller.
type Notifier interface {
	Pending(ctx context.Context, inc *Incident, pend map[types.UID]v1.PendingApproval)
	Applied(ctx context.Context, inc *Incident, ovs map[types.UID]v1.Override, auto bool)
	Resolved(ctx context.Context, incidentID string, verdict v1.Verdict, by string)
	Skipped(ctx context.Context, inc *Incident, reason string)
	Storm(ctx context.Context, triggers int)
}

// NoopNotifier discards every notification.
type NoopNotifier struct{}

func (NoopNotifier) Pending(context.Context, *Incident, map[types.UID]v1.PendingApproval) {}
func (NoopNotifier) Applied(context.Context, *Incident, map[types.UID]v1.Override, bool)  {}
func (NoopNotifier) Resolved(context.Context, string, v1.Verdict, string)                 {}
func (NoopNotifier) Skipped(context.Context, *Incident, string)                           {}
func (NoopNotifier) Storm(context.Context, int)                                           {}
