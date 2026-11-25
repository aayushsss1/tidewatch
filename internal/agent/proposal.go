package agent

import (
	"fmt"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

// ValidateProposal applies the guardrails of spec §7.6 to a proposal: it drops
// workloads outside the incident, clamps minReplicas to the workload's maximum,
// defaults and clamps the window, and drops windows already over. A proposal
// below minConf yields no overrides and the reason low_confidence.
func ValidateProposal(
	p tools.Proposal, inc *Incident, maxFor func(uid types.UID) int32, minConf float64, now time.Time,
) (map[types.UID]v1.Override, []string) {
	var reasons []string
	note := func(r string) {
		if !slices.Contains(reasons, r) {
			reasons = append(reasons, r)
		}
	}
	if p.Confidence < minConf {
		return nil, []string{reasonLowConf}
	}
	out := map[types.UID]v1.Override{}
	for _, po := range p.Overrides {
		uid, ok := memberUID(inc, po.Workload)
		if !ok {
			note(reasonOutOfScope)
			continue
		}
		floor := po.MinReplicas
		if mx := maxFor(uid); mx > 0 && floor > mx {
			floor = mx
			note(reasonClampedMax)
		}
		start := po.Start
		if start.Before(now) {
			start = now
		}
		if latest := now.Add(maxStartAhead); start.After(latest) {
			start = latest
			note(reasonWindowClamp)
		}
		end := po.End
		switch {
		case end.IsZero():
			end = start.Add(defaultOverrideWindow)
			note(reasonWindowClamp)
		case !end.After(now):
			note(reasonEnded)
			continue
		case !end.After(start):
			note(reasonInvalidWindow)
			continue
		case end.Sub(start) > maxOverrideWindow:
			end = start.Add(maxOverrideWindow)
			note(reasonWindowClamp)
		}
		out[uid] = v1.Override{
			IncidentID: inc.ID, MinReplicas: floor, Start: metav1.NewTime(start), End: metav1.NewTime(end),
		}
	}
	return out, reasons
}

// Brief is the investigation's opening message: the triggers and, per member,
// replicas, load and forecast peak, plus measured lags. At most 1500 bytes.
func Brief(inc *Incident, snaps []engine.Snapshot) string {
	var b strings.Builder
	fmt.Fprintf(&b, "Incident %s (policy %s/%s). Investigate and conclude with propose_scaling or no_action.\n",
		inc.ID, inc.Policy.Namespace, inc.Policy.Name)
	names := map[types.UID]string{}
	for _, sn := range snaps {
		names[sn.UID] = sn.TW.Name
		fmt.Fprintf(&b, "- %s: replicas %d (ready %d, max %d), load %.4g", sn.TW.Name, sn.Replicas, sn.Ready,
			sn.Cfg.MaxReplicas, sn.Actual)
		for _, f := range sn.Forecasts {
			if f.Model != sn.Trusted {
				continue
			}
			if p, ok := peakOf(f); ok {
				fmt.Fprintf(&b, ", forecast peak %.4g at %s", p.Point, p.T.UTC().Format("15:04"))
			}
		}
		if m := inc.Members[sn.UID]; m != nil {
			ts := make([]string, 0, len(m.Triggers))
			for _, t := range m.Triggers {
				d := cutBytes(t.Detail, briefDetailMax)
				ts = append(ts, fmt.Sprintf("%s(%s)", t.Type, d))
			}
			if len(ts) > 0 {
				fmt.Fprintf(&b, "; triggers: %s", strings.Join(ts, ", "))
			}
		}
		b.WriteByte('\n')
	}
	lags := make([]string, 0, len(inc.Lags))
	for _, uid := range sortedUIDs(inc.Lags) {
		lags = append(lags, fmt.Sprintf("%s lags lead by %s", nameOr(names, uid), inc.Lags[uid].Round(time.Second)))
	}
	if len(lags) > 0 {
		fmt.Fprintf(&b, "Measured lags: %s\n", strings.Join(lags, "; "))
	}
	return cutBytes(b.String(), briefMax)
}

func nameOr(names map[types.UID]string, uid types.UID) string {
	if n := names[uid]; n != "" {
		return n
	}
	return string(uid)
}
