package episodes

import (
	"context"
	"fmt"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

// Match is a retrieved episode with its similarity score.
type Match struct {
	Name, Namespace string
	Score           float64
	Summary         string // <= 400 characters
}

// Reader finds similar episodes. R must be uncached in production
// (mgr.GetAPIReader()) so the operator does not start a TideEpisode informer.
type Reader struct{ R client.Reader }

// Similar returns the top k episodes of policy ("<ns>.<name>") in the given
// namespaces whose score is at least min.
func (r *Reader) Similar(ctx context.Context, policy string, namespaces []string, q Query, k int, min float64) ([]Match, error) {
	var out []Match
	for _, ns := range namespaces {
		var list v1.TideEpisodeList
		if err := r.R.List(ctx, &list, client.InNamespace(ns), client.MatchingLabels{v1.LabelPolicy: policy}); err != nil {
			return nil, fmt.Errorf("list episodes in %s: %w", ns, err)
		}
		for i := range list.Items {
			e := &list.Items[i]
			if s := Similarity(q, e.Spec); s >= min {
				out = append(out, Match{Name: e.Name, Namespace: e.Namespace, Score: s, Summary: Summary(e.Name, s, e.Spec)})
			}
		}
	}
	sortMatches(out)
	if k >= 0 && len(out) > k {
		out = out[:k]
	}
	return out, nil
}

// Finder adapts a Reader to the agent's EpisodeFinder type. policyOf resolves
// the policy, its namespaces and the current query for a workload; ok=false
// means no episodes are available for it. Episodes live in their lead
// workload's namespace, so the search covers the namespaces policyOf returns
// plus memberNamespaces, the namespaces of the incident's members.
func Finder(
	r *Reader, policyOf func(tools.WorkloadRef) (policy string, namespaces []string, q Query, ok bool),
) func(ctx context.Context, w tools.WorkloadRef, memberNamespaces []string) ([]tools.EpisodeSummary, error) {
	return func(ctx context.Context, w tools.WorkloadRef, memberNamespaces []string) ([]tools.EpisodeSummary, error) {
		policy, nss, q, ok := policyOf(w)
		if !ok {
			return nil, nil
		}
		nss = slices.Compact(slices.Sorted(slices.Values(append(slices.Clone(nss), memberNamespaces...))))
		ms, err := r.Similar(ctx, policy, nss, q, 3, 0.5)
		if err != nil {
			return nil, err
		}
		out := make([]tools.EpisodeSummary, len(ms))
		for i, m := range ms {
			out[i] = tools.EpisodeSummary{Name: m.Name, Score: m.Score, Summary: m.Summary}
		}
		return out, nil
	}
}
