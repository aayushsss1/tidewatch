package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/internal/engine"
	"github.com/aayushsss1/tidewatch/internal/overrides"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
	"github.com/aayushsss1/tidewatch/pkg/prom"
	"github.com/aayushsss1/tidewatch/pkg/tools"
)

const (
	kindDeployment  = "Deployment"
	kindStatefulSet = "StatefulSet"
	queryTimeout    = 5 * time.Second
	maxEvents       = 20
	maxRollouts     = 5
	maxNotesBytes   = 2048
)

// +kubebuilder:rbac:groups=apps,resources=replicasets;controllerrevisions,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch

// EpisodeFinder returns past episodes similar to the current situation of w,
// searching the policy's namespace and memberNamespaces (those of the
// incident's members, where episodes are recorded). A nil EpisodeFinder means
// there are none.
type EpisodeFinder func(ctx context.Context, w tools.WorkloadRef, memberNamespaces []string) ([]tools.EpisodeSummary, error)

// PromFactory builds a Prometheus querier for a policy's URL.
type PromFactory func(url string, timeout time.Duration) (prom.Querier, error)

// source implements tools.Source for one incident.
type source struct {
	eng      *engine.Engine
	store    *overrides.Store
	rd       client.Reader
	corr     *Correlator
	episodes EpisodeFinder
	factory  PromFactory
	pol      *v1.TidePolicy
	members  map[tools.WorkloadRef]*Member
	now      func() time.Time

	qmu sync.Mutex
	q   prom.Querier
}

var _ tools.Source = (*source)(nil)

func (s *source) member(w tools.WorkloadRef) (*Member, error) {
	m := s.members[w]
	if m == nil {
		return nil, errors.New("workload not in incident")
	}
	return m, nil
}

func (s *source) snap(w tools.WorkloadRef) (engine.Snapshot, error) {
	m, err := s.member(w)
	if err != nil {
		return engine.Snapshot{}, err
	}
	sn, ok := s.eng.Snapshot(m.UID)
	if !ok {
		return engine.Snapshot{}, errors.New("no recent evaluation for workload")
	}
	return sn, nil
}

// template reads the target's pod template, UID and kind.
func (s *source) template(ctx context.Context, w tools.WorkloadRef) (corev1.PodTemplateSpec, types.UID, string, error) {
	var tw v1.TideWorkload
	if err := s.rd.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &tw); err != nil {
		return corev1.PodTemplateSpec{}, "", "", err
	}
	key := types.NamespacedName{Namespace: w.Namespace, Name: tw.Spec.TargetRef.Name}
	switch tw.Spec.TargetRef.Kind {
	case kindDeployment:
		var d appsv1.Deployment
		if err := s.rd.Get(ctx, key, &d); err != nil {
			return corev1.PodTemplateSpec{}, "", "", err
		}
		return d.Spec.Template, d.UID, kindDeployment, nil
	case kindStatefulSet:
		var d appsv1.StatefulSet
		if err := s.rd.Get(ctx, key, &d); err != nil {
			return corev1.PodTemplateSpec{}, "", "", err
		}
		return d.Spec.Template, d.UID, kindStatefulSet, nil
	}
	return corev1.PodTemplateSpec{}, "", "", fmt.Errorf("unsupported target kind %q", tw.Spec.TargetRef.Kind)
}

func (s *source) State(ctx context.Context, w tools.WorkloadRef) (tools.WorkloadState, error) {
	sn, err := s.snap(w)
	if err != nil {
		return tools.WorkloadState{}, err
	}
	st := tools.WorkloadState{
		Replicas: sn.Replicas, Ready: sn.Ready, TimeToReadyP90: sn.TimeToReady.Seconds(),
		Guardrails: map[string]string{
			"minReplicas":      fmt.Sprint(sn.Cfg.MinReplicas),
			"maxReplicas":      fmt.Sprint(sn.Cfg.MaxReplicas),
			"headroom":         fmt.Sprint(sn.Cfg.Headroom),
			"targetPerReplica": fmt.Sprint(sn.Cfg.TargetPerReplica),
		},
	}
	var tw v1.TideWorkload
	if err := s.rd.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &tw); err == nil {
		st.TimeToReadyP50 = float64(tw.Status.TimeToReady.P50Seconds)
	}
	if tpl, _, _, err := s.template(ctx, w); err == nil {
		st.Requests, st.Limits = sumResources(tpl.Spec.Containers)
	}
	ovs, pend, _ := s.store.Snapshot(sn.UID)
	for _, o := range ovs {
		st.ActiveOverrides = append(st.ActiveOverrides, fmt.Sprintf("incident %s min=%d %s..%s",
			o.IncidentID, o.MinReplicas, o.Start.Format(time.RFC3339), o.End.Format(time.RFC3339)))
	}
	for _, p := range pend {
		st.PendingApprovals = append(st.PendingApprovals, fmt.Sprintf("incident %s min=%d expires %s",
			p.IncidentID, p.Override.MinReplicas, p.Expires.Format(time.RFC3339)))
	}
	return st, nil
}

func sumResources(cs []corev1.Container) (req, lim map[string]string) {
	sum := func(pick func(corev1.Container) corev1.ResourceList) map[string]string {
		tot := map[corev1.ResourceName]*resource.Quantity{}
		for _, c := range cs {
			for n, q := range pick(c) {
				if tot[n] == nil {
					tot[n] = &resource.Quantity{}
				}
				tot[n].Add(q)
			}
		}
		if len(tot) == 0 {
			return nil
		}
		out := map[string]string{}
		for n, q := range tot {
			out[string(n)] = q.String()
		}
		return out
	}
	return sum(func(c corev1.Container) corev1.ResourceList { return c.Resources.Requests }),
		sum(func(c corev1.Container) corev1.ResourceList { return c.Resources.Limits })
}

func peakOf(f forecast.Forecast) (forecast.ForecastPoint, bool) {
	var best forecast.ForecastPoint
	found := false
	for _, p := range f.Points {
		if !found || p.Point > best.Point {
			best, found = p, true
		}
	}
	return best, found
}

func (s *source) Forecasts(_ context.Context, w tools.WorkloadRef) (tools.ForecastSet, error) {
	sn, err := s.snap(w)
	if err != nil {
		return tools.ForecastSet{}, err
	}
	var set tools.ForecastSet
	for _, f := range sn.Forecasts {
		mf := tools.ModelForecast{Name: f.Model}
		if p, ok := peakOf(f); ok {
			mf.Peak, mf.PeakAt, mf.PeakLow, mf.PeakHigh = p.Point, p.T, p.Lower, p.Upper
		}
		best := -1
		for _, sc := range sn.Scores { // the lead time with the most samples is the best-measured
			if sc.Model == f.Model && sc.Samples > best {
				best, mf.SMAPE, mf.Coverage = sc.Samples, sc.SMAPE, sc.Coverage
			}
		}
		for _, p := range f.Points {
			mf.Points = append(mf.Points, tools.ForecastPoint{T: p.T.Unix(), Value: p.Point, Lower: p.Lower, Upper: p.Upper})
		}
		set.Models = append(set.Models, mf)
	}
	return set, nil
}

func (s *source) Related(_ context.Context, w tools.WorkloadRef) ([]tools.Related, error) {
	m, err := s.member(w)
	if err != nil {
		return nil, err
	}
	var out []tools.Related
	for ref, o := range s.members {
		if o.UID == m.UID {
			continue
		}
		r := tools.Related{Workload: ref.Name}
		if sn, ok := s.eng.Snapshot(o.UID); ok {
			r.Load = sn.Actual
			for _, f := range sn.Forecasts {
				if f.Model == sn.Trusted {
					if p, ok := peakOf(f); ok {
						r.Peak = p.Point
					}
				}
			}
		}
		if c, lag, ok := s.corr.Related(m.UID, o.UID); ok {
			r.Corr, r.LagSeconds = c, int(lag.Seconds())
		}
		out = append(out, r)
	}
	slices.SortFunc(out, func(a, b tools.Related) int { return strings.Compare(a.Workload, b.Workload) })
	return out, nil
}

func (s *source) Outcomes(_ context.Context, w tools.WorkloadRef) ([]tools.Outcome, error) {
	m, err := s.member(w)
	if err != nil {
		return nil, err
	}
	_, _, recent := s.store.Snapshot(m.UID) // newest first
	out := make([]tools.Outcome, 0, len(recent))
	for _, r := range slices.Backward(recent) { // oldest first, as the tool keeps the tail
		o := tools.Outcome{At: r.Time.Time, Source: string(v1.SourceAgent), Verdict: string(r.Verdict)}
		if r.Outcome != nil {
			switch {
			case r.Outcome.Timely == nil:
				o.Score = 0.5 // not needed: harmless but wasted
			case *r.Outcome.Timely:
				o.Score = 1
			}
		}
		out = append(out, o)
	}
	return out, nil
}

func (s *source) Events(ctx context.Context, w tools.WorkloadRef, since time.Duration) ([]tools.Event, error) {
	if _, err := s.member(w); err != nil {
		return nil, err
	}
	var tw v1.TideWorkload
	if err := s.rd.Get(ctx, types.NamespacedName{Namespace: w.Namespace, Name: w.Name}, &tw); err != nil {
		return nil, err
	}
	target := tw.Spec.TargetRef.Name
	var evs []corev1.Event
	var own corev1.EventList
	if err := s.rd.List(ctx, &own, client.InNamespace(w.Namespace),
		client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("involvedObject.name", target)}); err != nil {
		return nil, err
	}
	evs = append(evs, own.Items...)
	var pods corev1.EventList // pod names are the target name plus a generated suffix
	if err := s.rd.List(ctx, &pods, client.InNamespace(w.Namespace),
		client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("involvedObject.kind", "Pod")}); err != nil {
		return nil, err
	}
	for _, e := range pods.Items {
		if strings.HasPrefix(e.InvolvedObject.Name, target+"-") {
			evs = append(evs, e)
		}
	}
	cut := s.now().Add(-since)
	var out []tools.Event
	for _, e := range evs {
		at := eventTime(e)
		if at.Before(cut) {
			continue
		}
		out = append(out, tools.Event{Reason: e.Reason, Type: e.Type, Count: max(e.Count, 1), LastSeen: at, Message: e.Message})
	}
	slices.SortStableFunc(out, func(a, b tools.Event) int { return a.LastSeen.Compare(b.LastSeen) })
	if len(out) > maxEvents { // keep the newest; chronological so the tool trims the oldest first
		out = out[len(out)-maxEvents:]
	}
	return out, nil
}

func eventTime(e corev1.Event) time.Time {
	switch {
	case !e.LastTimestamp.IsZero():
		return e.LastTimestamp.Time
	case !e.EventTime.IsZero():
		return e.EventTime.Time
	}
	return e.CreationTimestamp.Time
}

func images(tpl corev1.PodTemplateSpec) []string {
	out := make([]string, 0, len(tpl.Spec.Containers))
	for _, c := range tpl.Spec.Containers {
		out = append(out, c.Image)
	}
	return out
}

func (s *source) Rollouts(ctx context.Context, w tools.WorkloadRef) ([]tools.Rollout, error) {
	if _, err := s.member(w); err != nil {
		return nil, err
	}
	_, uid, kind, err := s.template(ctx, w)
	if err != nil {
		return nil, err
	}
	var out []tools.Rollout
	switch kind {
	case kindDeployment:
		var l appsv1.ReplicaSetList
		if err := s.rd.List(ctx, &l, client.InNamespace(w.Namespace)); err != nil {
			return nil, err
		}
		for _, rs := range l.Items {
			if !ownedBy(rs.OwnerReferences, uid) {
				continue
			}
			var want int32 = 1
			if rs.Spec.Replicas != nil {
				want = *rs.Spec.Replicas
			}
			out = append(out, tools.Rollout{
				At: rs.CreationTimestamp.Time, Images: images(rs.Spec.Template),
				Progress: fmt.Sprintf("%d/%d ready", rs.Status.ReadyReplicas, want),
			})
		}
	case kindStatefulSet:
		var l appsv1.ControllerRevisionList
		if err := s.rd.List(ctx, &l, client.InNamespace(w.Namespace)); err != nil {
			return nil, err
		}
		for _, cr := range l.Items {
			if ownedBy(cr.OwnerReferences, uid) {
				out = append(out, tools.Rollout{At: cr.CreationTimestamp.Time, Images: revisionImages(cr.Data.Raw)})
			}
		}
	}
	slices.SortStableFunc(out, func(a, b tools.Rollout) int { return a.At.Compare(b.At) })
	if len(out) > maxRollouts {
		out = out[len(out)-maxRollouts:]
	}
	return out, nil
}

func ownedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	return slices.ContainsFunc(refs, func(r metav1.OwnerReference) bool { return r.UID == uid })
}

// revisionImages extracts container images from a StatefulSet revision patch.
func revisionImages(raw []byte) []string {
	var p struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Image string `json:"image"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if json.Unmarshal(raw, &p) != nil {
		return nil
	}
	var out []string
	for _, c := range p.Spec.Template.Spec.Containers {
		if c.Image != "" {
			out = append(out, c.Image)
		}
	}
	return out
}

func (s *source) OpsNotes(ctx context.Context) (string, error) {
	a := s.pol.Spec.Agent.Context
	var parts []string
	if a.Notes != "" {
		parts = append(parts, a.Notes)
	}
	if a.ConfigMapRef != nil {
		var cm corev1.ConfigMap
		key := types.NamespacedName{Namespace: s.pol.Namespace, Name: a.ConfigMapRef.Name}
		if err := s.rd.Get(ctx, key, &cm); err == nil {
			for _, k := range slices.Sorted(maps.Keys(cm.Data)) {
				parts = append(parts, cm.Data[k])
			}
		}
	}
	return cutBytes(strings.Join(parts, "\n"), maxNotesBytes), nil
}

// cutBytes shortens s to at most n bytes without splitting a rune.
func cutBytes(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return s[:n]
}

func (s *source) SimilarEpisodes(ctx context.Context, w tools.WorkloadRef) ([]tools.EpisodeSummary, error) {
	if s.episodes == nil {
		return nil, nil
	}
	nss := make([]string, 0, len(s.members))
	for ref := range s.members {
		nss = append(nss, ref.Namespace)
	}
	return s.episodes(ctx, w, nss)
}

func (s *source) querier() (prom.Querier, error) {
	s.qmu.Lock()
	defer s.qmu.Unlock()
	if s.q == nil {
		q, err := s.factory(s.pol.Spec.Prometheus.URL, queryTimeout)
		if err != nil {
			return nil, err
		}
		s.q = q
	}
	return s.q, nil
}

func (s *source) QueryRange(ctx context.Context, promql string, rng, step time.Duration) (tools.Series, error) {
	q, err := s.querier()
	if err != nil {
		return tools.Series{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()
	end := s.now().Truncate(step)
	ser, err := q.RangeSeries(ctx, promql, end.Add(-rng), end, step)
	if err != nil {
		if errors.Is(err, prom.ErrMultipleSeries) {
			return tools.Series{}, errors.New("query returned more than one series; aggregate it (e.g. sum)")
		}
		return tools.Series{}, err
	}
	out := tools.Series{}
	for _, p := range ser.Points {
		if !math.IsNaN(p.V) {
			out.Points = append(out.Points, tools.Point{T: p.T.Unix(), V: p.V})
		}
	}
	return out, nil
}
