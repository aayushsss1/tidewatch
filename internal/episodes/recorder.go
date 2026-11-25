package episodes

import (
	"context"
	"crypto/sha1" //nolint:gosec // not security relevant, short stable name suffix
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

// LabelLead is set on episodes; its value is the lead workload name.
const LabelLead = "tidewatch.dev/lead"

// DefaultMaxPerLead caps recorded episodes per lead workload.
const DefaultMaxPerLead = 50

// IncidentView is the part of a closed incident the recorder needs. The agent
// side converts its incident into this view (episodes must not import agent).
type IncidentView struct {
	ID               string
	Policy           v1.PolicyRef
	Lead             string
	LeadNamespace    string // where the episode is written; defaults to the policy's namespace
	Members          []v1.EpisodeMember
	Start, End, Peak time.Time
	LeadSeries       forecast.Series
	PeakRatio        float64
	OpsNote          string
	Deploy           bool
	ProposalApplied  bool
	LongestAnomaly   time.Duration
	MaxReplicaChange float64 // largest relative replica change, 0.5 == 50%
	Action           string
	Outcome          *v1.Outcome
	Timezone         string
}

// +kubebuilder:rbac:groups=tidewatch.dev,resources=tideepisodes,verbs=get;list;watch;create;delete

// Recorder writes episodes and enforces the per-lead cap.
type Recorder struct {
	C          client.Client
	R          client.Reader // uncached, used for pruning
	Clock      clock.Clock
	MaxPerLead int
}

// Notable reports whether an incident deserves to be remembered.
func (r *Recorder) Notable(inc *IncidentView) bool {
	return inc.ProposalApplied || inc.LongestAnomaly >= 5*time.Minute || inc.MaxReplicaChange > 0.5
}

// Build converts an incident into a TideEpisode ready for Record.
func (r *Recorder) Build(inc *IncidentView) *v1.TideEpisode {
	end := inc.End
	if end.IsZero() && r.Clock != nil {
		end = r.Clock.Now()
	}
	tz := inc.Timezone
	if tz == "" {
		tz = utc
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc, tz = time.UTC, utc
	}
	local := inc.Start.In(loc)
	note := inc.OpsNote
	if rs := []rune(note); len(rs) > 200 {
		note = string(rs[:200])
	}
	cov := int32(0)
	if n := len(inc.LeadSeries.Points); n > 1 {
		cov = int32(inc.LeadSeries.Points[n-1].T.Sub(inc.LeadSeries.Points[0].T).Hours())
	}
	ns := inc.LeadNamespace
	if ns == "" {
		ns = inc.Policy.Namespace
	}
	return &v1.TideEpisode{
		ObjectMeta: metav1.ObjectMeta{
			Name:      episodeName(inc),
			Namespace: ns,
			Labels: map[string]string{
				v1.LabelPolicy: inc.Policy.Namespace + "." + inc.Policy.Name,
				LabelLead:      leadLabel(inc.Lead),
			},
		},
		Spec: v1.TideEpisodeSpec{
			PolicyRef: inc.Policy,
			Lead:      inc.Lead,
			Members:   inc.Members,
			Start:     metav1.NewTime(inc.Start),
			PeakAt:    metav1.NewTime(inc.Peak),
			End:       metav1.NewTime(end),
			Shape:     EncodeShape(Shape(inc.LeadSeries, inc.Start, end)),
			PeakRatio: strconv.FormatFloat(inc.PeakRatio, 'f', 2, 64),
			Calendar:  v1.EpisodeCalendar{Weekday: local.Weekday().String()[:3], Hour: int32(local.Hour()), Timezone: tz},
			Context:   v1.EpisodeContext{OpsNote: note, Deploy: inc.Deploy},
			Action:    inc.Action,
			Outcome:   inc.Outcome,
			Source:    "recorded",

			HistoryCoverageHours: cov,
		},
	}
}

func episodeName(inc *IncidentView) string {
	sum := sha1.Sum([]byte(inc.ID)) //nolint:gosec
	suffix := hex.EncodeToString(sum[:])[:6]
	stamp := inc.Start.UTC().Format("200601021504")
	lead := sanitize(inc.Lead)
	if lead == "" {
		lead = "episode"
	}
	tail := "-" + stamp + "-" + suffix
	return strings.TrimRight(truncate(lead, 63-len(tail)), "-.") + tail
}

// leadLabel makes a workload name a valid label value: at most 63 characters,
// starting and ending alphanumeric.
func leadLabel(s string) string {
	isAlnum := func(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }
	return strings.TrimFunc(truncate(s, 63), func(r rune) bool { return !isAlnum(r) })
}

func sanitize(s string) string {
	var b strings.Builder
	for _, c := range strings.ToLower(s) {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' {
			b.WriteRune(c)
		} else {
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// Record creates ep, then prunes the oldest recorded episodes of the same
// lead beyond MaxPerLead. Manual episodes are never deleted. Recording the
// same episode twice is a no-op.
func (r *Recorder) Record(ctx context.Context, ep *v1.TideEpisode) error {
	if err := r.C.Create(ctx, ep); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create episode: %w", err)
	}
	max := r.MaxPerLead
	if max <= 0 {
		max = DefaultMaxPerLead
	}
	var list v1.TideEpisodeList
	if err := r.R.List(ctx, &list, client.InNamespace(ep.Namespace), client.MatchingLabels{
		v1.LabelPolicy: ep.Labels[v1.LabelPolicy],
		LabelLead:      ep.Labels[LabelLead],
	}); err != nil {
		return fmt.Errorf("list episodes: %w", err)
	}
	var rec []v1.TideEpisode
	for _, e := range list.Items {
		if e.Spec.Source == "recorded" && e.Spec.Lead == ep.Spec.Lead {
			rec = append(rec, e)
		}
	}
	if len(rec) <= max {
		return nil
	}
	slices.SortFunc(rec, func(a, b v1.TideEpisode) int {
		if !a.Spec.Start.Equal(&b.Spec.Start) {
			if a.Spec.Start.Before(&b.Spec.Start) {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	for i := range rec[:len(rec)-max] {
		if err := r.C.Delete(ctx, &rec[i]); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("prune episode %s: %w", rec[i].Name, err)
		}
	}
	return nil
}
