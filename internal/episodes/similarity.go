// Package episodes is the agent's memory of past incidents: recording closed
// incidents as TideEpisodes, and retrieving the ones most similar to now.
package episodes

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	v1 "github.com/aayushsss1/tidewatch/api/v1alpha1"
	"github.com/aayushsss1/tidewatch/pkg/forecast"
)

const (
	shapePoints = 32
	minValid    = 8

	wShape, wCalendar, wContext = 0.6, 0.25, 0.15

	// ShapeOffset is added to every z-normalized value when a shape is stored,
	// because the CRD only allows non-negative decimals. |z| <= sqrt(31) < 6.
	ShapeOffset = 6.0

	maxSummary = 400
	utc        = "UTC"
)

// Query describes the current situation to match against stored episodes.
type Query struct {
	Shape   []float64
	Weekday time.Weekday
	Hour    int
	OpsNote string
	Deploy  bool
}

// Shape resamples the valid points of s within [from, to] to 32 points by
// linear interpolation over time and z-normalizes them. It returns nil when
// fewer than 8 valid points fall in the window.
func Shape(s forecast.Series, from, to time.Time) []float64 {
	var pts []forecast.Point
	for _, p := range s.Points {
		if !math.IsNaN(p.V) && !p.T.Before(from) && !p.T.After(to) {
			pts = append(pts, p)
		}
	}
	if len(pts) < minValid || !to.After(from) {
		return nil
	}
	out := make([]float64, shapePoints)
	span := to.Sub(from)
	j := 0
	for i := range out {
		t := from.Add(span * time.Duration(i) / (shapePoints - 1))
		for j < len(pts)-1 && pts[j+1].T.Before(t) {
			j++
		}
		switch {
		case !t.After(pts[0].T):
			out[i] = pts[0].V
		case !t.Before(pts[len(pts)-1].T):
			out[i] = pts[len(pts)-1].V
		default:
			a, b := pts[j], pts[j+1]
			f := float64(t.Sub(a.T)) / float64(b.T.Sub(a.T))
			out[i] = a.V + f*(b.V-a.V)
		}
	}
	var mean float64
	for _, v := range out {
		mean += v
	}
	mean /= shapePoints
	var ss float64
	for _, v := range out {
		ss += (v - mean) * (v - mean)
	}
	std := math.Sqrt(ss / shapePoints)
	for i, v := range out {
		if std < 1e-12 {
			out[i] = 0
		} else {
			out[i] = (v - mean) / std
		}
	}
	return out
}

// EncodeShape renders a z-normalized shape as CRD decimals (offset by ShapeOffset).
func EncodeShape(sh []float64) []string {
	if len(sh) == 0 {
		return nil
	}
	out := make([]string, len(sh))
	for i, v := range sh {
		out[i] = strconv.FormatFloat(math.Max(0, v+ShapeOffset), 'f', 4, 64)
	}
	return out
}

func decodeShape(in []string) []float64 {
	if len(in) != shapePoints {
		return nil
	}
	out := make([]float64, shapePoints)
	for i, s := range in {
		v, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil
		}
		out[i] = v - ShapeOffset
	}
	return out
}

// Similarity scores an episode against the current situation in [0, 1].
func Similarity(cur Query, ep v1.TideEpisodeSpec) float64 {
	cal := calendarSim(cur.Weekday, cur.Hour, ep.Calendar.Weekday, int(ep.Calendar.Hour))
	ctx := contextSim(cur.OpsNote, cur.Deploy, ep.Context.OpsNote, ep.Context.Deploy)
	epShape := decodeShape(ep.Shape)
	if len(cur.Shape) != shapePoints || epShape == nil {
		return (wCalendar*cal + wContext*ctx) / (wCalendar + wContext)
	}
	return wShape*shapeSim(cur.Shape, epShape) + wCalendar*cal + wContext*ctx
}

func shapeSim(a, b []float64) float64 {
	var ss float64
	for i := range a {
		d := a[i] - b[i]
		ss += d * d
	}
	return math.Min(1, math.Max(0, 1-math.Sqrt(ss)/(2*math.Sqrt(shapePoints))))
}

func parseWeekday(s string) (time.Weekday, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) < 3 {
		return 0, false
	}
	for d := time.Sunday; d <= time.Saturday; d++ {
		if strings.HasPrefix(strings.ToLower(d.String()), s[:3]) {
			return d, true
		}
	}
	return 0, false
}

func calendarSim(wd time.Weekday, hour int, epWeekday string, epHour int) float64 {
	var s float64
	if d, ok := parseWeekday(epWeekday); ok && d == wd {
		s = 0.5
	}
	dh := math.Abs(float64(hour - epHour))
	dh = math.Min(dh, 24-dh)
	return s + 0.5*math.Max(0, 1-dh/6)
}

func tokens(s string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, w := range strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	}) {
		if len([]rune(w)) >= 3 {
			out[w] = struct{}{}
		}
	}
	return out
}

func contextSim(note string, deploy bool, epNote string, epDeploy bool) float64 {
	a, b := tokens(note), tokens(epNote)
	var j float64
	if len(a) > 0 || len(b) > 0 {
		inter := 0
		for w := range a {
			if _, ok := b[w]; ok {
				inter++
			}
		}
		j = float64(inter) / float64(len(a)+len(b)-inter)
	}
	s := 0.7 * j
	if deploy == epDeploy {
		s += 0.3
	}
	return s
}

// Summary renders an episode as one line of at most 400 characters.
func Summary(name string, score float64, ep v1.TideEpisodeSpec) string {
	tz := ep.Calendar.Timezone
	if tz == "" {
		tz = utc
	}
	wd := ep.Calendar.Weekday
	if d, ok := parseWeekday(wd); ok {
		wd = d.String()[:3]
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%.2f): %s %02d:00 %s, %s× peak", name, score, wd, ep.Calendar.Hour, tz, ep.PeakRatio)
	for _, m := range ep.Members {
		fmt.Fprintf(&b, ", %s %+ds", m.Workload, m.LagSeconds)
	}
	if ep.Action != "" {
		b.WriteString("; action: " + ep.Action)
	}
	if o := ep.Outcome; o != nil {
		parts := []string{"unscored"}
		if o.Timely != nil {
			parts[0] = map[bool]string{true: "timely", false: "late"}[*o.Timely]
		}
		parts = append(parts, map[bool]string{true: "needed", false: "not needed"}[o.Needed])
		if o.WastedReplicaMinutes != "" {
			parts = append(parts, o.WastedReplicaMinutes+" wasted rep-min")
		}
		b.WriteString("; outcome: " + strings.Join(parts, ", "))
	}
	if r := []rune(b.String()); len(r) > maxSummary {
		return string(r[:maxSummary-1]) + "…"
	}
	return b.String()
}

func sortMatches(ms []Match) {
	slices.SortFunc(ms, func(a, b Match) int {
		if a.Score != b.Score {
			if a.Score > b.Score {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
}
