// Package baselines implements the comparison configurations of the benchmark simulator:
// HPA, KEDA (Prometheus, and Cron + Prometheus), the regex rule over the ops note, and the
// oracle. Controllers act directly on SimCluster; the regex rule is a Tidewatch-mode
// config.
package baselines

import (
	"regexp"
	"strconv"
	"time"
)

// NoteEvent is what a human (or the regex rule) extracts from an ops note: an expected
// traffic multiplier and the time of the event.
type NoteEvent struct {
	Multiplier float64
	EventTime  time.Time
}

var (
	// "expect ~4×", "expect 4x"; checked first so the stated forecast wins over other numbers.
	reExpect = regexp.MustCompile(`(?i)expect\w*\s*~?\s*(\d+(?:\.\d+)?)\s*[x×]`)
	// "4×", "4x", "4 x" (not "0x10": the x must end the token).
	reNx = regexp.MustCompile(`(?i)(?:^|[^\w.])~?(\d+(?:\.\d+)?)\s*[x×](?:[^\w]|$)`)
	// "×4", "x4".
	reXn    = regexp.MustCompile(`(?i)(?:^|[^\w])[x×]\s*(\d+(?:\.\d+)?)`)
	reRFC   = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`)
	reClock = regexp.MustCompile(`(?:^|[^\d])(\d{1,2}):(\d{2})(?:[^\d:]|$)`)
)

// ParseNote extracts the multiplier and event time from an ops note published at
// publishedAt. The time is an RFC3339 timestamp; failing that, a bare HH:MM is read as UTC
// and resolved to its next occurrence at or after publishedAt (the example scenarios'
// notes say "starts at 09:00"). A note with no multiplier, a non-positive one, or no time
// does not parse. A parsed note is followed literally, however implausible.
func ParseNote(text string, publishedAt time.Time) (NoteEvent, bool) {
	var mult float64
	for _, re := range []*regexp.Regexp{reExpect, reNx, reXn} {
		if m := re.FindStringSubmatch(text); m != nil {
			v, err := strconv.ParseFloat(m[1], 64)
			if err == nil && v > 0 {
				mult = v
				break
			}
			return NoteEvent{}, false
		}
	}
	if mult == 0 {
		return NoteEvent{}, false
	}
	if s := reRFC.FindString(text); s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return NoteEvent{Multiplier: mult, EventTime: t}, true
		}
	}
	if m := reClock.FindStringSubmatch(text); m != nil {
		h, _ := strconv.Atoi(m[1])
		min, _ := strconv.Atoi(m[2])
		if h < 24 && min < 60 {
			u := publishedAt.UTC()
			t := time.Date(u.Year(), u.Month(), u.Day(), h, min, 0, 0, time.UTC)
			if t.Before(u) {
				t = t.Add(24 * time.Hour)
			}
			return NoteEvent{Multiplier: mult, EventTime: t}, true
		}
	}
	return NoteEvent{}, false
}
