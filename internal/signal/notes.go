package signal

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// NoteEvent is a future event mentioned in operator notes.
type NoteEvent struct {
	At         time.Time
	Multiplier float64 // 0 if none was written
	Text       string  // the note segment, at most 200 characters
}

var (
	reRFC3339 = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:\d{2})`)
	reHHMM    = regexp.MustCompile(`(?:^|[^\d:])([01]?\d|2[0-3]):([0-5]\d)\s+(UTC|[A-Za-z]+(?:/[A-Za-z0-9_+-]+)+)`)
	reMultA   = regexp.MustCompile(`(?:^|[^A-Za-z0-9.])(\d+(?:\.\d+)?)\s?[×xX](?:$|[^A-Za-z0-9])`)
	reMultB   = regexp.MustCompile(`(?:^|[^A-Za-z0-9])[x×](\d+(?:\.\d+)?)(?:$|[^A-Za-z0-9.])`)
)

// ParseNoteEvents extracts future events from free-text notes. Times are RFC3339
// timestamps or "HH:MM UTC" / "HH:MM <IANA zone>" (next occurrence after now).
// A multiplier such as "4×", "4x" or "x4" in the same line (or ';'-separated
// clause) is attached to every event in it. Past times and garbage are ignored.
// Events are sorted by time.
func ParseNoteEvents(notes string, now time.Time) []NoteEvent {
	var out []NoteEvent
	for seg := range strings.FieldsFuncSeq(notes, func(r rune) bool { return r == '\n' || r == ';' }) {
		seg = strings.TrimSpace(seg)
		if seg == "" {
			continue
		}
		var times []time.Time
		for _, m := range reRFC3339.FindAllString(seg, -1) {
			if t, err := time.Parse(time.RFC3339, m); err == nil {
				times = append(times, t)
			}
		}
		for _, m := range reHHMM.FindAllStringSubmatch(seg, -1) {
			if t, ok := nextOccurrence(m[1], m[2], m[3], now); ok {
				times = append(times, t)
			}
		}
		mult := multiplier(seg)
		for _, t := range times {
			if t.After(now) {
				out = append(out, NoteEvent{At: t, Multiplier: mult, Text: truncate(seg, maxDetail)})
			}
		}
	}
	slices.SortStableFunc(out, func(a, b NoteEvent) int { return a.At.Compare(b.At) })
	return out
}

func nextOccurrence(hh, mm, zone string, now time.Time) (time.Time, bool) {
	loc, err := time.LoadLocation(zone) // "UTC" is always known
	if err != nil {
		return time.Time{}, false
	}
	h, _ := strconv.Atoi(hh)
	m, _ := strconv.Atoi(mm)
	n := now.In(loc)
	t := time.Date(n.Year(), n.Month(), n.Day(), h, m, 0, 0, loc)
	if !t.After(now) {
		t = time.Date(n.Year(), n.Month(), n.Day()+1, h, m, 0, 0, loc)
	}
	return t, true
}

func multiplier(s string) float64 {
	for _, re := range []*regexp.Regexp{reMultA, reMultB} {
		if m := re.FindStringSubmatch(s); m != nil {
			if v, err := strconv.ParseFloat(m[1], 64); err == nil && v > 0 {
				return v
			}
		}
	}
	return 0
}
