// Package baselines implements the comparison configurations of the benchmark simulator:
// HPA, KEDA (Prometheus, and Cron + Prometheus), the regex rule over the ops note, and the
// oracle. Controllers act directly on SimCluster; the regex rule is a Tidewatch-mode
// config.
package baselines

import (
	"time"

	"github.com/aayushsss1/tidewatch/internal/signal"
)

// NoteEvent is what a human (or the regex rule) extracts from an ops note: an expected
// traffic multiplier and the time of the event.
type NoteEvent struct {
	Multiplier float64
	EventTime  time.Time
}

// ParseNote extracts the multiplier and event time from an ops note published at
// publishedAt. It is a thin wrapper over the product's parser, signal.ParseNoteEvents, so
// the controller baselines see exactly the events the agent's ContextDue trigger sees
// (RFC3339, or "HH:MM UTC" / "HH:MM <IANA zone>" resolved to the next occurrence after
// publishedAt; a bare HH:MM is not an event). The first future event with a multiplier > 0
// wins: the rule and cron baselines need a multiplier, whereas the agent does not. A parsed
// note is followed literally, however implausible.
func ParseNote(text string, publishedAt time.Time) (NoteEvent, bool) {
	for _, ev := range signal.ParseNoteEvents(text, publishedAt) {
		if ev.Multiplier > 0 {
			return NoteEvent{Multiplier: ev.Multiplier, EventTime: ev.At}, true
		}
	}
	return NoteEvent{}, false
}
