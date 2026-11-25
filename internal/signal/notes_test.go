package signal

import (
	"testing"
	"time"
)

func TestParseNoteEvents_RFC3339AndMultiplier(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	for _, note := range []string{
		"flash sale at 2026-11-27T09:00:00Z, expect ~4× on frontend",
		"flash sale at 2026-11-27T09:00:00Z, expect 4x on frontend",
		"flash sale at 2026-11-27T09:00:00Z, expect x4 on frontend",
	} {
		evs := ParseNoteEvents(note, now)
		if len(evs) != 1 {
			t.Fatalf("%q: got %d events", note, len(evs))
		}
		want := time.Date(2026, 11, 27, 9, 0, 0, 0, time.UTC)
		if !evs[0].At.Equal(want) || evs[0].Multiplier != 4 || evs[0].Text != note {
			t.Fatalf("%q: got %+v", note, evs[0])
		}
	}
}

func TestParseNoteEvents_HHMMWithZone(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	evs := ParseNoteEvents("batch job at 14:30 UTC\nwebinar 09:00 America/New_York x2.5", now)
	if len(evs) != 2 {
		t.Fatalf("got %+v", evs)
	}
	if want := time.Date(2026, 11, 25, 14, 30, 0, 0, time.UTC); !evs[1].At.Equal(want) || evs[1].Multiplier != 0 {
		t.Fatalf("UTC: %+v", evs[1])
	}
	// 09:00 EST (UTC-5) on Nov 25 = 14:00 UTC; events are sorted by time.
	if want := time.Date(2026, 11, 25, 14, 0, 0, 0, time.UTC); !evs[0].At.Equal(want) {
		t.Fatalf("NY: %+v", evs[0])
	}
	if evs[0].Multiplier != 2.5 {
		t.Fatalf("multiplier %v", evs[0].Multiplier)
	}
	// An HH:MM already passed today rolls to tomorrow.
	evs = ParseNoteEvents("report at 08:00 UTC", now)
	if want := time.Date(2026, 11, 26, 8, 0, 0, 0, time.UTC); len(evs) != 1 || !evs[0].At.Equal(want) {
		t.Fatalf("next occurrence: %+v", evs)
	}
}

func TestParseNoteEvents_IgnoresPastAndGarbage(t *testing.T) {
	now := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	for _, note := range []string{
		"",
		"nothing to see here",
		"sale at 2026-11-24T09:00:00Z",
		"at 25:99 UTC",
		"at 10:00 Mars/Olympus",
		"at 10:00 sharp",
		"2026-13-45T99:00:00Z",
		"index 4 box",
	} {
		if evs := ParseNoteEvents(note, now); len(evs) != 0 {
			t.Fatalf("%q: got %+v", note, evs)
		}
	}
	// "box" is not a multiplier.
	if evs := ParseNoteEvents("x4box at 2026-11-26T00:00:00Z, 4 boxes", now); len(evs) != 1 || evs[0].Multiplier != 0 {
		t.Fatalf("got %+v", evs)
	}
}
