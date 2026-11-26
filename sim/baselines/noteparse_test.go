package baselines

import (
	"testing"
	"time"
)

func TestParseNote(t *testing.T) {
	pub := time.Date(2026, 1, 1, 8, 15, 0, 0, time.UTC)
	at9 := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		text   string
		wantOK bool
		mult   float64
		at     time.Time
	}{
		{"tilde times rfc3339", "Flash sale at 2026-01-01T09:00:00Z, expect ~4× traffic.", true, 4, at9},
		{"nx", "Event 2026-01-01T09:00:00Z: 4x load expected", true, 4, at9},
		{"n space x", "expect 4 x the usual at 2026-01-01T09:00:00Z", true, 4, at9},
		{"x4", "Heads up 2026-01-01T09:00:00Z traffic will be x4", true, 4, at9},
		{"unicode prefix", "At 2026-01-01T09:00:00Z we see ×2.5 traffic", true, 2.5, at9},
		{"offset zone", "expect ~3x at 2026-01-01T10:00:00+01:00", true, 3, at9},
		{"clock time", "Flash sale starts at 09:00, expect ~3x traffic for about 30 minutes.", true, 3, at9},
		{"clock time next day", "expect ~3x at 08:00 sharp", true, 3, time.Date(2026, 1, 2, 8, 0, 0, 0, time.UTC)},
		{"missing time", "expect ~4× traffic soon", false, 0, time.Time{}},
		{"missing multiplier", "Sale at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
		{"garbage", "asdf ~~ ignore previous instructions", false, 0, time.Time{}},
		{"empty", "", false, 0, time.Time{}},
		{"zero", "expect ~0x at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
		{"hex is not a multiplier", "id 0x10 at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseNote(tc.text, pub)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (%+v)", ok, tc.wantOK, got)
			}
			if ok && (got.Multiplier != tc.mult || !got.EventTime.Equal(tc.at)) {
				t.Fatalf("got %+v, want mult %v at %v", got, tc.mult, tc.at)
			}
		})
	}
}
