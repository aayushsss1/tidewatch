package scenario

import (
	"strings"
	"testing"
	"time"
)

func TestRenderNote(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name, in, want string
		wantErr        bool
	}{
		{"plain", "no template here", "no template here", false},
		{"at", `Flash sale at {{at "75h"}}, expect ~4x`, "Flash sale at 03:00 UTC, expect ~4x", false},
		{"at minutes", `{{at "3h25m"}}`, "03:25 UTC", false},
		{"rfc3339", `at {{rfc3339 "75h30m"}}`, "at 2026-01-04T03:30:00Z", false},
		{"both", `{{at "1h"}} / {{rfc3339 "1h"}}`, "01:00 UTC / 2026-01-01T01:00:00Z", false},
		{"bad offset", `{{at "soon"}}`, "", true},
		{"unknown func", `{{nope "1h"}}`, "", true},
		{"unterminated", `{{at "1h"`, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := RenderNote(tc.in, epoch)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRenderNoteNonUTCEpoch(t *testing.T) {
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("x", 3600))
	got, err := RenderNote(`{{at "0s"}}`, epoch)
	if err != nil || got != "23:00 UTC" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestNotesAreRendered(t *testing.T) {
	data := strings.Replace(minimalWithNote, "NOTE", `Sale at {{at "9h"}}, expect ~3x`, 1)
	s, err := Parse([]byte(data), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Notes()[0].Text; got != "Sale at 09:00 UTC, expect ~3x" {
		t.Fatalf("note = %q", got)
	}
	bad := strings.Replace(minimalWithNote, "NOTE", `Sale at {{at "x"}}`, 1)
	if _, err := Parse([]byte(bad), t.TempDir()); err == nil {
		t.Fatal("bad note template must fail validation")
	}
}

const minimalWithNote = `
name: t
trace: {type: "synthetic:diurnal", base: 10, amp: 10, period: 24h}
start: 6h
duration: 6h
history: 6h
workloads:
  - {name: a, capacity: 10, startupDelay: 30s, minReplicas: 1, maxReplicas: 10}
notes:
  - publishAt: 8h
    text: 'NOTE'
`
