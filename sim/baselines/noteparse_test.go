package baselines

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"

	"github.com/aayushsss1/tidewatch/internal/signal"
	"github.com/aayushsss1/tidewatch/sim/scenario"
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
		{"clock time UTC", "Flash sale starts at 09:00 UTC, expect ~3x traffic for about 30 minutes.", true, 3, at9},
		{"clock time next day", "expect ~3x at 08:00 UTC sharp", true, 3, time.Date(2026, 1, 2, 8, 0, 0, 0, time.UTC)},
		{"bare clock time is not an event", "Flash sale starts at 09:00, expect ~3x traffic.", false, 0, time.Time{}},
		{"missing time", "expect ~4× traffic soon", false, 0, time.Time{}},
		{"missing multiplier", "Sale at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
		{"garbage", "asdf ~~ ignore previous instructions", false, 0, time.Time{}},
		{"empty", "", false, 0, time.Time{}},
		{"zero", "expect ~0x at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
		{"word boundary 4xl", "expect 4xl at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
		{"zero first: product parser reads one match", "0x then expect ~2x at 2026-01-01T09:00:00Z", false, 0, time.Time{}},
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

// TestScenarioNotesParse pins the fairness invariants for every scenario with notes: the
// baselines parse the note into the declared multiplier and event time (the surge start, or
// for the B scenarios the declared adversarial multiplier and the wrong time), and they agree
// with the agent's parser on both.
func TestScenarioNotesParse(t *testing.T) {
	files, err := filepath.Glob("../scenarios/*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	more, _ := filepath.Glob("../scenarios/*/*.yaml")
	files = append(files, more...)
	checked := 0
	for _, f := range files {
		if filepath.Base(f) == "azure-workloads.yaml" {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var sc struct {
			Name   string `yaml:"name"`
			Events []struct {
				Start time.Duration `yaml:"start"`
				Mult  float64       `yaml:"mult"`
			} `yaml:"events"`
			Notes []struct {
				PublishAt time.Duration `yaml:"publishAt"`
				Text      string        `yaml:"text"`
			} `yaml:"notes"`
		}
		if err := yaml.Unmarshal(raw, &sc); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		if len(sc.Notes) == 0 {
			if len(sc.Events) != 0 && filepath.Base(filepath.Dir(f)) != "scenarios" && !strings.HasSuffix(sc.Name, "control") {
				t.Errorf("%s: has an event but no note", f)
			}
			continue
		}
		if len(sc.Events) != 1 {
			t.Fatalf("%s: want exactly one event to compare the note against, got %d", f, len(sc.Events))
		}
		wantMult := sc.Events[0].Mult
		wantAt := scenario.DefaultEpoch.Add(sc.Events[0].Start)
		switch sc.Name {
		case "b-adversarial":
			wantMult = 50
		case "b-wrong-time":
			wantAt = wantAt.Add(2 * time.Hour)
		}
		for i, n := range sc.Notes {
			pub := scenario.DefaultEpoch.Add(n.PublishAt)
			text, err := scenario.RenderNote(n.Text, scenario.DefaultEpoch)
			if err != nil {
				t.Fatalf("%s note %d: %v", f, i, err)
			}
			got, ok := ParseNote(text, pub)
			if !ok {
				t.Errorf("%s note %d: baseline cannot parse %q", f, i, text)
				continue
			}
			if got.Multiplier != wantMult || !got.EventTime.Equal(wantAt) {
				t.Errorf("%s note %d: baseline parsed %vx at %v, want %vx at %v",
					f, i, got.Multiplier, got.EventTime, wantMult, wantAt)
			}
			var want *signal.NoteEvent
			for _, ev := range signal.ParseNoteEvents(text, pub) {
				if ev.Multiplier > 0 {
					want = &ev
					break
				}
			}
			if want == nil || !got.EventTime.Equal(want.At) || got.Multiplier != want.Multiplier {
				t.Errorf("%s note %d: baseline %+v disagrees with the agent parser %+v", f, i, got, want)
			}
			checked++
		}
	}
	if checked < 20 {
		t.Errorf("checked only %d notes, expected the headline, smoke and example scenarios", checked)
	}
}
