package trace

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var fixtureDir string

// fixtureRows defines the hand-made functions: id, trigger, per-minute count fn.
func fixtureRows() []struct {
	id, trig string
	f        func(day, min int) int
} {
	return []struct {
		id, trig string
		f        func(day, min int) int
	}{
		// Strong daily cycle, high volume.
		{"aaa111", "http", func(_, m int) int { return 100 + int(90*math.Sin(2*math.Pi*float64(m)/1440)) }},
		// Strong daily cycle, lower volume.
		{"aab222", "http", func(_, m int) int { return 20 + int(18*math.Sin(2*math.Pi*float64(m)/1440)) }},
		// Flat-ish noise, huge volume: no seasonality.
		{"bbb333", "queue", func(d, m int) int { return 500 + (m*7919+d*104729)%97 }},
		// Tiny volume with seasonality: below min mean rate.
		{"ccc444", "timer", func(_, m int) int {
			if m%1440 < 720 {
				return 0
			}
			return 1
		}},
	}
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "azfix")
	if err != nil {
		panic(err)
	}
	for day := 1; day <= 2; day++ {
		var b strings.Builder
		b.WriteString("HashOwner,HashApp,HashFunction,Trigger")
		for i := 1; i <= 1440; i++ {
			fmt.Fprintf(&b, ",%d", i)
		}
		b.WriteString("\n")
		for _, r := range fixtureRows() {
			fmt.Fprintf(&b, "own,app,%s,%s", r.id, r.trig)
			for i := 0; i < 1440; i++ {
				fmt.Fprintf(&b, ",%d", r.f(day, i))
			}
			b.WriteString("\n")
		}
		name := filepath.Join(dir, fmt.Sprintf("invocations_per_function_md.anon.d%02d.csv", day))
		if err := os.WriteFile(name, []byte(b.String()), 0o644); err != nil {
			panic(err)
		}
	}
	fixtureDir = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestLoadAzure2019_ByPrefixConcatenatesDays(t *testing.T) {
	ws, err := LoadAzure2019(fixtureDir, Selector{Prefix: "aaa"})
	if err != nil {
		t.Fatal(err)
	}
	if len(ws) != 1 || ws[0].ID != "aaa111" || ws[0].Trigger != "http" {
		t.Fatalf("got %+v", ws)
	}
	if got := len(ws[0].Counts); got != 2880 {
		t.Fatalf("len = %d, want 2880", got)
	}
	f := fixtureRows()[0].f
	if ws[0].Counts[1440+10] != float64(f(2, 10)) {
		t.Errorf("day 2 minute 10 = %v, want %v", ws[0].Counts[1450], f(2, 10))
	}
}

func TestLoadAzure2019_PrefixNoMatch(t *testing.T) {
	if _, err := LoadAzure2019(fixtureDir, Selector{Prefix: "zzz"}); err == nil {
		t.Fatal("want error for no match")
	}
}

func TestLoadAzure2019_TopNSeasonalByVolume(t *testing.T) {
	ws, err := LoadAzure2019(fixtureDir, Selector{TopN: 5, MinMeanPerMin: 5})
	if err != nil {
		t.Fatal(err)
	}
	// bbb333 is non-seasonal, ccc444 is below min rate.
	if len(ws) != 2 || ws[0].ID != "aaa111" || ws[1].ID != "aab222" {
		ids := []string{}
		for _, w := range ws {
			ids = append(ids, w.ID)
		}
		t.Fatalf("selected %v, want [aaa111 aab222]", ids)
	}
	ws, err = LoadAzure2019(fixtureDir, Selector{TopN: 1, MinMeanPerMin: 5})
	if err != nil || len(ws) != 1 || ws[0].ID != "aaa111" {
		t.Fatalf("top1: %v %+v", err, ws)
	}
}

func TestLoadAzure2019_Errors(t *testing.T) {
	if _, err := LoadAzure2019(t.TempDir(), Selector{TopN: 1}); err == nil || !strings.Contains(err.Error(), "fetch-azure2019.sh") {
		t.Errorf("missing files error should mention fetch script, got %v", err)
	}
	if _, err := LoadAzure2019(fixtureDir, Selector{}); err == nil {
		t.Error("empty selector should error")
	}
}

func TestAutocorr(t *testing.T) {
	x := make([]float64, 400)
	for i := range x {
		x[i] = math.Sin(2 * math.Pi * float64(i) / 100)
	}
	if r := autocorr(x, 100); r < 0.7 {
		t.Errorf("periodic acf = %v", r)
	}
	if r := autocorr(x, 50); r > -0.5 {
		t.Errorf("half-period acf = %v", r)
	}
	if r := autocorr(make([]float64, 10), 3); r != 0 {
		t.Errorf("constant acf = %v", r)
	}
}

func TestRescaledAndSeries(t *testing.T) {
	ws, _ := LoadAzure2019(fixtureDir, Selector{Prefix: "aaa"})
	w := ws[0].Rescaled(50)
	if got := w.PeakRPS(); math.Abs(got-50) > 1e-9 {
		t.Errorf("peak = %v", got)
	}
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := w.Series(epoch)
	if s.Step != time.Minute || len(s.Points) != 2880 || !s.Points[1].T.Equal(epoch.Add(time.Minute)) {
		t.Fatalf("series = step %v len %d", s.Step, len(s.Points))
	}
	// At wraps and holds per minute.
	if w.At(30*time.Second) != s.Points[0].V || w.At(2880*time.Minute) != s.Points[0].V || w.At(-time.Minute) != s.Points[2879].V {
		t.Error("At wrap/hold wrong")
	}
	if ws[0].PeakRPS() == w.PeakRPS() {
		t.Error("Rescaled mutated or no-op")
	}
}
