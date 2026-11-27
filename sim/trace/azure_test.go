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

const (
	absentDay = 5
	idA       = "aaa111"
	trigHTTP  = "http"
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
		{idA, trigHTTP, func(_, m int) int { return 100 + int(90*math.Sin(2*math.Pi*float64(m)/1440)) }},
		// Strong daily cycle, lower volume.
		{"aab222", trigHTTP, func(_, m int) int { return 20 + int(18*math.Sin(2*math.Pi*float64(m)/1440)) }},
		// Flat-ish noise, huge volume: no seasonality.
		{"bbb333", "queue", func(d, m int) int { return 500 + (m*7919+d*104729)%97 }},
		// Biggest and seasonal, but absent from day 5 (never invoked that day).
		{"ddd555", "http", func(d, m int) int {
			if d == absentDay {
				return -1
			}
			return 300 + int(250*math.Sin(2*math.Pi*float64(m)/1440))
		}},
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
	for day := 1; day <= numDays; day++ {
		var b strings.Builder
		b.WriteString("HashOwner,HashApp,HashFunction,Trigger")
		for i := 1; i <= 1440; i++ {
			fmt.Fprintf(&b, ",%d", i)
		}
		b.WriteString("\n")
		for _, r := range fixtureRows() {
			if r.f(day, 0) < 0 {
				continue // absent this day
			}
			fmt.Fprintf(&b, "own,app,%s,%s", r.id, r.trig)
			for i := range 1440 {
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
	ws, err := LoadAzure2019(fixtureDir, Selector{Prefix: idA[:3]})
	if err != nil {
		t.Fatal(err)
	}
	if len(ws) != 1 || ws[0].ID != idA || ws[0].Trigger != trigHTTP {
		t.Fatalf("got %+v", ws)
	}
	if got := len(ws[0].Counts); got != numDays*1440 {
		t.Fatalf("len = %d, want numDays*1440", got)
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
	// ddd555 is the largest but missing from one day, so it is excluded.
	if len(ws) != 2 || ws[0].ID != idA || ws[1].ID != "aab222" {
		ids := make([]string, 0, len(ws))
		for _, w := range ws {
			ids = append(ids, w.ID)
		}
		t.Fatalf("selected %v, want [aaa111 aab222]", ids)
	}
	ws, err = LoadAzure2019(fixtureDir, Selector{TopN: 1, MinMeanPerMin: 5})
	if err != nil || len(ws) != 1 || ws[0].ID != idA {
		t.Fatalf("top1: %v %+v", err, ws)
	}
}

func TestLoadAzure2019_Errors(t *testing.T) {
	_, err := LoadAzure2019(t.TempDir(), Selector{TopN: 1})
	if err == nil || !strings.Contains(err.Error(), "fetch-azure2019.sh") {
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
	ws, _ := LoadAzure2019(fixtureDir, Selector{Prefix: idA[:3]})
	w := ws[0].Rescaled(50)
	if got := w.RobustPeakRPS(); math.Abs(got-50) > 1e-9 {
		t.Errorf("peak = %v", got)
	}
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := w.Series(epoch)
	if s.Step != time.Minute || len(s.Points) != numDays*1440 || !s.Points[1].T.Equal(epoch.Add(time.Minute)) {
		t.Fatalf("series = step %v len %d", s.Step, len(s.Points))
	}
	// At wraps and holds per minute.
	first, last := s.Points[0].V, s.Points[numDays*1440-1].V
	if w.At(30*time.Second) != first || w.At(numDays*1440*time.Minute) != first || w.At(-time.Minute) != last {
		t.Error("At wrap/hold wrong")
	}
	if ws[0].RobustPeakRPS() == w.RobustPeakRPS() {
		t.Error("Rescaled mutated or no-op")
	}
}

func copyFixture(t *testing.T, skip int) string {
	t.Helper()
	dst := t.TempDir()
	for d := 1; d <= numDays; d++ {
		if d == skip {
			continue
		}
		name := fmt.Sprintf("invocations_per_function_md.anon.d%02d.csv", d)
		b, err := os.ReadFile(filepath.Join(fixtureDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

func TestLoadAzure2019_MissingDayErrors(t *testing.T) {
	dir := copyFixture(t, 7)
	_, err := LoadAzure2019(dir, Selector{Prefix: idA[:3]})
	if err == nil || !strings.Contains(err.Error(), "d07") {
		t.Fatalf("err = %v, want mention of d07", err)
	}
}

func TestLoadAzure2019_AbsentFunctionDayIsZero(t *testing.T) {
	ws, err := LoadAzure2019(fixtureDir, Selector{Prefix: "ddd"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range ws[0].Counts[(absentDay-1)*1440 : absentDay*1440] {
		if c != 0 {
			t.Fatalf("absent day not zero-filled: %v", c)
		}
	}
	if len(ws[0].Counts) != numDays*1440 {
		t.Fatalf("len %d", len(ws[0].Counts))
	}
}

func TestLoadAzure2019_AmbiguousPrefixErrors(t *testing.T) {
	_, err := LoadAzure2019(fixtureDir, Selector{Prefix: "a"})
	if err == nil || !strings.Contains(err.Error(), "ambiguous") {
		t.Fatalf("err = %v", err)
	}
}

func TestLoadAzure2019_BadHeaderErrors(t *testing.T) {
	dir := copyFixture(t, 0)
	p := filepath.Join(dir, "invocations_per_function_md.anon.d03.csv")
	b, _ := os.ReadFile(p)
	bad := strings.Replace(string(b), "HashApp", "HashApps", 1)
	if err := os.WriteFile(p, []byte(bad), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadAzure2019(dir, Selector{Prefix: idA[:3]}); err == nil || !strings.Contains(err.Error(), "HashApp") {
		t.Fatalf("err = %v", err)
	}
}

func TestRescaledUsesP99NotMax(t *testing.T) {
	w := Workload{Counts: make([]float64, 1000)}
	for i := range w.Counts {
		w.Counts[i] = 60 // 1 rps
	}
	w.Counts[500] = 60000 // one-minute outlier, 1000 rps
	r := w.Rescaled(10)
	if got := r.Counts[0] / 60; math.Abs(got-10) > 1e-9 {
		t.Errorf("baseline minute = %v rps, want 10 (outlier must not set the scale)", got)
	}
}
