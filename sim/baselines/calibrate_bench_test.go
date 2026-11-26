//go:build bench

package baselines_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aayushsss1/tidewatch/sim/baselines"
	"github.com/aayushsss1/tidewatch/sim/harness"
	"github.com/aayushsss1/tidewatch/sim/scenario"
)

// TestHeadlineCalibration guards the headline scenarios against being too easy: they must
// actually hurt a reactive autoscaler, and the oracle must be able to prevent every breach.
// It needs the Azure cache and takes minutes (each scenario loads the 14 day files), so it is
// behind the bench tag: run `make bench-calibrate`. With KUBEBUILDER_ASSETS set it also logs
// the tidewatch-baseline column.
func TestHeadlineCalibration(t *testing.T) {
	if _, err := os.Stat(scenario.AzureDir()); err != nil {
		t.Skipf("Azure cache missing (%v); run scripts/bench/fetch-azure2019.sh", err)
	}
	files, err := filepath.Glob(filepath.Join("..", "scenarios", "headline", "w*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no headline scenarios: %v", err)
	}
	withTW := os.Getenv("KUBEBUILDER_ASSETS") != ""
	flash3Hit, flash3 := 0, 0
	t.Logf("%-18s %5s %7s %9s", "scenario", "hpa", "oracle", "tw-base")
	for _, f := range files {
		name := strings.TrimSuffix(filepath.Base(f), ".yaml")
		if strings.HasSuffix(name, "control") {
			continue
		}
		sc, err := scenario.Load(f)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		breach := func(cfg string) float64 {
			e, err := baselines.New(cfg, sc, baselines.Options{})
			if err != nil {
				t.Fatal(err)
			}
			res, err := e.Run(context.Background(), sc, 0, harness.Options{})
			if err != nil {
				t.Fatalf("%s/%s: %v", name, cfg, err)
			}
			b, _, _ := sumMetrics(res)
			return b
		}
		hpa, oracle := breach(baselines.NameHPA), breach(baselines.NameOracle)
		tw := "-"
		if withTW {
			tw = formatFloat(breach(baselines.NameTWBaseline))
		}
		t.Logf("%-18s %5.0f %7.0f %9s", name, hpa, oracle, tw)
		if oracle != 0 {
			t.Errorf("%s: oracle breach_min = %v, want 0", name, oracle)
		}
		switch {
		case strings.HasSuffix(name, "flash-3x"):
			flash3++
			if hpa > 0 {
				flash3Hit++
			}
		default: // flash-5x, cascade-4x
			if hpa <= 0 {
				t.Errorf("%s: hpa breach_min = %v, want > 0", name, hpa)
			}
		}
	}
	if flash3 != 6 || flash3Hit < 4 {
		t.Errorf("hpa breaches on %d of %d flash-3x scenarios, want >= 4 of 6", flash3Hit, flash3)
	}
}

func formatFloat(v float64) string { return fmt.Sprintf("%.0f", v) }
