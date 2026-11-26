// Command tidewatch-bench runs the benchmark simulator over a matrix of scenarios and
// configurations and writes the results as CSV.
//
//	tidewatch-bench run --matrix sim/matrices/smoke.yaml --out bin/bench-smoke --provider scripted
//	tidewatch-bench select-azure --top 6 --out sim/scenarios/azure-workloads.yaml
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const usage = `usage:
  tidewatch-bench run --matrix <file> --out <dir> [--provider anthropic|scripted]
        [--model claude-haiku-5-5] [--repeats N] [--only <glob>] [--config a,b]
        [--resume] [--max-usd X] [--guardrails matched|default|both]
  tidewatch-bench select-azure --top N --out <file> [--azure-dir <dir>]
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := dispatch(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "tidewatch-bench:", err)
		os.Exit(1)
	}
}

func dispatch(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return errors.New("a subcommand is required")
	}
	switch args[0] {
	case "run":
		return runCmd(ctx, args[1:], stdout)
	case "select-azure":
		return selectAzureCmd(args[1:], stdout)
	case "-h", "--help", "help":
		_, _ = fmt.Fprint(stdout, usage)
		return nil
	}
	_, _ = fmt.Fprint(stderr, usage)
	return fmt.Errorf("unknown subcommand %q", args[0])
}
