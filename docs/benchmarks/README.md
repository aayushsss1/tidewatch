# Tidewatch benchmarks

This directory documents the Tidewatch benchmark simulator and holds committed results, one
sub-directory per run. The claim being tested:

> An agent that can only lift replicas prevents X% of SLO-breach minutes in flash sales and
> cascades. It costs Y extra replica-minutes and $Z of LLM spend, compared with HPA, the Tidewatch
> baseline alone, a regex rule over the ops note, and an oracle.

The simulator lives in `sim/` and the runner in `cmd/tidewatch-bench`. Results are published
here once a full run with the agent has been made; until then no numbers are quoted.

## Methodology

### What is real and what is simulated

The simulator reuses the operator code. An in-process envtest API server hosts the Deployment,
TidePolicy and TideWorkload objects. The real policy controller, engine, agent runner,
investigator, tools and override store run against it. Only the world is simulated:

- a fake clock, advanced one step (1 minute) at a time;
- `SimCluster`, which implements the scale client: pods with a startup delay, a fixed per-pod
  capacity (requests per second), and ready and desired replica counts;
- `SimProm`, which serves each workload's load series and answers the agent's `query_metrics`
  calls. It returns only data at or before the simulated now, so no config sees future data.

The clock is frozen while an investigation runs, so LLM latency does not affect the simulation.

### SLO model

Evaluated every step, per workload:

- breach-minute: offered load exceeds ready replicas x pod capacity;
- needed replicas: `ceil(load / (capacity x headroom))`;
- replica-minutes are split into over-provisioning (ready above needed) and under-provisioning
  (below needed);
- lift (`lift_rm`): replica-minutes the agent's override added above what the baseline wanted.

### Configurations

| config | what it is |
|---|---|
| `hpa` | The Kubernetes HPA algorithm: target utilization 0.8, tolerance 0.1, 300s scale-down stabilization, default scale-up policy (100% or 4 pods per 15s, whichever is larger), same min and max as the Tidewatch policy. |
| `keda-prometheus` | KEDA's Prometheus scaler driving an HPA on request rate with an `AverageValue` target of capacity x 0.8 per pod. KEDA polls every 30s; the metric is a 1-minute trailing average (the lag of `rate(...[1m])`); the HPA syncs every 15s with the HPA defaults. At the 1-minute step it behaves like `hpa`: a 1-minute trailing average plus the 30s poll feeds the same HPA math (same target, tolerance, policies and stabilization) with the same metric, so the two produce the same replicas. `cooldownPeriod` only matters for scale-to-zero and every scenario has `minReplicas >= 1`, so it is not modelled. |
| `keda-cron` | KEDA Cron plus the Prometheus scaler, modelling a human who reads the ops note. A note that parses as "expect ~Nx" at a time opens a window from event minus 15 minutes to event plus 60 minutes with `desiredReplicas = ceil(N x pre-event daily peak / (capacity x 0.8))`. KEDA takes the max across triggers. The note is followed literally, including wrong or adversarial ones. The window opens a fixed 15 minutes ahead regardless of pod startup time. |
| `tidewatch-baseline` | The real engine with the agent disabled: forecasting plus guardrails. It sizes for `max(reactive, forecast upper bound over the next time-to-ready plus margin)`, so it carries forecast-driven headroom and over-provisions more than HPA (see `internal/engine/baseline.go`). |
| `regex-rule` | `tidewatch-baseline` plus a deterministic override that parses the note for a multiplier and a time. From event minus time-to-ready minus a margin to event plus 30 minutes it sets the floor to `ceil(N x currentLoad / (capacity x headroom))` on every workload in the policy. It goes through the same override store and engine, so the guardrails apply. It knows the measured startup time; the cron human does not. |
| `tidewatch-agent` | The real engine plus the LLM agent (default model `claude-haiku-5-5`). |
| `oracle` | Perfect foresight: `ceil(max load over [t, t + startup + margin] / (capacity x headroom))`, clamped to the bounds and ignoring cooldowns. An upper bound, not a real controller. |

HPA and KEDA are re-implementations of the published algorithms inside the simulator, not the
real controllers. They are simplified as described above (no scale-to-zero, one metric).

### Guardrail profiles

Tidewatch configs run under two profiles:

- `matched` (the headline): step limits as loose as the HPA's (max step equals the policy's
  `maxReplicas`), short cooldowns, and the HPA's 300s scale-down stabilization, so the
  comparison is not decided by the CRD's conservative defaults;
- `default` ("with safety rails"): the CRD defaults (scale up +5 per 60s, scale down -2 per
  300s, 10 minute stabilization).

Controller configs have `guardrails = n/a` and are compared against both profiles.

### Scenarios

Workloads come from the Azure Functions 2019 trace: six high-volume functions with daily
seasonality, each rescaled so its p99 per-minute rate is 120 rps (about 5 pods at 30 rps per pod).
Each scenario has 72 hours of pre-seeded history (the warm start, so forecasters are not cold)
followed by a 6-hour measured window. A surge event is injected at the point where the
workload's diurnal profile rises most steeply.

There are 38 scenarios in `sim/scenarios/headline/`: the six workloads times six families, plus two B scenarios.

| family | event |
|---|---|
| `flash-3x`, `flash-5x` | a single workload surges 3x or 5x; a true note is published 30 minutes ahead |
| `cascade-4x` | 4x surge on the frontend reaches `api` 90s later and `worker` 180s later |
| `flash-5x-slow`, `cascade-4x-slow` | the same, with 180s pod startup (JVM, model loading, cache warm-up) instead of 45s |
| `control` | no event and no note |
| `b-adversarial` | real surge is 3x; the note claims 50x and carries a prompt-injection line |
| `b-wrong-time` | the note names a time two hours after the surge actually starts |

The slow-start variants exist because with fast startup a reactive scaler can keep up with a
ramp; a scenario only discriminates when startup time exceeds what reaction can cover.
`make bench-calibrate` checks this: the oracle must never breach, HPA must breach on every
5x and cascade scenario and on each slow-start scenario (at least 4 minutes), and on at least
4 of 6 flash-3x scenarios.

### Pairing and common random numbers

The cluster seed (which drives the startup-delay stream) is constant across configs and
repeats, so every config sees identical load and identical startup delays. Join rows on
`(scenario, seed)`; Tidewatch-mode configs also join on `guardrails`. `repeat` indexes only the
agent's repeats, because LLM sampling is the only remaining randomness. All deltas in the report
are paired on this key, which removes scenario-to-scenario variance from the comparisons.

### Safety checks

Two layers, deliberately kept apart:

1. Lift invariant (regression guard). Each step the harness checks that the desired replicas
   after overrides are never below the baseline's desired replicas. `safety_violations` counts
   failures and must be 0, as must `skipped_steps` (steps the harness could not account). This
   guard is close to tautological: the engine's override path can only raise the baseline. It
   catches regressions in that code, and it is not the headline claim.
2. Paired empirical comparison. The agent's `underprov_rm` and `breach_min` are compared to the
   baseline's on the same scenario, seed and profile. The report prints the share of paired
   runs where the agent's under-provisioning is at most the baseline's. This is evidence over
   the runs made, not a proof: enforcer state (cooldowns, rate limits) diverges between runs
   after the first override.

### Threats to validity

- Simulated cluster. Pods start after a sampled delay and serve a fixed capacity; real clusters
  have noisy neighbours, image pulls, node scale-up and uneven load balancing.
- Startup model. Delay is uniform in mean x (1 +/- 20%) and two means are used (45s, 180s).
  Real startup distributions have heavier tails.
- Trace rescaling. Azure Functions invocation counts are per-minute and rescaled to a target
  peak rate; the shapes are real, the absolute rates and capacities are not. The injected
  events are synthetic.
- Note-parser coverage. The baselines that read notes (regex-rule, keda-cron) share the
  product's note parser. It needs an RFC3339 time or `HH:MM` with a zone (`UTC` or an IANA
  zone); a bare `HH:MM` is not an event and relative times ("in two hours") are unsupported.
  Scenario notes are written in the supported form, so these baselines are given their best
  case.
- LLM nondeterminism. Agent runs vary, hence 5 repeats and reporting medians and IQR. Results
  are specific to the model and prompts at the time of the run; the model name is recorded in
  the CSV.
- Six workloads from one trace and one event placement per workload is a small sample.

## Reproducing

All commands run from the repository root. The simulator needs envtest binaries; the make
targets set them up.

1. Fetch the trace (cached outside the repo, checksums verified):

   ```
   scripts/bench/fetch-azure2019.sh
   ```

2. Free smoke run (synthetic trace, scripted agent, no API calls); fails on any safety
   violation or skipped step:

   ```
   make bench-smoke
   ```

3. Check the headline scenarios discriminate (takes minutes, needs the trace):

   ```
   make bench-calibrate
   ```

4. Full run. This is paid. Read the key without echoing it or storing it in shell history:

   ```
   read -s ANTHROPIC_API_KEY && export ANTHROPIC_API_KEY
   make bench MATRIX=sim/matrices/headline.yaml OUT=bin/bench BENCH_FLAGS="--max-usd 5 --resume"
   ```

   `--max-usd 5` is the recommended spend limit (see the cost estimate); `--resume` skips runs already in `results.csv`. Pass `--guardrails both` in `BENCH_FLAGS` to
   also run the `default` profile (the default is `matched` only). The non-agent configs need
   no key: run them with `bin/tidewatch-bench run --provider scripted --config hpa,keda-prometheus,...`
   after `make build-bench`, with `KUBEBUILDER_ASSETS` set as in the Makefile.

5. Render the report:

   ```
   make bench-report CSV=bin/bench/results.csv OUT=docs/benchmarks/<run-name>
   ```

   This creates a virtualenv in `bin/bench-venv` from `scripts/bench/requirements.txt` and writes
   `summary.md`, `summary.json` and PNG figures. The report prints a loud warning if any
   safety counter is non-zero. Without agent rows it renders everything else and marks the
   agent numbers as pending. Run `bin/bench-venv/bin/python -m pytest scripts/bench` for the
   report tests.

### Cost estimate

The headline matrix has 38 scenarios and the agent repeats 5 times: 190 agent runs on
`claude-haiku-5-5` for the `matched` profile (380 with `--guardrails both`). Each investigation
is capped at 30,000 tokens and 8 tool calls, and the agent has an hourly token budget of
200,000 counted on the simulated clock, so a 6-hour run is bounded at 1.2M tokens. That is a
token bound, not a dollar cap. At the price table in `cmd/tidewatch-bench/price.go` (USD 0.10
per million input tokens, 0.50 per million output), 1.2M all-input tokens is about USD 0.12 per
run and 1.2M all-output tokens about USD 0.60 per run, so the worst case for 190 runs is about
USD 23 to USD 114. The real spend limit is `--max-usd`: the runner stops before a run once the
estimated spend would exceed it. The `usd` column is an estimate from list prices, not a bill.

## Data licensing

Workload traces come from the Azure Functions 2019 dataset, released under CC-BY-4.0 by
Microsoft Azure: <https://github.com/Azure/AzurePublicDataset>. The data is downloaded to a local cache and is not
redistributed in this repository; rescaling is applied at load time.

Attribution: Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum, Jason
Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, Ricardo Bianchini. "Serverless in the
Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider." USENIX
Annual Technical Conference (ATC), 2020.
