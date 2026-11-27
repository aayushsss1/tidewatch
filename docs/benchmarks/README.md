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
  Only the request-rate series (`http_requests_total`, through `rate`, `irate` and `sum`)
  exists in the simulator. Any other query (a latency histogram, `increase(...)`, another
  metric) returns no data, so the agent cannot see latency, errors or resource metrics here.

The clock is frozen while an investigation runs, so LLM latency does not affect the simulation.

### SLO model

Evaluated every step, per workload:

- breach-minute: offered load exceeds ready replicas x pod capacity;
- needed replicas: `ceil(load / (capacity x headroom))`, floored at 1;
- replica-minutes are split into over-provisioning (desired replicas, unready pods included,
  above needed) and under-provisioning (needed above ready replicas). The kind collector
  differs: it uses ready replicas for both sides and applies no floor of 1 to needed;
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
repeats, so every config sees identical load and the same startup-delay stream. Delays are
identical by pod-creation index (the nth pod a workload creates always gets the nth draw), not
per pod: configs that create pods at different times or in different numbers assign the same
draws to different pods. Join rows on
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
   baseline's on the same scenario, seed and profile. The report prints in how many paired
   runs the agent's total under-provisioning (summed over workloads) was never higher than the
   baseline's, and the same count per workload. This is evidence over
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
- Override timing. Rule overrides (regex-rule) are applied at step t, before `engine.Step`,
  while agent overrides are written during an investigation and take effect at t + step. This
  favours the rule baseline by one step.
- Frozen clock. The fake clock does not advance during an investigation, so LLM latency is not
  charged. This favours the agent on unannounced surges, where a real investigation would
  finish minutes later.
- Note windows on every workload. keda-cron, like regex-rule, opens its window on every
  workload in the policy even when the note names one service. That is generous to the
  baselines.
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
USD 23 to USD 114. Haiku's rate applies per call: USD 0.10 and 0.50 per million tokens for
prompts of up to 100K tokens, and USD 0.50 and 2.50 above that. The simulator tallies the
input and output tokens of calls with prompts over 100K and charges them at the higher rate;
the kind collector has only run totals and uses the lower rate. The real spend limit is
`--max-usd`: the runner stops before a run once the estimated spend would exceed it, and with
`--provider anthropic` it refuses to start an agent plan without one (`make bench` defaults
to `--max-usd 5`). The `usd` column is an estimate from list prices, not a bill.

## Real-cluster validation (kind)

The simulator re-implements HPA and KEDA. This check runs the real ones, with the real operator, on a
kind cluster, to see whether the simulator's ordering of configs holds. It is a sanity check on the
ordering, not a second headline benchmark: it covers one scenario, and its numbers are not comparable
in magnitude with the simulator's 38-scenario sums.

### Running it

Needs docker, kind, helm, kubectl, jq and python3. From the repository root:

```
make bench-kind CONFIGS=hpa,keda,tidewatch-baseline
```

Add `tidewatch-agent` to `CONFIGS` for the agent run. It needs `ANTHROPIC_API_KEY` (paid LLM calls;
set it as in the full-run step above) and is skipped with a message when the key is unset. The key
is passed to the operator through a Secret created over stdin and is never printed. Set
`TIDEWATCH_LLM_MODEL` to change the model.

The script (`hack/bench-kind.sh`) creates a cluster named `tidewatch-bench` (separate from the
demo's), installs metrics-server, kube-prometheus-stack and the operator, and runs the configs one
after another. Chart versions are pinned in the script. On macOS the script re-executes itself under
`caffeinate -ims` so an idle-sleeping laptop does not stall a multi-hour run. It uses its own
kubeconfig (`bin/bench-kind.kubeconfig`), so your current kubectl context is untouched, and it
restores `config/manager/kustomization.yaml` on exit if `make deploy` changed it. It aborts if setup
overruns the lead-in before the traffic starts (raise `LEAD_IN_SEC`). `make bench-kind-down` deletes the
cluster and the kubeconfig; the `tidewatch-llm` Secret (the API key, for the agent config) stays in the
cluster until then.
Each config takes about an hour (set `SURGE_AFTER_MIN`, `NOTE_LEAD_MIN`, `HOLD_MIN` and `TAIL_MIN`
for a shorter timeline). Results go to `bin/bench-kind/results.csv`, plus per-config replica logs and
operator logs in the same directory.

### The run

Before every config the shop namespace is deleted and recreated at 3 replicas per workload, every
HPA, ScaledObject and TidePolicy is removed, KEDA is uninstalled unless the config is `keda`, and the
Prometheus TSDB is wiped and re-seeded with one hour of the baseline pattern. Tidewatch and KEDA or
the HPA never manage the same workload at once.

Traffic is a gentle 20-minute cycle of 60 to 80 rps on the frontend. At 25 minutes a 4x surge ramps
over 2 minutes and holds for 15, followed by an 8 minute tail. The `api` and `worker` Deployments
receive the surge through the frontend's downstream calls (api at once, worker 90s later). Each pod
serves 50 rps and takes about 45s to become ready. A note, "Flash sale at HH:MM UTC, expect ~4×
traffic on frontend", is published 20 minutes before the surge.

| config | what runs |
|---|---|
| `hpa` | `autoscaling/v2` on CPU at 80%, default behavior, 1 to 10 replicas |
| `keda` | KEDA (Helm chart 2.21.1) with a Prometheus trigger on `bench:load_rps` (threshold 40 = capacity x 0.8) per workload. When the note is published a cron trigger is added: window from event minus 15 minutes to event plus 60 minutes, `desiredReplicas = ceil(4 x pre-event hourly peak / 40)`. This is the "human" config |
| `tidewatch-baseline` | The demo policy, agent disabled, matched guardrails (max step equal to max replicas, 15s cooldowns, 300s stabilization) |
| `tidewatch-agent` | The same with the agent in `Auto` mode |

### What it measures

Metrics are read from Prometheus at the end of each config (`cmd/tidewatch-bench kind-collect`), from
two recording rules in `demo/bench/rules.yaml` (`bench:load_rps`, `bench:ready_replicas`), sampled
once a minute:

- `breach_min`: minutes where load exceeds ready replicas x 50 rps;
- `overprov_rm` and `underprov_rm`: replica-minutes above and below `ceil(load / (50 x 0.8))`;
- `scale_events`: changes of the Deployment's `spec.replicas` over the window;
- `p95_ms`, `p99_ms`: from the app's `http_request_duration_seconds` histogram;
- for the agent: tokens, investigations and an estimated `usd` from the operator's metrics.

`results.csv` uses the simulator's schema plus `env` (`kind`), `p95_ms` and `p99_ms`. `guardrails` is
`matched` for Tidewatch configs and `n/a` otherwise. Columns the cluster run cannot measure
(`lift_rm`, `safety_violations`, `llm_calls`) are empty, not 0. `skipped_steps` counts minutes where
the load or ready series had no sample. The metric math is unit tested against a fake Prometheus.

### Limitations

- One node, shared with whatever else the machine runs. Pods start fast and uniformly, with none of
  the node scale-up, image pulls or cross-node effects of a real cluster.
- The HPA scales on a CPU proxy: each admitted request burns 2 ms of CPU in a calibrated busy loop
  (`BURN_CPU_PER_REQUEST`, set only by `demo/bench`), so that 80% of a 100m request is reached at
  40 rps per pod, the same point as the other configs' target. Every config runs the same pods, so
  the burn is not an HPA-only cost, but real services' CPU is not this proportional to requests.
- One scenario, one run per config, no repeats. Differences of a few minutes are noise.
- The latency histogram measures only the app's own time (rate-limiter wait plus the burn), not
  network or queueing in front of it, and a pod's 503s at capacity are fast.
- History is one hour with a 20-minute season, not 72 hours of daily seasonality, so the forecaster
  is warmer on the cycle and colder on a daily pattern than in the simulator.
- Capacity is 50 rps for all three workloads; the simulator's cascades use per-workload capacities.

## Data licensing

Workload traces come from the Azure Functions 2019 dataset, released under CC-BY-4.0 by
Microsoft Azure: <https://github.com/Azure/AzurePublicDataset>. The data is downloaded to a local cache and is not
redistributed in this repository; rescaling is applied at load time.

Attribution: Mohammad Shahrad, Rodrigo Fonseca, Inigo Goiri, Gohar Chaudhry, Paul Batum, Jason
Cooke, Eduardo Laureano, Colby Tresness, Mark Russinovich, Ricardo Bianchini. "Serverless in the
Wild: Characterizing and Optimizing the Serverless Workload at a Large Cloud Provider." USENIX
Annual Technical Conference (ATC), 2020.
