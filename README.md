# tidewatch

Tidewatch is a Kubernetes operator that scales workloads before traffic arrives. It forecasts load for every workload a policy selects. An LLM agent wakes only when something looks off, investigates with tools, and proposes a scaling action. The operator enforces hard guardrails before anything changes.

**Status:** pre-alpha, not yet released.

## Quickstart

Prerequisites: Docker, [kind](https://kind.sigs.k8s.io), [Helm](https://helm.sh), kubectl and Go.

```sh
make demo
```

This creates a kind cluster named `tidewatch`, installs a slim kube-prometheus-stack, builds and loads the operator, demo app and traffic generator images, deploys the `frontend -> api -> worker` shop, seeds one hour of request history, starts the traffic generator and applies a `TidePolicy`. The traffic follows a 20 minute cycle (40 to 300 rps) which starts in the trough, so the peak arrives about 10 minutes after `make demo` finishes.

Watch Tidewatch scale the frontend up before the peak, not after it:

```sh
kubectl get tideworkloads -n shop -w
kubectl get deploy -n shop -w
```

`frontend` should climb from 1 toward 8 replicas ahead of the peak, even though each pod takes 45 seconds to become ready. To graph `tidewatch_capacity_ratio`, run `kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090` and open http://localhost:9090.

For the demo the operator serves `/metrics` over plain HTTP (`--metrics-secure=false`) so a stock ServiceMonitor can scrape it. Do not do that in production.

Clean up with `make demo-down`. `make demo-e2e` runs the same demo and asserts the prescale, Act 1 only (it takes about 15 to 20 minutes).

## Agent (M2)

The baseline is a deterministic forecast-driven scaler, and it keeps running whether or not the agent exists. The agent is an optional layer on top: an LLM that wakes only when something looks off, investigates with read-only tools, and proposes a *temporary override* of the baseline. It is off by default (`spec.agent.enabled: false`, and the operator starts with `--llm-provider=none`).

### What it does

- **Triggers.** A workload raises a trigger when: the forecast breaches capacity (`ForecastBreach`), load looks anomalous (`Anomaly`), forecast models disagree (`ModelDisagreement`), the workload restarts or rolls out (`WorkloadChange`), the ops note or context says something is due (`ContextDue`), or it has been over-provisioned for a long time (`OverProvisioned`).
- **Incidents.** Related triggers within a policy are grouped into one incident (for example frontend, api and worker moving together). If many workloads trigger at once the agent enters storm mode and investigates the storm once, instead of once per workload.
- **Tools.** The agent can call `get_forecast`, `compare_forecasts`, `get_workload_state`, `get_events`, `get_rollout_history`, `get_decision_history`, `get_related_workloads`, `query_metrics`, `get_ops_notes` and `find_similar_episodes`, then `propose_scaling`. All of them are read-only except the proposal.
- **Overrides.** A proposal is a `minReplicas` floor with a start and an end, stored in `TideWorkload.status.overrides`. The baseline and the enforcer still decide the final replica count, so every override is clamped by the same guardrails (`maxReplicas`, `maxStep`, cooldown, the namespace total).
- **Approval.** `spec.agent.approval` is `Manual` or `Auto` (below).
- **Episodes.** When an incident ends, the operator records it as a `TideEpisode` (the lead workload, lagging members, a 32-point load shape, peak ratio, calendar slot, action and outcome). The agent retrieves similar past episodes with `find_similar_episodes`. You can also write one by hand (`source: manual`).
- **Context.** `spec.agent.context.notes` (and an optional ConfigMap) carry ops notes such as "flash sale at 14:00 UTC, expect ~4x on frontend".

Configure it per policy:

```yaml
spec:
  agent:
    enabled: true
    approval: Manual          # or Auto
    context:
      notes: "Flash sale at 2026-12-01T14:00:00Z UTC, expect ~4x on frontend"
    redactEventMessages: false
```

and give the operator a provider and key:

```sh
printf '%s' "$KEY" | kubectl create secret generic tidewatch-llm -n tidewatch-system --from-file=apiKey=/dev/stdin
# then add --llm-provider=anthropic (and optionally --llm-model) to the manager args
```

The key is read from the `TIDEWATCH_LLM_API_KEY` environment variable, which `config/manager/manager.yaml` maps from Secret `tidewatch-llm`, key `apiKey`. Without a key the agent stays disabled.

### Manual vs Auto

- **Manual** (default): a proposal waits in `status.pendingApprovals` until a human approves or rejects it, or it expires after `approvalTimeout` (default 10m). Nothing changes meanwhile.
- **Auto**: a proposal above `minConfidence` (default 0.6) is applied immediately, after the guardrails. Set `notifyOnAuto` to be notified through the Slack webhook when that happens.

Approve from the command line with the kubectl plugin:

```sh
make build-plugin          # builds bin/kubectl-tidewatch; put it on your PATH
kubectl tidewatch pending -A
kubectl tidewatch approve <incident-id> -n shop
kubectl tidewatch reject  <incident-id> -n shop
```

### Safety guarantees

- **Overrides only lift.** An override can raise the replica floor. It can never lower the baseline's number or scale a workload down.
- **The baseline is never blocked.** If the agent is disabled, slow, wrong, out of budget, or the provider is down, the baseline keeps scaling exactly as it would without the agent.
- **Guardrails still apply.** Overrides pass through the same `maxReplicas`, `maxStep`, cooldown and total-replica limits as baseline decisions.
- **Budget.** The agent spends at most `--agent-token-budget-per-hour` LLM tokens per hour (default 200000), plus per-investigation limits (`maxToolCalls`, `investigationTimeout`). The token budget is a soft limit: an investigation that is projected to run over its token cap is moved to its final turn instead of being cut off, and a single investigation can overshoot the hourly budget slightly.
- **Circuit breaker.** Repeated provider failures open a breaker and the agent stops calling the provider until it recovers.
- **Storm mode.** A burst of triggers is collapsed into a single investigation, so a cluster-wide event cannot fan out into many LLM calls.

### What data is sent to the LLM provider

Only what the tools return, and only when an investigation runs:

- workload names and namespaces;
- metric summaries and forecast numbers (peaks, intervals, accuracy, replica counts, resource requests and limits);
- event messages, truncated; set `spec.agent.redactEventMessages: true` to redact them;
- the text of your ops notes and any episode notes.

Secrets and full pod specs are never sent; resource requests/limits and image tags are.

### Running Act 2

Act 2 of the demo is a flash sale. The ops note announces it, the traffic generator spikes the frontend to 4x at 14 minutes after the demo epoch, and `api` and `worker` follow with a delay. With an API key, the agent prescales `api` and `worker` ahead of the cascade, and the ghost metric `tidewatch_baseline_only_replicas` shows the baseline alone would have lagged.

```sh
export ANTHROPIC_API_KEY=...          # this makes paid LLM calls
make demo-down demo-e2e-act2          # about 30 minutes; asserts the agent's override and the ghost metric
```

To pick the agent's model (default `claude-sonnet-5-5`), set `TIDEWATCH_LLM_MODEL`, e.g. `TIDEWATCH_LLM_MODEL=claude-haiku-5-5` for the cheapest run.

or `ANTHROPIC_API_KEY=... make demo` to run it without the assertions. `make demo` creates Secret `tidewatch-llm`, deploys with `--llm-provider=anthropic`, sets `agent.enabled: true` with `approval: Auto`, adds the ops note and seeds one hand-written `TideEpisode`. Without the key it prints `Act 2 skipped: set ANTHROPIC_API_KEY to run the agent` and Act 1 runs unchanged, with the same traffic shape. `make demo-e2e` runs Act 1 only: it strips `ANTHROPIC_API_KEY` from the environment, so an exported key never causes paid calls (CI uses it).

**Cost:** the first live Act 2 run (2026-11-26, `claude-haiku-5-5`, about 28 minutes) used 245,606 input and 18,834 output tokens across 9 investigations, roughly **$0.03** at Haiku 5.5 list prices. The hourly token budget (200k) was reached partway through and skipped 33 later incidents, so a run on Sonnet 5.5 costs more but is capped by the same budget. The agent set its override 12 minutes before the flash sale started, citing the ops note and both forecast models.

## License

Apache-2.0
