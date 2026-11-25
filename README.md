# tidewatch

Tidewatch is a Kubernetes operator that scales workloads before traffic arrives. It forecasts load for every workload a policy selects. An LLM agent wakes only when something looks off, investigates with tools, and proposes a scaling action. The operator enforces hard guardrails before anything changes.

**Status:** pre-alpha, not yet released.

## Quickstart

Prerequisites: Docker, [kind](https://kind.sigs.k8s.io), [Helm](https://helm.sh), kubectl and Go.

```sh
make demo
```

This creates a kind cluster named `tidewatch`, installs a slim kube-prometheus-stack, builds and loads the operator, demo app and traffic generator images, deploys the `frontend -> api -> worker` shop, seeds one hour of request history, starts the traffic generator and applies a `TidePolicy`. The traffic follows a 20 minute cycle (40 to 300 rps) whose peak lands about 4 minutes after the demo comes up.

Watch Tidewatch scale the frontend up before the peak, not after it:

```sh
kubectl get tideworkloads -n shop -w
kubectl get deploy -n shop -w
```

`frontend` should climb from 1 toward 8 replicas ahead of the peak, even though each pod takes 45 seconds to become ready. To graph `tidewatch_capacity_ratio`, run `kubectl -n monitoring port-forward svc/kps-kube-prometheus-stack-prometheus 9090:9090` and open http://localhost:9090.

For the demo the operator serves `/metrics` over plain HTTP (`--metrics-secure=false`) so a stock ServiceMonitor can scrape it. Do not do that in production.

Clean up with `make demo-down`. `make demo-e2e` runs the same demo and asserts the prescale (it takes about 10 minutes).

## License

Apache-2.0
