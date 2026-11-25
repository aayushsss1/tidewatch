#!/usr/bin/env bash
# Tidewatch demo, Act 1: kind + Prometheus + shop apps + seeded history + TidePolicy.
# Idempotent: safe to re-run. Fails fast on the first error.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER=tidewatch
CTX="kind-${CLUSTER}"
OP_IMG=tidewatch:dev
APP_IMG=tidewatch-demo-app:dev
TG_IMG=tidewatch-trafficgen:dev
PROM_SVC=kps-kube-prometheus-stack-prometheus
PROM_LOCAL=http://localhost:9090
PERIOD=20m
PERIOD_SECONDS=1200
BASE=40
AMP=260
NOISE=0.05
STATE_DIR="$ROOT/bin"

step() { printf '\n==> [%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
k() { kubectl --context "$CTX" "$@"; }

for tool in kind helm kubectl docker python3 go make; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing required tool: $tool" >&2; exit 1; }
done

PF_PID=""
cleanup() {
  if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

step "1/9 kind cluster '${CLUSTER}'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "cluster exists, skipping create"
else
  kind create cluster --name "$CLUSTER" --config demo/kind.yaml
fi
kubectl config use-context "$CTX" >/dev/null

step "2/9 kube-prometheus-stack (release kps, namespace monitoring)"
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
helm repo update prometheus-community >/dev/null
helm upgrade --install kps prometheus-community/kube-prometheus-stack \
  --kube-context "$CTX" --namespace monitoring --create-namespace \
  -f demo/prometheus-values.yaml --wait --timeout 8m
k -n monitoring rollout status "statefulset/prometheus-kps-kube-prometheus-stack-prometheus" --timeout=5m

step "3/9 build and load images"
make docker-build IMG="$OP_IMG"
docker build --target app -t "$APP_IMG" demo
docker build --target trafficgen -t "$TG_IMG" demo
for img in "$OP_IMG" "$APP_IMG" "$TG_IMG"; do
  kind load docker-image --name "$CLUSTER" "$img"
done
# Pre-build the seeder so no compile time lands between computing EPOCH and applying the policy.
mkdir -p "$STATE_DIR"
(cd demo && go build -o "$STATE_DIR/trafficgen" ./trafficgen)

step "4/9 install CRDs and deploy the operator"
make install deploy IMG="$OP_IMG"
# Demo only: serve /metrics over plain HTTP so a vanilla ServiceMonitor can scrape it
# (the kubebuilder default is HTTPS with TokenReview authn, which needs extra RBAC and TLS config).
OP_DEPLOY=tidewatch-controller-manager
k -n tidewatch-system patch deployment "$OP_DEPLOY" --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--metrics-secure=false"},
       {"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
k -n tidewatch-system rollout status "deployment/$OP_DEPLOY" --timeout=3m
k apply -f demo/operator/servicemonitor.yaml

step "5/9 shop apps"
k apply -f demo/manifests/namespace.yaml
k apply -f demo/manifests/apps.yaml -f demo/manifests/servicemonitor.yaml
for d in frontend api worker; do
  k -n shop rollout status "deployment/$d" --timeout=3m
done

step "6/9 seed 1h of history through Prometheus remote-write"
# Everything slow is done by now. Phase 0 of the pattern (the trough, ~40 rps) starts at EPOCH = now + 1m,
# roughly when the policy is applied, so the peak (EPOCH + period/2) is ~10 minutes out.
# Override with EPOCH=<RFC3339 UTC> to pin.
if [ -z "${EPOCH:-}" ]; then
  EPOCH="$(python3 - <<'PY'
import datetime as d
t = d.datetime.now(d.timezone.utc) + d.timedelta(minutes=1)
print(t.strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"
fi
PEAK="$(python3 - "$EPOCH" "$PERIOD_SECONDS" <<'PY'
import sys, datetime as d
e = d.datetime.strptime(sys.argv[1], "%Y-%m-%dT%H:%M:%SZ")
print((e + d.timedelta(seconds=int(sys.argv[2]) // 2)).strftime("%Y-%m-%dT%H:%M:%SZ"))
PY
)"
echo "EPOCH=$EPOCH PEAK=$PEAK"
mkdir -p "$STATE_DIR"
printf 'EPOCH=%s\nPEAK=%s\n' "$EPOCH" "$PEAK" > "$STATE_DIR/demo-state.env"

# Free :9090 of a kubectl port-forward left by a previous run; refuse to touch anything else.
if command -v lsof >/dev/null 2>&1; then
  for pid in $(lsof -ti tcp:9090 -sTCP:LISTEN 2>/dev/null || true); do
    if ps -o comm= -p "$pid" 2>/dev/null | grep -q kubectl; then
      echo "killing stale kubectl port-forward on :9090 (pid $pid)"
      kill "$pid" >/dev/null 2>&1 || true
      sleep 1
    else
      echo "port 9090 is in use by pid $pid ($(ps -o comm= -p "$pid" 2>/dev/null)); free it and re-run" >&2
      exit 1
    fi
  done
fi
# Call kubectl directly (not the k() shell function) so $! is kubectl's PID and cleanup kills it.
kubectl --context "$CTX" -n monitoring port-forward "svc/$PROM_SVC" 9090:9090 >"$STATE_DIR/demo-portforward.log" 2>&1 &
PF_PID=$!
for i in $(seq 1 60); do
  if curl -fsS "$PROM_LOCAL/-/ready" >/dev/null 2>&1; then break; fi
  if [ "$i" = 60 ]; then echo "Prometheus port-forward never became ready" >&2; exit 1; fi
  sleep 1
done
"$STATE_DIR/trafficgen" seed \
  --remote-write "$PROM_LOCAL/api/v1/write" --from 1h --step 15s \
  --base "$BASE" --amp "$AMP" --period "$PERIOD" --noise "$NOISE" --seed 1 --epoch "$EPOCH" \
  --series namespace=shop,app=frontend \
  --series namespace=shop,app=api \
  --series namespace=shop,app=worker
cleanup; PF_PID=""

step "7/9 start trafficgen run (same pattern and epoch)"
sed "s|\${EPOCH}|$EPOCH|g" demo/manifests/trafficgen.yaml | k apply -f -
k -n shop rollout status deployment/trafficgen --timeout=2m

step "8/9 apply the demo TidePolicy"
k apply -f config/samples/demo-policy.yaml

step "9/9 up"
cat <<MSG

Peak load is expected at ${PEAK} (about 10 minutes after the demo came up; it starts in the trough).
Watch Tidewatch prescale the frontend ahead of it:

  kubectl --context ${CTX} get tideworkloads -n shop -w
  kubectl --context ${CTX} get deploy -n shop -w

Prometheus UI:

  kubectl --context ${CTX} -n monitoring port-forward svc/${PROM_SVC} 9090:9090
  then open http://localhost:9090 and graph tidewatch_capacity_ratio{workload="frontend-deploy"}

Tear down with: make demo-down
MSG
