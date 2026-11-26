#!/usr/bin/env bash
# Real-cluster validation: runs the flash-sale cascade on a kind cluster under real HPA, real KEDA
# and the real Tidewatch operator, one config at a time, and scores each from Prometheus.
#
#   hack/bench-kind.sh hpa,keda,tidewatch-baseline[,tidewatch-agent]     (or: make bench-kind CONFIGS=...)
#
# Configs run sequentially on one cluster ("tidewatch-bench", separate from the demo's). Before each
# config the shop namespace is deleted and recreated at 3 replicas, every autoscaler object is
# removed, KEDA is uninstalled unless the config is keda, and the Prometheus TSDB is wiped, so no
# config sees another's history. Tidewatch and KEDA/HPA never manage the same workload at once.
#
# Timeline per config (override with the env vars below), T0 = start of the measured window:
#   T0 + SURGE_AFTER_MIN                       4x surge on the frontend, RAMP ramp, held HOLD_MIN
#   surge - NOTE_LEAD_MIN                      the note is published
#   surge + RAMP + HOLD_MIN + TAIL_MIN         end of the window; metrics are read from Prometheus
# tidewatch-agent needs ANTHROPIC_API_KEY (never printed) and is skipped with a message when unset.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER=tidewatch-bench
CTX="kind-${CLUSTER}"
KPS_VERSION=94.0.0
KEDA_VERSION=2.21.1
METRICS_SERVER_VERSION=3.13.1
OP_IMG=tidewatch:dev
APP_IMG=tidewatch-demo-app:dev
TG_IMG=tidewatch-trafficgen:dev
PROM_SVC=kps-kube-prometheus-stack-prometheus
PROM_PORT=19090
PROM="http://127.0.0.1:${PROM_PORT}"
OP_DEPLOY=tidewatch-controller-manager
POLICY_NS=tidewatch-system
CAPACITY=50
HEADROOM=0.8
MULT=4
SEED=1
SURGE_AFTER_MIN="${SURGE_AFTER_MIN:-25}"
NOTE_LEAD_MIN="${NOTE_LEAD_MIN:-20}"
RAMP_MIN="${RAMP_MIN:-2}"
HOLD_MIN="${HOLD_MIN:-15}"
TAIL_MIN="${TAIL_MIN:-8}"
LEAD_IN_SEC="${LEAD_IN_SEC:-90}"
OUT_DIR="${BENCH_OUT:-$ROOT/bin/bench-kind}"
RESULTS="$OUT_DIR/results.csv"
STATE_DIR="$ROOT/bin"
ALL_CONFIGS="hpa keda tidewatch-baseline tidewatch-agent"

step() { printf '\n==> [%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
k() { kubectl --context "$CTX" "$@"; }
fmt() { date -u -r "$1" "+$2" 2>/dev/null || date -u -d "@$1" "+$2"; }
iso() { fmt "$1" '%Y-%m-%dT%H:%M:%SZ'; }

for tool in kind helm kubectl docker python3 go make jq curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing required tool: $tool" >&2; exit 1; }
done
if [ "$NOTE_LEAD_MIN" -gt "$SURGE_AFTER_MIN" ]; then
  echo "NOTE_LEAD_MIN ($NOTE_LEAD_MIN) cannot exceed SURGE_AFTER_MIN ($SURGE_AFTER_MIN)" >&2; exit 1
fi

PF_PID=""
pf_stop() { if [ -n "$PF_PID" ]; then kill "$PF_PID" >/dev/null 2>&1 || true; wait "$PF_PID" 2>/dev/null || true; PF_PID=""; fi; }
trap pf_stop EXIT
# Port-forward Prometheus to :$PROM_PORT (not 9090, which the demo may hold).
pf_start() {
  pf_stop
  kubectl --context "$CTX" -n monitoring port-forward "svc/$PROM_SVC" "${PROM_PORT}:9090" >"$OUT_DIR/portforward.log" 2>&1 &
  PF_PID=$!
  for i in $(seq 1 60); do
    if curl -fsS "$PROM/-/ready" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "Prometheus port-forward never became ready" >&2; return 1
}

sleep_until() { while [ "$(date +%s)" -lt "$1" ]; do sleep 5; done; }

# ---------------------------------------------------------------------------------------------
# One-time setup (idempotent).
# ---------------------------------------------------------------------------------------------
setup_cluster() {
  step "cluster '${CLUSTER}'"
  if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
    echo "cluster exists, skipping create"
  else
    kind create cluster --name "$CLUSTER" --config demo/bench/kind.yaml
  fi
  kubectl config use-context "$CTX" >/dev/null

  step "metrics-server ${METRICS_SERVER_VERSION} (the HPA's CPU source)"
  helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/ >/dev/null 2>&1 || true
  helm repo update metrics-server >/dev/null
  helm upgrade --install metrics-server metrics-server/metrics-server --version "$METRICS_SERVER_VERSION" \
    --kube-context "$CTX" --namespace kube-system --set 'args={--kubelet-insecure-tls}' --wait --timeout 5m

  step "kube-prometheus-stack ${KPS_VERSION}"
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
  helm repo update prometheus-community >/dev/null
  helm upgrade --install kps prometheus-community/kube-prometheus-stack --version "$KPS_VERSION" \
    --kube-context "$CTX" --namespace monitoring --create-namespace \
    -f demo/prometheus-values.yaml -f demo/bench/prometheus-values.yaml --wait --timeout 8m
  k -n monitoring rollout status "statefulset/prometheus-kps-kube-prometheus-stack-prometheus" --timeout=5m
  k apply -f demo/bench/rules.yaml

  step "build and load images"
  make docker-build IMG="$OP_IMG"
  docker build --target app -t "$APP_IMG" demo
  docker build --target trafficgen -t "$TG_IMG" demo
  for img in "$OP_IMG" "$APP_IMG" "$TG_IMG"; do kind load docker-image --name "$CLUSTER" "$img"; done
  mkdir -p "$STATE_DIR"
  (cd demo && go build -o "$STATE_DIR/trafficgen" ./trafficgen)
  make build-bench

  step "operator"
  make install deploy IMG="$OP_IMG"
  # Same demo-only patch as hack/demo.sh: plain-HTTP /metrics so a vanilla ServiceMonitor can scrape it.
  k -n "$POLICY_NS" patch deployment "$OP_DEPLOY" --type=json -p '[
    {"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--metrics-secure=false"},
    {"op":"add","path":"/spec/template/spec/containers/0/imagePullPolicy","value":"IfNotPresent"}]'
  k -n "$POLICY_NS" rollout status "deployment/$OP_DEPLOY" --timeout=3m
  k apply -f demo/operator/servicemonitor.yaml
}

# Point the operator at the Anthropic provider (agent on) or at no provider (agent off), then restart
# it so its counters, cooldowns and overrides start from zero.
operator_reset() {
  local agent="$1" args
  args="$(k -n "$POLICY_NS" get deployment "$OP_DEPLOY" -o json |
    jq -c '.spec.template.spec.containers[0].args | map(select(startswith("--llm-") | not))')"
  if [ "$agent" = on ]; then
    args="$(jq -c '. + ["--llm-provider=anthropic"]' <<<"$args")"
    if [ -n "${TIDEWATCH_LLM_MODEL:-}" ]; then
      case "$TIDEWATCH_LLM_MODEL" in
        *[!a-z0-9.-]*) echo "TIDEWATCH_LLM_MODEL has unexpected characters" >&2; exit 1 ;;
      esac
      args="$(jq -c --arg m "--llm-model=$TIDEWATCH_LLM_MODEL" '. + [$m]' <<<"$args")"
    fi
    # The key goes in over stdin so it never appears in a process listing or in this script's output.
    printf '%s' "$ANTHROPIC_API_KEY" | k -n "$POLICY_NS" create secret generic tidewatch-llm \
      --from-file=apiKey=/dev/stdin --dry-run=client -o yaml | k apply -f - >/dev/null
  fi
  k -n "$POLICY_NS" patch deployment "$OP_DEPLOY" --type=json \
    -p "[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/args\",\"value\":$args}]"
  k -n "$POLICY_NS" rollout restart "deployment/$OP_DEPLOY" >/dev/null
  k -n "$POLICY_NS" rollout status "deployment/$OP_DEPLOY" --timeout=3m
}

# ---------------------------------------------------------------------------------------------
# Per-config reset.
# ---------------------------------------------------------------------------------------------
reset_env() {
  local cfg="$1"
  step "reset: remove every autoscaler, recreate the shop namespace"
  k -n "$POLICY_NS" delete tidepolicy --all --ignore-not-found --wait --timeout=2m
  # ScaledObjects first, while KEDA is still there to clear their finalizers.
  if k get crd scaledobjects.keda.sh >/dev/null 2>&1; then
    k -n shop delete scaledobject --all --ignore-not-found --wait --timeout=2m || true
  fi
  k -n shop delete hpa --all --ignore-not-found --wait --timeout=2m || true
  k delete namespace shop --ignore-not-found --wait --timeout=4m
  if [ "$cfg" != keda ] && helm status keda --kube-context "$CTX" -n keda >/dev/null 2>&1; then
    step "uninstall KEDA (not this config)"
    helm uninstall keda --kube-context "$CTX" -n keda --wait
  fi
  if [ "$cfg" = keda ]; then
    step "install KEDA ${KEDA_VERSION}"
    helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
    helm repo update kedacore >/dev/null
    helm upgrade --install keda kedacore/keda --version "$KEDA_VERSION" \
      --kube-context "$CTX" --namespace keda --create-namespace --wait --timeout 6m
  fi
  # A fresh TSDB: forecasts and KEDA's queries must not see the previous config's surge.
  step "wipe Prometheus"
  pf_start
  curl -fsS -X POST "$PROM/api/v1/admin/tsdb/delete_series" --data-urlencode 'match[]={__name__=~".+"}'
  curl -fsS -X POST "$PROM/api/v1/admin/tsdb/clean_tombstones"
  pf_stop
}

# ---------------------------------------------------------------------------------------------
# The scenario.
# ---------------------------------------------------------------------------------------------
apply_scaledobjects() { # $1 = cron replicas map "app=N,..." (empty: Prometheus trigger only)
  local app tmpl rep
  for app in frontend api worker; do
    rep=""
    case ",${1:-}," in *",$app="*) rep="$(sed -e "s/.*,$app=//" -e 's/,.*//' <<<",$1")" ;; esac
    tmpl="$(sed -e "s|\${APP}|$app|g" -e "s|\${THRESHOLD}|$(awk "BEGIN{print $CAPACITY*$HEADROOM}")|g" \
      demo/bench/scaledobject.yaml.tmpl)"
    if [ -z "$rep" ]; then
      tmpl="$(sed '/^#CRON-BEGIN$/,/^#CRON-END$/d' <<<"$tmpl")"
    else
      tmpl="$(sed -e "s|\${CRON_START}|$CRON_START|" -e "s|\${CRON_END}|$CRON_END|" -e "s|\${CRON_REPLICAS}|$rep|" \
        -e '/^#CRON-/d' <<<"$tmpl")"
    fi
    k apply -f - <<<"$tmpl"
  done
}

# The human reads the note and adds the cron trigger: desiredReplicas = ceil(MULT x pre-event peak /
# (capacity x headroom)), where the pre-event peak is the max of the last hour's load.
publish_note_keda() {
  local app peak n map=""
  pf_start
  for app in frontend api worker; do
    peak="$(curl -fsS "$PROM/api/v1/query" --data-urlencode \
      "query=max_over_time(sum(rate(http_requests_total{namespace=\"shop\",app=\"$app\"}[1m]))[1h:15s])" |
      jq -r '.data.result[0].value[1] // empty')"
    [ -n "$peak" ] || { echo "no pre-event peak for $app" >&2; return 1; }
    n="$(awk "BEGIN{x=$MULT*$peak/($CAPACITY*$HEADROOM); c=int(x); if (c<x) c++; print c}")"
    echo "cron for $app: pre-event peak ${peak} rps -> desiredReplicas $n"
    map="$map,$app=$n"
  done
  pf_stop
  apply_scaledobjects "${map#,}"
}

publish_note_tidewatch() {
  k -n "$POLICY_NS" patch tidepolicy shop --type=merge \
    -p "$(jq -nc --arg n "$NOTE" '{spec:{agent:{context:{notes:$n}}}}')"
}

replica_log() { # one line per minute: ready/desired per workload
  local line="$(date -u +%H:%M:%S)" w
  for w in frontend api worker; do
    line="$line $w=$(k -n shop get deploy "$w" -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || echo '?')"
  done
  echo "$line" >>"$OUT_DIR/$CFG-replicas.log"
}

run_config() {
  CFG="$1"
  local agent=off
  [ "$CFG" = tidewatch-agent ] && agent=on
  mkdir -p "$OUT_DIR"
  : >"$OUT_DIR/$CFG-replicas.log"
  step "config $CFG"
  reset_env "$CFG"
  operator_reset "$agent"

  step "shop apps"
  k apply -f demo/manifests/namespace.yaml
  kubectl --context "$CTX" kustomize --load-restrictor=LoadRestrictionsNone demo/bench | k apply -f -
  for d in frontend api worker; do k -n shop rollout status "deployment/$d" --timeout=5m; done
  # Let kube-state-metrics, the recording rules and the operator's scrape see the new pods.
  sleep 45

  T0=$(( $(date +%s) + LEAD_IN_SEC ))
  SPIKE=$(( (T0 + SURGE_AFTER_MIN * 60 + 59) / 60 * 60 ))
  NOTE_AT=$(( SPIKE - NOTE_LEAD_MIN * 60 ))
  END=$(( SPIKE + (RAMP_MIN + HOLD_MIN + TAIL_MIN) * 60 ))
  CRON_START="$(fmt $(( SPIKE - 15 * 60 )) '%-M %-H * * *')"
  CRON_END="$(fmt $(( SPIKE + 60 * 60 )) '%-M %-H * * *')"
  NOTE="Flash sale at $(fmt "$SPIKE" '%H:%M') UTC, expect ~${MULT}× traffic on frontend"
  echo "T0=$(iso $T0) surge=$(iso $SPIKE) note=$(iso $NOTE_AT) end=$(iso $END)"
  echo "note: $NOTE"

  # A distinct instance label per run: delete_series hides the previous run's seed, but its samples
  # stay in the TSDB head, and re-seeding the same series would be refused as duplicate samples.
  step "seed 1h of history (the same pattern and epoch as the traffic)"
  pf_start
  "$STATE_DIR/trafficgen" seed --remote-write "$PROM/api/v1/write" --from 1h --step 15s \
    --base 60 --amp 20 --period 20m --noise 0.05 --seed "$SEED" --epoch "$(iso $T0)" \
    --series "namespace=shop,app=frontend,instance=seed-$T0" --series "namespace=shop,app=api,instance=seed-$T0" \
    --series "namespace=shop,app=worker,instance=seed-$T0"
  pf_stop

  step "enable the autoscaler: $CFG"
  case "$CFG" in
    hpa) k apply -f demo/bench/hpa.yaml ;;
    keda) apply_scaledobjects "" ;;
    tidewatch-*)
      k apply -f demo/bench/tidepolicy.yaml
      if [ "$agent" = on ]; then
        k -n "$POLICY_NS" patch tidepolicy shop --type=merge -p '{"spec":{"agent":{"enabled":true,"approval":"Auto"}}}'
      fi ;;
  esac

  step "traffic"
  sed -e "s|\${EPOCH}|$(iso $T0)|g" -e "s|\${SPIKE_AT}|$(iso $SPIKE)|g" \
      -e "s|\${RAMP}|${RAMP_MIN}m|g" -e "s|\${HOLD}|${HOLD_MIN}m|g" demo/bench/trafficgen.yaml | k apply -f -
  k -n shop rollout status deployment/trafficgen --timeout=2m

  local published=false
  while [ "$(date +%s)" -lt "$END" ]; do
    if [ "$published" = false ] && [ "$(date +%s)" -ge "$NOTE_AT" ]; then
      step "publish the note"
      case "$CFG" in keda) publish_note_keda ;; tidewatch-*) publish_note_tidewatch ;; *) echo "no reader of notes in $CFG" ;; esac
      published=true
    fi
    replica_log
    sleep 30
  done
  replica_log

  step "collect metrics for $CFG"
  sleep 20 # one more rule evaluation after the window closes
  pf_start
  local extra=()
  if [ "$agent" = on ]; then extra=(--agent --model "${TIDEWATCH_LLM_MODEL:-claude-haiku-5-5}"); fi
  "$ROOT/bin/tidewatch-bench" kind-collect --prom "$PROM" --out "$RESULTS" --config "$CFG" \
    --start "$(iso $T0)" --end "$(iso $END)" --capacity "$CAPACITY" --headroom "$HEADROOM" --seed "$SEED" \
    ${extra[@]+"${extra[@]}"}
  pf_stop
  k -n "$POLICY_NS" logs "deployment/$OP_DEPLOY" --tail=2000 >"$OUT_DIR/$CFG-operator.log" 2>&1 || true
  k -n shop get deploy -o wide >"$OUT_DIR/$CFG-final-deployments.txt" 2>&1 || true
}

# ---------------------------------------------------------------------------------------------
# Entry point. Each config runs in its own bash, so a failure in one does not stop the rest.
# ---------------------------------------------------------------------------------------------
if [ "${1:-}" = --one ]; then
  mkdir -p "$OUT_DIR"
  run_config "$2"
  exit 0
fi

CONFIGS="${1:-${CONFIGS:-hpa,keda,tidewatch-baseline}}"
mkdir -p "$OUT_DIR"
run_list=()
for c in ${CONFIGS//,/ }; do
  case " $ALL_CONFIGS " in *" $c "*) ;; *) echo "unknown config: $c (want: ${ALL_CONFIGS// /, })" >&2; exit 1 ;; esac
  if [ "$c" = tidewatch-agent ] && [ -z "${ANTHROPIC_API_KEY:-}" ]; then
    echo "SKIP tidewatch-agent: ANTHROPIC_API_KEY is not set (export it and re-run with CONFIGS=tidewatch-agent)"
    continue
  fi
  run_list+=("$c")
done
[ ${#run_list[@]} -gt 0 ] || { echo "nothing to run" >&2; exit 1; }

setup_cluster
failed=()
for c in "${run_list[@]}"; do
  if ! bash "${BASH_SOURCE[0]}" --one "$c"; then
    echo "FAILED config: $c" >&2
    failed+=("$c")
  fi
done
step "done: results in $RESULTS"
[ ${#failed[@]} -eq 0 ] || { echo "failed configs: ${failed[*]}" >&2; exit 1; }
