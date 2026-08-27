#!/usr/bin/env bash
# GATE METRICS-FAILURE -- prove LoadBalancer reconcile FAILURES surface in
# ALERTABLE Prometheus metrics (not just logs / status conditions), end to end on
# live Cloudflare.
#
# WHY: alerts key off metrics. The other live gates (gate-map-replace/-nested-merge/-pool-weights/-geo-maps, gate-steering-policies..gate-monitor)
# assert the CF round-trip + Kubernetes conditions + logs, but none scrape
# /metrics -- so nothing proves a failing reconcile actually moves the numbers an
# alert fires on. This gate closes that gap.
#
# WHAT IT PROVES (all confirmed in code -- this gate is the live check):
#   - A failing LB reconcile calls setError(...,"CreateFailed"/"UpdateFailed",...),
#     and lbReadyState maps any Ready=False non-wait / non-dryrun reason to
#     state="error" (internal/controller/loadbalancing_state.go). The
#     loadbalancers gauge is recomputed from the full CR set every reconcile
#     (recomputeStateGauge), so:
#       cf_edge_operator_loadbalancers{zone_cr="livecf",state="error"} >= 1
#     while a broken LB exists.
#   - recordCFCall records the CF error's HTTP status on
#       cf_edge_operator_api_errors_by_code_total{resource="loadbalancer",
#                                                  operation="create"|"update",
#                                                  status_code="400"}
#     which increments on the CF 1002 (a validation error -> HTTP 400).
#   - On RECOVERY the error gauge returns to 0 for the zone (it is recomputed
#     each reconcile, so it clears -- it does NOT latch).
#
# THE DETERMINISTIC FAILURE: a steeringPolicy=proximity LB over two pools that
# have NO latitude/longitude. Proximity picks the closest pool by geographic
# distance, so Cloudflare rejects the create with error 1002 ("pools missing
# Latitude/Longitude"). This is a runtime CF rejection, not an admission
# rejection -- no CEL rule can express the cross-resource "proximity needs pool
# coords" constraint, so the LB is admitted and fails at the CF create call ->
# setError -> state="error". RECOVERY adds coordinates to both pools so the same
# LB's next create succeeds and the gauge clears back to 0.
#
# What it checks (self-cleaning: teardown deletes the LB, then the pools):
#   1. BASELINE: scrape /metrics; record the loadbalancer create+update
#      api_errors_by_code_total (may be 0 / absent) and the livecf error gauge.
#   2. FAIL: apply 2 pools WITHOUT coords + a proximity LB; poll the LB CR to
#      Ready=False (reason CreateFailed or UpdateFailed).
#   3. ASSERT (the core alerting proof): poll /metrics until
#      loadbalancers{zone_cr="livecf",state="error"} >= 1, AND the loadbalancer
#      create+update api_errors_by_code_total{status_code="400"} increased vs the
#      baseline.
#   4. RECOVER: add latitude/longitude to both pools so proximity succeeds; wait
#      the LB CR to Ready=True, then poll /metrics until
#      loadbalancers{zone_cr="livecf",state="error"} == 0 while
#      loadbalancers{zone_cr="livecf",state="ready"} >= 1 (a live, positive
#      companion series -- so the 0 means "recomputed to zero", never a dead
#      scrape). This proves the gauge clears, not latches.
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT on the LB binary and RUNNING against
#     kind-cf-lb in load-balancing mode (`./run-operator.sh lb`, drift-interval
#     ~15s). run-operator.sh runs it via `make dev-run`, which exposes the metrics
#     endpoint as plain HTTP with no auth (--metrics-secure=false
#     --metrics-bind-address=:8080); the gate scrapes LBKIT_METRICS_URL
#     (default http://127.0.0.1:8080/metrics).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - The TRAFFIC-STEERING ADD-ON is enabled on the Cloudflare account: proximity
#     is add-on gated (like geo). Without it the FAILURE leg still holds (the
#     create fails with a different message -> still setError -> still
#     state="error" + an api_errors increment), but the RECOVERY leg (proximity
#     succeeding once coords are added) needs the add-on. gate-geo-maps already requires
#     it, so it is expected to be present in the same environment.
#   - .env supplies the four CF_* identifiers (never printed).
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-gate-metrics-lb
POOLA=livecf-gate-metrics-pa
POOLB=livecf-gate-metrics-pb
HOST="lb-gate-metrics.${CF_ZONE_NAME}"

# How long to wait for the LB to reach the failed state after apply (the first
# create fails within a couple of seconds, but pools must resolve first).
FAIL_TIMEOUT="${LBKIT_FAIL_TIMEOUT:-120}"
# How long to wait for the LB to recover to Ready after coords are added (rides a
# few of the operator's ~15s self-requeues plus the pool update landing in CF).
RECOVER_TIMEOUT="${LBKIT_RECOVER_TIMEOUT:-180}"
# How long to wait for a /metrics scrape to reflect a state change. The gauge is
# recomputed each reconcile from the (cache-backed) CR list, which can lag the
# status write by a reconcile, so poll rather than scrape once.
METRIC_TIMEOUT="${LBKIT_METRIC_TIMEOUT:-90}"

cleanup() {
  # Delete the LB first (it references the pools), then the pools. Best-effort:
  # the operator clears the Cloudflare-side objects via finalizers, but cleanup
  # must never abort or hang the script (--ignore-not-found + a bounded --timeout,
  # errors swallowed).
  kc delete loadbalancer "${LB}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
  kc delete loadbalancerpool "${POOLA}" "${POOLB}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers -------------------------------------------------------------
# lb_ready / lb_reason echo the LB CR's Ready condition status and reason.
lb_ready() {
  kc get loadbalancer "${LB}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}
lb_reason() {
  kc get loadbalancer "${LB}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true
}

# lb_error_gauge / lb_ready_gauge read the livecf loadbalancers gauge for the
# error / ready state out of a captured /metrics text (0 when the series is absent).
lb_error_gauge() { metric_value "$1" cf_edge_operator_loadbalancers 'zone_cr="livecf"' 'state="error"'; }
lb_ready_gauge() { metric_value "$1" cf_edge_operator_loadbalancers 'zone_cr="livecf"' 'state="ready"'; }

# lb_err_count sums the loadbalancer create+update api-error counter at
# status_code="400" (the CF 1002 -> HTTP 400 path) out of a captured /metrics text.
# Either operation's series may be absent (reads 0); the CF 1002 on a first create
# lands as operation="create", but summing both covers the update path too.
lb_err_count() { # $1 = metrics text
  local t="$1" c u
  c="$(metric_value "$t" cf_edge_operator_api_errors_by_code_total 'resource="loadbalancer"' 'operation="create"' 'status_code="400"')"
  u="$(metric_value "$t" cf_edge_operator_api_errors_by_code_total 'resource="loadbalancer"' 'operation="update"' 'status_code="400"')"
  awk -v c="${c}" -v u="${u}" 'BEGIN{print c+u}'
}

# dump_lb_api_errors prints every loadbalancer api-error sample from a captured
# /metrics text (used on a status_code mismatch so the real HTTP code is visible).
dump_lb_api_errors() { # $1 = metrics text
  grep -E '^cf_edge_operator_api_errors_by_code_total\{' <<<"$1" \
    | grep -F 'resource="loadbalancer"' >&2 || echo "     (no loadbalancer api-error samples present)" >&2
}

# apply_pools: mode=plain -> 2 pools, 1 origin each, NO coords (the failing
# input); mode=geo -> the same 2 pools WITH latitude/longitude (the recovery
# input, so proximity can succeed). 2 origins total (the entry-tier origin cap).
apply_pools() { # $1 = plain | geo
  case "$1" in
  plain)
    kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLA}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  origins:
    - name: o1
      address: origin-a.${CF_ZONE_NAME}
---
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLB}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  origins:
    - name: o1
      address: origin-b.${CF_ZONE_NAME}
EOF
    ;;
  geo)
    kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLA}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  latitude: "37.7749"
  longitude: "-122.4194"
  origins:
    - name: o1
      address: origin-a.${CF_ZONE_NAME}
---
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLB}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  latitude: "40.7128"
  longitude: "-74.0060"
  origins:
    - name: o1
      address: origin-b.${CF_ZONE_NAME}
EOF
    ;;
  esac
}

# apply_lb applies the proximity LB over both pools. With the pools missing
# coords this is the deterministic CF 1002; once the pools have coords the same
# object's create succeeds.
apply_lb() {
  kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancer
metadata:
  name: ${LB}
  namespace: ${LBKIT_NS}
spec:
  zoneRef:
    name: livecf
  hostname: ${HOST}
  steeringPolicy: proximity
  defaultPoolRefs:
    - name: ${POOLA}
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
EOF
}

# ---- (1) baseline --------------------------------------------------------
info "Step 1: baseline scrape of ${LBKIT_METRICS_URL}"
base_text="$(metrics_scrape)" || exit 1
base_err="$(lb_err_count "${base_text}")"
base_gauge="$(lb_error_gauge "${base_text}")"
info "  baseline loadbalancer create+update errors (status_code=400): ${base_err}"
info "  baseline loadbalancers{zone_cr=livecf,state=error}:            ${base_gauge}"

# ---- (2) deterministic failure -------------------------------------------
echo
info "Step 2: apply 2 pools WITHOUT lat/long + a steeringPolicy=proximity LB (CF 1002: pools missing Latitude/Longitude)"
apply_pools plain
wait_ready loadbalancerpool "${POOLA}"
wait_ready loadbalancerpool "${POOLB}"
apply_lb

info "waiting up to ${FAIL_TIMEOUT}s for the LB to reach Ready=False (CreateFailed / UpdateFailed)"
fail_reason=""
for ((i = 0; i < FAIL_TIMEOUT; i += 3)); do
  st="$(lb_ready)"
  rs="$(lb_reason)"
  if [[ "${st}" == "False" && ( "${rs}" == "CreateFailed" || "${rs}" == "UpdateFailed" ) ]]; then
    fail_reason="${rs}"
    break
  fi
  sleep 3
done
if [[ -z "${fail_reason}" ]]; then
  bad "GATE METRICS-FAILURE(2): the LB did not reach Ready=False with CreateFailed/UpdateFailed within ${FAIL_TIMEOUT}s."
  echo "     last Ready status='$(lb_ready)' reason='$(lb_reason)'" >&2
  kc get loadbalancer "${LB}" -o yaml 2>/dev/null | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
ok "GATE METRICS-FAILURE(2): LB is Ready=False (reason ${fail_reason}) -- the proximity create was rejected by Cloudflare."
lb_pause "after applying the pools + proximity LB (create fails: pools missing lat/long)"

# ---- (3) core alerting proof --------------------------------------------
echo
info "Step 3: assert the failure moved the alertable metrics"
info "  poll up to ${METRIC_TIMEOUT}s for loadbalancers{zone_cr=livecf,state=error} >= 1"
fail_text=""
for ((i = 0; i < METRIC_TIMEOUT; i += 3)); do
  txt="$(metrics_scrape)" || { sleep 3; continue; }
  g="$(lb_error_gauge "${txt}")"
  if num_ge "${g}" 1; then
    fail_text="${txt}"
    break
  fi
  sleep 3
done
if [[ -z "${fail_text}" ]]; then
  bad "GATE METRICS-FAILURE(3): loadbalancers{zone_cr=livecf,state=error} never reached >= 1 within ${METRIC_TIMEOUT}s."
  echo "     last observed error gauge: $(lb_error_gauge "$(metrics_scrape || true)")" >&2
  exit 1
fi
ok "GATE METRICS-FAILURE(3): loadbalancers{zone_cr=livecf,state=error} = $(lb_error_gauge "${fail_text}") (>= 1)."

now_err="$(lb_err_count "${fail_text}")"
info "  loadbalancer create+update errors (status_code=400): baseline=${base_err} now=${now_err}"
if num_gt "${now_err}" "${base_err}"; then
  ok "GATE METRICS-FAILURE(3): api_errors_by_code_total{resource=loadbalancer,operation=create|update,status_code=400} increased (${base_err} -> ${now_err})."
else
  bad "GATE METRICS-FAILURE(3): the loadbalancer create+update 400-error counter did NOT increase (baseline=${base_err}, now=${now_err})."
  echo "     If the LB failed with a status_code OTHER than 400, this assertion is wrong for this CF error; the live loadbalancer api-error samples are:" >&2
  dump_lb_api_errors "${fail_text}"
  exit 1
fi

# ---- (4) recovery: gauge clears, not latches -----------------------------
echo
info "Step 4: recover -- add latitude/longitude to both pools so proximity succeeds"
apply_pools geo
wait_reconciled loadbalancerpool "${POOLA}"
wait_reconciled loadbalancerpool "${POOLB}"
if ! wait_ready loadbalancer "${LB}" "${RECOVER_TIMEOUT}"; then
  bad "GATE METRICS-FAILURE(4): the LB did not recover to Ready within ${RECOVER_TIMEOUT}s after coords were added."
  echo "     Ready status='$(lb_ready)' reason='$(lb_reason)'. If the create still fails, proximity may be" >&2
  echo "     blocked on this account (the traffic-steering add-on is required -- see PRECONDITIONS)." >&2
  exit 1
fi
ok "GATE METRICS-FAILURE(4): LB recovered to Ready=True after coords were added."
lb_pause "after adding lat/long to both pools (proximity LB now succeeds)"

info "  poll up to ${METRIC_TIMEOUT}s for loadbalancers{zone_cr=livecf,state=error} == 0 with state=ready >= 1"
cleared=0
last_e="" last_r=""
for ((i = 0; i < METRIC_TIMEOUT; i += 3)); do
  txt="$(metrics_scrape)" || { sleep 3; continue; }
  e="$(lb_error_gauge "${txt}")"
  r="$(lb_ready_gauge "${txt}")"
  last_e="${e}"
  last_r="${r}"
  # error must be exactly 0, and the ready companion series must be >= 1 so the 0
  # is a live recomputed value, never a dead scrape reading everything as absent.
  if awk -v e="${e}" 'BEGIN{exit !(e==0)}' && num_ge "${r}" 1; then
    cleared=1
    break
  fi
  sleep 3
done
if [[ "${cleared}" -ne 1 ]]; then
  bad "GATE METRICS-FAILURE(4): error gauge did not clear -- last state=error=${last_e:-<none>}, state=ready=${last_r:-<none>} (want error==0 with ready>=1)."
  exit 1
fi
ok "GATE METRICS-FAILURE(4): loadbalancers{zone_cr=livecf,state=error} cleared to 0 (state=ready=${last_r}) -- the gauge is recomputed, it does not latch."

echo
ok "GATE METRICS-FAILURE: converged -- a failing LB reconcile drove state=error >= 1 and the api_errors counter up, and recovery cleared state=error back to 0."
echo "     Record in the PR: LB reconcile failures are alertable -- cf_edge_operator_loadbalancers{state=\"error\"} and" >&2
echo "     cf_edge_operator_api_errors_by_code_total{resource=\"loadbalancer\"} both move on a CF 1002 and clear on recovery (verified on live CF)." >&2
