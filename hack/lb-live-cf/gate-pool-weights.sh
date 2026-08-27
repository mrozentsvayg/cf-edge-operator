#!/usr/bin/env bash
# GATE pool-weights -- LoadBalancer pool_weights, end to end on live Cloudflare.
#
# The operator folds the per-pool weight into each defaultPoolRefs entry (plus an
# optional top-level spec.defaultWeight); both map to Cloudflare's per-LB
# random_steering.pool_weights / random_steering.default_weight, and both take
# effect only under a weighted steering policy (random /
# least_outstanding_requests / least_connections -- see weightedSteeringActive
# in internal/controller/loadbalancer_controller.go). This is the BASE tier: only
# steeringPolicy=random exercises weights without the traffic-steering add-on, so
# that is what this gate uses. 2 pools, 1 origin each = 2 origins TOTAL (the hard
# per-account origin cap on the entry LB tier).
#
# What it checks:
#   (i)   CEL weight-gate (admission only -- no operator/CF needed): the apiserver
#         REJECTS an LB with steeringPolicy=off that carries a pool weight, whether
#         the weight is on a defaultPoolRefs[].weight or on spec.defaultWeight. Run
#         via `kubectl apply --dry-run=server` so no object is ever persisted.
#   (ii)  round-trip: apply 2 pools + a weighted LB (pa:0.7, pb:0.3, steering=random),
#         wait Ready, read Cloudflare back, assert random_steering.pool_weights ==
#         { <cf-pa>:0.7, <cf-pb>:0.3 } EXACTLY (folded model -> exact-match, no echo).
#   (iii) drift:
#         (a) PATCH Cloudflare's pool_weights out of band to a WRONG value
#             (pa:0.1); the operator must RESTORE 0.7/0.3 within a drift interval.
#         (b) edit the LB CR to DROP pb's weight (pb stays a default member); the
#             operator must null pb out of Cloudflare's pool_weights -> { <cf-pa>:0.7 }
#             and then CONVERGE (LB stays Ready, pool_weights stable, no drift-loop).
#
# PRECONDITIONS (this gate does NOT build/install/start anything):
#   - The operator is already REBUILT with the load-balancing controllers and
#     RUNNING against kind-cf-lb in load-balancing mode
#     (`./run-operator.sh lb`, drift-interval 15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - .env supplies the four CF_* identifiers (never printed).
#
# Self-cleaning: a trap on EXIT deletes the LB + both pools (the operator removes
# the Cloudflare-side objects via finalizers). The CEL step (i) persists nothing.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-gate-pool-weights-lb
POOLA=livecf-gate-pool-weights-pa
POOLB=livecf-gate-pool-weights-pb
HOST="lb-gate-pool-weights.${CF_ZONE_NAME}"
CEL_HOST="lb-gate-pool-weights-cel.${CF_ZONE_NAME}"
# Drift correction rides the operator's self-requeue (run-operator.sh sets
# --drift-interval=15s); allow several intervals before calling it a failure.
DRIFT_TIMEOUT="${LBKIT_DRIFT_TIMEOUT:-90}"

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
# cf_pw echoes the LB's random_steering.pool_weights as compact JSON ({} if unset).
cf_pw() { cf_get_lb "$1" | jq -c '.random_steering.pool_weights // {}'; }

# pw_equals returns 0 iff the LB's live pool_weights equals the expected JSON
# object (semantic compare -- key order and numeric form do not matter).
pw_equals() { # $1 = lb cf-id, $2 = expected JSON object
  local got
  got="$(cf_pw "$1")" || return 1
  jq -e -n --argjson got "${got}" --argjson exp "$2" '$got == $exp' >/dev/null 2>&1
}

# poll_pw polls until pw_equals holds or the timeout (seconds) elapses.
poll_pw() { # $1 = lb cf-id, $2 = expected JSON object, $3 = timeout
  local timeout="$3" i
  for ((i = 0; i < timeout; i += 3)); do
    pw_equals "$1" "$2" && return 0
    sleep 3
  done
  return 1
}

# lb_ready echoes the LB CR's Ready condition status (True/False/empty).
lb_ready() {
  kc get loadbalancer "${LB}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}

# apply_cel_lb server-dry-run applies an LB that pairs steeringPolicy=off with a
# weight (so the CEL weight-gate must reject it). Nothing is persisted. `off` is
# quoted because YAML would otherwise coerce the bare token to boolean false.
apply_cel_lb() { # $1 = poolweight | defaultweight
  case "$1" in
  poolweight)
    kctl apply --dry-run=server -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancer
metadata:
  name: ${LB}-celreject
  namespace: ${LBKIT_NS}
spec:
  zoneRef:
    name: livecf
  hostname: ${CEL_HOST}
  steeringPolicy: "off"
  defaultPoolRefs:
    - name: ${POOLA}
      weight: "0.7"
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
EOF
    ;;
  defaultweight)
    kctl apply --dry-run=server -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancer
metadata:
  name: ${LB}-celreject
  namespace: ${LBKIT_NS}
spec:
  zoneRef:
    name: livecf
  hostname: ${CEL_HOST}
  steeringPolicy: "off"
  defaultPoolRefs:
    - name: ${POOLA}
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
  defaultWeight: "0.7"
EOF
    ;;
  esac
}

# expect_cel_reject asserts apply_cel_lb is rejected by the apiserver with a
# message that names steeringPolicy (the weight-gate rule).
expect_cel_reject() { # $1 = mode, $2 = human label
  local out rc
  if out="$(apply_cel_lb "$1" 2>&1)"; then
    rc=0
  else
    rc=$?
  fi
  if [[ "${rc}" -eq 0 ]]; then
    bad "GATE pool-weights(i): apiserver ACCEPTED steeringPolicy=off with ${2} -- CEL weight-gate NOT enforced."
    echo "     observed: apply succeeded; expected: rejection citing steeringPolicy." >&2
    exit 1
  fi
  if ! grep -q 'steeringPolicy' <<<"${out}"; then
    bad "GATE pool-weights(i): rejected steeringPolicy=off with ${2}, but the message does not cite steeringPolicy:"
    echo "${out}" >&2
    exit 1
  fi
  ok "GATE pool-weights(i): apiserver REJECTED steeringPolicy=off with ${2} (message cites steeringPolicy)."
  info "     observed: $(grep -m1 -o 'pool weights[^\"]*' <<<"${out}" | head -1)"
}

# apply_lb applies the weighted LB. mode=both -> pa:0.7 + pb:0.3; mode=onlya ->
# pa:0.7 and pb an UNWEIGHTED default member (its weight dropped).
apply_lb() { # $1 = both | onlya
  case "$1" in
  both)
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
  steeringPolicy: random
  defaultPoolRefs:
    - name: ${POOLA}
      weight: "0.7"
    - name: ${POOLB}
      weight: "0.3"
  fallbackPoolRef:
    name: ${POOLA}
EOF
    ;;
  onlya)
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
  steeringPolicy: random
  defaultPoolRefs:
    - name: ${POOLA}
      weight: "0.7"
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
EOF
    ;;
  esac
}

apply_pools() {
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
}

# ---- (i) CEL admission ---------------------------------------------------
info "Step i: CEL weight-gate -- steeringPolicy=off + a weight must be rejected at admission"
expect_cel_reject poolweight "a defaultPoolRefs[].weight"
expect_cel_reject defaultweight "spec.defaultWeight"

# ---- (ii) round-trip -----------------------------------------------------
echo
info "Step ii: create 2 pools (2 origins) + a weighted LB {pa:0.7, pb:0.3}, steering=random"
apply_pools
wait_ready loadbalancerpool "${POOLA}"
wait_ready loadbalancerpool "${POOLB}"

cfa="$(cr_id loadbalancerpool "${POOLA}")"
cfb="$(cr_id loadbalancerpool "${POOLB}")"
if [[ -z "${cfa}" || -z "${cfb}" ]]; then
  err "a pool has no status.id -- pa='${cfa}' pb='${cfb}' (pool not created in Cloudflare)"
  exit 1
fi
info "Cloudflare pool ids: pa=${cfa} pb=${cfb}"

apply_lb both
wait_ready loadbalancer "${LB}"

unresolved="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.unresolvedPoolRefs}' 2>/dev/null || true)"
if [[ -n "${unresolved}" && "${unresolved}" != "[]" ]]; then
  err "LB has unresolved pool refs ${unresolved} -- not all pools resolved; aborting before the weight assert"
  exit 1
fi

lb_id="$(cr_id loadbalancer "${LB}")"
if [[ -z "${lb_id}" ]]; then
  err "LB has no status.id -- it was not created in Cloudflare"
  kc get loadbalancer "${LB}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
info "Cloudflare LB id: ${lb_id}"

# Expected pool_weights maps, keyed by the Cloudflare pool ids.
exp_both="$(jq -n --arg a "${cfa}" --arg b "${cfb}" '{($a):0.7,($b):0.3}')"
exp_onlya="$(jq -n --arg a "${cfa}" '{($a):0.7}')"

observed="$(cf_pw "${lb_id}")"
info "CF pool_weights: ${observed}"
info "expected       : $(jq -c . <<<"${exp_both}")"
if pw_equals "${lb_id}" "${exp_both}"; then
  ok "GATE pool-weights(ii): round-trip pool_weights match EXACTLY {pa:0.7, pb:0.3}."
else
  bad "GATE pool-weights(ii): pool_weights mismatch -- observed ${observed}, expected $(jq -c . <<<"${exp_both}")."
  exit 1
fi
lb_pause "after creating the weighted LB {pa:0.7, pb:0.3}"

# ---- (iii)(a) out-of-band drift -> restore -------------------------------
echo
info "Step iii(a): PATCH Cloudflare's pool_weights out of band to a WRONG value {pa:0.1}"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg a "${cfa}" '{random_steering:{pool_weights:{($a):0.1}}}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF pool_weights right after the out-of-band PATCH: $(cf_pw "${lb_id}")"
lb_pause "after seeding out-of-band drift pool_weights={pa:0.1}"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore {pa:0.7, pb:0.3} (drift-interval ~15s)"
if poll_pw "${lb_id}" "${exp_both}" "${DRIFT_TIMEOUT}"; then
  ok "GATE pool-weights(iii)(a): operator RESTORED pool_weights to $(cf_pw "${lb_id}") (expected $(jq -c . <<<"${exp_both}"))."
else
  bad "GATE pool-weights(iii)(a): pool_weights NOT restored -- observed $(cf_pw "${lb_id}"), expected $(jq -c . <<<"${exp_both}")."
  exit 1
fi

# ---- (iii)(b) drop a weight -> null out + converge -----------------------
echo
info "Step iii(b): edit the LB CR to DROP pb's weight (pb stays a default member)"
apply_lb onlya
wait_reconciled loadbalancer "${LB}"
info "waiting up to ${DRIFT_TIMEOUT}s for Cloudflare's pool_weights to become {pa:0.7} (pb nulled out)"
if poll_pw "${lb_id}" "${exp_onlya}" "${DRIFT_TIMEOUT}"; then
  ok "GATE pool-weights(iii)(b): pb nulled out of Cloudflare -- pool_weights $(cf_pw "${lb_id}") (expected $(jq -c . <<<"${exp_onlya}"))."
else
  bad "GATE pool-weights(iii)(b): pb not nulled -- observed $(cf_pw "${lb_id}"), expected $(jq -c . <<<"${exp_onlya}")."
  exit 1
fi
lb_pause "after dropping pb's weight (pb nulled out of pool_weights)"

info "confirming convergence -- pool_weights stable + LB Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_pw "${lb_id}")"
  rdy="$(lb_ready)"
  info "  poll ${i}: pool_weights=${now} Ready=${rdy:-<none>}"
  pw_equals "${lb_id}" "${exp_onlya}" || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
echo
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE pool-weights(iii)(b): NOT converged -- pool_weights flapped or the LB left Ready (see the polls above)."
  exit 1
fi
ok "GATE pool-weights: converged -- weights round-trip exact, out-of-band drift restored, dropped weight nulled, no drift-loop."
echo "     Record in the PR: pool_weights verified on live CF (round-trip + drift restore + null-on-drop + convergence)." >&2
