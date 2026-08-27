#!/usr/bin/env bash
# GATE steering-policies -- advanced steering POLICIES (dynamic_latency + proximity), end to
# end on live Cloudflare.
#
# The geo policy is already covered by gate-geo-maps (region/country/pop maps). This
# gate covers the two remaining advanced, add-on-gated scalar policies:
# steeringPolicy = dynamic_latency and proximity. Both map straight through to
# Cloudflare's per-LB `steering_policy` scalar (see loadbalancer_controller.go:
# p.SteeringPolicy = load_balancers.SteeringPolicy(lb.Spec.SteeringPolicy), and
# the drift check `string(cf.SteeringPolicy) != lb.Spec.SteeringPolicy`). Per the
# CRD doc, only `off` is available on the base tier -- every advanced policy
# (geo/dynamic_latency/proximity/least_*) fails LB creation without the traffic-
# steering add-on, and proximity may additionally be entitlement-gated. So a
# rejection here is surfaced fail-fast with the Cloudflare error, exactly like
# gate-geo-maps does for geo error 1002.
#
# 2 pools, 1 origin each = 2 origins TOTAL (the hard per-account origin cap on
# the staging tier). The two policies REUSE the same 2 pools -- only the LB's
# steeringPolicy changes between steps, so the account never exceeds 2 origins.
#
# What it checks (self-cleaning: teardown deletes the LB, then the pools):
#   1. POOLS: apply 2 pools (2 origins) and wait Ready + status.id.
#   2. dynamic_latency ACCEPTED + ROUND-TRIP: apply the LB with
#      steeringPolicy=dynamic_latency; it must reach Ready with a status.id (if
#      not, FAIL FAST with the Ready message -- a CF rejection means the traffic-
#      steering add-on is not enabling dynamic_latency). Then read Cloudflare back
#      and assert .steering_policy == "dynamic_latency" EXACTLY.
#   3. proximity ACCEPTED + ROUND-TRIP: re-apply the SAME LB with
#      steeringPolicy=proximity; same Ready + status.id gate (FAIL FAST with the
#      CF error if rejected -- proximity may be entitlement-gated beyond the
#      add-on). Then assert .steering_policy == "proximity" EXACTLY.
#   4. DRIFT restore: put the LB back on steeringPolicy=dynamic_latency, then
#      PATCH Cloudflare's steering_policy out of band to a DIFFERENT value
#      ("off", always accepted on the base tier). The operator must RESTORE
#      "dynamic_latency" within a drift interval, then CONVERGE (steering_policy
#      stable + LB Ready over ~2 more drift intervals, no drift-loop).
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT with the load-balancing controllers and
#     RUNNING against kind-cf-lb in load-balancing mode (`./run-operator.sh lb`,
#     drift-interval ~15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - The TRAFFIC-STEERING ADD-ON is enabled on the Cloudflare account (both
#     dynamic_latency and proximity need it; proximity may need an extra grant).
#   - .env supplies the four CF_* identifiers (never printed).
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-gate-steering-policies-lb
POOLA=livecf-gate-steering-policies-pa
POOLB=livecf-gate-steering-policies-pb
HOST="lb-gate-steering-policies.${CF_ZONE_NAME}"
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
# cf_steering echoes the LB's live steering_policy scalar ("" when unset).
cf_steering() { cf_get_lb "$1" | jq -r '.steering_policy // ""'; }

# steering_equals returns 0 iff the LB's live steering_policy equals $2.
steering_equals() { # $1 = lb cf-id, $2 = expected policy
  local got
  got="$(cf_steering "$1")" || return 1
  [[ "${got}" == "$2" ]]
}

# poll_steering polls until steering_equals holds or the timeout (s) elapses.
poll_steering() { # $1 = lb cf-id, $2 = expected policy, $3 = timeout
  local timeout="$3" i
  for ((i = 0; i < timeout; i += 3)); do
    steering_equals "$1" "$2" && return 0
    sleep 3
  done
  return 1
}

# assert_steering asserts the live steering_policy equals its expected value,
# printing observed vs expected (mirrors gate-pool-weights / gate-geo-maps round-trip asserts).
# Exits 1 on mismatch.
assert_steering() { # $1 = lb cf-id, $2 = expected policy, $3 = step label
  local got
  got="$(cf_steering "$1")"
  info "  CF steering_policy: ${got:-<none>}"
  info "  expected          : $2"
  if steering_equals "$1" "$2"; then
    ok "GATE steering-policies(${3}): steering_policy round-trip matches EXACTLY \"$2\"."
  else
    bad "GATE steering-policies(${3}): steering_policy mismatch -- observed \"${got}\", expected \"$2\"."
    exit 1
  fi
}

# lb_ready echoes the LB CR's Ready condition status (True/False/empty).
lb_ready() {
  kc get loadbalancer "${LB}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}

# require_lb_ready waits for the LB to reach Ready with a status.id, and fails
# fast with the Cloudflare-surfaced Ready message if the policy is rejected
# (add-on / entitlement gate). Echoes the CF LB id on success.
require_lb_ready() { # $1 = policy, $2 = step label
  local msg lb_id unresolved
  if ! wait_ready loadbalancer "${LB}"; then
    bad "GATE steering-policies($2): the steeringPolicy=$1 LB did not reach Ready -- Cloudflare likely REJECTED the policy."
    msg="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
    echo "     Ready message: ${msg:-<none>}" >&2
    echo "     A CF rejection here means the traffic-steering add-on does not enable $1" >&2
    echo "     on this account (proximity may need an extra entitlement). Grant it before re-running." >&2
    exit 1
  fi
  unresolved="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.unresolvedPoolRefs}' 2>/dev/null || true)"
  if [[ -n "${unresolved}" && "${unresolved}" != "[]" ]]; then
    err "LB has unresolved pool refs ${unresolved} -- not all pools resolved; aborting before the steering assert"
    exit 1
  fi
  lb_id="$(cr_id loadbalancer "${LB}")"
  if [[ -z "${lb_id}" ]]; then
    bad "GATE steering-policies($2): LB is Ready but has no status.id -- it was not created in Cloudflare (policy $1 rejected?)."
    kc get loadbalancer "${LB}" -o yaml | sed -n '/status:/,$p' >&2 || true
    exit 1
  fi
  printf '%s' "${lb_id}"
}

apply_pools() {
  # Both pools carry latitude/longitude: proximity steering REQUIRES pool coordinates
  # (Cloudflare rejects a proximity LB whose pools lack lat/long with 400 code 1002
  # "Proximity Load Balancer has pools missing Latitude/Longitude"). Distinct coords
  # (POOLA ~San Jose, POOLB ~New York) so proximity has a real topology to route on.
  kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLA}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  latitude: "37.3382"
  longitude: "-121.8863"
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
}

# apply_lb applies the LB with steeringPolicy=$1. The hostname and pool refs are
# identical across policies (hostname is immutable), so only the scalar policy
# changes between steps -- and the account stays at 2 origins throughout.
apply_lb() { # $1 = steering policy
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
  steeringPolicy: $1
  defaultPoolRefs:
    - name: ${POOLA}
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
EOF
}

# ---- (1) pools -----------------------------------------------------------
info "Step 1: create 2 pools (2 origins) shared by both policy steps"
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

# ---- (2) dynamic_latency accepted + round-trip ---------------------------
echo
info "Step 2: apply the LB with steeringPolicy=dynamic_latency (add-on sufficiency + round-trip)"
apply_lb dynamic_latency
lb_id="$(require_lb_ready dynamic_latency 2)"
ok "GATE steering-policies(2): dynamic_latency accepted by Cloudflare (Ready, id=${lb_id})."
info "Cloudflare LB id: ${lb_id}"
assert_steering "${lb_id}" dynamic_latency 2
lb_pause "after creating the LB with steeringPolicy=dynamic_latency"

# ---- (3) proximity accepted + round-trip ---------------------------------
echo
info "Step 3: re-apply the SAME LB with steeringPolicy=proximity (entitlement check + round-trip)"
apply_lb proximity
wait_reconciled loadbalancer "${LB}"
# lb_id is stable across the edit (same object), but re-derive to be safe and to
# re-run the Ready/id/rejection gate for the proximity policy.
lb_id="$(require_lb_ready proximity 3)"
ok "GATE steering-policies(3): proximity accepted by Cloudflare (Ready, id=${lb_id})."
info "waiting up to ${DRIFT_TIMEOUT}s for Cloudflare's steering_policy to become \"proximity\""
if ! poll_steering "${lb_id}" proximity "${DRIFT_TIMEOUT}"; then
  bad "GATE steering-policies(3): steering_policy did not become proximity -- observed \"$(cf_steering "${lb_id}")\"."
  exit 1
fi
assert_steering "${lb_id}" proximity 3
lb_pause "after switching steeringPolicy to proximity"

# ---- (4) out-of-band drift -> restore ------------------------------------
echo
info "Step 4: put the LB back on dynamic_latency, then seed out-of-band drift"
apply_lb dynamic_latency
wait_reconciled loadbalancer "${LB}"
if ! poll_steering "${lb_id}" dynamic_latency "${DRIFT_TIMEOUT}"; then
  bad "GATE steering-policies(4): steering_policy did not settle on dynamic_latency before drift seeding -- observed \"$(cf_steering "${lb_id}")\"."
  exit 1
fi
lb_pause "after putting the LB back on steeringPolicy=dynamic_latency"

info "PATCH Cloudflare's steering_policy out of band to a DIFFERENT value (\"off\")"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n '{steering_policy:"off"}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF steering_policy right after the out-of-band PATCH: $(cf_steering "${lb_id}")"
lb_pause "after seeding out-of-band drift steering_policy=off"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore \"dynamic_latency\" (drift-interval ~15s)"
if poll_steering "${lb_id}" dynamic_latency "${DRIFT_TIMEOUT}"; then
  ok "GATE steering-policies(4): operator RESTORED steering_policy to \"$(cf_steering "${lb_id}")\" (expected \"dynamic_latency\")."
else
  bad "GATE steering-policies(4): steering_policy NOT restored -- observed \"$(cf_steering "${lb_id}")\", expected \"dynamic_latency\", seeded \"off\"."
  exit 1
fi

info "confirming convergence -- steering_policy stable + LB Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_steering "${lb_id}")"
  rdy="$(lb_ready)"
  info "  poll ${i}: steering_policy=${now:-<none>} Ready=${rdy:-<none>}"
  steering_equals "${lb_id}" dynamic_latency || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
echo
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE steering-policies(4): NOT converged -- steering_policy flapped or the LB left Ready (see the polls above)."
  exit 1
fi

ok "GATE steering-policies: converged -- dynamic_latency + proximity accepted and round-trip exact, out-of-band drift restored, no drift-loop."
echo "     Record in the PR: advanced steering policies dynamic_latency + proximity verified on live CF (accept + round-trip + drift restore)." >&2
