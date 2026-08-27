#!/usr/bin/env bash
# GATE adaptive-location -- adaptiveRouting + locationStrategy, end to end on live Cloudflare.
#
# adaptiveRouting.failoverAcrossPools maps to Cloudflare's
# adaptive_routing.failover_across_pools (a leave-alone *bool: unset keeps CF's
# value, set writes it). locationStrategy {mode, preferECS} maps to
# location_strategy.mode / location_strategy.prefer_ecs (leave-alone strings).
# See buildAdaptiveRouting / buildLocationStrategy and the drift block in
# internal/controller/loadbalancer_controller.go. Both feature families are on
# the BASE load-balancing tier -- they need no traffic-steering add-on -- so this
# gate leaves steeringPolicy at its default ("off") and exercises the fields
# directly. 2 pools, 1 origin each = 2 origins TOTAL (the hard per-account origin
# cap on the entry LB tier); failoverAcrossPools=true is only meaningful with >1
# pool, so both pools are default members.
#
# Valid enum values used (verified against loadbalancer_types.go + the
# cloudflare-go v6 load_balancers SDK):
#   - adaptiveRouting.failoverAcrossPools : true              -> failover_across_pools
#   - locationStrategy.mode               : resolver_ip       -> location_strategy.mode
#                                           (enum: pop | resolver_ip)
#   - locationStrategy.preferECS          : always            -> location_strategy.prefer_ecs
#                                           (enum: always | never | proximity | geo)
#
# What it checks (self-cleaning: teardown deletes the LB, then the pools):
#   1. ACCEPTED (tier sufficiency): apply 2 pools + an LB carrying
#      adaptiveRouting.failoverAcrossPools=true and locationStrategy
#      {preferECS: always, mode: resolver_ip}; the LB must reach Ready with a
#      status.id. If it does not (CF rejects on this tier, surfaced via the Ready
#      message / a missing status.id), FAIL FAST with that message -- an
#      entitlement/tier gate here means these fields are not usable on the account.
#   2. ROUND-TRIP: read Cloudflare back and assert EXACTLY
#         .adaptive_routing.failover_across_pools == true
#         .location_strategy.prefer_ecs          == "always"
#         .location_strategy.mode                == "resolver_ip"
#      (observed vs expected printed for each).
#   3. DRIFT restore (location): PATCH Cloudflare's location_strategy.prefer_ecs
#      out of band to a WRONG value ("never"); the operator must RESTORE "always"
#      within a drift interval, with mode left untouched.
#   4. DRIFT restore (adaptive): PATCH Cloudflare's
#      adaptive_routing.failover_across_pools out of band to a WRONG value
#      (false); the operator must RESTORE true within a drift interval. Then
#      confirm the operator CONVERGES (all three fields stable + LB Ready over ~2
#      more drift intervals, no drift-loop).
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT with the load-balancing controllers and RUNNING
#     against kind-cf-lb in load-balancing mode (`./run-operator.sh lb`, drift-interval ~15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - .env supplies the four CF_* identifiers (never printed).
#   - No add-on required: adaptive routing + location strategy are base-tier.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-gate-adaptive-location-lb
POOLA=livecf-gate-adaptive-location-pa
POOLB=livecf-gate-adaptive-location-pb
HOST="lb-gate-adaptive-location.${CF_ZONE_NAME}"
# Drift correction rides the operator's self-requeue (run-operator.sh sets
# --drift-interval=15s); allow several intervals before calling it a failure.
DRIFT_TIMEOUT="${LBKIT_DRIFT_TIMEOUT:-90}"

# Expected round-trip values.
EXP_FAILOVER=true
EXP_PREFER_ECS=always
EXP_MODE=resolver_ip

cleanup() {
  # Delete the LB first (it references the pools), then the pools. Best-effort:
  # the operator clears the Cloudflare-side objects via finalizers, but cleanup
  # must never abort or hang the script (--ignore-not-found + a bounded --timeout,
  # errors swallowed). No monitors are created by this gate.
  kc delete loadbalancer "${LB}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
  kc delete loadbalancerpool "${POOLA}" "${POOLB}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers -------------------------------------------------------------
# cf_field echoes the value of a jq expression against the live LB JSON (raw
# string; "null" when the field is unset).
cf_field() { # $1 = lb cf-id, $2 = jq expression
  cf_get_lb "$1" | jq -r "$2"
}

# field_equals returns 0 iff the live field equals the expected value.
field_equals() { # $1 = lb cf-id, $2 = jq expression, $3 = expected value
  local got
  got="$(cf_field "$1" "$2")" || return 1
  [[ "${got}" == "$3" ]]
}

# poll_field polls until field_equals holds or the timeout (seconds) elapses.
poll_field() { # $1 = lb cf-id, $2 = jq expression, $3 = expected, $4 = timeout
  local timeout="$4" i
  for ((i = 0; i < timeout; i += 3)); do
    field_equals "$1" "$2" "$3" && return 0
    sleep 3
  done
  return 1
}

# assert_field asserts one field equals its expected value, printing observed vs
# expected (mirrors the pool-weights / geo-maps gates' round-trip asserts). Exits 1 on mismatch.
assert_field() { # $1 = lb cf-id, $2 = jq expression, $3 = expected, $4 = label
  local got
  got="$(cf_field "$1" "$2")"
  info "  CF ${4}: ${got}"
  info "  expected : $3"
  if [[ "${got}" == "$3" ]]; then
    ok "GATE adaptive-location(2): ${4} matches EXACTLY ($3)."
  else
    bad "GATE adaptive-location(2): ${4} mismatch -- observed ${got}, expected $3."
    exit 1
  fi
}

# lb_ready echoes the LB CR's Ready condition status (True/False/empty).
lb_ready() {
  kc get loadbalancer "${LB}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
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

# apply_lb applies the LB with adaptiveRouting.failoverAcrossPools=true and
# locationStrategy {preferECS: always, mode: resolver_ip}. steeringPolicy is
# left at its default ("off") -- both feature families are base-tier.
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
  defaultPoolRefs:
    - name: ${POOLA}
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
  adaptiveRouting:
    failoverAcrossPools: true
  locationStrategy:
    preferECS: ${EXP_PREFER_ECS}
    mode: ${EXP_MODE}
EOF
}

# ---- (1) accepted (tier sufficiency) -------------------------------------
info "Step 1: create 2 pools (2 origins) + an LB with adaptiveRouting + locationStrategy"
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

apply_lb
if ! wait_ready loadbalancer "${LB}"; then
  bad "GATE adaptive-location(1): the LB did not reach Ready -- Cloudflare likely REJECTED adaptiveRouting/locationStrategy on this tier."
  msg="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
  echo "     Ready message: ${msg:-<none>}" >&2
  echo "     A CF entitlement/tier rejection here means adaptive_routing or" >&2
  echo "     location_strategy is not usable on this account. Surface it before re-running." >&2
  exit 1
fi

unresolved="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.unresolvedPoolRefs}' 2>/dev/null || true)"
if [[ -n "${unresolved}" && "${unresolved}" != "[]" ]]; then
  err "LB has unresolved pool refs ${unresolved} -- not all pools resolved; aborting before the asserts"
  exit 1
fi

lb_id="$(cr_id loadbalancer "${LB}")"
if [[ -z "${lb_id}" ]]; then
  bad "GATE adaptive-location(1): LB is Ready but has no status.id -- it was not created in Cloudflare (rejected?)."
  kc get loadbalancer "${LB}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
ok "GATE adaptive-location(1): LB accepted by Cloudflare (Ready, id=${lb_id}) -- adaptive routing + location strategy usable on this tier."
info "Cloudflare LB id: ${lb_id}"

# ---- (2) round-trip ------------------------------------------------------
echo
info "Step 2: round-trip -- read Cloudflare back and assert the fields EXACTLY"
assert_field "${lb_id}" '.adaptive_routing.failover_across_pools' "${EXP_FAILOVER}"   "adaptive_routing.failover_across_pools"
assert_field "${lb_id}" '.location_strategy.prefer_ecs'           "${EXP_PREFER_ECS}" "location_strategy.prefer_ecs"
assert_field "${lb_id}" '.location_strategy.mode'                 "${EXP_MODE}"       "location_strategy.mode"
lb_pause "after creating the LB with adaptiveRouting + locationStrategy"

# ---- (3) out-of-band drift -> restore (location) -------------------------
echo
info "Step 3: PATCH Cloudflare's location_strategy.prefer_ecs out of band to a WRONG value (never)"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n '{location_strategy:{prefer_ecs:"never"}}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed prefer_ecs drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF prefer_ecs right after the out-of-band PATCH: $(cf_field "${lb_id}" '.location_strategy.prefer_ecs')"
lb_pause "after seeding out-of-band drift location_strategy.prefer_ecs=never"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore prefer_ecs=${EXP_PREFER_ECS} (drift-interval ~15s)"
if poll_field "${lb_id}" '.location_strategy.prefer_ecs' "${EXP_PREFER_ECS}" "${DRIFT_TIMEOUT}"; then
  ok "GATE adaptive-location(3): operator RESTORED prefer_ecs to $(cf_field "${lb_id}" '.location_strategy.prefer_ecs') (expected ${EXP_PREFER_ECS})."
else
  bad "GATE adaptive-location(3): prefer_ecs NOT restored -- observed $(cf_field "${lb_id}" '.location_strategy.prefer_ecs'), expected ${EXP_PREFER_ECS}."
  exit 1
fi
# The restore must not have collaterally changed mode.
if field_equals "${lb_id}" '.location_strategy.mode' "${EXP_MODE}"; then
  ok "GATE adaptive-location(3): restore scoped -- location_strategy.mode $(cf_field "${lb_id}" '.location_strategy.mode') survived."
else
  bad "GATE adaptive-location(3): the prefer_ecs restore collaterally changed mode -- mode=$(cf_field "${lb_id}" '.location_strategy.mode'), expected ${EXP_MODE}."
  exit 1
fi

# ---- (4) out-of-band drift -> restore (adaptive) + converge --------------
echo
info "Step 4: PATCH Cloudflare's adaptive_routing.failover_across_pools out of band to a WRONG value (false)"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n '{adaptive_routing:{failover_across_pools:false}}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed failover_across_pools drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF failover_across_pools right after the out-of-band PATCH: $(cf_field "${lb_id}" '.adaptive_routing.failover_across_pools')"
lb_pause "after seeding out-of-band drift adaptive_routing.failover_across_pools=false"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore failover_across_pools=${EXP_FAILOVER} (drift-interval ~15s)"
if poll_field "${lb_id}" '.adaptive_routing.failover_across_pools' "${EXP_FAILOVER}" "${DRIFT_TIMEOUT}"; then
  ok "GATE adaptive-location(4): operator RESTORED failover_across_pools to $(cf_field "${lb_id}" '.adaptive_routing.failover_across_pools') (expected ${EXP_FAILOVER})."
else
  bad "GATE adaptive-location(4): failover_across_pools NOT restored -- observed $(cf_field "${lb_id}" '.adaptive_routing.failover_across_pools'), expected ${EXP_FAILOVER}."
  exit 1
fi

info "confirming convergence -- all three fields stable + LB Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  fo="$(cf_field "${lb_id}" '.adaptive_routing.failover_across_pools')"
  pe="$(cf_field "${lb_id}" '.location_strategy.prefer_ecs')"
  md="$(cf_field "${lb_id}" '.location_strategy.mode')"
  rdy="$(lb_ready)"
  info "  poll ${i}: failover_across_pools=${fo} prefer_ecs=${pe} mode=${md} Ready=${rdy:-<none>}"
  [[ "${fo}" == "${EXP_FAILOVER}" ]] || stable=0
  [[ "${pe}" == "${EXP_PREFER_ECS}" ]] || stable=0
  [[ "${md}" == "${EXP_MODE}" ]] || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
echo
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE adaptive-location(4): NOT converged -- a field flapped or the LB left Ready (see the polls above)."
  exit 1
fi
ok "GATE adaptive-location: converged -- adaptiveRouting + locationStrategy round-trip exact, out-of-band drift restored on both, no drift-loop."
echo "     Record in the PR: adaptive_routing.failover_across_pools + location_strategy.{prefer_ecs,mode} verified on live CF (round-trip + drift restore + convergence)." >&2
