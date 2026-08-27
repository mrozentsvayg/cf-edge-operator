#!/usr/bin/env bash
# GATE pool-list -- POOL + LIST coverage (regression guard), end to end on live
# Cloudflare.
#
# The probe probe-list-semantics.sh (DIRECT CF API, no operator) already showed
# that BOTH list removals are LIST_REPLACE-safe: a PATCH of LoadBalancer
# .default_pools and of LoadBalancerPool .origins REPLACES the list (CF does not
# union the old elements back in). This gate is the OPERATOR-level regression
# guard for that behaviour plus a pool-field round-trip. It has three parts, each
# fully self-contained and TORN DOWN before the next so the account never holds
# more than 2 origins at once (the hard per-account origin cap on the entry LB
# tier):
#
#   (a) default_pools ELEMENT-REMOVAL (2 origins: pa+pb, 1 origin each):
#       create pool pa + pool pb, then an LB with defaultPoolRefs [pa,pb]
#       (steeringPolicy=off, fallback=pa). Read CF back and assert
#       .default_pools == [pa,pb] EXACTLY (ordered). Then edit the CR to
#       defaultPoolRefs [pa]; poll until CF .default_pools == [pa] and confirm
#       the operator CONVERGES (default_pools stable + LB Ready over ~2 drift
#       intervals, no drift-loop). Proves the operator drives a default-pool
#       drop through CF's LIST_REPLACE semantics. Torn down before (b).
#
#   (b) origins ELEMENT-REMOVAL (2 origins in ONE pool):
#       create ONE pool with 2 origins [o1,o2]. Read CF back and assert the pool
#       has origins [o1,o2]. Then edit the CR to origins [o1]; poll until CF's
#       pool .origins length == 1 (o1 only, o2 dropped) and confirm convergence.
#       Proves an origin drop is REPLACE, not union. Torn down before (c).
#
#   (c) pool-field ROUND-TRIP (2 origins: o1 enabled, o2 disabled):
#       create a pool that sets a spread of scalar fields -- pool enabled=false,
#       minimumOrigins=2, notificationEmail, description, latitude/longitude, and
#       origin name/port/enabled/weight (o1 enabled=true so the pool has >=1 enabled
#       origin; o2 enabled=false to exercise origin enabled=false) -- then read CF
#       back and assert every field round-trips EXACT (observed vs expected printed).
#
# FAIL-FAST: any CF reject (a pool/LB that never reaches Ready or has no
# status.id) is surfaced with the Ready message and treated as an
# entitlement/tier gate, mirroring how gate-geo-maps surfaces the geo 1002 reject.
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT with the load-balancing controllers and
#     RUNNING against kind-cf-lb in load-balancing mode (`./run-operator.sh lb`,
#     drift-interval ~15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - .env supplies the four CF_* identifiers (never printed).
#   - No add-on is required: steeringPolicy=off + default_pools + a plain pool are
#     all base-tier features.
#
# Self-cleaning: a trap on EXIT deletes any LB first, then every pool this gate
# creates (best-effort, --ignore-not-found + bounded --timeout, errors swallowed),
# so the gate self-cleans even on failure. Each part also tears down explicitly
# before the next to respect the 2-origin cap.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

# Part (a) resources: 2 pools (1 origin each) + an LB.
LBA=livecf-gate-pool-list-a-lb
POOLA=livecf-gate-pool-list-a-pa
POOLB=livecf-gate-pool-list-a-pb
HOSTA="lb-gate-pool-list-a.${CF_ZONE_NAME}"
# Part (b) resource: one 2-origin pool.
POOLB2=livecf-gate-pool-list-b-pool
# Part (c) resource: one 2-origin pool (o1 enabled, o2 disabled).
POOLC=livecf-gate-pool-list-c-pool

# Drift correction rides the operator's self-requeue (run-operator.sh sets
# --drift-interval=15s); allow several intervals before calling it a failure.
DRIFT_TIMEOUT="${LBKIT_DRIFT_TIMEOUT:-90}"

cleanup() {
  # Delete the LB first (it references the pools), then every pool this gate may
  # have created across all three parts. Best-effort: the operator clears the
  # Cloudflare-side objects via finalizers, but cleanup must never abort or hang
  # the script (--ignore-not-found + a bounded --timeout, errors swallowed).
  kc delete loadbalancer "${LBA}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
  kc delete loadbalancerpool "${POOLA}" "${POOLB}" "${POOLB2}" "${POOLC}" \
    --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers -------------------------------------------------------------
# cr_ready echoes a CR's Ready condition status (True/False/empty).
cr_ready() { # $1 = kind, $2 = name
  kc get "$1" "$2" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}

# ready_or_die fails fast (with the Ready message) if a CR does not reach Ready.
# A CF reject (entitlement/tier gate, an invalid field) shows up here as an LB /
# pool that never goes Ready, so surface the message like gate-geo-maps does for geo.
ready_or_die() { # $1 = kind, $2 = name, $3 = human label
  if wait_ready "$1" "$2"; then
    return 0
  fi
  local msg
  msg="$(kc get "$1" "$2" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
  bad "GATE pool-list: ${3} did not reach Ready -- Cloudflare likely REJECTED a field."
  echo "     Ready message: ${msg:-<none>}" >&2
  echo "     A CF reject here (e.g. an invalid value or a tier/entitlement gate)" >&2
  echo "     means the config is not accepted on this account -- surface it, do NOT merge." >&2
  exit 1
}

# id_or_die echoes a CR's status.id, failing fast if empty (not created in CF).
id_or_die() { # $1 = kind, $2 = name, $3 = human label
  local cfid
  cfid="$(cr_id "$1" "$2")"
  if [[ -z "${cfid}" ]]; then
    bad "GATE pool-list: ${3} is Ready but has no status.id -- it was not created in Cloudflare."
    kc get "$1" "$2" -o yaml 2>/dev/null | sed -n '/status:/,$p' >&2 || true
    exit 1
  fi
  echo "${cfid}"
}

# cf_dp echoes the LB's default_pools as compact JSON ([] when unset). ORDERED.
cf_dp() { cf_get_lb "$1" | jq -c '.default_pools // []'; }

# dp_equals returns 0 iff the LB's live default_pools equals the expected JSON
# array EXACTLY, including order (jq array == is order-sensitive).
dp_equals() { # $1 = lb cf-id, $2 = expected JSON array
  local got
  got="$(cf_dp "$1")" || return 1
  jq -e -n --argjson got "${got}" --argjson exp "$2" '$got == $exp' >/dev/null 2>&1
}

# poll_dp polls until dp_equals holds or the timeout (seconds) elapses.
poll_dp() { # $1 = lb cf-id, $2 = expected JSON array, $3 = timeout
  local timeout="$3" i
  for ((i = 0; i < timeout; i += 3)); do
    dp_equals "$1" "$2" && return 0
    sleep 3
  done
  return 1
}

# cf_origin_names echoes a pool's origin names as a compact JSON array (ORDERED).
cf_origin_names() { cf_get_pool "$1" | jq -c '[.origins[].name]'; }

# on_equals returns 0 iff the pool's live origin-name list equals the expected
# JSON array EXACTLY, including order.
on_equals() { # $1 = pool cf-id, $2 = expected JSON array
  local got
  got="$(cf_origin_names "$1")" || return 1
  jq -e -n --argjson got "${got}" --argjson exp "$2" '$got == $exp' >/dev/null 2>&1
}

# poll_on polls until on_equals holds or the timeout (seconds) elapses.
poll_on() { # $1 = pool cf-id, $2 = expected JSON array, $3 = timeout
  local timeout="$3" i
  for ((i = 0; i < timeout; i += 3)); do
    on_equals "$1" "$2" && return 0
    sleep 3
  done
  return 1
}

# assert_pool_field reads one field off the live CF pool (via a jq filter) and
# asserts it equals the expected JSON value EXACTLY (numbers, strings, bools),
# printing observed vs expected. Exits 1 on mismatch. Mirrors gate-geo-maps's asserts.
assert_pool_field() { # $1 = pool cf-id, $2 = jq filter, $3 = expected JSON, $4 = label
  local got exp
  got="$(cf_get_pool "$1" | jq -c "$2")"
  exp="$(jq -c . <<<"$3")"
  info "  ${4}: CF=${got} expected=${exp}"
  if jq -e -n --argjson got "${got}" --argjson exp "${exp}" '$got == $exp' >/dev/null 2>&1; then
    ok "GATE pool-list(c): ${4} round-trips EXACT (${exp})."
  else
    bad "GATE pool-list(c): ${4} mismatch -- observed ${got}, expected ${exp}."
    exit 1
  fi
}

# teardown_a / teardown_pool block until the resource is gone so the CF-side
# origins are freed (finalizer removal) before the next part exceeds the 2-origin
# cap. --ignore-not-found + a bounded --timeout; errors are swallowed but a slow
# delete is flagged.
teardown_a() {
  kc delete loadbalancer "${LBA}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
  kc delete loadbalancerpool "${POOLA}" "${POOLB}" --ignore-not-found --timeout=90s >/dev/null 2>&1 \
    || warn "part (a) pool teardown did not complete cleanly -- check the 2-origin cap before (b)."
}
teardown_pool() { # $1 = pool name, $2 = part label
  kc delete loadbalancerpool "$1" --ignore-not-found --timeout=90s >/dev/null 2>&1 \
    || warn "part ($2) pool teardown did not complete cleanly."
}

# ==========================================================================
# PART (a) -- default_pools element-removal
# ==========================================================================
info "PART (a): default_pools element-removal -- 2 pools (2 origins) + an LB [pa,pb] -> [pa]"

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

ready_or_die loadbalancerpool "${POOLA}" "pool pa"
ready_or_die loadbalancerpool "${POOLB}" "pool pb"
cfa="$(id_or_die loadbalancerpool "${POOLA}" "pool pa")"
cfb="$(id_or_die loadbalancerpool "${POOLB}" "pool pb")"
info "Cloudflare pool ids: pa=${cfa} pb=${cfb}"

kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancer
metadata:
  name: ${LBA}
  namespace: ${LBKIT_NS}
spec:
  zoneRef:
    name: livecf
  hostname: ${HOSTA}
  steeringPolicy: "off"
  defaultPoolRefs:
    - name: ${POOLA}
    - name: ${POOLB}
  fallbackPoolRef:
    name: ${POOLA}
EOF

ready_or_die loadbalancer "${LBA}" "LB [pa,pb]"

unresolved="$(kc get loadbalancer "${LBA}" -o 'jsonpath={.status.unresolvedPoolRefs}' 2>/dev/null || true)"
if [[ -n "${unresolved}" && "${unresolved}" != "[]" ]]; then
  err "LB has unresolved pool refs ${unresolved} -- not all pools resolved; aborting before the default_pools assert"
  exit 1
fi

lb_id="$(id_or_die loadbalancer "${LBA}" "LB [pa,pb]")"
info "Cloudflare LB id: ${lb_id}"

# Expected default_pools arrays, keyed by the Cloudflare pool ids (ORDERED).
exp_dp_both="$(jq -n --arg a "${cfa}" --arg b "${cfb}" '[$a,$b]')"
exp_dp_onlya="$(jq -n --arg a "${cfa}" '[$a]')"

info "Step (a.1): round-trip -- assert CF default_pools == [pa,pb] EXACTLY (ordered)"
observed="$(cf_dp "${lb_id}")"
info "  CF default_pools: ${observed}"
info "  expected        : $(jq -c . <<<"${exp_dp_both}")"
if dp_equals "${lb_id}" "${exp_dp_both}"; then
  ok "GATE pool-list(a): default_pools round-trip match EXACTLY [pa,pb]."
else
  bad "GATE pool-list(a): default_pools mismatch -- observed ${observed}, expected $(jq -c . <<<"${exp_dp_both}")."
  exit 1
fi
lb_pause "after creating the LB with defaultPoolRefs [pa,pb]"

info "Step (a.2): edit the LB CR to defaultPoolRefs [pa] (drop pb)"
kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancer
metadata:
  name: ${LBA}
  namespace: ${LBKIT_NS}
spec:
  zoneRef:
    name: livecf
  hostname: ${HOSTA}
  steeringPolicy: "off"
  defaultPoolRefs:
    - name: ${POOLA}
  fallbackPoolRef:
    name: ${POOLA}
EOF
wait_reconciled loadbalancer "${LBA}"
info "waiting up to ${DRIFT_TIMEOUT}s for CF default_pools to become [pa] (pb dropped, LIST_REPLACE)"
if poll_dp "${lb_id}" "${exp_dp_onlya}" "${DRIFT_TIMEOUT}"; then
  ok "GATE pool-list(a): pb dropped from CF default_pools -- $(cf_dp "${lb_id}") (expected $(jq -c . <<<"${exp_dp_onlya}"))."
else
  bad "GATE pool-list(a): pb still present in default_pools -- observed $(cf_dp "${lb_id}"), expected $(jq -c . <<<"${exp_dp_onlya}")."
  echo "     A lingering pb would mean the operator's default_pools write is not REPLACE-safe. Do NOT merge." >&2
  exit 1
fi
lb_pause "after dropping pb from defaultPoolRefs (now [pa])"

info "confirming convergence -- default_pools stable + LB Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_dp "${lb_id}")"
  rdy="$(cr_ready loadbalancer "${LBA}")"
  info "  poll ${i}: default_pools=${now} Ready=${rdy:-<none>}"
  dp_equals "${lb_id}" "${exp_dp_onlya}" || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE pool-list(a): NOT converged -- default_pools flapped or the LB left Ready (see the polls above)."
  exit 1
fi
ok "GATE pool-list(a): converged -- default_pools round-trip exact, pb dropped via LIST_REPLACE, no drift-loop."

info "tearing down part (a) (LB + pa + pb) to free origins before part (b)"
teardown_a

# ==========================================================================
# PART (b) -- origins element-removal
# ==========================================================================
echo
info "PART (b): origins element-removal -- ONE pool [o1,o2] -> [o1] (2 origins total)"

kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLB2}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  origins:
    - name: o1
      address: origin-a.${CF_ZONE_NAME}
    - name: o2
      address: origin-b.${CF_ZONE_NAME}
EOF

ready_or_die loadbalancerpool "${POOLB2}" "2-origin pool"
poolb2_id="$(id_or_die loadbalancerpool "${POOLB2}" "2-origin pool")"
info "Cloudflare pool id: ${poolb2_id}"

exp_on_both="$(jq -n '["o1","o2"]')"
exp_on_onlya="$(jq -n '["o1"]')"

info "Step (b.1): round-trip -- assert the pool has origins [o1,o2]"
observed="$(cf_origin_names "${poolb2_id}")"
info "  CF origins: ${observed}"
info "  expected  : $(jq -c . <<<"${exp_on_both}")"
if on_equals "${poolb2_id}" "${exp_on_both}"; then
  ok "GATE pool-list(b): origins round-trip match EXACTLY [o1,o2]."
else
  bad "GATE pool-list(b): origins mismatch -- observed ${observed}, expected $(jq -c . <<<"${exp_on_both}")."
  exit 1
fi
lb_pause "after creating the 2-origin pool [o1,o2]"

info "Step (b.2): edit the pool CR to origins [o1] (drop o2)"
kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLB2}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  origins:
    - name: o1
      address: origin-a.${CF_ZONE_NAME}
EOF
wait_reconciled loadbalancerpool "${POOLB2}"
info "waiting up to ${DRIFT_TIMEOUT}s for CF pool origins to become [o1] (o2 dropped, LIST_REPLACE)"
if poll_on "${poolb2_id}" "${exp_on_onlya}" "${DRIFT_TIMEOUT}"; then
  ok "GATE pool-list(b): o2 dropped from CF origins -- $(cf_origin_names "${poolb2_id}") (expected $(jq -c . <<<"${exp_on_onlya}"))."
else
  bad "GATE pool-list(b): o2 still present in origins -- observed $(cf_origin_names "${poolb2_id}"), expected $(jq -c . <<<"${exp_on_onlya}")."
  echo "     A lingering o2 would mean the operator's origins write is not REPLACE-safe. Do NOT merge." >&2
  exit 1
fi
lb_pause "after dropping o2 from the pool origins (now [o1])"

info "confirming convergence -- origins stable + pool Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_origin_names "${poolb2_id}")"
  rdy="$(cr_ready loadbalancerpool "${POOLB2}")"
  info "  poll ${i}: origins=${now} Ready=${rdy:-<none>}"
  on_equals "${poolb2_id}" "${exp_on_onlya}" || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE pool-list(b): NOT converged -- origins flapped or the pool left Ready (see the polls above)."
  exit 1
fi
ok "GATE pool-list(b): converged -- origins round-trip exact, o2 dropped via LIST_REPLACE, no drift-loop."

info "tearing down part (b) pool to free origins before part (c)"
teardown_pool "${POOLB2}" b

# ==========================================================================
# PART (c) -- pool-field round-trip
# ==========================================================================
echo
info "PART (c): pool-field round-trip -- a 2-origin pool (o1 enabled, o2 disabled) with a spread of scalar fields"

# Expected values (kept in variables so the assert and the manifest cannot drift).
C_LAT="37.7749"
C_LON="-122.4194"
C_EMAIL="lb-gate-pool-list@${CF_ZONE_NAME}"
C_DESC="cf-edge-operator gate pool-list pool round-trip"
# minimumOrigins=2 is a NON-DEFAULT value (CF's default is 1), so the round-trip
# assert below actually distinguishes "operator sent it" from "operator omitted it".
# Valid because this pool has 2 origins (minimum_origins must be <= origin count).
C_MINORIG=2
C_ORIGIN_PORT=8443
C_ORIGIN_WEIGHT="0.5"

# Two origins: o1 ENABLED so the pool has >=1 enabled origin (CF may reject a pool
# whose every origin is disabled), o2 DISABLED so origin enabled=false is still
# exercised. pool enabled=false stays a non-default distinguisher for pool.enabled.
kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOLC}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  enabled: false
  minimumOrigins: ${C_MINORIG}
  notificationEmail: "${C_EMAIL}"
  description: "${C_DESC}"
  latitude: "${C_LAT}"
  longitude: "${C_LON}"
  origins:
    - name: o1
      address: origin-a.${CF_ZONE_NAME}
      port: ${C_ORIGIN_PORT}
      enabled: true
      weight: "${C_ORIGIN_WEIGHT}"
    - name: o2
      address: origin-b.${CF_ZONE_NAME}
      port: ${C_ORIGIN_PORT}
      enabled: false
      weight: "${C_ORIGIN_WEIGHT}"
EOF

ready_or_die loadbalancerpool "${POOLC}" "field round-trip pool"
poolc_id="$(id_or_die loadbalancerpool "${POOLC}" "field round-trip pool")"
info "Cloudflare pool id: ${poolc_id}"

info "reading CF back and asserting each field round-trips EXACT"
assert_pool_field "${poolc_id}" '.enabled'                "false"                          "pool enabled"
assert_pool_field "${poolc_id}" '.minimum_origins'        "${C_MINORIG}"                   "pool minimum_origins"
assert_pool_field "${poolc_id}" '.notification_email'     "$(jq -Rn --arg s "${C_EMAIL}" '$s')" "pool notification_email"
assert_pool_field "${poolc_id}" '.description'            "$(jq -Rn --arg s "${C_DESC}" '$s')"  "pool description"
assert_pool_field "${poolc_id}" '.latitude'              "${C_LAT}"                       "pool latitude"
assert_pool_field "${poolc_id}" '.longitude'             "${C_LON}"                       "pool longitude"
assert_pool_field "${poolc_id}" '.origins[0].name'        "$(jq -Rn --arg s "o1" '$s')"    "origin[0] name"
assert_pool_field "${poolc_id}" '.origins[0].port'        "${C_ORIGIN_PORT}"               "origin[0] port"
assert_pool_field "${poolc_id}" '.origins[0].enabled'     "true"                           "origin[0] enabled"
assert_pool_field "${poolc_id}" '.origins[0].weight'      "${C_ORIGIN_WEIGHT}"             "origin[0] weight"
assert_pool_field "${poolc_id}" '.origins[1].name'        "$(jq -Rn --arg s "o2" '$s')"    "origin[1] name"
assert_pool_field "${poolc_id}" '.origins[1].enabled'     "false"                          "origin[1] enabled"
lb_pause "after creating the pool-field round-trip pool"

info "tearing down part (c) pool"
teardown_pool "${POOLC}" c

echo
ok "GATE pool-list: PASS -- default_pools element-removal (LIST_REPLACE), origins element-removal (LIST_REPLACE), and pool-field round-trip all verified on live CF; each part converged with no drift-loop."
echo "     Record in the PR: POOL+LIST coverage confirmed on live CF -- default_pools drop and origins drop are REPLACE-safe end to end via the operator, and pool scalar fields (enabled, minimum_origins, notification_email, description, latitude/longitude, origin port/enabled/weight) round-trip exact." >&2
