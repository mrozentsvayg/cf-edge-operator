#!/usr/bin/env bash
# GATE geo-maps -- geo pool maps (region_pools / country_pools / pop_pools), end to end
# on live Cloudflare.
#
# This is the GEO analog of the map-replace gate's map-key removal. The map-replace gate proved the operator
# removes a dropped top-level map key on the monitor `header` map via an explicit
# merge-patch null (option.WithJSONSet + mapWithNulls) rather than by omission --
# because Cloudflare DEEP-MERGES map properties on PATCH, so a key the CR dropped
# would otherwise linger forever and the operator would drift-loop. The geo maps
# (region_pools / country_pools / pop_pools) were INFERRED to use that same
# per-key-null path -- but this gate proved otherwise: Cloudflare REJECTS a per-key
# null/[] on the geo maps (code 1002 "array length must be in range [1, 16]"),
# unlike the header map. So the geo key removal instead CLEARS the whole affected
# map with a top-level null (which Cloudflare accepts) in a pre-edit PATCH, then
# re-adds the remaining keys via the main edit's deep-merge (clearRemovedGeoKeys).
# Geo maps are gated behind the traffic-steering add-on (a higher tier); now that
# it is enabled, this gate exercises the real geo removal path.
#
# What it checks (self-cleaning: teardown deletes the LB, then the pools):
#   1. GEO ACCEPTED (add-on sufficiency): apply 2 pools + a steeringPolicy=geo LB
#      with region/country maps; the LB must reach Ready with a status.id. If
#      it does not (CF rejects geo, surfaced via the Ready message / a missing
#      status.id), FAIL FAST with that message -- a CF error 1002 here means the
#      traffic-steering add-on did NOT unblock geo.
#   2. ROUND-TRIP: resolve the CF pool ids for pa/pb and assert Cloudflare's
#      .region_pools == {WNAM:[pa], ENAM:[pb]}, .country_pools == {US:[pa]}
#      EXACTLY (semantic compare; observed vs expected printed for each).
#   3. KEY REMOVAL via clear-then-readd (the crux): edit the LB CR to DROP
#      regionPools.ENAM (keep WNAM; keep country). Poll until Cloudflare's
#      .region_pools == {WNAM:[pa]} -- ENAM removed -- then confirm the operator
#      CONVERGES (region_pools stable + LB Ready over ~2 more drift intervals, no
#      drift-loop). This proves the geo key removal works via clearRemovedGeoKeys
#      (clear the whole region_pools with a top-level null, then re-add WNAM via
#      the deep-merge): because CF deep-merges AND rejects a per-key null on the
#      geo maps, neither omission NOR a per-key null would remove ENAM. Also
#      confirms the clear was scoped to region_pools -- country survives untouched.
#   4. DRIFT restore: PATCH Cloudflare's region_pools out of band to a WRONG value
#      ({WNAM:[pb]}); the operator must RESTORE {WNAM:[pa]} within a drift interval.
#
# POP geosteering (pop_pools) is IMPLEMENTED via the same clearRemovedGeoKeys path
# but NOT tested here -- it needs a CF-account-side PoP-geosteering entitlement (not
# enabled on this account, and not self-serve); see docs/architecture.md.
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT with the load-balancing controllers (including
#     the geo key-removal path) and RUNNING against kind-cf-lb in load-balancing
#     mode (`./run-operator.sh lb`, drift-interval ~15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - The TRAFFIC-STEERING ADD-ON is enabled on the Cloudflare account (geo needs it).
#   - .env supplies the four CF_* identifiers (never printed).
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-gate-geo-maps-lb
POOLA=livecf-gate-geo-maps-pa
POOLB=livecf-gate-geo-maps-pb
HOST="lb-gate-geo-maps.${CF_ZONE_NAME}"
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
# cf_geo echoes the LB's <field> geo map (region_pools | country_pools |
# pop_pools) as compact JSON ({} when Cloudflare has the field unset).
cf_geo() { cf_get_lb "$1" | jq -c --arg f "$2" '.[$f] // {}'; }

# geo_equals returns 0 iff the LB's live <field> equals the expected JSON object.
# jq's object == is key-order insensitive, and every region/country/pop list here
# is a SINGLE pool id, so array order is moot -- the compare is exact. (The
# operator's own drift check, mapListsEqual -> stringSlicesEqual, is order-
# sensitive per list, which single-element lists satisfy trivially.)
geo_equals() { # $1 = lb cf-id, $2 = field, $3 = expected JSON object
  local got
  got="$(cf_geo "$1" "$2")" || return 1
  jq -e -n --argjson got "${got}" --argjson exp "$3" '$got == $exp' >/dev/null 2>&1
}

# poll_geo polls until geo_equals holds or the timeout (seconds) elapses.
poll_geo() { # $1 = lb cf-id, $2 = field, $3 = expected JSON object, $4 = timeout
  local timeout="$4" i
  for ((i = 0; i < timeout; i += 3)); do
    geo_equals "$1" "$2" "$3" && return 0
    sleep 3
  done
  return 1
}

# assert_geo asserts one geo map equals its expected value, printing observed vs
# expected (mirrors the pool-weights gate's round-trip assert). Exits 1 on mismatch.
assert_geo() { # $1 = lb cf-id, $2 = field, $3 = expected JSON object, $4 = label
  local got exp
  got="$(cf_geo "$1" "$2")"
  exp="$(jq -c . <<<"$3")"
  info "  CF ${2}: ${got}"
  info "  expected : ${exp}"
  if geo_equals "$1" "$2" "$3"; then
    ok "GATE geo-maps(2): ${4} match EXACTLY ${exp}."
  else
    bad "GATE geo-maps(2): ${4} mismatch -- observed ${got}, expected ${exp}."
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

# apply_lb applies the geo LB. mode=full -> regionPools {WNAM:pa, ENAM:pb};
# mode=dropenam -> regionPools {WNAM:pa} only (ENAM dropped). country is
# identical in both, so the ENAM removal is scoped to region_pools.
apply_lb() { # $1 = full | dropenam
  local region_block
  case "$1" in
  full)
    region_block=$'    WNAM:\n      - name: '"${POOLA}"$'\n    ENAM:\n      - name: '"${POOLB}"
    ;;
  dropenam)
    region_block=$'    WNAM:\n      - name: '"${POOLA}"
    ;;
  esac
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
  steeringPolicy: geo
  defaultPoolRefs:
    - name: ${POOLA}
  fallbackPoolRef:
    name: ${POOLA}
  regionPools:
${region_block}
  countryPools:
    US:
      - name: ${POOLA}
EOF
}

# ---- (1) geo accepted (add-on sufficiency) -------------------------------
info "Step 1: create 2 pools (2 origins) + a steeringPolicy=geo LB (add-on sufficiency check)"
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

apply_lb full
if ! wait_ready loadbalancer "${LB}"; then
  bad "GATE geo-maps(1): the geo LB did not reach Ready -- Cloudflare likely REJECTED geo."
  msg="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
  echo "     Ready message: ${msg:-<none>}" >&2
  echo "     A CF error 1002 (or a geo / traffic-steering rejection) here means the" >&2
  echo "     traffic-steering add-on did NOT unblock geo. Enable it before re-running." >&2
  exit 1
fi

unresolved="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.unresolvedPoolRefs}' 2>/dev/null || true)"
if [[ -n "${unresolved}" && "${unresolved}" != "[]" ]]; then
  err "LB has unresolved pool refs ${unresolved} -- not all pools resolved; aborting before the geo asserts"
  exit 1
fi

lb_id="$(cr_id loadbalancer "${LB}")"
if [[ -z "${lb_id}" ]]; then
  bad "GATE geo-maps(1): LB is Ready but has no status.id -- it was not created in Cloudflare (geo rejected?)."
  kc get loadbalancer "${LB}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
ok "GATE geo-maps(1): geo LB accepted by Cloudflare (Ready, id=${lb_id}) -- traffic-steering add-on is sufficient for geo."
info "Cloudflare LB id: ${lb_id}"

# Expected geo maps, keyed by CF pool ids.
exp_region_full="$(jq -n --arg a "${cfa}" --arg b "${cfb}" '{WNAM:[$a],ENAM:[$b]}')"
exp_region_wnam="$(jq -n --arg a "${cfa}" '{WNAM:[$a]}')"
exp_region_drift="$(jq -n --arg b "${cfb}" '{WNAM:[$b]}')"
exp_country="$(jq -n --arg a "${cfa}" '{US:[$a]}')"

# ---- (2) round-trip ------------------------------------------------------
echo
info "Step 2: round-trip -- read Cloudflare back and assert the geo maps EXACTLY"
assert_geo "${lb_id}" region_pools  "${exp_region_full}" "region_pools {WNAM:pa, ENAM:pb}"
assert_geo "${lb_id}" country_pools "${exp_country}"     "country_pools {US:pa}"
lb_pause "after creating the geo LB (region {WNAM:pa, ENAM:pb}, country {US:pa})"

# ---- (3) key removal via merge-patch null + converge ---------------------
echo
info "Step 3: edit the LB CR to DROP regionPools.ENAM (keep WNAM; keep country)"
apply_lb dropenam
wait_reconciled loadbalancer "${LB}"
info "waiting up to ${DRIFT_TIMEOUT}s for Cloudflare's region_pools to become {WNAM:[pa]} (ENAM removed)"
if poll_geo "${lb_id}" region_pools "${exp_region_wnam}" "${DRIFT_TIMEOUT}"; then
  ok "GATE geo-maps(3): ENAM removed from Cloudflare's region_pools -- $(cf_geo "${lb_id}" region_pools) (expected $(jq -c . <<<"${exp_region_wnam}"))."
  echo "     Cloudflare DEEP-MERGES geo maps AND rejects a per-key null on them, so the" >&2
  echo "     operator cleared the whole region_pools (top-level null) then re-added WNAM" >&2
  echo "     via the deep-merge -- clearRemovedGeoKeys." >&2
else
  bad "GATE geo-maps(3): ENAM still present in region_pools -- observed $(cf_geo "${lb_id}" region_pools), expected $(jq -c . <<<"${exp_region_wnam}")."
  echo "     The clear-then-readd geo key removal did not land: either the pre-edit whole-map" >&2
  echo "     clear was not sent or the re-add failed. Do NOT merge." >&2
  exit 1
fi

# The null must be scoped to ENAM -- country_pools was kept in the CR and must survive.
if geo_equals "${lb_id}" country_pools "${exp_country}"; then
  ok "GATE geo-maps(3): removal scoped to ENAM -- country_pools $(cf_geo "${lb_id}" country_pools) survived."
else
  bad "GATE geo-maps(3): the ENAM drop collaterally changed country -- country_pools=$(cf_geo "${lb_id}" country_pools)."
  exit 1
fi
lb_pause "after dropping regionPools.ENAM (region now {WNAM:pa})"

info "confirming convergence -- region_pools stable + LB Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_geo "${lb_id}" region_pools)"
  rdy="$(lb_ready)"
  info "  poll ${i}: region_pools=${now} Ready=${rdy:-<none>}"
  geo_equals "${lb_id}" region_pools "${exp_region_wnam}" || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE geo-maps(3): NOT converged -- region_pools flapped or the LB left Ready (see the polls above)."
  exit 1
fi
ok "GATE geo-maps(3): converged -- ENAM removed via clear-then-readd, region_pools stable, no drift-loop."

# ---- (4) out-of-band drift -> restore ------------------------------------
echo
info "Step 4: PATCH Cloudflare's region_pools out of band to a WRONG value {WNAM:[pb]}"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg b "${cfb}" '{region_pools:{WNAM:[$b]}}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF region_pools right after the out-of-band PATCH: $(cf_geo "${lb_id}" region_pools)"
lb_pause "after seeding out-of-band drift region_pools={WNAM:pb}"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore {WNAM:[pa]} (drift-interval ~15s)"
if poll_geo "${lb_id}" region_pools "${exp_region_wnam}" "${DRIFT_TIMEOUT}"; then
  ok "GATE geo-maps(4): operator RESTORED region_pools to $(cf_geo "${lb_id}" region_pools) (expected $(jq -c . <<<"${exp_region_wnam}"))."
else
  bad "GATE geo-maps(4): region_pools NOT restored -- observed $(cf_geo "${lb_id}" region_pools), expected $(jq -c . <<<"${exp_region_wnam}"), seeded $(jq -c . <<<"${exp_region_drift}")."
  exit 1
fi

echo
ok "GATE geo-maps: converged -- geo maps round-trip exact, geo key removed via clear-then-readd, removal scoped, no drift-loop, out-of-band drift restored."
echo "     Record in the PR: geo map-key removal via clearRemovedGeoKeys (clear-whole-map-then-readd) confirmed on live CF for region_pools -- CF rejects a per-key null on the geo maps (unlike the monitor header)." >&2
