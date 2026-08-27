#!/usr/bin/env bash
# GATE session-affinity -- SESSION AFFINITY, end to end on live Cloudflare.
#
# Exercises spec.sessionAffinity + spec.sessionAffinityAttributes (multiple
# non-default subfields) + spec.sessionAffinityTtl on a LoadBalancer, and proves
# that the operator round-trips them EXACTLY, corrects out-of-band drift, and
# relies on Cloudflare's DEEP-MERGE of session_affinity_attributes so that a
# subfield the CR does NOT manage survives a benign edit made through the operator.
#
# Mapping (CR -> Cloudflare wire, from loadbalancer_controller.go +
# cloudflare-go v6 load_balancers):
#   sessionAffinity                         -> session_affinity            ("cookie")
#   sessionAffinityTtl                      -> session_affinity_ttl        (seconds; float)
#   sessionAffinityAttributes.drainDuration -> session_affinity_attributes.drain_duration
#   sessionAffinityAttributes.samesite      -> session_affinity_attributes.samesite   (Auto|Lax|None|Strict)
#   sessionAffinityAttributes.secure        -> session_affinity_attributes.secure     (Auto|Always|Never)
#   sessionAffinityAttributes.zeroDowntimeFailover -> session_affinity_attributes.zero_downtime_failover (none|temporary|sticky)
#
# This gate SETS three non-default cookie subfields on the CR (drainDuration=60,
# samesite=Strict, secure=Always) and INTENTIONALLY leaves zeroDowntimeFailover
# UNSET on the CR -- that unset subfield is the one the deep-merge step seeds
# out-of-band and asserts survives (Cloudflare's default is "none", so a seeded
# "temporary" is unambiguous). CF requires session_affinity_ttl in [1800,604800]
# for cookie affinity, so this uses 5000. Session affinity is a BASE-tier feature
# (no traffic-steering add-on), so steeringPolicy is left at "off"; a single pool
# with a single origin keeps the account at 1 origin, well under the 2-origin cap.
#
# What it checks (self-cleaning: teardown deletes the LB, then the pool):
#   1. ACCEPTED / round-trip EXACT: apply 1 pool (1 origin) + a cookie-affinity LB;
#      the LB must reach Ready with a status.id. If it does NOT (CF rejects session
#      affinity on this tier, surfaced via the Ready message / a missing status.id),
#      FAIL FAST with that message. Then read Cloudflare back and assert
#      session_affinity == "cookie", session_affinity_ttl == 5000, and the three
#      SAA subfields (drain_duration=60, samesite=Strict, secure=Always) EXACTLY
#      (observed vs expected printed for each).
#   2. DRIFT restore: PATCH Cloudflare's session_affinity_attributes.samesite out
#      of band to a WRONG value ("Lax"); the operator must RESTORE "Strict" within
#      a drift interval, then CONVERGE (samesite stable + LB Ready over ~2 more
#      drift intervals, no drift-loop).
#   3. SAA DEEP-MERGE contract (through the operator): PATCH
#      session_affinity_attributes.zero_downtime_failover="temporary" out of band
#      (a subfield the CR does NOT manage), confirm the seed took, then make a
#      BENIGN unrelated edit through the operator (change spec.description). After
#      the operator reconciles that edit -- which re-sends session_affinity_attributes
#      with ONLY the CR-managed subfields -- assert zero_downtime_failover is STILL
#      "temporary" (Cloudflare deep-merged; the operator's partial write did not
#      wipe it) AND every managed subfield is still correct. This proves the
#      deep-merge contract (CF deep-merges session_affinity_attributes) holds
#      through the operator, so partial-object writes are not silent data loss.
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT with the load-balancing controllers and RUNNING
#     against kind-cf-lb in load-balancing mode (`./run-operator.sh lb`, drift-interval ~15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#   - .env supplies the four CF_* identifiers (never printed).
#   - No add-on required: session affinity is a base-tier feature.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-gate-session-affinity-lb
POOLA=livecf-gate-session-affinity-pa
HOST="lb-gate-session-affinity.${CF_ZONE_NAME}"
# Drift correction rides the operator's self-requeue (run-operator.sh sets
# --drift-interval=15s); allow several intervals before calling it a failure.
DRIFT_TIMEOUT="${LBKIT_DRIFT_TIMEOUT:-90}"

# Desired session-affinity values (kept in one place so the CR body and the
# asserts cannot drift apart).
SA_MODE=cookie
SA_TTL=5000
SA_DRAIN=60
SA_SAMESITE=Strict
SA_SECURE=Always
# Managed subfields left UNSET on the CR: zeroDowntimeFailover. CF default is
# "none"; the deep-merge step seeds "temporary" out of band and asserts it survives.
SA_ZDF_SEED=temporary

cleanup() {
  # Delete the LB first (it references the pool), then the pool. Best-effort:
  # the operator clears the Cloudflare-side objects via finalizers, but cleanup
  # must never abort or hang the script (--ignore-not-found + a bounded --timeout,
  # errors swallowed).
  kc delete loadbalancer "${LB}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
  kc delete loadbalancerpool "${POOLA}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers -------------------------------------------------------------
# cf_field echoes a single LB field via a jq filter ("null" when unset).
cf_field() { cf_get_lb "$1" | jq -r "$2"; }

# cf_saa echoes the LB's session_affinity_attributes as compact JSON ({} if unset).
cf_saa() { cf_get_lb "$1" | jq -c '.session_affinity_attributes // {}'; }

# lb_ready echoes the LB CR's Ready condition status (True/False/empty).
lb_ready() {
  kc get loadbalancer "${LB}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}

# assert_field asserts one LB field equals its expected value (exact string
# compare), printing observed vs expected (mirrors the geo-maps gate's assert_geo). Exits
# 1 on mismatch.
assert_field() { # $1 = lb cf-id, $2 = jq filter, $3 = expected, $4 = label
  local got
  got="$(cf_field "$1" "$2")"
  info "  CF ${4}: ${got}"
  info "  expected : ${3}"
  if [[ "${got}" == "${3}" ]]; then
    ok "GATE session-affinity(1): ${4} == ${3}."
  else
    bad "GATE session-affinity(1): ${4} mismatch -- observed ${got}, expected ${3}."
    exit 1
  fi
}

# poll_field polls until an LB field equals expected or the timeout (s) elapses.
poll_field() { # $1 = lb cf-id, $2 = jq filter, $3 = expected, $4 = timeout
  local timeout="$4" i got
  for ((i = 0; i < timeout; i += 3)); do
    got="$(cf_field "$1" "$2")"
    [[ "${got}" == "$3" ]] && return 0
    sleep 3
  done
  return 1
}

apply_pool() {
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
EOF
}

# apply_lb applies the cookie-affinity LB. $1 is the description, which the
# deep-merge step changes to force a benign, unrelated reconcile/edit. The
# session-affinity block is identical across calls: drainDuration/samesite/secure
# are managed, zeroDowntimeFailover is deliberately absent. steeringPolicy is
# quoted so YAML does not coerce the bare token "off" to boolean false.
apply_lb() { # $1 = description
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
  steeringPolicy: "off"
  description: "$1"
  defaultPoolRefs:
    - name: ${POOLA}
  fallbackPoolRef:
    name: ${POOLA}
  sessionAffinity: ${SA_MODE}
  sessionAffinityTtl: ${SA_TTL}
  sessionAffinityAttributes:
    drainDuration: ${SA_DRAIN}
    samesite: ${SA_SAMESITE}
    secure: ${SA_SECURE}
EOF
}

# ---- (1) accepted + round-trip EXACT -------------------------------------
info "Step 1: create 1 pool (1 origin) + a cookie-affinity LB; assert the round-trip EXACT"
apply_pool
wait_ready loadbalancerpool "${POOLA}"

cfa="$(cr_id loadbalancerpool "${POOLA}")"
if [[ -z "${cfa}" ]]; then
  err "pool ${POOLA} has no status.id -- pool not created in Cloudflare"
  exit 1
fi
info "Cloudflare pool id: pa=${cfa}"

apply_lb "gate session-affinity initial"
if ! wait_ready loadbalancer "${LB}"; then
  bad "GATE session-affinity(1): the cookie-affinity LB did not reach Ready -- Cloudflare likely REJECTED session affinity."
  msg="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
  echo "     Ready message: ${msg:-<none>}" >&2
  echo "     A CF rejection here means session affinity is not available on this tier for this LB." >&2
  exit 1
fi

unresolved="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.unresolvedPoolRefs}' 2>/dev/null || true)"
if [[ -n "${unresolved}" && "${unresolved}" != "[]" ]]; then
  err "LB has unresolved pool refs ${unresolved} -- not all pools resolved; aborting before the asserts"
  exit 1
fi

lb_id="$(cr_id loadbalancer "${LB}")"
if [[ -z "${lb_id}" ]]; then
  bad "GATE session-affinity(1): LB is Ready but has no status.id -- it was not created in Cloudflare (session affinity rejected?)."
  kc get loadbalancer "${LB}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
ok "GATE session-affinity(1): cookie-affinity LB accepted by Cloudflare (Ready, id=${lb_id})."
info "Cloudflare LB id: ${lb_id}"
info "CF session_affinity_attributes: $(cf_saa "${lb_id}")"

assert_field "${lb_id}" '.session_affinity'                             "${SA_MODE}"     "session_affinity"
assert_field "${lb_id}" '.session_affinity_ttl'                         "${SA_TTL}"      "session_affinity_ttl"
assert_field "${lb_id}" '.session_affinity_attributes.drain_duration'   "${SA_DRAIN}"    "saa.drain_duration"
assert_field "${lb_id}" '.session_affinity_attributes.samesite'         "${SA_SAMESITE}" "saa.samesite"
assert_field "${lb_id}" '.session_affinity_attributes.secure'           "${SA_SECURE}"   "saa.secure"
ok "GATE session-affinity(1): round-trip EXACT -- session_affinity, session_affinity_ttl, and all 3 SAA subfields match."
lb_pause "after creating the cookie-affinity LB"

# ---- (2) out-of-band drift -> restore + converge -------------------------
echo
info "Step 2: PATCH Cloudflare's saa.samesite out of band to a WRONG value (Lax)"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n '{session_affinity_attributes:{samesite:"Lax"}}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF saa right after the out-of-band PATCH: $(cf_saa "${lb_id}")"
lb_pause "after seeding out-of-band drift saa.samesite=Lax"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore saa.samesite=${SA_SAMESITE} (drift-interval ~15s)"
if poll_field "${lb_id}" '.session_affinity_attributes.samesite' "${SA_SAMESITE}" "${DRIFT_TIMEOUT}"; then
  ok "GATE session-affinity(2): operator RESTORED saa.samesite to $(cf_field "${lb_id}" '.session_affinity_attributes.samesite') (expected ${SA_SAMESITE})."
else
  bad "GATE session-affinity(2): saa.samesite NOT restored -- observed $(cf_field "${lb_id}" '.session_affinity_attributes.samesite'), expected ${SA_SAMESITE}."
  exit 1
fi

info "confirming convergence -- saa.samesite stable + LB Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_field "${lb_id}" '.session_affinity_attributes.samesite')"
  rdy="$(lb_ready)"
  info "  poll ${i}: saa.samesite=${now} Ready=${rdy:-<none>}"
  [[ "${now}" == "${SA_SAMESITE}" ]] || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE session-affinity(2): NOT converged -- saa.samesite flapped or the LB left Ready (see the polls above)."
  exit 1
fi
ok "GATE session-affinity(2): converged -- out-of-band saa.samesite drift restored, no drift-loop."

# ---- (3) SAA deep-merge contract through the operator ---------------------
echo
info "Step 3: seed an UNMANAGED subfield out of band -- saa.zero_downtime_failover=${SA_ZDF_SEED}"
resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg z "${SA_ZDF_SEED}" '{session_affinity_attributes:{zero_downtime_failover:$z}}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed the unmanaged subfield) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
seeded="$(cf_field "${lb_id}" '.session_affinity_attributes.zero_downtime_failover')"
if [[ "${seeded}" != "${SA_ZDF_SEED}" ]]; then
  bad "GATE session-affinity(3): the out-of-band seed did not take -- saa.zero_downtime_failover=${seeded}, expected ${SA_ZDF_SEED}."
  exit 1
fi
info "seed confirmed: saa=$(cf_saa "${lb_id}")"
lb_pause "after seeding unmanaged saa.zero_downtime_failover=temporary"

info "making a BENIGN unrelated edit through the operator (change spec.description)"
apply_lb "gate session-affinity benign edit"
wait_reconciled loadbalancer "${LB}"
# Confirm the benign edit actually landed on Cloudflare (so the operator did issue
# the edit whose SAA partial-write we are testing).
if poll_field "${lb_id}" '.description' "gate session-affinity benign edit" "${DRIFT_TIMEOUT}"; then
  ok "GATE session-affinity(3): benign edit landed -- CF description updated (the operator re-sent session_affinity_attributes)."
else
  bad "GATE session-affinity(3): benign edit did not land -- CF description=$(cf_field "${lb_id}" '.description'); cannot conclude the deep-merge test."
  exit 1
fi
lb_pause "after the benign edit (operator re-sent session_affinity_attributes)"

# The crux: the operator's edit re-sent session_affinity_attributes with only the
# CR-managed subfields (drain_duration/samesite/secure). If Cloudflare deep-merges,
# the unmanaged zero_downtime_failover survives; if it REPLACED, it would reset to
# the CF default "none" -- silent data loss.
info "CF saa after the benign edit: $(cf_saa "${lb_id}")"
zdf_after="$(cf_field "${lb_id}" '.session_affinity_attributes.zero_downtime_failover')"
if [[ "${zdf_after}" == "${SA_ZDF_SEED}" ]]; then
  ok "GATE session-affinity(3): DEEP-MERGE holds -- unmanaged saa.zero_downtime_failover survived the operator edit (=${zdf_after})."
else
  bad "GATE session-affinity(3): DEEP-MERGE VIOLATED -- saa.zero_downtime_failover=${zdf_after} after the benign edit (expected ${SA_ZDF_SEED})."
  echo "     The operator's partial session_affinity_attributes write wiped an unmanaged subfield -- silent data loss. Do NOT merge." >&2
  exit 1
fi

# The managed subfields must still be exactly what the CR sets.
assert_field "${lb_id}" '.session_affinity'                           "${SA_MODE}"     "session_affinity (post-edit)"
assert_field "${lb_id}" '.session_affinity_ttl'                       "${SA_TTL}"      "session_affinity_ttl (post-edit)"
assert_field "${lb_id}" '.session_affinity_attributes.drain_duration' "${SA_DRAIN}"    "saa.drain_duration (post-edit)"
assert_field "${lb_id}" '.session_affinity_attributes.samesite'       "${SA_SAMESITE}" "saa.samesite (post-edit)"
assert_field "${lb_id}" '.session_affinity_attributes.secure'         "${SA_SECURE}"   "saa.secure (post-edit)"

echo
ok "GATE session-affinity: converged -- session affinity round-trips EXACT, out-of-band drift restored, and the SAA deep-merge contract holds through the operator (unset subfields survive a benign edit)."
echo "     Record in the PR: session affinity (cookie + drainDuration/samesite/secure + ttl) verified on live CF -- exact round-trip, drift restore, and CF deep-merge of session_affinity_attributes confirmed through the operator." >&2
