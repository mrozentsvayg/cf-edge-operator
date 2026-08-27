#!/usr/bin/env bash
# GATE monitor -- LoadBalancerMonitor round-trip / drift / edit, end to end on live
# Cloudflare.
#
# A LoadBalancerMonitor is a Cloudflare health-check definition. It is
# account-scoped (created under /accounts/<id>/load_balancers/monitors) and needs
# NO origins, so this gate is ORIGIN-CAP-FREE: it never creates a pool or an LB
# and so never touches the hard 2-origins-per-account staging cap. It exercises
# every modeled scalar the operator manages on an https monitor:
#   type, method, path, port, expectedCodes, expectedBody, interval (>=60 on this
#   tier), retries, timeout, followRedirects, allowInsecure, consecutiveUp,
#   consecutiveDown, probeZone.
# (method/path/expectedCodes/expectedBody/probeZone/followRedirects/allowInsecure
# are only meaningful for http/https, which is why this gate uses type=https.)
#
# What it checks (self-cleaning: the trap deletes the monitor):
#   1. CREATE + ROUND-TRIP: apply an https monitor with every field above set to
#      a distinct, non-default value; wait Ready; read Cloudflare back and assert
#      EACH field EXACTLY (observed vs expected printed per field). If Cloudflare
#      REJECTS the monitor (no Ready / no status.id), FAIL FAST with the Ready
#      message -- surfacing an entitlement/tier gate the way the geo-maps gate surfaces the
#      geo 1002 rejection.
#   2. OUT-OF-BAND DRIFT: PATCH Cloudflare's expected_codes to a WRONG value
#      ("500") directly via the API; the operator must RESTORE "200" within a
#      drift interval (poll up to DRIFT_TIMEOUT).
#   3. IN-PLACE EDIT: change a scalar on the CR (interval 60 -> 120); wait for the
#      controller to reconcile the new generation, then confirm Cloudflare
#      reflects interval == 120, and that the monitor CONVERGES (interval stable
#      at 120 + Ready over ~2 more drift intervals, no drift-loop).
#
# PRECONDITIONS (this gate builds / installs / starts NOTHING):
#   - The operator is already REBUILT with the load-balancing controllers and
#     RUNNING against kind-cf-lb in load-balancing mode (`./run-operator.sh lb`,
#     drift-interval ~15s).
#   - The load-balancing CRDs are already applied (`make install` / `./setup.sh`).
#   - The identity substrate Account/Zone "livecf" is Initialized (`./setup.sh`).
#     (A monitor only needs the Account for accountRef; no zone/add-on required.)
#   - .env supplies the four CF_* identifiers (never printed).
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

MON=livecf-gate-monitor-mon
# Drift correction rides the operator's self-requeue (run-operator.sh sets
# --drift-interval=15s); allow several intervals before calling it a failure.
DRIFT_TIMEOUT="${LBKIT_DRIFT_TIMEOUT:-90}"

cleanup() {
  # A monitor references nothing, so there is no LB/pool to delete first; the
  # monitor is the only object this gate creates. Best-effort: the operator
  # removes the Cloudflare-side monitor via its finalizer, but cleanup must
  # never abort or hang the script (--ignore-not-found + a bounded --timeout,
  # errors swallowed).
  kc delete loadbalancermonitor "${MON}" --ignore-not-found --timeout=90s >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ---- helpers -------------------------------------------------------------
# cf_mon_field echoes one field of the CF monitor as a raw scalar (empty when
# Cloudflare has the field unset).
cf_mon_field() { # $1 = monitor cf-id, $2 = CF json field
  cf_get_monitor "$1" | jq -r --arg f "$2" '.[$f] // empty' 2>/dev/null || true
}

# assert_snap_field asserts one field of a captured CF-monitor JSON snapshot
# equals its expected value, printing observed vs expected. Exits 1 on mismatch.
assert_snap_field() { # $1 = json snapshot, $2 = CF json field, $3 = expected, $4 = label
  local got
  got="$(jq -r --arg f "$2" '.[$f]' <<<"$1")"
  if [[ "${got}" == "$3" ]]; then
    ok "GATE monitor(1): ${4} (${2}) == ${3}"
  else
    bad "GATE monitor(1): ${4} (${2}) mismatch -- observed '${got}', expected '$3'."
    exit 1
  fi
}

# poll_mon_field polls until the CF monitor's <field> equals <expected> or the
# timeout (seconds) elapses.
poll_mon_field() { # $1 = mon cf-id, $2 = field, $3 = expected, $4 = timeout
  local timeout="$4" i got
  for ((i = 0; i < timeout; i += 3)); do
    got="$(cf_mon_field "$1" "$2")"
    [[ "${got}" == "$3" ]] && return 0
    sleep 3
  done
  return 1
}

# mon_ready echoes the monitor CR's Ready condition status (True/False/empty).
mon_ready() {
  kc get loadbalancermonitor "${MON}" \
    -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true
}

# apply_monitor applies the monitor CR. $1 = interval, $2 = expectedCodes; every
# other field is held constant so the drift / edit steps mutate exactly one
# scalar at a time. `probeZone` uses the real zone name so Cloudflare accepts it.
apply_monitor() { # $1 = interval, $2 = expectedCodes
  kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerMonitor
metadata:
  name: ${MON}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  type: https
  method: GET
  path: /gate-monitor-health
  port: 8443
  expectedCodes: "$2"
  expectedBody: gate-monitor-ok
  followRedirects: true
  allowInsecure: true
  interval: $1
  retries: 3
  timeout: 5
  consecutiveUp: 2
  consecutiveDown: 3
  probeZone: ${CF_ZONE_NAME}
EOF
}

# ---- (1) create + round-trip ---------------------------------------------
info "Step 1: create an https monitor with every modeled scalar set (no origins -- cap-free)"
apply_monitor 60 "200"
if ! wait_ready loadbalancermonitor "${MON}"; then
  bad "GATE monitor(1): the monitor did not reach Ready -- Cloudflare likely REJECTED it."
  msg="$(kc get loadbalancermonitor "${MON}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null || true)"
  echo "     Ready message: ${msg:-<none>}" >&2
  echo "     A CF rejection here (e.g. an unsupported field/enum, or a plan/tier gate)" >&2
  echo "     means the monitor spec was not accepted -- surface it and do NOT merge." >&2
  exit 1
fi

mon_id="$(cr_id loadbalancermonitor "${MON}")"
if [[ -z "${mon_id}" ]]; then
  bad "GATE monitor(1): monitor is Ready but has no status.id -- it was not created in Cloudflare."
  kc get loadbalancermonitor "${MON}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
info "Cloudflare monitor id: ${mon_id}"

echo
info "Step 1: round-trip -- read Cloudflare back and assert EACH field EXACTLY"
snap="$(cf_get_monitor "${mon_id}")"
if [[ -z "${snap}" || "${snap}" == "null" ]]; then
  err "could not read the CF monitor ${mon_id} back for the round-trip assert"
  exit 1
fi
assert_snap_field "${snap}" type            https           "type"
assert_snap_field "${snap}" method          GET             "method"
assert_snap_field "${snap}" path            /gate-monitor-health "path"
assert_snap_field "${snap}" port            8443            "port"
assert_snap_field "${snap}" expected_codes  200             "expectedCodes"
assert_snap_field "${snap}" expected_body   gate-monitor-ok     "expectedBody"
assert_snap_field "${snap}" follow_redirects true           "followRedirects"
assert_snap_field "${snap}" allow_insecure  true            "allowInsecure"
assert_snap_field "${snap}" interval        60              "interval"
assert_snap_field "${snap}" retries         3               "retries"
assert_snap_field "${snap}" timeout         5               "timeout"
assert_snap_field "${snap}" consecutive_up   2              "consecutiveUp"
assert_snap_field "${snap}" consecutive_down 3              "consecutiveDown"
assert_snap_field "${snap}" probe_zone      "${CF_ZONE_NAME}" "probeZone"
ok "GATE monitor(1): round-trip EXACT for all 14 modeled fields."
lb_pause "after creating the https monitor"

# ---- (2) out-of-band drift -> restore ------------------------------------
echo
info "Step 2: PATCH Cloudflare's expected_codes out of band to a WRONG value (\"500\")"
resp="$(cf_api PATCH "/accounts/${CF_ACCOUNT_ID}/load_balancers/monitors/${mon_id}" \
  --data "$(jq -n '{expected_codes:"500"}')")"
if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
  err "direct CF PATCH (to seed drift) failed:"
  jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
  exit 1
fi
info "CF expected_codes right after the out-of-band PATCH: $(cf_mon_field "${mon_id}" expected_codes)"
lb_pause "after seeding out-of-band drift expected_codes=500"
info "waiting up to ${DRIFT_TIMEOUT}s for the operator to restore expected_codes=200 (drift-interval ~15s)"
if poll_mon_field "${mon_id}" expected_codes "200" "${DRIFT_TIMEOUT}"; then
  ok "GATE monitor(2): operator RESTORED expected_codes to $(cf_mon_field "${mon_id}" expected_codes) (expected 200)."
else
  bad "GATE monitor(2): expected_codes NOT restored -- observed $(cf_mon_field "${mon_id}" expected_codes), expected 200 (seeded 500)."
  exit 1
fi

# ---- (3) in-place edit -> converge ---------------------------------------
echo
info "Step 3: edit the CR to change a scalar (interval 60 -> 120) and confirm CF reflects it"
apply_monitor 120 "200"
wait_reconciled loadbalancermonitor "${MON}"
info "waiting up to ${DRIFT_TIMEOUT}s for Cloudflare's interval to become 120"
if poll_mon_field "${mon_id}" interval "120" "${DRIFT_TIMEOUT}"; then
  ok "GATE monitor(3): CF interval updated to $(cf_mon_field "${mon_id}" interval) (expected 120)."
else
  bad "GATE monitor(3): interval NOT updated -- observed $(cf_mon_field "${mon_id}" interval), expected 120."
  exit 1
fi
lb_pause "after editing the monitor interval to 120"

info "confirming convergence -- interval stable at 120 + monitor Ready over ~2 more drift intervals"
stable=1
for i in 1 2; do
  sleep 18
  now="$(cf_mon_field "${mon_id}" interval)"
  rdy="$(mon_ready)"
  info "  poll ${i}: interval=${now} Ready=${rdy:-<none>}"
  [[ "${now}" == "120" ]] || stable=0
  [[ "${rdy}" == "True" ]] || stable=0
done
echo
if [[ "${stable}" -ne 1 ]]; then
  bad "GATE monitor(3): NOT converged -- interval flapped or the monitor left Ready (see the polls above)."
  exit 1
fi
ok "GATE monitor: converged -- monitor round-trip exact for all modeled fields, out-of-band drift restored, in-place edit reflected, no drift-loop."
echo "     Record in the PR: LoadBalancerMonitor scalar/collection fields verified on live CF (round-trip exact + drift restore + in-place edit + convergence)." >&2
