#!/usr/bin/env bash
# HARD PRE-MERGE GATE nested-merge -- nested object is DEEP-MERGED, not replaced.
#
# For load_shedding (pool) and sessionAffinityAttributes (LB) the operator
# manages each subfield independently ("leave-alone per subfield"): it sends a
# subfield only when the CR sets it, and relies on Cloudflare's PATCH to
# preserve the OTHER subfields it did not send. If CF instead replaced the whole
# nested object, an unsent subfield would be silently cleared -- so those fields
# would have to switch to full-object always-send.
#
# NOTE the expectation is the OPPOSITE of the map-replace gate: top-level map KEYS are removed via
# explicit nulls, but nested OBJECTS must DEEP-MERGE (unsent subfields preserved).
#
# The pool is MONITORLESS, so the edit landing also exercises a second behavior:
# editing a monitorless pool must send monitor:null (Cloudflare rejects monitor=""
# with 412 code 1004). The verdict therefore checks in two stages -- (a) did the edit
# land at all (default_percent -> 15)? a non-landing edit is that empty-monitor
# rejection, diagnosed separately; (b) given it landed, did the unsent subfields
# survive (deep-merge)?
#
# Procedure (on a dedicated pool, so the map-replace gate is untouched):
#   1. Create a pool with loadShedding {defaultPercent, defaultPolicy,
#      sessionPercent, sessionPolicy} all set; read back -> expect all four.
#   2. Edit the CR to loadShedding {defaultPercent: 15} only (drop the rest); read back.
#        PASS -> edit landed (default_percent=15) AND sessionPercent/Policy persist (deep-merge).
#        FAIL -> edit did not land (rejection / empty-monitor 412), OR subfields cleared (CF replaced).
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

POOL=livecf-pool-ls

apply_pool() {
  # $1 = indented loadShedding: block body
  kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata:
  name: ${POOL}
  namespace: ${LBKIT_NS}
spec:
  accountRef:
    name: livecf
  origins:
    - name: ls1
      address: origin-ls.${CF_ZONE_NAME}
  loadShedding:
$1
EOF
}

info "Step 1: create pool with loadShedding {default 10/random, session 20/hash}"
apply_pool "$(printf '    defaultPercent: "10.0"\n    defaultPolicy: random\n    sessionPercent: "20.0"\n    sessionPolicy: hash')"
wait_ready loadbalancerpool "${POOL}"

pool_id="$(cr_id loadbalancerpool "${POOL}")"
if [[ -z "${pool_id}" ]]; then
  err "pool has no status.id -- it was not created in Cloudflare"
  kc get loadbalancerpool "${POOL}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
info "Cloudflare pool id: ${pool_id}"

before="$(cf_get_pool "${pool_id}")"
info "load_shedding now: $(jq -c '.load_shedding' <<<"${before}")"
if ! jq -e '.load_shedding.default_percent == 10 and .load_shedding.session_percent == 20 and
            .load_shedding.default_policy == "random" and .load_shedding.session_policy == "hash"' \
    <<<"${before}" >/dev/null; then
  err "load_shedding did not round-trip as sent -- aborting before the merge test"
  exit 1
fi
ok "all four load_shedding subfields present in Cloudflare before the edit"
lb_pause "after creating the pool with all four loadShedding subfields"

echo
info "Step 2: edit the CR to loadShedding {defaultPercent: 15} only"
apply_pool '    defaultPercent: "15.0"'
wait_reconciled loadbalancerpool "${POOL}"

after="$(cf_get_pool "${pool_id}")"
info "load_shedding now: $(jq -c '.load_shedding' <<<"${after}")"
lb_pause "after dropping three loadShedding subfields (kept defaultPercent)"

echo
# Stage (a): did the edit land at all? default_percent must be the new value (15). If it
# is still 10, the PATCH never applied -- on this MONITORLESS pool that is the monitor=""
# -> 412 code 1004 bug (needs the monitor:null write). That is NOT a deep-merge failure, so
# diagnose it distinctly instead of blaming nested-object replace (the trap that misled
# us the first time).
if ! jq -e '.load_shedding.default_percent == 15' <<<"${after}" >/dev/null; then
  bad "GATE nested-merge: the edit did not land -- default_percent is $(jq -c '.load_shedding.default_percent' <<<"${after}"), not 15."
  ready="$(kc get loadbalancerpool "${POOL}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
  echo "     pool Ready=${ready:-<none>}. On a monitorless pool a non-landing edit is the"
  echo "     monitor=\"\" -> 412 code 1004 bug (send monitor:null), NOT deep-merge."
  LOG="${LBKIT_OPERATOR_LOG:-/tmp/cf-edge-operator.log}"
  if [[ -f "${LOG}" ]]; then
    echo "     recent ${POOL} update errors:"
    grep "${POOL}" "${LOG}" 2>/dev/null | grep -iE 'update failed|412|1004' | tail -2 | sed 's/^/       /' || true
  fi
  exit 1
fi

# Stage (b): the edit landed -- now the actual nested-merge question: did the unsent subfields survive?
if jq -e '.load_shedding.session_percent == 20 and .load_shedding.session_policy == "hash"' <<<"${after}" >/dev/null; then
  ok "GATE nested-merge: Cloudflare DEEP-MERGED the nested object -- defaultPercent updated to 15, sessionPercent/Policy survived."
  echo "     Record in the PR: nested-object PATCH = DEEP-MERGE (confirmed on live CF)."
  exit 0
else
  bad "GATE nested-merge: edit landed (default_percent=15) but unsent subfields were cleared (session_percent=$(jq -c '.load_shedding.session_percent' <<<"${after}"))."
  echo "     Cloudflare REPLACED the nested object. The per-subfield leave-alone design for"
  echo "     load_shedding / sessionAffinityAttributes must switch to full-object always-send."
  echo "     Do NOT merge until this is addressed."
  exit 1
fi
