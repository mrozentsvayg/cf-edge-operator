#!/usr/bin/env bash
# Pool-weight (random_steering) CF-semantics repros -- DIRECT CF API, no
# operator/CRD involved, so this measures raw Cloudflare behavior the drift clause
# depends on. Uses only 2 pools (2 origins) to respect the account's origin cap.
# Creates temp pools + an LB, inspects, cleans up on exit.
#
#   R1 (echo):        with default=[pa,pb] and only pa weighted, does CF echo the
#                     UNWEIGHTED member pb into pool_weights (at default_weight)?
#                     -> if yes, drift clause(b) must tolerate a CF key whose value
#                     == effective default_weight.
#   R2 (nested null): after weighting pb too, does a null inside
#                     random_steering.pool_weights REMOVE pb on PATCH?
#                     -> clause(b) nulls out-of-band weights this way.
#   R3 (least_conns): does CF accept steering_policy=least_connections on a zone (L7)
#                     LB? -> accepted: weight-based, keep in enum + add to gate;
#                     rejected: drop from enum / document.
#   R4 (loop-safety): does CF DROP a pool weight whose value == the effective
#                     default_weight? -> if it drops, drift must treat an absent
#                     key as equal to default_weight (else the operator loops).
#   DW (omit semant): does CF partial-merge random_steering (keep an omitted
#                     default_weight) or replace it (reset to 1)? -> decides
#                     whether the operator may leave default_weight unset and
#                     "rely on CF's default_weight" declaratively.
#
# Requires the four CF_* vars via .env (never printed). Mutates live Cloudflare.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

# This repro pauses between mutations (via lb_pause) so a human can eyeball the CF
# dashboard against the API state just printed. lb_pause only blocks when LBKIT_PAUSE
# is set and stdin is an interactive TTY; otherwise it just logs the pause point, so
# a non-interactive run streams straight through.

# show_rs prints both random_steering fields the drift clause cares about, from an
# LB result blob ($1), so every GET below reports the same two values side by side.
show_rs() { # $1 = LB result JSON
  info "default_weight = $(jq '.random_steering.default_weight' <<<"$1")"
  info "pool_weights   = $(jq -c '.random_steering.pool_weights' <<<"$1")"
}

SUFFIX=pwrepro
HOST="lb-${SUFFIX}.${CF_ZONE_NAME}"
HOST_DW="lb-dwrepro.${CF_ZONE_NAME}"
created_pools=()
lb_id=""
lb_dw=""

cleanup() {
  if [[ -n "${lb_id}" ]]; then
    cf_api DELETE "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${lb_dw}" ]]; then
    cf_api DELETE "/zones/${CF_ZONE_ID}/load_balancers/${lb_dw}" >/dev/null 2>&1 || true
  fi
  local pid
  for pid in "${created_pools[@]:-}"; do
    if [[ -n "${pid}" ]]; then
      cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pid}" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

mk_pool() { # $1=name-suffix $2=origin-addr -> echoes pool id on stdout
  local resp id
  resp="$(cf_api POST "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" \
    --data "$(jq -n --arg n "pool-${SUFFIX}-$1" --arg a "$2" \
      '{name:$n, origins:[{name:"o1", address:$a, enabled:true}]}')")"
  id="$(jq -r '.result.id // empty' <<<"${resp}")"
  [[ -n "${id}" ]] || { err "pool $1 create failed:"; jq -c '.errors' <<<"${resp}" >&2; exit 1; }
  echo "${id}"
}

info "Creating 2 pools (pa, pb) -- 2 origins, within the account cap"
pa="$(mk_pool pa "origin-a.${CF_ZONE_NAME}")"; created_pools+=("${pa}")
pb="$(mk_pool pb "origin-b.${CF_ZONE_NAME}")"; created_pools+=("${pb}")
info "pa=${pa} pb=${pb}"

info "Creating LB: default_pools=[pa,pb], pool_weights={pa:0.6}, steering=random (pb is an UNWEIGHTED member)"
lb_resp="$(cf_api POST "/zones/${CF_ZONE_ID}/load_balancers" \
  --data "$(jq -n --arg h "${HOST}" --arg pa "${pa}" --arg pb "${pb}" \
    '{name:$h, default_pools:[$pa,$pb], fallback_pool:$pa, steering_policy:"random",
      random_steering:{default_weight:1, pool_weights:{($pa):0.6}}}')")"
lb_id="$(jq -r '.result.id // empty' <<<"${lb_resp}")"
[[ -n "${lb_id}" ]] || { err "LB create failed:"; jq -c '.errors' <<<"${lb_resp}" >&2; exit 1; }
info "lb=${lb_id}"

echo
info "R1 (echo): reading back pool_weights (pb is a default member with no explicit weight)"
lb_now="$(cf_get_lb "${lb_id}")"
info "CF pool_weights = $(jq -c '.random_steering.pool_weights' <<<"${lb_now}")"
if jq -e --arg pb "${pb}" '.random_steering.pool_weights | has($pb)' <<<"${lb_now}" >/dev/null; then
  warn "R1: CF ECHOES the unweighted member pb -> clause(b) MUST tolerate a CF key whose value == default_weight"
else
  ok "R1: CF does NOT echo pb -> pool_weights holds only explicitly-weighted keys"
fi

echo
info "R2 (nested null): weight pb too, confirm {pa,pb}, then PATCH pool_weights={pa:0.6, pb:null}"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" --arg pb "${pb}" '{random_steering:{pool_weights:{($pa):0.6, ($pb):0.3}}}')" >/dev/null
info "after weighting both: $(cf_get_lb "${lb_id}" | jq -c '.random_steering.pool_weights')"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" --arg pb "${pb}" '{random_steering:{pool_weights:{($pa):0.6, ($pb):null}}}')" >/dev/null
lb_now="$(cf_get_lb "${lb_id}")"
info "after nulling pb: $(jq -c '.random_steering.pool_weights' <<<"${lb_now}")"
if jq -e --arg pb "${pb}" '.random_steering.pool_weights | has($pb) | not' <<<"${lb_now}" >/dev/null; then
  ok "R2: nested null REMOVED pb -> nested pool_weights null-removal works (clause(b) mechanism valid)"
else
  bad "R2: pb still present -> nested null-removal does NOT work; clause(b) needs another mechanism"
fi

echo
info "R3 (least_connections): PATCH steering_policy=least_connections on this zone LB"
r3="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" --data '{"steering_policy":"least_connections"}')"
if [[ "$(jq -r '.success' <<<"${r3}")" == "true" ]]; then
  warn "R3: CF ACCEPTED least_connections on a zone LB -> valid here + weight-based -> keep in enum, ADD to the weight gate"
else
  ok "R3: CF REJECTED least_connections: $(jq -c '.errors' <<<"${r3}") -> drop from enum (or document L4-only)"
fi

echo
info "R4 (loop-safety): does CF DROP a pool weight whose value == the effective default_weight?"
info "R4a: PATCH pool_weights={pa:1} on the existing LB (default_weight left at its default of 1)"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" '{random_steering:{pool_weights:{($pa):1}}}')" >/dev/null
lb_now="$(cf_get_lb "${lb_id}")"
show_rs "${lb_now}"
if jq -e --arg pa "${pa}" '.random_steering.pool_weights | has($pa)' <<<"${lb_now}" >/dev/null; then
  ok "R4a: CF KEEPS pa=1 even though it equals default_weight -> no phantom drift"
else
  warn "R4a: CF DROPS pa=1 (== default_weight) -> operator would re-add it every reconcile (loop); drift must treat an absent key as == default_weight"
fi
lb_pause "after R4a: pool_weights={pa:1} (weight == default_weight)"

info "R4b: PATCH pool_weights={pa:0.5}, default_weight=0.5 (weight again == default_weight)"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" '{random_steering:{pool_weights:{($pa):0.5},default_weight:0.5}}')" >/dev/null
lb_now="$(cf_get_lb "${lb_id}")"
show_rs "${lb_now}"
if jq -e --arg pa "${pa}" '.random_steering.pool_weights | has($pa)' <<<"${lb_now}" >/dev/null; then
  ok "R4b: CF KEEPS pa=0.5 (== default_weight 0.5) -> no phantom drift"
else
  warn "R4b: CF DROPS pa=0.5 (== default_weight 0.5) -> same loop risk as R4a"
fi
lb_pause "after R4b: pool_weights={pa:0.5}, default_weight=0.5"

echo
# DW probes whether CF partial-merges random_steering (a JSON merge-patch that keeps
# an omitted default_weight) or REPLACES the whole object (resetting an omitted key
# to CF's default of 1). This is what decides whether the operator's "rely on CF's
# default_weight" model is declarative: if omitting the key on any random_steering
# write resets it, the operator can never leave default_weight unset and must always
# send it explicitly. A dedicated fresh LB is used so step A observes a pristine,
# never-written default_weight; pools pa/pb are reused (no new origins -> account cap).
info "DW (default_weight omit semantics): partial-merge (keeps omitted default_weight) or replace (resets it)?"
info "DW-A: create lb_dw with steering=random, default_pools=[pa,pb], fallback=pa, and NO random_steering block"
dw_resp="$(cf_api POST "/zones/${CF_ZONE_ID}/load_balancers" \
  --data "$(jq -n --arg h "${HOST_DW}" --arg pa "${pa}" --arg pb "${pb}" \
    '{name:$h, default_pools:[$pa,$pb], fallback_pool:$pa, steering_policy:"random"}')")"
lb_dw="$(jq -r '.result.id // empty' <<<"${dw_resp}")"
[[ -n "${lb_dw}" ]] || { err "lb_dw create failed:"; jq -c '.errors' <<<"${dw_resp}" >&2; exit 1; }
info "lb_dw=${lb_dw}"
lb_now="$(cf_get_lb "${lb_dw}")"
show_rs "${lb_now}"
info "DW-A question: is default_weight present, and is it 1 (CF's documented default)?"
lb_pause "after DW-A: fresh LB with no random_steering block"

info "DW-B: PATCH default_weight=0.2 (no pool_weights key)"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_dw}" \
  --data "$(jq -n '{random_steering:{default_weight:0.2}}')" >/dev/null
lb_now="$(cf_get_lb "${lb_dw}")"
show_rs "${lb_now}"
info "DW-B question: did default_weight become 0.2?"
lb_pause "after DW-B: default_weight patched to 0.2"

# DW-C / DW-C2 are the crux: each PATCHes random_steering WITHOUT a default_weight
# key. If CF partial-merges, the 0.2 from DW-B PERSISTS (omitting the key is safe
# and declarative); if CF replaces random_steering, the omitted key REVERTS to the
# default of 1 (the operator must always send default_weight).
info "DW-C: PATCH pool_weights={} (NO default_weight key) -- does the 0.2 persist or revert?"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_dw}" \
  --data "$(jq -n '{random_steering:{pool_weights:{}}}')" >/dev/null
lb_now="$(cf_get_lb "${lb_dw}")"
show_rs "${lb_now}"
if jq -e '.random_steering.default_weight == 0.2' <<<"${lb_now}" >/dev/null; then
  ok "DW-C: default_weight PERSISTS at 0.2 -> CF partial-merges random_steering; omitting the key is declarative"
else
  warn "DW-C: default_weight REVERTED (not 0.2) -> CF REPLACES random_steering; operator must always send default_weight"
fi
lb_pause "after DW-C: patched pool_weights={} with no default_weight key"

info "DW-C2: PATCH pool_weights={pa:0.6} (NO default_weight key) -- persist or revert?"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_dw}" \
  --data "$(jq -n --arg pa "${pa}" '{random_steering:{pool_weights:{($pa):0.6}}}')" >/dev/null
lb_now="$(cf_get_lb "${lb_dw}")"
show_rs "${lb_now}"
if jq -e '.random_steering.default_weight == 0.2' <<<"${lb_now}" >/dev/null; then
  ok "DW-C2: default_weight PERSISTS at 0.2 alongside a real weight -> partial-merge confirmed"
else
  warn "DW-C2: default_weight REVERTED with a real weight present -> replace semantics confirmed; always send default_weight"
fi
lb_pause "after DW-C2: patched pool_weights={pa:0.6} with no default_weight key"

echo
ok "repros complete -- cleaning up (2 LBs + 2 pools)"
