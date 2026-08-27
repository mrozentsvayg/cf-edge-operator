#!/usr/bin/env bash
# List replace-vs-union probes -- DIRECT CF API, no operator/CRD.
# Probe 1: LoadBalancer.default_pools  -- create [pa,pb], PATCH to [pa], observe.
# Probe 2: LoadBalancerPool.origins    -- create 2-origin pool, PATCH to 1, observe.
# Runs STRICTLY SEQUENTIALLY: probe 1 fully cleaned up before probe 2 starts, to
# respect the 2-origin-total account cap. Self-cleaning trap on exit.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

SUFFIX=plist
created_pools=()
lb_id=""

cleanup() {
  if [[ -n "${lb_id}" ]]; then
    cf_api DELETE "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" >/dev/null 2>&1 || true
  fi
  local pid
  for pid in "${created_pools[@]:-}"; do
    if [[ -n "${pid}" ]]; then
      cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pid}" >/dev/null 2>&1 || true
    fi
  done
}
trap cleanup EXIT

mk_pool1() { # $1=name-suffix $2=origin-addr -> echoes pool id (1 origin)
  local resp id
  resp="$(cf_api POST "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" \
    --data "$(jq -n --arg n "pool-${SUFFIX}-$1" --arg a "$2" \
      '{name:$n, origins:[{name:"o1", address:$a, enabled:true}]}')")"
  id="$(jq -r '.result.id // empty' <<<"${resp}")"
  [[ -n "${id}" ]] || { err "pool $1 create failed:"; jq -c '.errors' <<<"${resp}" >&2; exit 1; }
  echo "${id}"
}

echo "############################################################"
echo "## PROBE 1: LoadBalancer.default_pools LIST replace-vs-union"
echo "############################################################"

pa="$(mk_pool1 pa "origin-a.${CF_ZONE_NAME}")"; created_pools+=("${pa}")
pb="$(mk_pool1 pb "origin-b.${CF_ZONE_NAME}")"; created_pools+=("${pb}")
info "pa=${pa} pb=${pb} (2 origins total = cap)"

HOST="lb-${SUFFIX}.${CF_ZONE_NAME}"
info "Create LB default_pools=[pa,pb], fallback=pa"
lb_resp="$(cf_api POST "/zones/${CF_ZONE_ID}/load_balancers" \
  --data "$(jq -n --arg h "${HOST}" --arg pa "${pa}" --arg pb "${pb}" \
    '{name:$h, default_pools:[$pa,$pb], fallback_pool:$pa}')")"
lb_id="$(jq -r '.result.id // empty' <<<"${lb_resp}")"
[[ -n "${lb_id}" ]] || { err "LB create failed:"; jq -c '.errors' <<<"${lb_resp}" >&2; exit 1; }
info "lb=${lb_id}"

dp_before="$(cf_get_lb "${lb_id}" | jq -c '.default_pools')"
info "baseline default_pools = ${dp_before}"

info "PATCH default_pools=[pa] (drop pb)"
p1_resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" '{default_pools:[$pa]}')")"
p1_success="$(jq -r '.success' <<<"${p1_resp}")"
info "PATCH success=${p1_success} errors=$(jq -c '.errors' <<<"${p1_resp}")"
dp_after="$(cf_get_lb "${lb_id}" | jq -c '.default_pools')"
info "after PATCH default_pools = ${dp_after}"
dp_len="$(jq 'length' <<<"${dp_after}")"
has_pb="$(jq -r --arg pb "${pb}" 'index($pb) != null' <<<"${dp_after}")"
if [[ "${dp_len}" == "1" && "${has_pb}" == "false" ]]; then
  ok "PROBE1 default_pools: LIST_REPLACE (result=[pa], pb dropped)"
elif [[ "${has_pb}" == "true" ]]; then
  bad "PROBE1 default_pools: LIST_UNION (pb lingered) result=${dp_after}"
else
  warn "PROBE1 default_pools: OTHER result=${dp_after}"
fi

info "Cleanup probe 1: delete LB then pools"
cf_api DELETE "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" >/dev/null && lb_id=""
cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pa}" >/dev/null
cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pb}" >/dev/null
created_pools=()
info "probe 1 resources deleted"

echo
echo "############################################################"
echo "## PROBE 2: LoadBalancerPool.origins LIST replace-vs-union"
echo "############################################################"

info "Create pool with 2 origins (o1,o2) = 2 origins total = cap"
pool_resp="$(cf_api POST "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" \
  --data "$(jq -n --arg n "pool-${SUFFIX}-origins" \
    --arg a1 "origin-a.${CF_ZONE_NAME}" --arg a2 "origin-b.${CF_ZONE_NAME}" \
    '{name:$n, origins:[{name:"o1", address:$a1, enabled:true},
                        {name:"o2", address:$a2, enabled:true}]}')")"
pool2="$(jq -r '.result.id // empty' <<<"${pool_resp}")"
[[ -n "${pool2}" ]] || { err "2-origin pool create failed:"; jq -c '.errors' <<<"${pool_resp}" >&2; exit 1; }
created_pools+=("${pool2}")
info "pool=${pool2}"

or_before="$(cf_get_pool "${pool2}" | jq -c '[.origins[].name]')"
info "baseline origins = ${or_before}"

info "PATCH origins=[o1] (drop o2)"
p2_resp="$(cf_api PATCH "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pool2}" \
  --data "$(jq -n --arg a1 "origin-a.${CF_ZONE_NAME}" \
    '{origins:[{name:"o1", address:$a1, enabled:true}]}')")"
p2_success="$(jq -r '.success' <<<"${p2_resp}")"
info "PATCH success=${p2_success} errors=$(jq -c '.errors' <<<"${p2_resp}")"
or_after="$(cf_get_pool "${pool2}" | jq -c '[.origins[].name]')"
info "after PATCH origins = ${or_after}"
or_len="$(cf_get_pool "${pool2}" | jq '.origins | length')"
has_o2="$(cf_get_pool "${pool2}" | jq -r '[.origins[].name] | index("o2") != null')"
if [[ "${or_len}" == "1" && "${has_o2}" == "false" ]]; then
  ok "PROBE2 origins: LIST_REPLACE (result=[o1], o2 dropped)"
elif [[ "${has_o2}" == "true" ]]; then
  bad "PROBE2 origins: LIST_UNION (o2 lingered) result=${or_after}"
else
  warn "PROBE2 origins: OTHER result=${or_after}"
fi

info "Cleanup probe 2: delete pool"
cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pool2}" >/dev/null
created_pools=()
info "probe 2 resources deleted"

echo
info "================ FINAL STATE CHECK ================"
info "LBs matching ${HOST}: $(cf_lb_exists_by_hostname "${HOST}" || echo NONE)"
info "account pools named pool-${SUFFIX}-*:"
cf_result "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" \
  | jq -r --arg p "pool-${SUFFIX}-" '.[] | select(.name|startswith($p)) | "\(.id) \(.name)"' || true
ok "probes complete"
