#!/usr/bin/env bash
# PATCH-semantics probe for session_affinity_attributes + location_strategy.
# DIRECT CF API (no operator). Decides, per nested object, whether Cloudflare
# DEEP-MERGES it on PATCH (unset subfields preserved) or REPLACES it (unset
# subfields reset to CF defaults). REPLACE => operator partial-object writes are
# a silent data-loss bug.
#
# Entitlement caps (staging): <=2 origins total at any instant; probes run
# SEQUENTIALLY, each pool has 1 origin, and every LB+pool is deleted before the
# next probe. Non-default subfield values are used so a reset is unambiguous.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

SUFFIX=saals
lb_id=""
pool_id=""

cleanup() {
  if [[ -n "${lb_id}" ]]; then
    cf_api DELETE "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" >/dev/null 2>&1 || true
    lb_id=""
  fi
  if [[ -n "${pool_id}" ]]; then
    cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pool_id}" >/dev/null 2>&1 || true
    pool_id=""
  fi
}
trap cleanup EXIT

mk_pool() { # $1=name-suffix -> echoes pool id
  local resp id
  resp="$(cf_api POST "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" \
    --data "$(jq -n --arg n "pool-${SUFFIX}-$1" --arg a "origin-$1.${CF_ZONE_NAME}" \
      '{name:$n, origins:[{name:"o1", address:$a, enabled:true}]}')")"
  echo "POOL CREATE $1 raw: ${resp}" >&2
  id="$(jq -r '.result.id // empty' <<<"${resp}")"
  [[ -n "${id}" ]] || { err "pool $1 create failed"; jq -c '.errors' <<<"${resp}" >&2; exit 1; }
  echo "${id}"
}

del_lb_pool() {
  if [[ -n "${lb_id}" ]]; then
    echo "DELETE LB ${lb_id}: $(cf_api DELETE "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" | jq -c '.success')" >&2
    lb_id=""
  fi
  if [[ -n "${pool_id}" ]]; then
    echo "DELETE POOL ${pool_id}: $(cf_api DELETE "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/${pool_id}" | jq -c '.success')" >&2
    pool_id=""
  fi
}

echo "############################################################"
echo "PROBE 1 -- session_affinity_attributes"
echo "############################################################"

pool_id="$(mk_pool p1)"
info "probe1 pool=${pool_id}"

HOST1="lb-${SUFFIX}-1.${CF_ZONE_NAME}"
# Non-default subfields: samesite=Strict (default Auto), secure=Always (default
# Auto), zero_downtime_failover=sticky (default none), drain_duration=10.
create1="$(cf_api POST "/zones/${CF_ZONE_ID}/load_balancers" \
  --data "$(jq -n --arg h "${HOST1}" --arg p "${pool_id}" \
    '{name:$h, default_pools:[$p], fallback_pool:$p, session_affinity:"cookie",
      session_affinity_attributes:{drain_duration:10, samesite:"Strict",
        secure:"Always", zero_downtime_failover:"sticky"}}')")"
echo "P1 LB CREATE raw: ${create1}"
c1_success="$(jq -r '.success' <<<"${create1}")"
if [[ "${c1_success}" != "true" ]]; then
  echo "P1 VERDICT: BLOCKED_ENTITLEMENT (create rejected) errors=$(jq -c '.errors' <<<"${create1}")"
  del_lb_pool
else
  lb_id="$(jq -r '.result.id' <<<"${create1}")"
  info "probe1 lb=${lb_id}"
  base1="$(cf_get_lb "${lb_id}")"
  echo "P1 BASELINE session_affinity=$(jq -c '.session_affinity' <<<"${base1}") saa=$(jq -c '.session_affinity_attributes' <<<"${base1}")"
  b_ss="$(jq -r '.session_affinity_attributes.samesite' <<<"${base1}")"
  b_sec="$(jq -r '.session_affinity_attributes.secure' <<<"${base1}")"
  b_zdf="$(jq -r '.session_affinity_attributes.zero_downtime_failover' <<<"${base1}")"
  b_dd="$(jq -r '.session_affinity_attributes.drain_duration' <<<"${base1}")"

  # PARTIAL PATCH: drain_duration only (mirrors operator sending only CR-set subfields)
  patch1="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
    --data "$(jq -n '{session_affinity_attributes:{drain_duration:20}}')")"
  echo "P1 PATCH(drain_duration:20 only) raw: ${patch1}"
  p1_success="$(jq -r '.success' <<<"${patch1}")"
  after1="$(cf_get_lb "${lb_id}")"
  echo "P1 AFTER saa=$(jq -c '.session_affinity_attributes' <<<"${after1}")"
  a_ss="$(jq -r '.session_affinity_attributes.samesite' <<<"${after1}")"
  a_sec="$(jq -r '.session_affinity_attributes.secure' <<<"${after1}")"
  a_zdf="$(jq -r '.session_affinity_attributes.zero_downtime_failover' <<<"${after1}")"
  a_dd="$(jq -r '.session_affinity_attributes.drain_duration' <<<"${after1}")"
  echo "P1 EVIDENCE before{samesite=${b_ss} secure=${b_sec} zdf=${b_zdf} drain=${b_dd}} after{samesite=${a_ss} secure=${a_sec} zdf=${a_zdf} drain=${a_dd}} patch_success=${p1_success}"
  if [[ "${a_ss}" == "${b_ss}" && "${a_sec}" == "${b_sec}" && "${a_zdf}" == "${b_zdf}" && "${a_dd}" == "20" ]]; then
    echo "P1 VERDICT: DEEP_MERGE_OBJECT (unset subfields preserved; drain updated) -- operator SAFE"
  else
    echo "P1 VERDICT: REPLACE (unset subfields reset to CF defaults) -- DATA-LOSS BUG"
  fi
  del_lb_pool
fi

echo
echo "############################################################"
echo "PROBE 2 -- location_strategy"
echo "############################################################"

pool_id="$(mk_pool p2)"
info "probe2 pool=${pool_id}"

HOST2="lb-${SUFFIX}-2.${CF_ZONE_NAME}"
# Non-default subfields: prefer_ecs=always (default proximity), mode=resolver_ip (default pop).
create2="$(cf_api POST "/zones/${CF_ZONE_ID}/load_balancers" \
  --data "$(jq -n --arg h "${HOST2}" --arg p "${pool_id}" \
    '{name:$h, default_pools:[$p], fallback_pool:$p,
      location_strategy:{prefer_ecs:"always", mode:"resolver_ip"}}')")"
echo "P2 LB CREATE raw: ${create2}"
c2_success="$(jq -r '.success' <<<"${create2}")"
if [[ "${c2_success}" != "true" ]]; then
  echo "P2 VERDICT: BLOCKED_ENTITLEMENT (create rejected) errors=$(jq -c '.errors' <<<"${create2}")"
  del_lb_pool
else
  lb_id="$(jq -r '.result.id' <<<"${create2}")"
  info "probe2 lb=${lb_id}"
  base2="$(cf_get_lb "${lb_id}")"
  echo "P2 BASELINE location_strategy=$(jq -c '.location_strategy' <<<"${base2}")"
  b_pe="$(jq -r '.location_strategy.prefer_ecs' <<<"${base2}")"
  b_mode="$(jq -r '.location_strategy.mode' <<<"${base2}")"

  patch2="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
    --data "$(jq -n '{location_strategy:{mode:"pop"}}')")"
  echo "P2 PATCH(mode:pop only) raw: ${patch2}"
  p2_success="$(jq -r '.success' <<<"${patch2}")"
  after2="$(cf_get_lb "${lb_id}")"
  echo "P2 AFTER location_strategy=$(jq -c '.location_strategy' <<<"${after2}")"
  a_pe="$(jq -r '.location_strategy.prefer_ecs' <<<"${after2}")"
  a_mode="$(jq -r '.location_strategy.mode' <<<"${after2}")"
  echo "P2 EVIDENCE before{prefer_ecs=${b_pe} mode=${b_mode}} after{prefer_ecs=${a_pe} mode=${a_mode}} patch_success=${p2_success}"
  if [[ "${a_pe}" == "${b_pe}" && "${a_mode}" == "pop" ]]; then
    echo "P2 VERDICT: DEEP_MERGE_OBJECT (prefer_ecs preserved; mode updated) -- operator SAFE"
  else
    echo "P2 VERDICT: REPLACE (prefer_ecs reset to CF default) -- DATA-LOSS BUG"
  fi
  del_lb_pool
fi

echo
echo "############################################################"
echo "FINAL CLEANUP CHECK"
echo "############################################################"
cleanup
echo "remaining pools: $(cf_result "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" | jq -c '[.[]|{id,name}]')"
echo "remaining LBs:   $(cf_result "/zones/${CF_ZONE_ID}/load_balancers" | jq -c '[.[]|{id,name}]')"
echo "monitors (fyi):  $(cf_result "/accounts/${CF_ACCOUNT_ID}/load_balancers/monitors" | jq -c '[.[]|{id,type}]')"
echo "DONE"
