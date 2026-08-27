#!/usr/bin/env bash
# GEO map-key removal CF-semantics repro -- DIRECT CF API, no operator/CRD.
# Measures raw Cloudflare behavior for removing a region_pools key, to decide the
# fix for the geo-map key-removal bug. Uses only 2 pools (1 origin each) to
# respect the account's base-tier origin/pool cap. Self-cleaning trap deletes the
# LB then the pools on exit. Non-interactive (no pauses): drive with tee.
#
#   OMIT:  PATCH region_pools WITHOUT ENAM (no null). If ENAM disappears, CF
#          REPLACES region_pools on write; if ENAM lingers, CF DEEP-MERGES.
#   NULL:  PATCH region_pools with ENAM:null. Accepted -> null-removal works.
#   EMPTY: (only if OMIT did not remove ENAM) PATCH region_pools with ENAM:[].
#          Accepted or rejected?
#
# Requires the four CF_* vars via .env (never printed). Mutates live Cloudflare.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

SUFFIX=georepro
HOST="lb-${SUFFIX}.${CF_ZONE_NAME}"
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

mk_pool() { # $1=name-suffix $2=origin-addr -> echoes pool id on stdout
  local resp id
  resp="$(cf_api POST "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools" \
    --data "$(jq -n --arg n "pool-${SUFFIX}-$1" --arg a "$2" \
      '{name:$n, origins:[{name:"o1", address:$a, enabled:true}]}')")"
  id="$(jq -r '.result.id // empty' <<<"${resp}")"
  [[ -n "${id}" ]] || { err "pool $1 create failed:"; jq -c '.errors' <<<"${resp}" >&2; exit 1; }
  echo "${id}"
}

# rp echoes the LB's live region_pools as compact JSON.
rp() { cf_get_lb "${lb_id}" | jq -c '.region_pools // {}'; }
# has_enam returns 0 iff region_pools currently has an ENAM key.
has_enam() { cf_get_lb "${lb_id}" | jq -e '.region_pools | has("ENAM")' >/dev/null; }

# ---- setup: 2 pools + geo LB with region_pools={WNAM:pa, ENAM:pb} --------
info "Creating 2 pools (pa, pb) -- 1 origin each, within the base-tier cap"
pa="$(mk_pool pa "origin-a.${CF_ZONE_NAME}")"; created_pools+=("${pa}")
pb="$(mk_pool pb "origin-b.${CF_ZONE_NAME}")"; created_pools+=("${pb}")
info "pa=${pa} pb=${pb}"

info "Creating zone LB: steering=geo, default_pools=[pa], fallback=pa, region_pools={WNAM:[pa], ENAM:[pb]}"
lb_resp="$(cf_api POST "/zones/${CF_ZONE_ID}/load_balancers" \
  --data "$(jq -n --arg h "${HOST}" --arg pa "${pa}" --arg pb "${pb}" \
    '{name:$h, default_pools:[$pa], fallback_pool:$pa, steering_policy:"geo",
      region_pools:{WNAM:[$pa], ENAM:[$pb]}}')")"
lb_id="$(jq -r '.result.id // empty' <<<"${lb_resp}")"
[[ -n "${lb_id}" ]] || { err "LB create failed:"; jq -c '.errors' <<<"${lb_resp}" >&2; exit 1; }
info "lb=${lb_id}"

info "Baseline GET region_pools = $(rp)"
if has_enam; then
  ok "SETUP: region_pools has both WNAM and ENAM"
else
  bad "SETUP: ENAM missing right after create -- $(rp); aborting"
  exit 1
fi

# ---- reset helper: force region_pools back to {WNAM:pa, ENAM:pb} ---------
# Uses NULL removal if available; if that is not accepted we fall back to a
# create-order rewrite. Only used between tests; its own success is not a probe.
reset_full() {
  cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
    --data "$(jq -n --arg pa "${pa}" --arg pb "${pb}" \
      '{region_pools:{WNAM:[$pa], ENAM:[$pb]}}')" >/dev/null
}

# ============================ OMIT =======================================
echo
info "OMIT: PATCH region_pools={WNAM:[pa]} (ENAM OMITTED, no null)"
cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" '{region_pools:{WNAM:[$pa]}}')" >/dev/null
omit_after="$(rp)"
info "after OMIT: region_pools = ${omit_after}"
if has_enam; then
  OMIT_RESULT="ENAM STILL PRESENT -> CF DEEP-MERGES region_pools"
  warn "OMIT: ${OMIT_RESULT}"
  omit_removed=0
else
  OMIT_RESULT="ENAM GONE -> CF REPLACES region_pools on write"
  ok "OMIT: ${OMIT_RESULT}"
  omit_removed=1
fi

# ============================ NULL =======================================
echo
info "NULL: reset to {WNAM,ENAM}, then PATCH region_pools={WNAM:[pa], ENAM:null}"
reset_full
info "after reset: region_pools = $(rp)"
null_resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
  --data "$(jq -n --arg pa "${pa}" '{region_pools:{WNAM:[$pa], ENAM:null}}')")"
null_success="$(jq -r '.success' <<<"${null_resp}")"
null_after="$(rp)"
if [[ "${null_success}" == "true" ]]; then
  NULL_RESULT="ACCEPTED (success:true); region_pools now ${null_after}$(has_enam && echo ' [ENAM still present!]' || echo ' [ENAM removed]')"
  ok "NULL: ${NULL_RESULT}"
else
  null_err="$(jq -c '.errors' <<<"${null_resp}")"
  NULL_RESULT="REJECTED errors=${null_err}"
  bad "NULL: ${NULL_RESULT}"
fi

# ============================ EMPTY ======================================
# Only meaningful if OMIT did NOT remove ENAM (i.e. CF deep-merges): does an
# explicit empty list clear the key?
echo
if [[ "${omit_removed}" -eq 1 ]]; then
  EMPTY_RESULT="SKIPPED (OMIT already removed ENAM -> replace semantics; empty-list probe unnecessary)"
  info "EMPTY: ${EMPTY_RESULT}"
else
  info "EMPTY: reset to {WNAM,ENAM}, then PATCH region_pools={WNAM:[pa], ENAM:[]}"
  reset_full
  info "after reset: region_pools = $(rp)"
  empty_resp="$(cf_api PATCH "/zones/${CF_ZONE_ID}/load_balancers/${lb_id}" \
    --data "$(jq -n --arg pa "${pa}" '{region_pools:{WNAM:[$pa], ENAM:[]}}')")"
  empty_success="$(jq -r '.success' <<<"${empty_resp}")"
  empty_after="$(rp)"
  if [[ "${empty_success}" == "true" ]]; then
    EMPTY_RESULT="ACCEPTED (success:true); region_pools now ${empty_after}$(has_enam && echo ' [ENAM still present]' || echo ' [ENAM removed]')"
    ok "EMPTY: ${EMPTY_RESULT}"
  else
    empty_err="$(jq -c '.errors' <<<"${empty_resp}")"
    EMPTY_RESULT="REJECTED errors=${empty_err}"
    bad "EMPTY: ${EMPTY_RESULT}"
  fi
fi

# ============================ SUMMARY ====================================
echo
info "================ FINDING SUMMARY ================"
info "OMIT  : ${OMIT_RESULT}"
info "NULL  : ${NULL_RESULT}"
info "EMPTY : ${EMPTY_RESULT}"
if [[ "${omit_removed}" -eq 1 ]]; then
  info "CONCLUSION: region_pools is REPLACE-on-write; operator sends the full desired map WITHOUT the key (no null needed)."
else
  info "CONCLUSION: region_pools is DEEP-MERGE; operator must send an explicit merge-patch null for the dropped key."
fi
ok "repro complete -- cleaning up (1 LB + 2 pools)"
