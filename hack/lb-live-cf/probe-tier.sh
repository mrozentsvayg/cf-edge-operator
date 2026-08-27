#!/usr/bin/env bash
# probe-tier.sh -- discover which Cloudflare Load Balancing features THIS account's
# LB subscription permits, and print a capability matrix.
#
# Cloudflare Load Balancing is a paid add-on: every tier, including the entry-level
# one, is a paid subscription. This script does NOT assume any particular tier -- it
# applies one minimal CR per tier-sensitive dimension, lets the operator reconcile it
# against Cloudflare, and records whether Cloudflare ACCEPTED it (Ready=True) or
# REJECTED it (Ready=False + Cloudflare's verbatim error). The result is a matrix of
# what your subscription actually allows -- useful for choosing the right fields for
# the pre-merge PATCH gates, for the tier-behavior docs, and for anyone sizing the
# operator against their own account.
#
# Observe-only: each probe is applied, observed, then deleted. Probes run one at a
# time with full cleanup between them, so a per-account resource cap (e.g. a total
# origin limit) can't make one probe falsely fail because another is still present.
#
# Prereqs: the operator running in load-balancing mode (./run-operator.sh lb) and the
# base substrate applied (./setup.sh gives a validated Account "livecf" + Zone "livecf").
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

if ! curl -sf "http://localhost:8081/healthz" >/dev/null 2>&1; then
  warn "operator health probe (:8081) not responding -- start it with ./run-operator.sh lb"
  warn "continuing anyway; if every probe shows PENDING, the operator isn't running"
fi

rows=()

# probe_outcome waits for <kind>/<name> to get a Ready condition, then echoes a
# tab-separated "OUTCOME<TAB>reason<TAB>message" (message trimmed to one line, 100 chars).
probe_outcome() {
  local kind="$1" name="$2" timeout="${3:-45}" st reason msg i
  for ((i = 0; i < timeout; i++)); do
    st="$(kc get "${kind}" "${name}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
    [[ -n "${st}" ]] && break
    sleep 1
  done
  reason="$(kc get "${kind}" "${name}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  msg="$(kc get "${kind}" "${name}" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].message}' 2>/dev/null | tr '\n' ' ' | cut -c1-400 || true)"
  case "${st}" in
    True) printf 'ACCEPT\t%s\t\n' "${reason}" ;;
    False) printf 'REJECT\t%s\t%s\n' "${reason}" "${msg}" ;;
    *) printf 'PENDING\t\tno Ready condition after %ss\n' "${timeout}" ;;
  esac
}

# finish_probe records the outcome for an applied CR, then deletes it (waiting for the
# finalizer so any Cloudflare-side resource is cleaned before the next probe).
finish_probe() { # label kind name
  local label="$1" kind="$2" name="$3"
  info "probe: ${label}"
  rows+=("${label}	$(probe_outcome "${kind}" "${name}")")
  kc delete "${kind}" "${name}" --ignore-not-found --timeout=60s >/dev/null 2>&1 ||
    kc delete "${kind}" "${name}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}

# ---- Monitor probes (account-scoped, standalone) -------------------------
mon() { # name interval [extra-yaml]
  kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerMonitor
metadata: {name: $1, namespace: ${LBKIT_NS}}
spec:
  accountRef: {name: livecf}
  type: https
  method: GET
  path: /healthz
  expectedCodes: "200"
  timeout: 5
  retries: 2
  interval: $2
${3:-}
EOF
}

mon probe-mon-60 60 && finish_probe "monitor interval=60" loadbalancermonitor probe-mon-60
mon probe-mon-30 30 && finish_probe "monitor interval=30" loadbalancermonitor probe-mon-30
mon probe-mon-hdr 60 $'  header:\n    Host: ["a.example.com"]\n    X-Probe: ["b.example.com"]' &&
  finish_probe "monitor header map (2 keys)" loadbalancermonitor probe-mon-hdr

# ---- Pool probes (account-scoped, standalone; monitorless to isolate) -----
pool() { # name extra-spec-yaml  (origins provided by extra)
  kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancerPool
metadata: {name: $1, namespace: ${LBKIT_NS}}
spec:
  accountRef: {name: livecf}
$2
EOF
}

pool probe-pool-2 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n    - {name: o2, address: origin-2.'"${CF_ZONE_NAME}"$'}' &&
  finish_probe "pool 2 origins" loadbalancerpool probe-pool-2
pool probe-pool-3 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n    - {name: o2, address: origin-2.'"${CF_ZONE_NAME}"$'}\n    - {name: o3, address: origin-3.'"${CF_ZONE_NAME}"$'}' &&
  finish_probe "pool 3 origins" loadbalancerpool probe-pool-3
pool probe-pool-cr1 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  checkRegions: [WNAM]' &&
  finish_probe "pool checkRegions=1 (WNAM)" loadbalancerpool probe-pool-cr1
pool probe-pool-cr2 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  checkRegions: [WNAM, WEU]' &&
  finish_probe "pool checkRegions=2 (WNAM,WEU)" loadbalancerpool probe-pool-cr2
pool probe-pool-cr3 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  checkRegions: [WNAM, WEU, EEU]' &&
  finish_probe "pool checkRegions=3" loadbalancerpool probe-pool-cr3
pool probe-pool-cr5 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  checkRegions: [WNAM, WEU, EEU, SEAS, OC]' &&
  finish_probe "pool checkRegions=5" loadbalancerpool probe-pool-cr5
pool probe-pool-cr13 $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  checkRegions: [WNAM, ENAM, WEU, EEU, NSAM, SSAM, OC, ME, NAF, SAF, SAS, SEAS, NEAS]' &&
  finish_probe "pool checkRegions=13 (all named)" loadbalancerpool probe-pool-cr13
pool probe-pool-crall $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  checkRegions: [ALL_REGIONS]' &&
  finish_probe "pool checkRegions=ALL_REGIONS" loadbalancerpool probe-pool-crall
pool probe-pool-ls $'  origins:\n    - {name: o1, address: origin-1.'"${CF_ZONE_NAME}"$'}\n  loadShedding:\n    defaultPercent: "10.0"\n    defaultPolicy: random\n    sessionPercent: "20.0"\n    sessionPolicy: hash' &&
  finish_probe "pool loadShedding (nested)" loadbalancerpool probe-pool-ls

# ---- LoadBalancer probes (zone-scoped; need one resolvable pool) ----------
# A single monitorless base pool (1 origin) backs the LB probes; LBs have no origins
# of their own, so it is the only origin consumer during this phase.
info "creating base pool for LoadBalancer probes"
pool probe-base-pool $'  origins:\n    - {name: b1, address: origin-base.'"${CF_ZONE_NAME}"$'}' >/dev/null 2>&1 || true
if wait_ready loadbalancerpool probe-base-pool 60; then
  lb() { # name extra-spec-yaml
    kctl apply -f - <<EOF
apiVersion: loadbalancing.cf-edge.io/v1beta1
kind: LoadBalancer
metadata: {name: $1, namespace: ${LBKIT_NS}}
spec:
  zoneRef: {name: livecf}
  hostname: $1.${CF_ZONE_NAME}
  defaultPoolRefs: [{name: probe-base-pool}]
  fallbackPoolRef: {name: probe-base-pool}
$2
EOF
  }
  lb probe-lb-off $'  steeringPolicy: "off"' && finish_probe "LB steeringPolicy=off" loadbalancer probe-lb-off
  lb probe-lb-geo $'  steeringPolicy: geo\n  regionPools:\n    WNAM: [{name: probe-base-pool}]' &&
    finish_probe "LB steeringPolicy=geo + regionPools" loadbalancer probe-lb-geo
  lb probe-lb-dyn $'  steeringPolicy: dynamic_latency' && finish_probe "LB steeringPolicy=dynamic_latency" loadbalancer probe-lb-dyn
  lb probe-lb-sa $'  sessionAffinity: cookie\n  sessionAffinityAttributes:\n    drainDuration: 60\n    samesite: Auto\n    secure: Auto' &&
    finish_probe "LB sessionAffinity=cookie + attributes" loadbalancer probe-lb-sa
  kc delete loadbalancerpool probe-base-pool --ignore-not-found --timeout=60s >/dev/null 2>&1 || true
else
  warn "base pool did not provision -- skipping LoadBalancer/steering/affinity probes"
  rows+=("LB probes"$'\t'"SKIPPED"$'\t'"base pool not provisionable"$'\t')
  kc delete loadbalancerpool probe-base-pool --ignore-not-found --wait=false >/dev/null 2>&1 || true
fi

# ---- matrix --------------------------------------------------------------
echo
echo "=== Cloudflare LB tier capability matrix (account: ${CF_ACCOUNT_ID}) ==="
{
  printf 'FEATURE\tOUTCOME\tREASON\n'
  printf '%s\n' "${rows[@]}" | cut -f1-3
} | column -t -s "$(printf '\t')"
echo
echo "--- Rejections (full Cloudflare message) ---"
printf '%s\n' "${rows[@]}" | awk -F'\t' '$2 == "REJECT" { printf "  %s:\n    %s\n", $1, $4 }'
echo
echo "ACCEPT = your subscription allows it; REJECT = it does not (full message above);"
echo "PENDING = the operator did not reconcile it (is ./run-operator.sh lb running?)."
