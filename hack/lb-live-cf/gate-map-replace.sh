#!/usr/bin/env bash
# GATE map-replace -- top-level map key removal via merge-patch nulls.
#
# Cloudflare DEEP-MERGES top-level map properties on PATCH (confirmed on live CF), so
# the operator cannot remove a key the CR dropped by omission -- it must send an
# explicit JSON null for that key (option.WithJSONSet). Without the null, the dropped
# key lingers on Cloudflare forever and the operator drift-loops. This gate verifies
# the operator's null-removal on the monitor `header` map (the entry LB tier allows it;
# geo region_pools needs traffic steering, a higher tier). The same merge-patch-null
# behavior backs region_pools / country_pools / pop_pools -- see the tier-gated geo gate.
#
# Procedure:
#   1. Create a monitor with header {Host, X-Probe}; read it back -> expect both keys.
#   2. Edit the CR to header {Host} (drop X-Probe); read it back.
#        PASS -> X-Probe gone (operator sent a null; CF removed it) AND the operator
#                converges: header stays {Host} with no repeating drift over ~2 intervals.
#        FAIL -> X-Probe persists (null not sent/applied), OR the operator keeps
#                re-detecting drift (loop).
#
# Leaves the monitor in place for inspection; teardown.sh removes it.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

MON=livecf-gate-map-replace-mon

apply_mon() {
  # $1 = "two" (Host + X-Probe) or "one" (Host only)
  local header_block
  if [[ "$1" == "two" ]]; then
    header_block=$'    Host: ["gate-map-replace.'"${CF_ZONE_NAME}"$'"]\n    X-Probe: ["drop-me.'"${CF_ZONE_NAME}"$'"]'
  else
    header_block=$'    Host: ["gate-map-replace.'"${CF_ZONE_NAME}"$'"]'
  fi
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
  path: /healthz
  expectedCodes: "200"
  interval: 60
  timeout: 5
  retries: 2
  header:
${header_block}
EOF
}

header_keys() { jq -r '.header // {} | keys | sort | join(",")'; }

info "Step 1: create monitor with header {Host, X-Probe}"
apply_mon two
wait_ready loadbalancermonitor "${MON}"

mon_id="$(cr_id loadbalancermonitor "${MON}")"
if [[ -z "${mon_id}" ]]; then
  err "monitor has no status.id -- it was not created in Cloudflare"
  kc get loadbalancermonitor "${MON}" -o yaml | sed -n '/status:/,$p' >&2 || true
  exit 1
fi
info "Cloudflare monitor id: ${mon_id}"

before="$(cf_get_monitor "${mon_id}")"
before_keys="$(header_keys <<<"${before}")"
info "header now: $(jq -c '.header' <<<"${before}")"
if [[ "${before_keys}" != "Host,X-Probe" ]]; then
  err "expected header keys {Host,X-Probe}, got {${before_keys}} -- aborting"
  exit 1
fi
ok "both header keys present in Cloudflare before the edit"
lb_pause "after creating the monitor with header {Host, X-Probe}"

echo
info "Step 2: edit the CR to header {Host} only (drop X-Probe)"
apply_mon one
wait_reconciled loadbalancermonitor "${MON}"

after="$(cf_get_monitor "${mon_id}")"
after_keys="$(header_keys <<<"${after}")"
info "header now: $(jq -c '.header' <<<"${after}")"

echo
if [[ "${after_keys}" != "Host" ]]; then
  bad "GATE map-replace: X-Probe still present after the edit (keys={${after_keys}})."
  echo "     Cloudflare deep-merges the top-level map and the operator did NOT remove the"
  echo "     dropped key -- the merge-patch null was not sent/applied. Do NOT merge."
  exit 1
fi
ok "X-Probe removed -- operator sent an explicit merge-patch null for the dropped key"
echo "     (Cloudflare DEEP-MERGES maps, so omission alone would not remove it)."
lb_pause "after dropping X-Probe from the monitor header"

# No-drift-loop check. The symptom this guards against is the operator re-detecting
# drift forever (it sent the reduced map, CF kept the omitted key). Confirm
# convergence: over ~2 drift intervals the header stays {Host} and the operator
# logs no new drift for it.
echo
info "Step 3: confirm convergence -- no drift-loop over ~20s"
LOG="${LBKIT_OPERATOR_LOG:-/tmp/cf-edge-operator.log}"
drift_count() { if [[ -f "${LOG}" ]]; then grep 'drift detected' "${LOG}" 2>/dev/null | grep -c "${MON}" || true; else echo "n/a"; fi; }
before_loops="$(drift_count)"
sleep 20
stable_keys="$(header_keys <<<"$(cf_get_monitor "${mon_id}")")"
after_loops="$(drift_count)"
if [[ "${stable_keys}" != "Host" ]]; then
  bad "GATE map-replace: header changed after settling (keys={${stable_keys}}) -- operator is flapping."
  exit 1
fi
if [[ "${before_loops}" != "n/a" && "${after_loops}" -gt "${before_loops}" ]]; then
  bad "GATE map-replace: operator re-detected drift for ${MON} during settle (${before_loops} -> ${after_loops}) -- drift-loop."
  exit 1
fi
ok "GATE map-replace: converged -- key removed via merge-patch null, header stable, no drift-loop."
echo "     Record in the PR: top-level map key removal via merge-patch nulls confirmed on live CF (monitor header)."
exit 0
