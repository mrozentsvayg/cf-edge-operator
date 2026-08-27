#!/usr/bin/env bash
# Degradation test: with load balancing DISABLED, the LB controllers must not
# run, so a LoadBalancer CR is left completely untouched (no status, no
# Cloudflare resource created). This confirms the feature gate actually gates.
#
# The script restarts the operator in `off` mode, applies a probe LoadBalancer,
# waits, asserts it was ignored on both the K8s and Cloudflare sides, then
# restarts the operator back in `lb` mode so the gates can run afterwards.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

LB=livecf-lb-degraded
HOST="${LB}.${CF_ZONE_NAME}"

info "Restarting operator with load balancing OFF"
"${LBKIT_DIR}/run-operator.sh" off

info "Applying probe LoadBalancer ${LB}"
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
  defaultPoolRefs:
    - name: livecf-pool-a
  fallbackPoolRef:
    name: livecf-pool-a
EOF

info "Waiting 20s to give any (wrongly-enabled) controller time to act"
sleep 20

fail=0

conds="$(kc get loadbalancer "${LB}" -o 'jsonpath={.status.conditions}' 2>/dev/null || true)"
id="$(cr_id loadbalancer "${LB}")"
if [[ -z "${id}" && ( -z "${conds}" || "${conds}" == "[]" ) ]]; then
  ok "K8s side: CR was not reconciled (no status.id, no conditions)"
else
  bad "K8s side: CR shows reconcile activity (status.id='${id}', conditions='${conds}')"
  fail=1
fi

cf_id="$(cf_lb_exists_by_hostname "${HOST}")"
if [[ -z "${cf_id}" ]]; then
  ok "Cloudflare side: no load balancer named ${HOST} was created"
else
  bad "Cloudflare side: a load balancer ${HOST} exists (id ${cf_id}) -- should not have been created"
  fail=1
fi

info "Cleaning up the probe CR"
kc delete loadbalancer "${LB}" --ignore-not-found --wait=false

info "Restarting operator back in load-balancing mode"
"${LBKIT_DIR}/run-operator.sh" lb

echo
if [[ "${fail}" -eq 0 ]]; then
  ok "DEGRADATION: load balancing gate holds when disabled."
  exit 0
else
  bad "DEGRADATION: the feature acted while disabled."
  exit 1
fi
