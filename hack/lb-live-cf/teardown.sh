#!/usr/bin/env bash
# Tear the kit down. Deletes the CRs in dependency order (LoadBalancers ->
# Pools -> Monitors -> Account/Zone) so Cloudflare accepts each delete, then
# removes the namespace.
#
# IMPORTANT: the operator must be RUNNING in `lb` mode during teardown. With
# --delete-policy=always (the default) the finalizer on each CR triggers the
# matching Cloudflare delete; if the operator is down the finalizers block and
# the CF resources are orphaned. If that happens, restart the operator and
# re-run, or use ./cf-get.sh to find the ids and delete them by hand.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

del() {
  # delete every CR of a kind in the namespace and wait for finalizers
  local kind="$1"
  local names
  names="$(kc get "${kind}" -o 'jsonpath={.items[*].metadata.name}' 2>/dev/null || true)"
  [[ -z "${names}" ]] && return 0
  info "Deleting ${kind}: ${names}"
  # shellcheck disable=SC2086
  kc delete "${kind}" ${names} --timeout=90s || \
    warn "some ${kind} did not delete cleanly (operator down? finalizer stuck?)"
}

del loadbalancer
del loadbalancerpool
del loadbalancermonitor
del account
del zone

info "Deleting the credentials Secret"
kc delete secret cf-livecf-token --ignore-not-found

info "Deleting namespace ${LBKIT_NS}"
kctl delete namespace "${LBKIT_NS}" --ignore-not-found --timeout=60s || \
  warn "namespace not fully removed"

echo
ok "teardown complete"
echo "  CRDs were left installed; remove them with: make -C ${REPO_ROOT} uninstall"
