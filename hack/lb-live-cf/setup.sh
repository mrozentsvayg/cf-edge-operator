#!/usr/bin/env bash
# Install the CRDs and stand up the identity substrate (namespace, Secret,
# Account, Zone) against the pinned kube context, then wait for the Account and
# Zone to validate against real Cloudflare. Kept lean -- no Monitor or Pools -- so a
# low per-account origin cap (entry LB tier) is never consumed by the substrate;
# each gate script creates its own minimal test resources on top.
#
# Prereq: the operator must be running in load-balancing mode. Start it with
#   ./run-operator.sh lb
# in another terminal (or before running this), so the Account and Zone controllers
# can reconcile what this applies.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

info "Installing CRDs (make install)"
make -C "${REPO_ROOT}" install KUBECONTEXT="${LBKIT_CONTEXT}"

info "Applying base substrate into namespace ${LBKIT_NS}"
render base.yaml.tmpl | kctl apply -f -

info "Waiting for Account/Zone to initialize (needs the operator running)"
wait_initialized account livecf
wait_initialized zone livecf

echo
info "Identity substrate ready:"
kc get account,zone
echo
ok "setup complete -- run ./gate-map-replace.sh and ./gate-nested-merge.sh"
