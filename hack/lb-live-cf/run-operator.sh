#!/usr/bin/env bash
# Build and run the operator locally against the current kube context, wired to
# the kit namespace. It talks to real Cloudflare using the token in the
# in-cluster Secret. Backgrounds the process via `make dev-run` (writes
# /tmp/cf-edge-operator.log and .pid); stop it with ./run-operator.sh stop.
#
# Modes:
#   lb         load balancing on, custom hostname off (default; for the gates)
#   lb-health  same, plus --enable-pool-health (opt-in pool-health axis)
#   off        both features off (for the degradation test)
#   stop       stop the running operator
#
# Requires a Go toolchain matching .go-version on PATH. If yours is elsewhere,
# export LBKIT_GO_BIN=/path/to/go/bin and it will be prepended to PATH.
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
load_env

mode="${1:-lb}"

if [[ -n "${LBKIT_GO_BIN:-}" ]]; then
  export PATH="${LBKIT_GO_BIN}:${PATH}"
fi

if [[ "${mode}" == "stop" ]]; then
  info "Stopping operator"
  make -C "${REPO_ROOT}" dev-stop
  exit 0
fi

common=(--drift-interval=15s --zap-log-level=1)
case "${mode}" in
  lb)        args=(--enable-loadbalancing --enable-customhostname=false "${common[@]}") ;;
  lb-health) args=(--enable-loadbalancing --enable-customhostname=false --enable-pool-health "${common[@]}") ;;
  off)       args=(--enable-loadbalancing=false --enable-customhostname=false "${common[@]}") ;;
  *) err "unknown mode: ${mode} (use lb | lb-health | off | stop)"; exit 1 ;;
esac

info "Starting operator [mode=${mode}] against namespace ${LBKIT_NS} (context ${LBKIT_CONTEXT:-current})"
make -C "${REPO_ROOT}" dev-run DEV_OPERATOR_NS="${LBKIT_NS}" KUBECONTEXT="${LBKIT_CONTEXT}" ARGS="${args[*]}"
info "Logs: tail -f /tmp/cf-edge-operator.log"
