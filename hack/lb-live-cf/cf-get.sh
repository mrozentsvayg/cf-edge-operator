#!/usr/bin/env bash
# Dump the raw Cloudflare state of a kit resource, resolved from the CR's
# status.id. Handy for eyeballing what the operator actually wrote.
#
# Usage: ./cf-get.sh <lb|pool|monitor> <cr-name>
#   ./cf-get.sh lb livecf-lb
#   ./cf-get.sh pool livecf-pool-a
#   ./cf-get.sh monitor livecf-http
set -euo pipefail

# shellcheck disable=SC1091
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
init

kind="${1:-}"
name="${2:-}"
if [[ -z "${kind}" || -z "${name}" ]]; then
  err "usage: $0 <lb|pool|monitor> <cr-name>"
  exit 1
fi

case "${kind}" in
  lb)      crkind=loadbalancer;        getter=cf_get_lb ;;
  pool)    crkind=loadbalancerpool;    getter=cf_get_pool ;;
  monitor) crkind=loadbalancermonitor; getter=cf_get_monitor ;;
  *) err "unknown kind: ${kind} (use lb | pool | monitor)"; exit 1 ;;
esac

id="$(cr_id "${crkind}" "${name}")"
if [[ -z "${id}" ]]; then
  err "${crkind}/${name} has no status.id (not created in Cloudflare yet?)"
  exit 1
fi

info "${crkind}/${name} -> Cloudflare id ${id}"
"${getter}" "${id}"
