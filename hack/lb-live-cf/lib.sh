# shellcheck shell=bash
# Shared helpers for the live-Cloudflare load-balancing test kit.
# Sourced by the setup/run/gate/test scripts in this directory; not executable
# on its own.

# Resolve this kit's directory and the repo root regardless of the caller's CWD.
LBKIT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${LBKIT_DIR}/../.." && pwd)"
export LBKIT_DIR REPO_ROOT

# ---- logging -------------------------------------------------------------
# All diagnostics go to stderr so stdout is reserved for data a caller may pipe
# (e.g. cf-get.sh's JSON into jq). Only actual result data should reach stdout.
info() { printf '\033[36m==>\033[0m %s\n' "$*" >&2; }
ok()   { printf '  \033[32mPASS\033[0m %s\n' "$*" >&2; }
bad()  { printf '  \033[31mFAIL\033[0m %s\n' "$*" >&2; }
warn() { printf '  \033[33mWARN\033[0m %s\n' "$*" >&2; }
err()  { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; }

# ---- observation pause ---------------------------------------------------
# lb_pause halts between mutations so a human can inspect the Cloudflare dashboard,
# ONLY when LBKIT_PAUSE is set (any non-empty value); a no-op otherwise. It ALWAYS
# logs the pause point when enabled (so non-interactive runs show where it would
# pause), but only BLOCKS for Enter on an interactive TTY -- so it never hangs a
# piped/CI run.
lb_pause() {
  [[ -n "${LBKIT_PAUSE:-}" ]] || return 0
  info "PAUSE: ${1:-paused} -- inspect the Cloudflare dashboard"
  if [[ -t 0 ]]; then read -rp "  >>> press Enter to continue... " _ || true
  else info "  (non-interactive stdin; not blocking)"; fi
}

# ---- environment ---------------------------------------------------------
# load_env sources ./.env (if present), then applies defaults. Values already
# in the environment win over the file.
load_env() {
  local envf="${LBKIT_ENV:-${LBKIT_DIR}/.env}"
  if [[ -f "${envf}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${envf}"
    set +a
  fi
  : "${CF_API:=https://api.cloudflare.com/client/v4}"
  # The operator's Prometheus /metrics endpoint. run-operator.sh runs the operator
  # via `make dev-run`, whose recipe passes --metrics-secure=false
  # --metrics-bind-address=:8080 (DEV_METRICS_PORT) -- and cmd/main.go installs the
  # authn/authz filter ONLY when --metrics-secure is true -- so the endpoint is
  # plain HTTP with no auth and the kit can scrape it directly. Override this if you
  # changed DEV_METRICS_PORT.
  : "${LBKIT_METRICS_URL:=http://127.0.0.1:8080/metrics}"
  : "${LBKIT_NS:=cf-edge-operator-system}"
  # LBKIT_CONTEXT pins the kubeconfig context for every kubectl call, the CRD
  # install (make KUBECONTEXT), and the operator (--kube-context), so the kit
  # never depends on your current-context. Defaults to the cluster the README
  # creates; set to "" to fall back to current-context.
  : "${LBKIT_CONTEXT:=kind-cf-lb}"
  # LBKIT_PAUSE is an opt-in observation flag (see lb_pause); intentionally left
  # with no default so it stays unset unless the caller exports it.
  export CF_API LBKIT_METRICS_URL LBKIT_NS LBKIT_CONTEXT LBKIT_PAUSE
}

# require_env fails unless the four Cloudflare identifiers are set.
require_env() {
  local missing=0 v
  for v in CF_API_TOKEN CF_ACCOUNT_ID CF_ZONE_ID CF_ZONE_NAME; do
    if [[ -z "${!v:-}" ]]; then
      err "required env var ${v} is not set"
      missing=1
    fi
  done
  if [[ "${missing}" -ne 0 ]]; then
    err "set them in ${LBKIT_DIR}/.env (copy env.example)"
    exit 1
  fi
}

# preflight checks the CLI tools every script needs.
preflight() {
  local t missing=0
  for t in kubectl jq curl envsubst; do
    if ! command -v "${t}" >/dev/null 2>&1; then
      err "missing required tool: ${t}"
      missing=1
    fi
  done
  [[ "${missing}" -eq 0 ]] || exit 1
}

# init runs the standard preamble for a script: strict mode is set by the
# caller; this loads env, checks tools, and validates the identifiers.
init() {
  preflight
  load_env
  require_env
}

# ---- kubectl -------------------------------------------------------------
# kctl runs kubectl pinned to LBKIT_CONTEXT (falls back to current-context when
# LBKIT_CONTEXT is empty). kc adds the kit namespace on top.
kctl() {
  if [[ -n "${LBKIT_CONTEXT:-}" ]]; then
    kubectl --context "${LBKIT_CONTEXT}" "$@"
  else
    kubectl "$@"
  fi
}
kc() { kctl -n "${LBKIT_NS}" "$@"; }

# render templates one of the manifests/*.tmpl files, substituting only the
# kit's own variables (so a literal $ in the token is never mangled).
render() {
  # The single-quoted arg is envsubst's variable allowlist: it must stay literal
  # so the shell does not expand it (that is the whole point of the allowlist).
  # shellcheck disable=SC2016
  envsubst '${LBKIT_NS} ${CF_API_TOKEN} ${CF_ACCOUNT_ID} ${CF_ZONE_ID} ${CF_ZONE_NAME}' \
    < "${LBKIT_DIR}/manifests/$1"
}

# cr_id echoes a resource's Cloudflare-assigned status.id (empty if unset).
cr_id() { kc get "$1" "$2" -o 'jsonpath={.status.id}' 2>/dev/null || true; }

# wait_condition polls until <kind>/<name> has status condition <type>=True.
wait_condition() {
  local kind="$1" name="$2" ctype="$3" timeout="${4:-180}" st i
  for ((i = 0; i < timeout; i++)); do
    st="$(kc get "${kind}" "${name}" \
      -o "jsonpath={.status.conditions[?(@.type==\"${ctype}\")].status}" 2>/dev/null || true)"
    [[ "${st}" == "True" ]] && return 0
    sleep 1
  done
  err "timed out after ${timeout}s waiting for ${kind}/${name} ${ctype}=True"
  kc get "${kind}" "${name}" -o yaml 2>/dev/null | sed -n '/status:/,$p' >&2 || true
  return 1
}

wait_ready()       { wait_condition "$1" "$2" Ready "${3:-180}"; }
wait_initialized() { wait_condition "$1" "$2" Initialized "${3:-120}"; }

# wait_reconciled polls until the Ready condition's observedGeneration has
# caught up to metadata.generation -- i.e. the controller has processed the
# latest spec edit and (for a drift) issued the Cloudflare write. This is the
# signal a gate uses before reading Cloudflare back.
wait_reconciled() {
  local kind="$1" name="$2" timeout="${3:-120}" gen og i
  gen="$(kc get "${kind}" "${name}" -o 'jsonpath={.metadata.generation}' 2>/dev/null || true)"
  for ((i = 0; i < timeout; i++)); do
    og="$(kc get "${kind}" "${name}" \
      -o 'jsonpath={.status.conditions[?(@.type=="Ready")].observedGeneration}' 2>/dev/null || true)"
    [[ -n "${og}" && "${og}" == "${gen}" ]] && return 0
    sleep 1
  done
  err "timed out after ${timeout}s waiting for ${kind}/${name} to reconcile generation ${gen}"
  return 1
}

# ---- Cloudflare API ------------------------------------------------------
# cf_api issues a raw request. Args: METHOD PATH [extra curl args...].
cf_api() {
  local method="$1" path="$2"
  shift 2
  curl -sS -X "${method}" \
    -H "Authorization: Bearer ${CF_API_TOKEN}" \
    -H "Content-Type: application/json" \
    "${CF_API}${path}" "$@"
}

# cf_result GETs PATH and echoes .result, failing (with the API errors on
# stderr) if the call was not successful.
cf_result() {
  local resp
  resp="$(cf_api GET "$1")" || return 1
  if [[ "$(jq -r '.success' <<<"${resp}")" != "true" ]]; then
    err "Cloudflare API error for GET $1:"
    jq -c '.errors' <<<"${resp}" >&2 || echo "${resp}" >&2
    return 1
  fi
  jq '.result' <<<"${resp}"
}

cf_get_lb()      { cf_result "/zones/${CF_ZONE_ID}/load_balancers/$1"; }
cf_get_pool()    { cf_result "/accounts/${CF_ACCOUNT_ID}/load_balancers/pools/$1"; }
cf_get_monitor() { cf_result "/accounts/${CF_ACCOUNT_ID}/load_balancers/monitors/$1"; }

# cf_lb_exists_by_hostname echoes the CF LB id whose name matches $1, or empty.
cf_lb_exists_by_hostname() {
  cf_result "/zones/${CF_ZONE_ID}/load_balancers" \
    | jq -r --arg n "$1" '.[] | select(.name == $n) | .id' 2>/dev/null || true
}

# ---- Prometheus /metrics -------------------------------------------------
# Helpers for the metrics-alerting gate. The endpoint is plain HTTP with no auth
# under `make dev-run` (see LBKIT_METRICS_URL in load_env).

# metrics_scrape fetches the operator's Prometheus text exposition to stdout.
# Fails (non-zero, message on stderr) if the endpoint is unreachable or returns
# an empty body, so a caller never mistakes a dead scrape for a zero-valued
# metric (an absent series and a broken endpoint both read as "no line").
metrics_scrape() {
  local out
  if ! out="$(curl -sS --max-time 10 "${LBKIT_METRICS_URL}" 2>/dev/null)" || [[ -z "${out}" ]]; then
    err "failed to scrape operator metrics at ${LBKIT_METRICS_URL} -- is the operator running (./run-operator.sh lb)?"
    return 1
  fi
  printf '%s\n' "${out}"
}

# metric_value reads one sample's value from a captured /metrics text.
# Args: <metrics-text> <metric-name> [label=value ...]. It selects the sample
# whose series name matches AND whose label block contains every requested
# key="value" pair (order-independent -- client_golang sorts label names, so
# callers must not assume an order; the closing quote in each pair keeps
# resource="loadbalancer" from also matching resource="loadbalancerpool"). Echoes
# the sample's numeric value, or 0 when no sample matches: an absent counter or
# gauge series reads as 0, which is exactly what a baseline (never-incremented
# counter) or a cleared gauge looks like. Guard a 0 that must mean "live and
# zero" by scraping through metrics_scrape (which fails on a dead endpoint) and,
# where possible, also asserting a positive companion series.
metric_value() {
  local text="$1" name="$2"
  shift 2
  local lines sel
  # Series lines are "name{labels} value" (labelled) or "name value" (bare);
  # HELP/TYPE lines start with '#' and never match this anchored name.
  lines="$(grep -E "^${name}(\{|[[:space:]])" <<<"${text}" || true)"
  for sel in "$@"; do
    lines="$(grep -F -- "${sel}" <<<"${lines}" || true)"
  done
  if [[ -z "${lines}" ]]; then
    echo 0
    return 0
  fi
  awk 'NR==1{print $(NF)}' <<<"${lines}"
}

# num_gt / num_ge compare two (possibly floating-point) metric values, returning
# 0 (true) when the relation holds. Used instead of bash integer tests because
# Prometheus values are floats.
num_gt() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>b)}'; }
num_ge() { awk -v a="$1" -v b="$2" 'BEGIN{exit !(a>=b)}'; }
