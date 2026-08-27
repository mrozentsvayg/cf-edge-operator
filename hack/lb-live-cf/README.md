# Live-Cloudflare load-balancing test kit

A small harness for running the operator's load-balancing controllers against a
**real Cloudflare account** from your workstation. The operator process runs on
your host and talks to `api.cloudflare.com`; a local Kubernetes cluster (kind
works well) just holds the CRs.

It exists to answer questions the mock-based tests cannot, above all the two
**hard pre-merge gates** on how Cloudflare's PATCH behaves. Those assumptions are
load-bearing for the reconcile-via-PATCH design and can only be confirmed on live
Cloudflare.

> This is developer tooling. It needs a real API token and it is never run in CI.
> Nothing here is imported by the operator or the chart.

## The two hard gates (opposite expectations)

The operator reconciles via PATCH and manages only its curated subset of each
resource. Two field shapes depend on *opposite* Cloudflare PATCH behavior:

| Gate | Field shape | Example | Must behave as | Why |
|------|-------------|---------|----------------|-----|
| **map-replace** | top-level **map** property | LoadBalancer `regionPools`, monitor `header` | **REPLACE** (whole map overwritten) | The operator sends the full map; a key the CR dropped only disappears if CF overwrites the property. If CF deep-merged, removed keys would linger and the operator would drift-loop. |
| **nested-merge** | **nested object** | pool `loadShedding`, LB `sessionAffinityAttributes` | **DEEP-MERGE** (unsent subfields kept) | The operator manages each subfield independently and sends only the ones the CR sets. If CF replaced the whole object, unsent subfields would be silently cleared. |

`gate-map-replace.sh` and `gate-nested-merge.sh` verify each on live
Cloudflare and print `PASS`/`FAIL`. Both must PASS before the LB PR merges. Put
the results in the PR body.

The operator's own drift check cannot catch a wrong answer here: it only compares
its curated subset, so it reports "no drift" whether CF merged or replaced. That
is why the gates read Cloudflare's raw API response back directly (curl + jq).

## Prerequisites

- A Kubernetes cluster on your current kube context. A throwaway kind cluster is
  ideal: `kind create cluster --name cf-lb`.
- A Go toolchain matching [`.go-version`](../../.go-version) on `PATH` (needed by
  `make dev-run`). If yours lives elsewhere, set `LBKIT_GO_BIN` in `.env`.
- `kubectl`, `jq`, `curl`, `envsubst` (from gettext) on `PATH`.
- A Cloudflare API token plus the account id, zone id, and zone apex domain. See
  [`env.example`](env.example) for the exact token permissions.

## Quick start

```sh
cd hack/lb-live-cf
cp env.example .env && $EDITOR .env      # fill in token + ids (.env is gitignored)

# terminal 1: build + run the operator locally (load balancing on, CH off)
./run-operator.sh lb                     # backgrounds it; logs -> /tmp/cf-edge-operator.log

# terminal 2: install CRDs + identity substrate (Secret/Account/Zone)
./setup.sh

# the two hard gates
./gate-map-replace.sh                    # top-level map REPLACE
./gate-nested-merge.sh                   # nested object DEEP-MERGE

# the other live checks
./test-degradation.sh                    # feature off -> operator no-ops
./probe-tier.sh                          # discover which LB features your subscription allows (matrix)

# clean up (operator must still be running so CF deletes happen)
./teardown.sh
./run-operator.sh stop
```

## Scripts

| Script | What it does |
|--------|--------------|
| `run-operator.sh [lb\|lb-health\|off\|stop]` | Build + run the operator locally via `make dev-run`, wired to the kit namespace. `lb` is the default; `lb-health` adds `--enable-pool-health`; `off` disables both features. |
| `setup.sh` | `make install`, apply the identity substrate (Secret/Account/Zone), wait for Account/Zone Initialized. Lean by design so a low origin cap isn't consumed; each gate owns its test resources. |
| `gate-map-replace.sh` | **Hard gate map-replace.** Create a monitor with a `header` map `{Host, X-Probe}`, drop `X-Probe`, confirm Cloudflare removed it (top-level map = REPLACE). Uses the monitor header because geo `regionPools` needs traffic steering (a higher tier); the CF PATCH semantics are the same. |
| `gate-nested-merge.sh` | **Hard gate nested-merge.** Set pool `loadShedding` with four subfields, drop three, confirm the unsent ones survive. |
| `test-degradation.sh` | Restart the operator with load balancing off, apply an LB CR, confirm it is untouched on both the K8s and CF sides, restart in `lb` mode. |
| `probe-tier.sh` | Discover which Cloudflare LB features your subscription allows: applies one minimal CR per tier-capped dimension (origin count, check interval, check regions, steering, load shedding, session affinity, monitor header), records CF's accept/reject + verbatim message, and prints a capability matrix. Observe-only (each probe is cleaned up). |
| `cf-get.sh <lb\|pool\|monitor> <cr-name>` | Dump a resource's raw Cloudflare JSON, resolved from its `status.id`. |
| `teardown.sh` | Delete the CRs in dependency order (triggering CF deletes), then the namespace. |

## Notes

- **Origins are placeholders.** The gates verify config round-trips, not real
  traffic health, so pool origins point at non-existent `origin-*.<zone>`
  hostnames. Cloudflare accepts them at create time; health is asynchronous and
  irrelevant here.
- **One namespace.** Everything lives in `LBKIT_NS` (default
  `cf-edge-operator-system`) and the operator runs with `--operator-namespace`
  set to it, so every name-only ref resolves regardless of which default it uses.
- **Context is pinned, never current-context.** Every kubectl call, the CRD
  install (`make install KUBECONTEXT=`), and the operator (`--kube-context`) use
  `LBKIT_CONTEXT` (default `kind-cf-lb`), so the kit can't touch a prod cluster
  even if your current-context points there. Set `LBKIT_CONTEXT` to your kind
  context if you named the cluster differently, or `""` to use current-context.
- **Tier behavior is account-dependent.** Cloudflare Load Balancing is a paid
  add-on and its features vary by subscription tier; `probe-tier.sh` discovers what
  yours allows (a capability matrix) and records Cloudflare's verbatim rejection
  messages -- observe and record before writing any tier docs.
- **Teardown needs the operator up.** With `--delete-policy=always` (the default)
  the finalizer on each CR triggers the matching Cloudflare delete. If the
  operator is down, CRs stick in Terminating and CF resources are orphaned;
  restart it and re-run, or delete by hand using `cf-get.sh` to find the ids.
- **Pausing to watch the dashboard.** Set `LBKIT_PAUSE=1` to make the gates pause
  after each Cloudflare mutation (create, edit, drift-seed) so you can compare the
  Cloudflare dashboard against the API state before the next change. It blocks for
  Enter only on an interactive terminal, so a piped or non-interactive run just
  logs each pause point and keeps going; unset (the default) it never pauses.
