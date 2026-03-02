# cf-edge-operator Architecture

## Overview

cf-edge-operator manages Cloudflare for SaaS (custom hostnames, Origin SNI overrides) via Kubernetes CRDs. It deliberately excludes DNS record management, which belongs to external-dns or similar tools.

## CRDs

### Zone (`cf.cf-edge.io/v1alpha1`)
Represents a Cloudflare zone. Lives in the operator namespace. Holds zone ID and a reference to a secret containing the Cloudflare API token. All CustomHostname resources in app namespaces reference a Zone.

### CustomHostname (`cf.cf-edge.io/v1alpha1`)
Represents a Cloudflare custom hostname with origin server and optional SNI override. Lives in app namespaces. References a Zone cross-namespace.

## Controller Architecture: Coordinator / Worker Split

Two controllers with clearly separated responsibilities:

```
┌─────────────────────────────────────────────────────────────┐
│ Zone Controller (Coordinator — reads only)                   │
│                                                             │
│  Triggers: Zone CR changes, CustomHostname CR changes,      │
│            periodic requeue (5 min)                         │
│                                                             │
│  1. Bulk paginated GET /zones/:id/custom_hostnames          │
│     → ~10 API calls for 1000 hostnames (100/page)          │
│  2. List all CustomHostname CRs for this zone               │
│  3. Diff desired vs actual                                  │
│  4. Enqueue drifted CustomHostname CRs for reconciliation   │
│     (never writes to Cloudflare)                            │
└─────────────────────┬───────────────────────────────────────┘
                      │ enqueues via event channel
                      ▼
┌─────────────────────────────────────────────────────────────┐
│ CustomHostname Controller (Worker — writes only)             │
│                                                             │
│  Triggers: CR spec changes (generation predicate),          │
│            enqueue from Zone controller                     │
│                                                             │
│  1. List(hostname=spec.hostname) → 1 Cloudflare API call   │
│  2. Found → adopt ID, update if drifted (PATCH)            │
│     Not found → create (POST)                              │
│  3. Update status (ID, SSL state, validation records)       │
│  4. Requeue after 30s while SSL pending, else done          │
│  5. On delete: DELETE from Cloudflare, remove finalizer     │
│                                                             │
│  Failures: requeue only this CR with exponential backoff    │
│            (blast radius = one hostname)                    │
└─────────────────────────────────────────────────────────────┘
```

## Why This Split

**Zone as coordinator (bulk reader):**
- One paginated list call per zone per cycle regardless of how many hostnames exist
- Drift detection catches external deletions (Cloudflare dashboard, other tools)
- On operator restart, only Zone CRs are enqueued — not all 1000 CustomHostname CRs
- Zone reconcile failures are safe: no writes occurred, requeue the zone

**CustomHostname as worker (targeted writer):**
- Single CR → single API call: fast write latency (200-500ms) for individual changes
- Failure isolation: Cloudflare 500 on hostname #42 does not affect the other 999
- controller-runtime exponential backoff applies per CR natively
- Finalizer ensures Cloudflare deletion on CR delete, even across operator restarts

## State

State is distributed across two systems — no in-memory cache:

| What | Where |
|------|-------|
| Desired state | Kubernetes (CustomHostname CR spec) |
| Cloudflare hostname ID | Kubernetes (CustomHostname CR status.id) |
| SSL verification state | Kubernetes (CustomHostname CR status.ssl) |
| Actual state | Cloudflare API (source of truth for drift detection) |

`status.id` is a performance cache for `Get(id)` in the CustomHostname controller. It is validated against Cloudflare on every reconcile via `List(hostname)`, so a stale or missing ID is self-healing.

## Restart Behavior

On operator restart:
1. Zone CRs are enqueued (typically 1-5 zones)
2. Each Zone reconcile does one bulk list from Cloudflare
3. Only CustomHostname CRs with actual drift are enqueued
4. Spec-unchanged CRs are never individually reconciled

`GenerationChangedPredicate` is applied to the CustomHostname controller as an additional guard against spurious reconciles from status-only updates during normal operation.

## SSL Provisioning (Async)

Cloudflare SSL verification is asynchronous (minutes to hours). The CustomHostname controller handles this via:
- `status.ssl.status` reflects current Cloudflare SSL state
- `status.ssl.validationRecords` surfaces DCV tokens so operators can complete validation
- While `ssl.status != "active"`: `Ready=False`, requeue after 30s
- When `ssl.status == "active"`: `Ready=True`, no further requeue

## Cloudflare API Efficiency

| Operation | API calls |
|-----------|-----------|
| Drift detection (1000 hostnames) | ~10 (paginated, 100/page) |
| Single CR create/update | 1 List + 1 POST/PATCH |
| Single CR delete | 1 DELETE |
| Restart (1000 CRs, 5 zones) | 5 × ~10 = ~50 |

## Credential Flow

```
CustomHostname CR (app namespace)
  └─ zoneRef → Zone CR (operator namespace)
                 └─ credentialsRef → Secret (operator namespace)
                                      └─ apiToken → Cloudflare API
```

App namespaces never hold Cloudflare credentials.

## Future Work

- **`status.createCount`** — track how many times a hostname has been recreated in Cloudflare. A high count indicates an external entity is repeatedly deleting it. Useful for alerting.
- **Prometheus metrics** — `creates_total`, `deletes_total`, `updates_total` (drift corrections), `orphans_total` (per zone), `ssl_status` gauge. controller-runtime already exposes a metrics endpoint.
- **`--delete-policy` flag** — control delete behavior when origin has drifted from spec at deletion time. `always` (default): delete by ID regardless. `own-only`: only delete if current CF origin matches `spec.originServer`, otherwise release finalizer without deleting.
- **Precise delete via `findByHostname`** — look up current CF state before deleting. If ID matches `status.id` → delete. If ID differs → another entity owns it, release finalizer. Eliminates reliance on 404 handling.
