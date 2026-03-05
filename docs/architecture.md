# cf-edge-operator Architecture

## Overview

cf-edge-operator manages Cloudflare (CF) for SaaS (custom hostnames, Origin Server Name Indication (SNI) overrides) via Kubernetes CRDs. It deliberately excludes DNS record management, which belongs to external-dns or similar tools.

See also: [docs/operations.md](operations.md) for configuration flags, performance tuning, and HA setup. [docs/runbook.md](runbook.md) for alert investigation.

## CRDs

### Zone (`domains.cf-edge.io/v1beta1`)
Represents a Cloudflare zone. Lives in the operator namespace. Holds zone ID and a reference to a secret containing the Cloudflare API token. All CustomHostname resources in app namespaces reference a Zone.

### CustomHostname (`saas.cf-edge.io/v1beta1`)
Represents a Cloudflare custom hostname with origin server and optional SNI override. Lives in app namespaces. References a Zone cross-namespace.

Key spec fields:
- `spec.hostname` — the custom hostname (e.g. `app.customer.com`)
- `spec.originServer` — the origin server CF proxies to
- `spec.originSNI` — (optional) overrides the SNI sent to the origin during TLS handshake. If omitted, the operator does not manage Origin SNI — CF uses its default (the origin server hostname) and external changes to the SNI are not corrected. When set, the operator enforces the value on every reconcile. The special value `:request_host_header:` instructs CF to forward the incoming request's Host header as the SNI — useful when the origin validates connections by the hostname. Requires an account-level entitlement from Cloudflare; setting this field on a zone without the entitlement produces a CF API error.

## Controller Architecture: Coordinator / Worker Split

Two controllers with clearly separated responsibilities. The coordinator/worker pattern is designed to extend to additional Cloudflare resource types (WAF rules, routing, etc.) — each would follow the same split with its own worker controller and API group.

```mermaid
sequenceDiagram
    participant ZC as Zone Controller (Coordinator)
    participant CF as Cloudflare API
    participant CHC as CustomHostname Controller (Worker)

    Note over ZC: every --drift-interval (default 1m) · Zone/CustomHostname CR changes
    ZC->>CF: bulk GET custom_hostnames (~10 calls / 1000 hostnames)
    CF-->>ZC: hostname list with current state
    ZC->>ZC: diff desired (CRs) vs actual (CF)
    ZC->>CHC: enqueue drifted CRs via event channel

    Note over CHC: per enqueue · spec change
    CHC->>CF: findByHostname (1 API call)
    CF-->>CHC: current state or not found
    CHC->>CF: POST / PATCH / DELETE
    CF-->>CHC: result
    CHC->>CHC: update status (no self-requeue; zone detects SSL changes)
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

`fastWritePredicate` is applied to the CustomHostname controller: on informer Create events, it only passes CRs with no `status.id` (genuinely new) or with a DeletionTimestamp (terminating). CRs with an existing `status.id` are skipped — the Zone controller handles them via drift detection. On Update events, it filters by generation change, guarding against spurious reconciles from status-only updates.

## SSL Provisioning (Async)

Cloudflare SSL verification is asynchronous (minutes to hours). SSL status transitions are detected via the zone controller's periodic bulk list — no per-CR polling:
- Each zone drift cycle compares the SSL status from Cloudflare against what is stored in the CustomHostname CR status
- When the status differs (e.g., `pending_validation` → `active`), the CR is enqueued for the CustomHostname controller
- `status.ssl.status` reflects current Cloudflare SSL state
- `status.ssl.validationRecords` surfaces Domain Control Validation (DCV) tokens so operators can complete validation
- While `ssl.status != "active"`: `Ready=False`; the next zone cycle detects the change
- When `ssl.status == "active"`: `Ready=True`, SSL provisioning duration metric observed

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

**Why Zone is namespace-scoped, not cluster-scoped:** Secrets are namespace-scoped in Kubernetes; keeping the Zone CR in the same namespace as its Secret keeps RBAC simple (a Role suffices, no ClusterRole needed). App teams still get cluster-wide reach in practice — `zoneRef.namespace` defaults to `--operator-namespace`, so they can omit the namespace entirely and the operator resolves it. Cluster-scoped Zones would grant no UX benefit while complicating credential access patterns.

## Delete Policy

The `--delete-policy` flag sets the operator-wide default behavior when a CustomHostname CR is deleted. Individual CRs can override it via `spec.deletePolicy`:

- **`always`** (default): delete from Cloudflare by `status.id` regardless of current state. Treats 404 as success.
- **`own-only`**: look up the current Cloudflare state via `findByHostname` before deleting. Only deletes if the current CF ID matches `status.id`. If IDs differ or the hostname is not found, releases the finalizer (removes the CR from Kubernetes without touching Cloudflare).

### When to use `own-only`

**Migration from external-dns:** During the transition period, external-dns and cf-edge-operator may both be active for the same zone. External-dns can delete and recreate a hostname (new CF ID), leaving `status.id` stale. If a CR is deleted during this window:

- `always`: tries to delete by stale ID → 404 → releases. The live hostname survives by accident.
- `own-only`: looks up current CF state, sees ID mismatch → releases without any CF API call. Explicitly safe.

**Recommended:** deploy cf-edge-operator with `--delete-policy=own-only` during any migration phase where another tool may concurrently manage the same CF zone.

See [docs/migration.md](migration.md) for the full migration guide.

## Observability

### Status fields

| Field | Purpose |
|-------|---------|
| `status.id` | CF custom hostname ID, used for updates and deletes |
| `status.ssl` | SSL state (status, expiresOn, validationRecords) |
| `status.createCount` | How many times this hostname was (re)created in CF. Values > 1 indicate external deletions. |
| `status.consecutiveErrors` | Consecutive reconcile failures. Resets to 0 on success. |
| `status.conditions[Ready].reason` | `HostnameConflict` when another CR already owns this hostname in Cloudflare. Clears automatically when the owning CR is deleted. |

### Prometheus metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cf_edge_operator_zone_ready{zone_cr}` | gauge | 1 if Zone CR credentials are valid and CF API is reachable, 0 otherwise. Uses CR name (always available, even on failure). |
| `cf_edge_operator_operations_total{resource,operation}` | counter | Successful CF write operations; `resource`: customhostname; `operation`: create, recreate, update, delete |
| `cf_edge_operator_customhostnames{zone,state}` | gauge | CRs by zone and state (ready/pending/unhealthy/conflict). Sum = total CRs in zone |
| `cf_edge_operator_zone_customhostnames{zone,type}` | gauge | CF custom hostnames by type (managed/orphan). Sum = CF quota usage for the zone |
| `cf_edge_operator_api_duration_seconds{resource,operation}` | histogram | CF API call latency; `resource`: customhostname, zone; `operation`: get, list, create, update, delete |
| `cf_edge_operator_api_errors_by_code_total{resource,operation,status_code}` | counter | CF API errors by resource, operation, and HTTP status code (`unknown` for non-HTTP errors) |
| `cf_edge_operator_ssl_provisioning_duration_seconds{zone,hostname,method}` | histogram | Time from CF create to `ssl.status == active`. Buckets span 1m–1w. |

Controller-runtime also exposes `controller_runtime_reconcile_total` and `controller_runtime_reconcile_time_seconds` per controller.

### Alerting

The Helm chart ships a `PrometheusRule` (disabled by default):

| Alert | Fires when |
|-------|------------|
| `CfEdgeOperatorZoneNotReady` | Zone CR unhealthy (bad secret or CF API error) for 5 min |
| `CfEdgeOperatorDown` | Metrics endpoint unreachable for 2 min |
| `CfEdgeOperatorUnhealthyHostnames` | Any CR in `unhealthy` state for 5 min |
| `CfEdgeOperatorConflictHostnames` | Any CR in `conflict` state for 5 min |
| `CfEdgeOperatorHighAPIErrorRate` | Cloudflare 5xx error rate > 0.1/s for 5 min |
| `CfEdgeOperatorSSLProvisioningStalled` | Any CR in `pending` state for 24 h |

Enable with `prometheusRule.enabled=true`. See [docs/runbook.md](runbook.md) for investigation and resolution steps per alert.

## Future Work

- **CI integration tests** — end-to-end tests against a dedicated CF zone with real API credentials.
- **Additional CF resource types** — Web Application Firewall (WAF) rules, routing, and other Cloudflare primitives via dedicated API groups (e.g. `security.cf-edge.io`, `routing.cf-edge.io`), each with its own worker controller following the coordinator/worker pattern.
