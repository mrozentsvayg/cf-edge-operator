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
- `spec.hostname` -- the custom hostname (e.g. `app.customer.com`)
- `spec.originServer` -- the origin server CF proxies to
- `spec.originSNI` -- (optional) overrides the SNI sent to the origin during TLS handshake. If omitted, the operator does not manage Origin SNI -- CF uses its default (the origin server hostname) and external changes to the SNI are not corrected. When set, the operator enforces the value on every reconcile. The special value `:request_host_header:` instructs CF to forward the incoming request's Host header as the SNI -- useful when the origin validates connections by the hostname. Requires an account-level entitlement from Cloudflare; setting this field on a zone without the entitlement produces a CF API error.
- `spec.zoneRef` -- references the Zone CR (name + optional namespace, defaults to operator namespace)
- `spec.ssl` -- (optional) SSL configuration: CA (`lets_encrypt`/`google`/`ssl_com`), minTLSVersion (`1.0`-`1.3`), method (`http`/`txt`/`email`), type (`dv`). Empty fields use `--ssl-*` operator defaults; method defaults to `http`, type to `dv`; CA and minTLSVersion default to CF defaults.
- `spec.managementPolicy` -- (optional) per-CR override for `--management-policy`. See [Management Policy](#management-policy).
- `spec.deletePolicy` -- (optional) per-CR override for `--delete-policy`. See [Delete Policy](#delete-policy).

## Controller Architecture: Coordinator / Worker Split

Two controllers with clearly separated responsibilities. The coordinator/worker pattern is designed to extend to additional Cloudflare resource types (WAF rules, routing, etc.) -- each would follow the same split with its own worker controller and API group.

```mermaid
sequenceDiagram
    participant ZC as Zone Controller (Coordinator)
    participant CF as Cloudflare API
    participant CHC as CustomHostname Controller (Worker)

    Note over ZC: every --drift-interval (default 1m) / Zone/CustomHostname CR changes
    ZC->>CF: bulk GET custom_hostnames (~10 calls / 1000 hostnames)
    CF-->>ZC: hostname list with current state
    ZC->>ZC: diff desired (CRs) vs actual (CF)
    ZC->>CHC: enqueue drifted CRs via event channel

    Note over CHC: per enqueue / spec change
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
- On operator restart, only Zone CRs are enqueued -- not all 1000 CustomHostname CRs
- Zone reconcile failures are safe: no writes occurred, requeue the zone

**CustomHostname as worker (targeted writer):**
- Single CR -> single API call: fast write latency (200-500ms) for individual changes
- Failure isolation: Cloudflare 500 on hostname #42 does not affect the other 999
- controller-runtime exponential backoff applies per CR natively
- Finalizer ensures Cloudflare deletion on CR delete, even across operator restarts

## State

State is distributed across two systems -- no in-memory cache:

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

`fastWritePredicate` is applied to the CustomHostname controller: on informer Create events, it only passes CRs with no `status.id` (genuinely new) or with a DeletionTimestamp (terminating). CRs with an existing `status.id` are skipped -- the Zone controller handles them via drift detection. On Update events, it filters by generation change, guarding against spurious reconciles from status-only updates.

## SSL Provisioning (Async)

Cloudflare SSL verification is asynchronous (minutes to hours). SSL status transitions are detected via the zone controller's periodic bulk list -- no per-CR polling:
- Each zone drift cycle compares origin server, SNI, SSL config (CA, minTLSVersion, method, type), and full SSL status against the CR spec/status
- When any field differs -- including external changes via CF dashboard or API -- the CR is enqueued for status refresh
- `status.ssl` reflects current Cloudflare SSL state: certificate status, CA, method, type, minTLS, cert ID, issuer, serial number, bundle method, wildcard, hosts, uploaded/expires timestamps, validation records/errors
- External cert reissues are detectable via `status.ssl.id` or `status.ssl.serialNumber` changes
- While `ssl.status != "active"`: `Ready=False`; the next zone cycle detects the change
- When `ssl.status == "active"`: `Ready=True`, SSL provisioning duration metric observed

### Field lifecycle: set vs. unset

Removing a managed field from the CR spec (setting it to empty or removing the key) stops the operator from enforcing that field -- it does **not** revert the Cloudflare value to its default. This applies to both `originSNI` and all `ssl.*` fields:

- **originSNI:** Setting it applies it on every reconcile. Removing it means the operator stops managing SNI; the existing CF value is preserved until changed externally (dashboard, API, or another tool).
- **ssl fields (CA, minTLSVersion, method, type):** Same behavior. An empty spec field means "don't manage this field on edits." To revert to CF defaults, clear the field via the CF dashboard or API.

Additionally, operator-wide SSL defaults (`--ssl-certificate-authority`, `--ssl-min-tls-version`, `--ssl-method`, `--ssl-type`) are applied only on **create**, not on drift correction edits. This prevents the operator from overriding intentional per-hostname customizations made after initial provisioning.

## Cloudflare API Efficiency

| Operation | API calls |
|-----------|-----------|
| Drift detection (1000 hostnames) | ~21 (paginated, 50/page + 1 end marker) |
| Single CR create/update | 1 List + 1 POST/PATCH |
| Single CR delete | 1 DELETE |
| Restart (1000 CRs, 5 zones) | 5 x ~21 = ~105 |

See [operations.md](operations.md#cf-api-timeout-and-retry-budget) for timeout/retry budget per drift cycle.

## Credential Flow

```
CustomHostname CR (app namespace)
  └─ zoneRef -> Zone CR (operator namespace)
                 └─ credentialsRef -> Secret (operator namespace)
                                      └─ apiToken -> Cloudflare API
```

App namespaces never hold Cloudflare credentials.

**Why Zone is namespace-scoped, not cluster-scoped:** Secrets are namespace-scoped in Kubernetes; keeping the Zone CR in the same namespace as its Secret keeps RBAC simple (a Role suffices, no ClusterRole needed). App teams still get cluster-wide reach in practice -- `zoneRef.namespace` defaults to `--operator-namespace`, so they can omit the namespace entirely and the operator resolves it. Cluster-scoped Zones would grant no UX benefit while complicating credential access patterns.

## Management Policy

The `--management-policy` flag sets the operator-wide default for how the operator interacts with Cloudflare. Individual CRs can override it via `spec.managementPolicy`:

- **`manage`** (default): full lifecycle -- create, update (drift correction), and delete per `deletePolicy`.
- **`create`**: provisions the hostname if missing, but never updates it. Safe coexistence with external-dns or other automation that may also modify the hostname. Deletes per `deletePolicy`.
- **`observe`**: read-only -- tracks an externally-managed hostname without creating, updating, or deleting it. `deletePolicy` is ignored; the finalizer is always released on CR deletion.

**Recommended:** deploy with `--management-policy=create` during any migration phase where another tool may concurrently manage the same CF zone. See [docs/migration.md](migration.md).

## Delete Policy

The `--delete-policy` flag sets the operator-wide default behavior when a CustomHostname CR is deleted. Individual CRs can override it via `spec.deletePolicy`:

- **`always`** (default): delete from Cloudflare by `status.id` regardless of current state. Treats 404 as success.
- **`own-only`**: look up the current Cloudflare state via `findByHostname` before deleting. Only deletes if the current CF ID matches `status.id`. If IDs differ or the hostname is not found, releases the finalizer (removes the CR from Kubernetes without touching Cloudflare).
- **`never`**: releases the finalizer without any CF API call, regardless of ID match.

Note: when `managementPolicy` is `observe`, the operator always releases the finalizer without deleting -- `deletePolicy` is ignored entirely.

### When to use `own-only`

**Migration from external-dns:** During the transition period, external-dns and cf-edge-operator may both be active for the same zone. External-dns can delete and recreate a hostname (new CF ID), leaving `status.id` stale. If a CR is deleted during this window:

- `always`: tries to delete by stale ID -> 404 -> releases. The live hostname survives by accident.
- `own-only`: looks up current CF state, sees ID mismatch -> releases without any CF API call. Explicitly safe.

**Recommended:** deploy with `--delete-policy=own-only` and `--management-policy=create` during migration. See [docs/migration.md](migration.md).

## Observability

### Status fields

| Field | Purpose |
|-------|---------|
| `status.id` | CF custom hostname ID, used for updates and deletes |
| `status.ssl` | Full SSL state from Cloudflare -- refreshed on every drift cycle. See README for complete field list. |
| `status.createCount` | How many times this hostname was (re)created in CF. Values > 1 indicate external deletions. |
| `status.consecutiveErrors` | Consecutive reconcile failures. Resets to 0 on success. |
| `status.sslProvisioningStartedAt` | Timestamp set on each create/recreate. Source for the `ssl_provisioning_duration_seconds` metric. |
| `status.conditions[Ready].reason` | `HostnameConflict` when another CR already owns this hostname in Cloudflare. Clears automatically when the owning CR is deleted. |

### Drift log format

Drift logs use a structured nested object under the `drift` key with three categories:

- **`drifted`** -- fields that differ between spec and CF, with `{cf, spec}` value pairs
- **`matched`** -- managed fields that match, with the current value
- **`unmanaged`** -- fields not in spec, showing CF's current value

Example (spec drift):
```json
{"drift": {"drifted": {"origin": {"cf": "old.example.com", "spec": "new.example.com"}}, "matched": {"sni": ":request_host_header:", "ca": "google"}, "unmanaged": {"minTLS": "1.2", "method": "http", "type": "dv"}}}
```

Status-only changes (external CF changes to unmanaged fields) use the message `custom hostname - enqueuing, status.ssl changed` with a `changed` map showing `{status, cf}` pairs:
```json
{"changed": {"minTLS": {"status": "", "cf": "1.2"}}}
```

Parse with jq: `jq '.drift.drifted | keys'` or `jq '.changed | to_entries[] | "\(.key): \(.value.status) -> \(.value.cf)"'`

### Prometheus metrics

| Metric | Type | Description |
|--------|------|-------------|
| `cf_edge_operator_zone_initialized{zone_cr}` | gauge | 1 if Zone CR has been initialized (zone name resolved from CF API). Set once on first successful zone GET; not toggled on transient failures. |
| `cf_edge_operator_operations_total{resource,operation}` | counter | Successful CF operations; `resource`: customhostname; `operation`: adopt, create, recreate, update, delete |
| `cf_edge_operator_customhostnames{zone_cr,state}` | gauge | CRs by zone CR and state (ready/pending/unhealthy/conflict). Sum = total CRs in zone |
| `cf_edge_operator_zone_customhostnames{zone_cr,type}` | gauge | CF custom hostnames by zone CR and type (managed/orphan/drifted/total). `orphan` = no associated CR. `total` = CF quota usage for the zone |
| `cf_edge_operator_api_duration_seconds{resource,operation}` | histogram | CF API call latency; `resource`: customhostname, zone; `operation`: get, list, create, update, delete. For `list`, duration spans all paginated HTTP calls (can exceed per-request timeout) |
| `cf_edge_operator_api_errors_by_code_total{resource,operation,status_code}` | counter | CF API errors by resource, operation, and HTTP status code. `timeout` for request timeouts, `canceled` for context cancellation (shutdown), `unknown` for other non-HTTP errors |
| `cf_edge_operator_api_retries_total{resource,operation}` | counter | Retry attempts for single (non-paginated) CF API calls. Non-zero means first attempts are failing |
| `cf_edge_operator_ssl_provisioning_duration_seconds{zone_cr,hostname,method}` | gauge | Time from CF create to `ssl.status == active`. Set once per provisioning cycle, expires after 3 minutes (TTL-based cleanup to bound per-hostname cardinality). |
| `cf_edge_operator_drift_buffer_depth{resource}` | gauge | Current items in the drift event channel by resource type. Approaching `--drift-buffer` capacity means the worker controller is not draining fast enough. |
| `cf_edge_operator_drift_buffer_overflow_total{resource}` | counter | Times the drift buffer was full by resource type, causing the zone controller to block. Non-zero indicates capacity issue. |
| `cf_edge_operator_drift_detection_errors_total{resource}` | counter | Drift detection failures by resource type. Non-zero means drift detection is failing for one or more zones. |

Controller-runtime also exposes `controller_runtime_reconcile_total` and `controller_runtime_reconcile_time_seconds` per controller.

### Alerting

The Helm chart ships a `PrometheusRule` (disabled by default):

| Alert | Fires when |
|-------|------------|
| `CfEdgeOperatorZoneNotInitialized` | Zone CR not initialized (zone GET never succeeded) for 10 min |
| `CfEdgeOperatorDown` | Metrics endpoint unreachable for 2 min |
| `CfEdgeOperatorUnhealthyHostnames` | Any CR in `unhealthy` state for 5 min |
| `CfEdgeOperatorConflictHostnames` | Any CR in `conflict` state for 5 min |
| `CfEdgeOperatorHighAPIErrorRate` | Cloudflare 5xx error rate > 0.1/s for 5 min |
| `CfEdgeOperatorSSLProvisioningStalled` | Any CR in `pending` state for 24 h |

Enable with `prometheusRule.enabled=true`. See [docs/runbook.md](runbook.md) for investigation and resolution steps per alert.

## Adding a New Resource Type

The zone controller is a thin orchestrator -- credential validation and scheduling. Each CF resource type has its own drift detection in a separate file (`zone_*_drift.go`). To add a new resource type (e.g., rulesets):

1. **New API group and CRD** -- e.g., `api/security/v1beta1/ruleset_types.go`, `security.cf-edge.io/v1beta1`
2. **New drift detection** -- `internal/controller/zone_ruleset_drift.go` with `detectRulesetDrift()` following the same pattern as `zone_customhostname_drift.go`
3. **New worker controller** -- `internal/controller/ruleset_controller.go` handling individual CF API writes (create/update/delete)
4. **Wire in `cmd/main.go`** -- new drift channel, new controller pair
5. **One line in zone controller** -- `_ = r.detectRulesetDrift(ctx, cf, &zone)` in `Reconcile()`

No existing CH code is modified. Each resource type's drift detection, metrics, and channel are self-contained.

## Future Work

- **Integration tests** -- `integration_test.go` uses envtest (real K8s API) + httptest (mock CF API) to test full controller flows: lifecycle, Zone recovery, terminating CRs, SSL defaults cascade, drift correction, and conflict detection. End-to-end tests against a real CF zone are a separate future item.
- **Additional CF resource types** -- Web Application Firewall (WAF) rules, routing, and other Cloudflare primitives via dedicated API groups (e.g. `security.cf-edge.io`, `routing.cf-edge.io`). See [Adding a New Resource Type](#adding-a-new-resource-type) for the implementation guide.
