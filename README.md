# cf-edge-operator

A Kubernetes operator for managing [Cloudflare for SaaS](https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/) custom hostnames. It handles custom hostname lifecycle (create, update, delete), SSL provisioning, Origin SNI overrides, and drift detection -- things that don't belong in external-dns. It can also optionally manage Cloudflare Load Balancing (Accounts, Pools, Monitors, LoadBalancers) as a single-owner control-plane role.

## Quick Start

### 1. Install

```bash
helm install cf-edge-operator oci://ghcr.io/mrozentsvayg/cf-edge-operator/charts/cf-edge-operator \
  --namespace cf-edge-operator-system \
  --create-namespace
```

Or point ArgoCD at the chart path in this repo.

### 2. Create a Cloudflare API token secret

```bash
kubectl create secret generic cloudflare-api-token \
  --namespace cf-edge-operator-system \
  --from-literal=apiToken=<your-token>
```

The token needs `Zone: Read` and `SSL and Certificates: Edit` permissions.

### 3. Create a Zone CR

```yaml
apiVersion: domains.cf-edge.io/v1beta1
kind: Zone
metadata:
  name: my-zone
  namespace: cf-edge-operator-system
spec:
  id: "<cloudflare-zone-id>"
  credentialsRef:
    name: cloudflare-api-token
```

### 4. Create a CustomHostname CR

```yaml
apiVersion: saas.cf-edge.io/v1beta1
kind: CustomHostname
metadata:
  name: customer-acme
  namespace: my-app
spec:
  hostname: api.acme.com
  originServer: origin.internal.example.com
  zoneRef:
    name: my-zone
```

### 5. Check status

```bash
kubectl get customhostnames -A
# NAME            HOSTNAME       ORIGIN                        SSL                  READY   CREATES   ERRORS
# customer-acme   api.acme.com   origin.internal.example.com   pending_validation   False   1         0
```

`Ready=True` when SSL becomes active (may take minutes to hours depending on DCV method).

---

## CRD Reference

### Zone

| Field | Required | Description |
|-------|----------|-------------|
| `spec.id` | yes | Cloudflare zone ID (32-char hex, immutable) |
| `spec.credentialsRef.name` | yes | Secret name containing the API token |
| `spec.credentialsRef.key` | no | Secret key, defaults to `apiToken` |

### CustomHostname

| Field | Required | Description |
|-------|----------|-------------|
| `spec.hostname` | yes | The custom hostname (e.g. `api.acme.com`, immutable) |
| `spec.originServer` | yes | Origin the hostname routes to. Must belong to the zone. |
| `spec.originSNI` | no | SNI sent to the origin. If omitted, the operator does not manage SNI. Requires CF account entitlement. |
| `spec.zoneRef.name` | yes | Zone CR name |
| `spec.zoneRef.namespace` | no | Zone CR namespace. Defaults to the operator namespace. |
| `spec.ssl.certificateAuthority` | no | CA: `lets_encrypt`, `google`, `ssl_com`. Empty = `--ssl-certificate-authority` default, then CF default |
| `spec.ssl.minTLSVersion` | no | `1.0`, `1.1`, `1.2`, or `1.3`. Empty = `--ssl-min-tls-version` default, then CF default |
| `spec.ssl.method` | no | DCV method: `http`, `txt`, or `email`. Empty = `--ssl-method` default, then `http` |
| `spec.ssl.type` | no | Validation type: `dv`. Empty = `--ssl-type` default, then `dv` |
| `spec.managementPolicy` | no | Per-CR management policy: `manage`, `create`, or `observe`. Overrides `--management-policy`. |
| `spec.deletePolicy` | no | Per-CR delete policy: `always`, `own-only`, or `never`. Overrides `--delete-policy`. Useful during migration to protect against deleting hostnames managed by other tools. |

### Status fields

| Field | Description |
|-------|-------------|
| `status.id` | Cloudflare-assigned hostname ID |
| `status.hostnameStatus` | CF custom hostname activation status (`active`, `pending`, `active_redeploying`, `blocked`, `moved`, `deleted`, etc.). Refreshed on every drift cycle. |
| `status.ssl.status` | SSL state: `pending_validation`, `active`, `expired`, etc. |
| `status.ssl.certificateAuthority` | CA that issued the certificate (`lets_encrypt`, `google`, `ssl_com`) |
| `status.ssl.minTLSVersion` | Minimum TLS version configured for this hostname |
| `status.ssl.method` | DCV method used (`http`, `txt`, `email`) |
| `status.ssl.type` | Validation type (`dv`) |
| `status.ssl.id` | Cloudflare SSL certificate identifier. Changes on reissue. |
| `status.ssl.issuer` | Certificate issuer (e.g. "Google Trust Services LLC") |
| `status.ssl.serialNumber` | Certificate serial number. Changes on reissue. |
| `status.ssl.bundleMethod` | Certificate chain bundling (`ubiquitous`, `optimal`, `force`) |
| `status.ssl.wildcard` | Whether the certificate covers a wildcard hostname |
| `status.ssl.hosts` | Hostnames covered by this certificate |
| `status.ssl.uploadedOn` | Time the certificate was issued/uploaded |
| `status.ssl.expiresOn` | Certificate expiration time |
| `status.ssl.validationRecords` | DCV tokens to complete SSL issuance |
| `status.ssl.validationErrors` | Errors encountered during SSL validation |
| `status.createCount` | How many times the hostname was (re)created. Values > 1 indicate external deletions. |
| `status.consecutiveErrors` | Consecutive reconcile failures. Resets to 0 on success. |
| `status.conditions[Ready].reason` | `HostnameConflict` when another CR already owns this hostname in Cloudflare. Clears automatically when the owning CR is deleted. |
| `status.sslProvisioningStartedAt` | Timestamp set on each create/recreate in Cloudflare. Source for the `ssl_provisioning_duration_seconds` metric. |

### Load Balancing (control-plane role)

Optional. The operator can also manage Cloudflare Load Balancing through four CRDs. `Account` is a foundational credential resource in its own `accounts.cf-edge.io/v1beta1` group (parallel to `Zone` in `domains.cf-edge.io`); the three load-balancing kinds live in `loadbalancing.cf-edge.io/v1beta1`:

| Kind | Group | Scope | Purpose |
|------|-------|-------|---------|
| `Account` | `accounts.cf-edge.io` | account | Cloudflare account ID + credentials for account-scoped LB resources (the account-scope analog of a Zone) |
| `LoadBalancerMonitor` | `loadbalancing.cf-edge.io` | account | Health check that probes pool origins; referenced by pools |
| `LoadBalancerPool` | `loadbalancing.cf-edge.io` | account | Origin pool; references an Account and, optionally, a Monitor. Models origins (`port`, `virtualNetworkID`, per-origin `header`), `originSteering`, `checkRegions`, and `loadShedding` |
| `LoadBalancer` | `loadbalancing.cf-edge.io` | zone | Geo-steered hostname; references a Zone and LoadBalancerPool CRs by name. Models steering, per-pool weights (`defaultPoolRefs[].weight` + `defaultWeight`), session affinity (with `sessionAffinityAttributes`/`ttl`), `adaptiveRouting`, `locationStrategy`, and `networks` |

Load balancing is a single-owner control-plane role: enable it on exactly one (control) cluster with `features.loadBalancing.enabled=true` (Helm) / `--enable-loadbalancing`. All other clusters leave it off, and the LB CRDs, RBAC, controllers, and metrics are omitted. See [docs/architecture.md](docs/architecture.md#load-balancing-reconciliation-model) for the reconciliation model and per-CRD field reference.

---

## Helm Values

| Value | Default | Description |
|-------|---------|-------------|
| `image.repository` | `ghcr.io/mrozentsvayg/cf-edge-operator` | Image repository |
| `image.tag` | chart appVersion | Image tag |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `imagePullSecrets` | `[]` | Pull secret names for private registries |
| `podLabels` | `{}` | Additional labels for the pod template |
| `operatorNamespace` | release namespace | Namespace where Zone CRs live |
| `managementPolicy` | `manage` | `manage`, `create`, or `observe`. See [docs/architecture.md](docs/architecture.md#management-policy). |
| `deletePolicy` | `always` | `always`, `own-only`, or `never`. See [docs/architecture.md](docs/architecture.md#delete-policy). |
| `features.customhostname.enabled` | `true` | Enable the CustomHostname controller (and the shared Zone controller), plus their RBAC. On by default -- this is the operator's original role. |
| `features.loadBalancing.enabled` | `false` | Enable the load-balancing controllers, CRDs, and RBAC (single-owner control-plane role; set `true` on exactly one cluster). See [docs/architecture.md](docs/architecture.md#load-balancing-reconciliation-model). |
| `features.loadBalancing.poolHealth` | `false` | Opt-in pool-health metrics (`--enable-pool-health`): one extra CF read per Pool per reconcile publishes pool/origin health gauges. Ignored unless `loadBalancing.enabled`. See [docs/operations.md](docs/operations.md#pool-health-monitoring). |
| `crds.enabled` | `true` | Let the chart install and manage the CRDs (gated per feature). Set `false` to manage them out-of-band. See [docs/operations.md](docs/operations.md#crd-management). |
| `crds.keep` | `true` | Add `helm.sh/resource-policy: keep` + ArgoCD `Prune=false` so `helm uninstall`/prune never deletes CRDs (which would cascade-delete all CRs). |
| `leaderElect` | `true` | Enable leader election (required for HA) |
| `replicaCount` | `1` | Number of replicas |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `128Mi` | Memory limit |
| `dryRun` | `false` | Log CF operations without executing them |
| `driftInterval` | `1m` | How often the zone controller bulk-lists CF to detect drift; also the load-balancing controllers' self-requeue cadence |
| `driftBuffer` | `1024` | Internal channel buffer for drift events |
| `cfAPITimeout` | `5s` | Per-request timeout for CF API read calls (GET) |
| `cfAPIWriteTimeout` | `15s` | Per-request timeout for CF API write calls (create/update/delete) |
| `cfAPIMaxRetries` | `1` | Retries for single CF API calls (immediate, no backoff) |
| `cfAPIBulkTimeout` | `5s` | Per-page timeout for paginated bulk drift list |
| `cfAPIBulkMaxRetries` | `0` | Per-page retries for bulk drift list (SDK-level, ~2s backoff) |
| `cfAPIWriteDelay` | `250ms` | Pause after each successful CF write (create/edit/delete). Paces bulk changes. |
| `sslCertificateAuthority` | _(empty)_ | Default CA for new CHs. Empty = CF default |
| `sslMinTLSVersion` | _(empty)_ | Default min TLS version for new CHs. Empty = CF default |
| `sslMethod` | _(empty)_ | Default DCV method for new CHs. Empty = `http` |
| `sslType` | _(empty)_ | Default validation type for new CHs. Empty = `dv` |
| `zapDevel` | `true` | Development mode logger (console output). Set `false` for production JSON logs |
| `zapLogLevel` | _(empty)_ | Log verbosity: `0` = INFO, `1` = DEBUG, `2` = TRACE. See [operations.md](docs/operations.md#log-levels) |
| `podDisruptionBudget.enabled` | `false` | Create a PodDisruptionBudget (recommended when `replicaCount > 1`) |
| `podDisruptionBudget.minAvailable` | `1` | Minimum available replicas during voluntary disruptions |
| `serviceMonitor.enabled` | `false` | Create a ServiceMonitor for Prometheus Operator |
| `prometheusRule.enabled` | `false` | Create a PrometheusRule with alert definitions |

> **CRD lifecycle:** CRDs are chart-managed and gated per feature, so `helm upgrade` updates them in place (no manual `kubectl apply` step) and they survive `helm uninstall`/prune by default (`crds.keep=true`). Upgrading from a pre-consolidation chart requires adopting the existing CustomHostname/Zone CRDs once -- see [docs/operations.md](docs/operations.md#crd-management).

---

## Migrating from external-dns

If you currently use external-dns annotations to manage Cloudflare custom hostnames, see [docs/migration.md](docs/migration.md) for the step-by-step migration guide. Use `--delete-policy=own-only` during migration.

---

## Architecture

See [docs/architecture.md](docs/architecture.md) for the coordinator/worker design, drift detection, API efficiency, and observability reference.

## Metrics

Metrics are exposed on `:8080/metrics` (HTTP, via Helm chart). See [docs/architecture.md](docs/architecture.md#prometheus-metrics) for the full reference. Key metrics:

- `cf_edge_operator_zone_initialized{zone_cr}` -- 1 if zone has been initialized (zone name resolved from CF API), set once
- `cf_edge_operator_operations_total{resource,operation}` -- successful CF operations (adopt, create, recreate, update, delete)
- `cf_edge_operator_customhostnames{zone_cr,state}` -- CRs by zone and state (ready/pending/unhealthy/conflict)
- `cf_edge_operator_customhostname_status{zone_cr,status}` -- managed CF hostnames by activation status (active/pending/active_redeploying/blocked/moved/deleted)
- `cf_edge_operator_zone_customhostnames{zone_cr,type}` -- CF custom hostnames by type (managed/orphan/drifted/total); orphan = no associated CR
- `cf_edge_operator_ssl_provisioning_duration_seconds{zone_cr,hostname,method}` -- time from CF create to SSL active (gauge, expires after 3 min)
- `cf_edge_operator_api_errors_by_code_total{resource,operation,status_code}` -- CF API errors by HTTP status code
- `cf_edge_operator_api_retries_total{resource,operation}` -- retry attempts for single CF API calls
- `cf_edge_operator_api_duration_seconds{resource,operation}` -- CF API latency histogram
- `cf_edge_operator_drift_buffer_depth{resource}` -- current items in the drift event channel
- `cf_edge_operator_drift_buffer_overflow_total{resource}` -- times the drift buffer was full (zone controller blocked)
- `cf_edge_operator_drift_detection_errors_total{resource,source}` -- drift detection failures by error source (`cf_list`, `k8s_list`)

## Development

```bash
# Unit tests
go test -v ./internal/controller/...

# Lint
make lint

# Regenerate CRDs after API type changes
make generate manifests
```

Tests include unit tests for pure functions and integration tests using envtest (real K8s API server) + httptest (mock Cloudflare API). See [docs/architecture.md](docs/architecture.md) for details.
