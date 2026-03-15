# cf-edge-operator

A Kubernetes operator for managing [Cloudflare for SaaS](https://developers.cloudflare.com/cloudflare-for-platforms/cloudflare-for-saas/) custom hostnames. It handles custom hostname lifecycle (create, update, delete), SSL provisioning, Origin SNI overrides, and drift detection — things that don't belong in external-dns.

## Quick Start

### 1. Install

```bash
helm install cf-edge-operator oci://ghcr.io/mrozentsvayg/cf-edge-operator/charts/cf-edge-operator \
  --namespace cf-edge-operator-system \
  --create-namespace \
  --set image.tag=latest
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
| `spec.ssl.type` | no | DV type. Default: `dv` |
| `spec.ssl.method` | no | DCV method: `http`, `txt`, or `email`. Empty = `--ssl-method` default, then CF default |
| `spec.ssl.certificateAuthority` | no | CA: `lets_encrypt`, `google`, `ssl_com`. Empty = `--ssl-certificate-authority` default, then CF default |
| `spec.ssl.minTLSVersion` | no | `1.0`, `1.1`, `1.2`, or `1.3`. Empty = `--ssl-min-tls-version` default, then CF default |
| `spec.managementPolicy` | no | Per-CR management policy: `manage`, `create`, or `observe`. Overrides `--management-policy`. |
| `spec.deletePolicy` | no | Per-CR delete policy: `always`, `own-only`, or `never`. Overrides `--delete-policy`. Useful during migration to protect against deleting hostnames managed by other tools. |

### Status fields

| Field | Description |
|-------|-------------|
| `status.id` | Cloudflare-assigned hostname ID |
| `status.ssl.status` | SSL state: `pending_validation`, `active`, `expired`, etc. |
| `status.ssl.certificateAuthority` | CA that issued the certificate (`lets_encrypt`, `google`, `ssl_com`) |
| `status.ssl.minTLSVersion` | Minimum TLS version configured for this hostname |
| `status.ssl.method` | DCV method used (`http`, `txt`, `email`) |
| `status.ssl.type` | Validation type (`dv`) |
| `status.ssl.validationRecords` | DCV tokens to complete SSL issuance |
| `status.createCount` | How many times the hostname was (re)created. Values > 1 indicate external deletions. |
| `status.consecutiveErrors` | Consecutive reconcile failures. Resets to 0 on success. |
| `status.conditions[Ready].reason` | `HostnameConflict` when another CR already owns this hostname in Cloudflare. Clears automatically when the owning CR is deleted. |
| `status.sslProvisioningStartedAt` | Timestamp set on each create/recreate in Cloudflare. Source for the `ssl_provisioning_duration_seconds` metric. |

---

## Helm Values

| Value | Default | Description |
|-------|---------|-------------|
| `image.repository` | `ghcr.io/mrozentsvayg/cf-edge-operator` | Image repository |
| `image.tag` | chart appVersion | Image tag |
| `operatorNamespace` | release namespace | Namespace where Zone CRs live |
| `deletePolicy` | `always` | `always`, `own-only`, or `never`. See [docs/architecture.md](docs/architecture.md#delete-policy). |
| `leaderElect` | `true` | Enable leader election (required for HA) |
| `replicaCount` | `1` | Number of replicas |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `128Mi` | Memory limit |
| `dryRun` | `false` | Log CF operations without executing them |
| `driftInterval` | `1m` | How often the zone controller bulk-lists CF to detect drift |
| `driftBuffer` | `1024` | Internal channel buffer for drift events |
| `sslMethod` | _(empty)_ | Default DCV method for new CHs. Empty = CF default |
| `sslCertificateAuthority` | _(empty)_ | Default CA for new CHs. Empty = CF default |
| `sslMinTLSVersion` | _(empty)_ | Default min TLS version for new CHs. Empty = CF default |
| `podDisruptionBudget.enabled` | `false` | Create a PodDisruptionBudget (recommended when `replicaCount > 1`) |
| `podDisruptionBudget.minAvailable` | `1` | Minimum available replicas during voluntary disruptions |
| `serviceMonitor.enabled` | `false` | Create a ServiceMonitor for Prometheus Operator |
| `prometheusRule.enabled` | `false` | Create a PrometheusRule with alert definitions |

> **CRD upgrades:** Helm does not update CRDs on `helm upgrade`. When upgrading to a new chart version, apply CRDs manually first:
> ```bash
> kubectl apply -f charts/cf-edge-operator/crds/
> helm upgrade cf-edge-operator charts/cf-edge-operator ...
> ```

---

## Migrating from external-dns

If you currently use external-dns annotations to manage Cloudflare custom hostnames, see [docs/migration.md](docs/migration.md) for the step-by-step migration guide. Use `--delete-policy=own-only` during migration.

---

## Architecture

See [docs/architecture.md](docs/architecture.md) for the coordinator/worker design, drift detection, API efficiency, and observability reference.

## Metrics

Metrics are exposed on `:8080/metrics` (HTTP, via Helm chart). See [docs/architecture.md](docs/architecture.md#prometheus-metrics) for the full reference. Key metrics:

- `cf_edge_operator_zone_ready{zone_cr}` — 1 if zone credentials are valid and CF API is reachable, 0 otherwise
- `cf_edge_operator_operations_total{resource,operation}` — successful CF write operations (create, recreate, update, delete)
- `cf_edge_operator_customhostnames{zone,state}` — CRs by zone and state (ready/pending/unhealthy/conflict)
- `cf_edge_operator_zone_customhostnames{zone,type}` — CF custom hostnames by type (managed/orphan)
- `cf_edge_operator_ssl_provisioning_duration_seconds{zone_cr,hostname,method}` — time from CF create to SSL active
- `cf_edge_operator_api_errors_by_code_total{resource,operation,status_code}` — CF API errors by HTTP status code
- `cf_edge_operator_api_duration_seconds{resource,operation}` — CF API latency histogram
- `cf_edge_operator_drift_buffer_depth{resource}` — current items in the drift event channel
- `cf_edge_operator_drift_buffer_overflow_total{resource}` — times the drift buffer was full (zone controller blocked)

