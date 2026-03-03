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

The token needs `Zone:Custom Hostnames:Edit` and `Zone:Zone:Read` permissions.

### 3. Create a Zone CR

```yaml
apiVersion: domains.cf-edge.io/v1alpha1
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
apiVersion: saas.cf-edge.io/v1alpha1
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
| `spec.id` | yes | Cloudflare zone ID |
| `spec.credentialsRef.name` | yes | Secret name containing the API token |
| `spec.credentialsRef.key` | no | Secret key, defaults to `apiToken` |

### CustomHostname

| Field | Required | Description |
|-------|----------|-------------|
| `spec.hostname` | yes | The custom hostname (e.g. `api.acme.com`) |
| `spec.originServer` | yes | Origin the hostname routes to. Must belong to the zone. |
| `spec.originSNI` | no | SNI sent to the origin. Defaults to `originServer`. Requires CF account entitlement. |
| `spec.zoneRef.name` | yes | Zone CR name |
| `spec.zoneRef.namespace` | no | Zone CR namespace. Defaults to the operator namespace. |
| `spec.ssl.type` | no | DV type. Default: `dv` |
| `spec.ssl.method` | no | DCV method: `http`, `txt`, or `email`. Default: `http` |
| `spec.ssl.certificateAuthority` | no | CA: `lets_encrypt`, `google`, `ssl_com`. Enterprise only. |
| `spec.ssl.minTLSVersion` | no | `1.0`, `1.1`, `1.2`, or `1.3` |
| `spec.deletePolicy` | no | Per-CR delete policy: `always` or `own-only`. Overrides `--delete-policy`. Useful during migration to protect against deleting hostnames managed by other tools. |

### Status fields

| Field | Description |
|-------|-------------|
| `status.id` | Cloudflare-assigned hostname ID |
| `status.ssl.status` | SSL state: `pending_validation`, `active`, `expired`, etc. |
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
| `deletePolicy` | `always` | `always` or `own-only`. See [docs/architecture.md](docs/architecture.md#delete-policy). |
| `leaderElect` | `true` | Enable leader election (required for HA) |
| `replicaCount` | `1` | Number of replicas |
| `resources.limits.cpu` | `500m` | CPU limit |
| `resources.limits.memory` | `128Mi` | Memory limit |
| `podDisruptionBudget.enabled` | `false` | Create a PodDisruptionBudget (recommended when `replicaCount > 1`) |
| `podDisruptionBudget.minAvailable` | `1` | Minimum available replicas during voluntary disruptions |

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

Metrics are exposed on `:8080/metrics` (HTTP). Key metrics:

- `cf_edge_operator_customhostname_operations_total{operation}` — successful CF write operations (create, recreate, update, delete)
- `cf_edge_operator_ssl_provisioning_duration_seconds{zone,hostname,method}` — time from CF create to `ssl.status == active` (1m–1w buckets)
- `cf_edge_operator_customhostnames{zone,state}` — CRs by zone and state (ready/pending/unhealthy/conflict); sum = total CRs in zone
- `cf_edge_operator_zone_customhostnames{zone,type}` — CF custom hostnames by type (managed/orphan); sum = CF quota usage for the zone
- `cf_edge_operator_api_errors_total` — Cloudflare API error count
- `cf_edge_operator_api_duration_seconds{operation}` — CF API latency histogram

