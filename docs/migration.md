# Migrating from external-dns to cf-edge-operator

This guide covers migrating custom hostname management from external-dns (using the `external-dns.alpha.kubernetes.io/cloudflare-proxied` and related annotations) to cf-edge-operator CRDs.

## Overview

external-dns manages Cloudflare (CF) custom hostnames as a side-effect of DNS record management. cf-edge-operator manages them explicitly via `CustomHostname` CRs, giving you:

- Origin Server Name Indication (SNI) override support
- SSL lifecycle visibility (validation records, expiry)
- Drift detection and automatic reconciliation
- Prometheus metrics
- Per-hostname observability (`status.createCount`, `status.consecutiveErrors`)

## Migration Strategy

The recommended approach is **incremental**: migrate one hostname at a time, with both systems active during the transition. The `--delete-policy=own-only` flag makes this safe.

### Step 1: Deploy cf-edge-operator with `own-only` delete policy

```yaml
# values.yaml
deletePolicy: "own-only"
```

This ensures that if a `CustomHostname` CR is deleted during the transition, the operator will not delete a hostname that external-dns has taken over (different CF ID).

### Step 2: Create Zone CR

```yaml
apiVersion: domains.cf-edge.io/v1beta1
kind: Zone
metadata:
  name: my-zone
  namespace: cf-edge-operator-system
spec:
  id: "<your-cloudflare-zone-id>"
  credentialsRef:
    name: cloudflare-api-token
    key: apiToken
```

### Step 3: For each hostname, create a CustomHostname CR

The operator will call `findByHostname` on reconcile and adopt the existing CF hostname (created by external-dns). No new CF resource is created.

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

After the first reconcile, `status.id` will reflect the CF hostname ID that external-dns originally created.

### Step 4: Remove the external-dns annotation

Once the `CustomHostname` CR is `Ready=True`, remove the annotation that caused external-dns to manage this hostname. At this point only cf-edge-operator manages it.

### Step 5: Verify and switch to `always` policy

Once all hostnames are migrated and external-dns is no longer managing any hostnames in the zone, you can switch to `--delete-policy=always` for cleaner semantics.

## Why `own-only` Matters During Migration

During the migration window, external-dns may still run sync cycles against hostnames you've already created CRs for. If external-dns deletes and recreates a hostname (common when annotations change), the CF hostname gets a new ID while `status.id` is still stale.

If you then delete the CR (e.g., rolling back the migration):

| Policy | Behavior |
|--------|----------|
| `always` | Tries `DELETE <stale-id>` → 404 → releases the finalizer. The live hostname survives by accident. |
| `own-only` | Looks up current CF state, sees ID mismatch → releases the finalizer without any CF API call. Explicitly safe. |

`own-only` makes migration rollback safe by design rather than safe by accident.

## Checking Migration Status

```bash
# See all CustomHostname CRs and their status
kubectl get customhostnames -A

# Expected columns: HOSTNAME, ORIGIN, SSL, READY, CREATES, ERRORS
# CREATES > 0 means the operator has successfully provisioned the hostname
# READY=True means SSL is active

# Check a specific hostname
kubectl describe customhostname customer-acme -n my-app
```

## Rollback

If you need to roll back to external-dns management:

1. Remove the `CustomHostname` CR — with `own-only`, the operator will not delete the CF hostname if external-dns has taken it over.
2. Re-add the external-dns annotation to the relevant Kubernetes Service or Ingress.

