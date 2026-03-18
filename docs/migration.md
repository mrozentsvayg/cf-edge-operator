# Migrating from external-dns to cf-edge-operator

This guide covers migrating custom hostname management from external-dns (using the `external-dns.alpha.kubernetes.io/cloudflare-custom-hostname` and related annotations) to cf-edge-operator CRDs.

## Overview

external-dns manages Cloudflare (CF) custom hostnames as a side-effect of DNS record management. cf-edge-operator manages them explicitly via `CustomHostname` CRs, giving you:

- Origin Server Name Indication (SNI) override support
- SSL lifecycle visibility (validation records, expiry)
- Drift detection and automatic reconciliation (origin, SNI, SSL config: CA, minTLSVersion, method, type)
- Prometheus metrics
- Per-hostname observability (`status.createCount`, `status.consecutiveErrors`)

## Migration Strategy

The recommended approach is **incremental**: migrate one hostname at a time, with both systems active during the transition. Use `managementPolicy` and `deletePolicy` to control the operator's behavior at each stage.

### Management Policy

`--management-policy` sets the operator-wide default; `spec.managementPolicy` overrides per CR:

| Policy | Create | Update (drift correction) | Delete |
|--------|--------|---------------------------|--------|
| `manage` *(default)* | Yes | Yes | Per `deletePolicy` |
| `create` | Yes | No -- logs drift but does not correct it | Per `deletePolicy` |
| `observe` | No -- waits for external creation | No | Releases finalizer unconditionally (ignores `deletePolicy`) |

**`create`** is the recommended policy during coexistence with external-dns or other automation. The operator provisions new hostnames if missing, but never updates them -- preventing change loops where both tools fight over origin/SNI settings.

**`observe`** is for tracking hostnames you explicitly don't want the operator to touch at all. Useful for monitoring hostnames managed entirely by another system.

### Step 1: Deploy cf-edge-operator

```yaml
# values.yaml
managementPolicy: "create"   # safe coexistence -- no updates by default
deletePolicy: "never"         # never delete CF hostnames during coexistence
```

`managementPolicy: "create"` ensures the operator never updates existing hostnames -- preventing change loops with external-dns. `deletePolicy: "never"` ensures that if a CR is deleted during the transition, the operator releases the finalizer without any CF API call -- the safest option during coexistence when hostnames are shared with external-dns. Individual CRs can override either setting via `spec.managementPolicy` and `spec.deletePolicy`.

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

With the operator-wide `managementPolicy: "create"` from Step 1, CRs don't need per-CR overrides:

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
  # managementPolicy and deletePolicy inherited from operator flags
```

The operator will call `findByHostname` on reconcile and adopt the existing CF hostname (created by external-dns). No new CF resource is created. No updates are made.

After the first reconcile, `status.id` will reflect the CF hostname ID that external-dns originally created.

### Step 4: Remove the external-dns annotation

Once the `CustomHostname` CR is `Ready=True`, remove the annotation that caused external-dns to manage this hostname.

### Step 5: Switch to full management

Once external-dns is no longer managing the hostname, switch to full management:

```yaml
spec:
  managementPolicy: manage     # operator now owns lifecycle
  deletePolicy: own-only       # safe delete during transition
```

### Step 6: Switch to `always` policy

Once all hostnames are migrated and external-dns is no longer managing any hostnames in the zone, you can switch to `--delete-policy=always` for cleaner semantics.

## Delete Policy Progression

The recommended `deletePolicy` progression during migration:

1. **`never`** (Step 1) -- during coexistence with external-dns. No CF API calls on CR deletion, simplest and safest.
2. **`own-only`** (Step 5) -- after external-dns is removed per hostname. Allows safe deletes with ID verification.
3. **`always`** (Step 6) -- after full migration. Clean lifecycle management.

### Why this matters

During coexistence, external-dns may still run sync cycles against hostnames you've already created CRs for. If external-dns deletes and recreates a hostname (common when annotations change), the CF hostname gets a new ID while `status.id` is still stale.

If you then delete the CR (e.g., rolling back the migration):

| Policy | Behavior |
|--------|----------|
| `always` | Tries `DELETE <stale-id>` -> 404 -> releases the finalizer. The live hostname survives by accident. |
| `own-only` | Looks up current CF state, sees ID mismatch -> releases the finalizer without any CF API call. Explicitly safe. |
| `never` | Releases the finalizer without any CF API call, regardless of ID match. Safest during coexistence. |

Note: when `managementPolicy: observe`, the operator always releases the finalizer without deleting -- `deletePolicy` is ignored entirely.

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

1. Remove the `CustomHostname` CR -- with `never`, the operator releases the finalizer without any CF API call, leaving the CF hostname intact for external-dns.
2. Re-add the external-dns annotation to the relevant Kubernetes Service or Ingress.

