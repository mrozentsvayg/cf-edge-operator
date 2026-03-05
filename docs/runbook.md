# cf-edge-operator Runbook

This runbook covers the alert rules shipped with the operator's PrometheusRule. Each section describes what the alert means, how to investigate, and how to resolve.

## CfEdgeOperatorZoneNotReady

**Severity:** critical

**Meaning:** A Zone CR has been unable to validate its credentials or reach the Cloudflare API for 5 minutes. Drift detection and CustomHostname reconciliation are fully suspended for that zone.

**Investigate:**

```bash
# Check Zone CR status conditions
kubectl get zones -n <operator-namespace>
kubectl describe zone <zone_cr> -n <operator-namespace>

# Check that the referenced secret exists and has the right key
kubectl get secret <credentialsRef.name> -n <operator-namespace>
kubectl get secret <credentialsRef.name> -n <operator-namespace> \
  -o jsonpath='{.data.apiToken}' | base64 -d | wc -c  # should be non-zero

# Operator logs for the zone reconcile errors
kubectl logs -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator \
  | grep -E "(SecretError|CloudflareError|zone)"
```

**Common causes:**

| Condition reason | Cause | Resolution |
|-----------------|-------|------------|
| `SecretError` | Secret missing, wrong name, or key absent | Verify `spec.credentialsRef.name` and `spec.credentialsRef.key` match the actual secret |
| `CloudflareError` | Invalid API token, wrong zone ID, or CF API degradation | Verify `spec.id` matches the zone in the CF dashboard; check token permissions (`Zone: Read`, `SSL and Certificates: Edit`) |

**Resolve:**

Fix the secret name, zone ID, or API token. The Zone reconciler retries on its standard schedule; `zone_ready` will flip to 1 on the next successful reconcile.

---

## CfEdgeOperatorDown

**Severity:** critical

**Meaning:** The operator metrics endpoint has been unreachable for 2 minutes. All custom hostname management (create, update, delete, drift detection) is suspended.

**Investigate:**

```bash
# Check pod status
kubectl get pods -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator

# Check events and recent restarts
kubectl describe pod -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator

# Check logs from the most recent pod (including previous container if crashed)
kubectl logs -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator --previous
kubectl logs -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator
```

**Resolve:**

- If the pod is in CrashLoopBackOff, check logs for a fatal startup error (bad flag, missing RBAC, etc.).
- If the pod is evicted (OOMKilled), increase `resources.limits.memory` in the Helm values.
- If the Deployment has 0 replicas, check if it was accidentally scaled down.
- If the node is NotReady, the operator will reschedule automatically once a healthy node is available.

---

## CfEdgeOperatorUnhealthyHostnames

**Severity:** warning

**Meaning:** One or more CustomHostname CRs have consecutive reconcile failures (`status.consecutiveErrors > 0`, `Ready=False`). The operator retries with exponential backoff; this alert fires when the condition persists for 5 minutes.

**Investigate:**

```bash
# List all unhealthy CustomHostname CRs
kubectl get customhostnames -A -o json | \
  jq -r '.items[] | select(.status.consecutiveErrors > 0) |
    [.metadata.namespace, .metadata.name, .status.consecutiveErrors,
     (.status.conditions[]? | select(.type=="Ready") | .message)] | @tsv'

# Inspect a specific CR
kubectl describe customhostname <name> -n <namespace>

# Operator logs around the failure
kubectl logs -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator \
  | grep -E "(ERROR|hostname=<hostname>)"
```

**Common causes:**

| Symptom | Cause | Resolution |
|---------|-------|------------|
| `403 Forbidden` from Cloudflare API | API token revoked or lacks permissions | Rotate the token in the Zone CR's secret; token needs `Zone: Read` and `SSL and Certificates: Edit` |
| `429 Too Many Requests` | CF API rate limit | Transient; operator backs off automatically. Check for unusual reconcile volume. |
| `zone not found` or `500` | CF API service degradation | Check https://www.cloudflarestatus.com; operator will recover when CF is healthy. |
| Kubernetes API errors | RBAC or network issue | Check operator RBAC and cluster network connectivity. |

**Resolve:**

Fix the underlying issue (token, RBAC, network). `consecutiveErrors` resets to 0 on the next successful reconcile.

---

## CfEdgeOperatorConflictHostnames

**Severity:** warning

**Meaning:** Two or more CustomHostname CRs in the same zone specify the same `spec.hostname`. Only the first CR to create the hostname in Cloudflare is active; duplicate CRs are marked `HostnameConflict` and are not reconciled.

**Investigate:**

```bash
# Find conflicted CRs
kubectl get customhostnames -A -o json | \
  jq -r '.items[] |
    select(.status.conditions[]? |
      select(.type=="Ready" and .reason=="HostnameConflict")) |
    [.metadata.namespace, .metadata.name, .spec.hostname] | @tsv'

# Find all CRs for a specific hostname (to identify the active vs duplicate)
HOSTNAME="app.example.com"
kubectl get customhostnames -A -o json | \
  jq -r --arg h "$HOSTNAME" '.items[] | select(.spec.hostname==$h) |
    [.metadata.namespace, .metadata.name, .status.id // "no-id"] | @tsv'
```

The CR with a non-empty `status.id` is the active one that owns the Cloudflare hostname. CRs without an ID (or with a mismatched ID) are the duplicates.

**Resolve:**

1. Identify which CR should own the hostname (the one with `status.id` populated is currently active in Cloudflare).
2. Delete the duplicate CR(s) with `Ready=False/HostnameConflict`.
3. The conflict condition on the remaining CR clears automatically on the next zone drift detection cycle (within `--drift-interval`, default 1m).

If the duplicate was created intentionally (e.g., migration between namespaces), ensure the old CR is fully deleted before the new one is expected to become active.

---

## CfEdgeOperatorHighAPIErrorRate

**Severity:** warning

**Meaning:** The operator is receiving sustained 5xx errors from the Cloudflare API at a rate greater than 0.1/s (6/min) for more than 5 minutes. Note: 4xx errors (409 conflict, 404 not found) are expected in normal operation and are not included.

**Investigate:**

```bash
# Check operator logs for recent API errors
kubectl logs -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator \
  | grep -i "cloudflare\|api error\|status.*5[0-9][0-9]"

# Check Cloudflare status page
open https://www.cloudflarestatus.com

# PromQL to see which operations are failing
# rate(cf_edge_operator_api_errors_by_code_total[5m])
```

**Resolve:**

- **CF service degradation:** Wait for Cloudflare to recover. The operator will resume automatically.
- **Invalid API token:** A token that was valid may have been rotated or had permissions changed. Update the token in the Zone CR's secret.
- **Zone ID misconfiguration:** A Zone CR with an invalid or deleted zone ID will produce sustained errors. Correct `spec.id` in the Zone CR.

---

## CfEdgeOperatorSSLProvisioningStalled

**Severity:** warning

**Meaning:** Hostnames have been in a non-ready (pending SSL) state for over 24 hours. Cloudflare SSL provisioning normally completes within minutes to a few hours; 24 hours indicates a likely DCV (Domain Control Validation) issue.

> **Note:** This alert may fire during large batch provisioning (e.g., a migration that creates hundreds of hostnames simultaneously). Add a Prometheus silence for the duration of a planned migration.

**Investigate:**

```bash
# List pending CustomHostname CRs
kubectl get customhostnames -A -o json | \
  jq -r '.items[] |
    select(.status.conditions[]? | select(.type=="Ready" and .status=="False")) |
    select(.status.ssl.status != "active") |
    [.metadata.namespace, .metadata.name, .spec.hostname,
     .status.ssl.status // "unknown"] | @tsv'

# Get DCV validation records for a specific CR
kubectl get customhostname <name> -n <namespace> \
  -o jsonpath='{.status.ssl.validationRecords}' | jq .
```

The `status.ssl.validationRecords` field contains the DCV tokens Cloudflare requires. Depending on the validation method:

- **HTTP:** A `/.well-known/pki-validation/<token>.txt` file must be served at the hostname.
- **TXT:** A `_cf-custom-hostname.<hostname>` TXT DNS record must exist.
- **Email:** Cloudflare sends an email to contacts in the domain's WHOIS record (uncommon for custom hostnames).

**Resolve:**

1. Check `status.ssl.status` — if it is `pending_validation`, Cloudflare is waiting for DCV.
2. Confirm the validation record is correctly placed and publicly resolvable.
3. Once DCV is satisfied, Cloudflare activates the certificate and the CR transitions to `Ready=True` within the next requeue cycle (30s).

If the SSL status is `pending_issuance` or `pending_deployment`, DCV has passed and Cloudflare is issuing/deploying the certificate — no action needed, just wait.

---

## General Diagnostic Commands

```bash
# Operator health and version
kubectl get pods -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator
kubectl logs -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator | tail -100

# Zone CR status (credential and API token validation)
kubectl get zones -n <operator-namespace>
kubectl describe zone <name> -n <operator-namespace>

# CustomHostname CRs overview
kubectl get customhostnames -A
kubectl get customhostnames -A -o wide

# Operator metrics (port-forward if no Ingress/LoadBalancer)
kubectl port-forward -n <operator-namespace> \
  svc/$(kubectl get svc -n <operator-namespace> -l app.kubernetes.io/name=cf-edge-operator -o name | head -1) \
  8080:8080
curl -s localhost:8080/metrics | grep cf_edge_operator
```
