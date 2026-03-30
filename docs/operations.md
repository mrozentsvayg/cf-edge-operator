# cf-edge-operator Operations Guide

Deployment configuration, performance tuning, and HA setup for production. Custom hostnames are abbreviated as CHs throughout this document.

## Configuration Flags

All flags are set via Helm values, which are passed as container args in the Deployment.

| Flag | Helm default | Helm value | Description |
|------|-------------|-----------|-------------|
| `--operator-namespace` | release namespace | `operatorNamespace` | Namespace where Zone CRs are managed |
| `--management-policy` | `manage` | `managementPolicy` | `manage`, `create`, or `observe` -- see [migration.md](migration.md) |
| `--delete-policy` | `always` | `deletePolicy` | `always`, `own-only`, or `never` -- see [migration.md](migration.md) |
| `--dry-run` | `false` | `dryRun` | Log Cloudflare (CF) operations without executing them |
| `--drift-interval` | `1m` | `driftInterval` | How often the zone controller bulk-lists CF hostnames |
| `--drift-buffer` | `1024` | `driftBuffer` | Internal channel buffer for drift events |
| `--cf-api-timeout` | `3s` | `cfAPITimeout` | Per-request timeout for single CF API calls (zone lookup, CH get/create/update/delete) |
| `--cf-api-max-retries` | `1` | `cfAPIMaxRetries` | Retries for single CF API calls (immediate, no backoff). Skips retry on 429 |
| `--cf-api-bulk-timeout` | `5s` | `cfAPIBulkTimeout` | Per-page timeout for paginated CF API calls (bulk drift list) |
| `--cf-api-bulk-max-retries` | `0` | `cfAPIBulkMaxRetries` | Per-page retries for paginated CF API calls (SDK-level, ~2s backoff). Only the failed page is retried |
| `--leader-elect` | `true` | `leaderElect` | Required when running multiple replicas. Binary default is `false`; Helm default is `true` (safe for production) |
| `--zap-devel` | `true` | `zapDevel` | Development logger: human-readable console format, DPanic panics. Set `false` for JSON output (recommended for production log aggregation). |
| `--zap-log-level` | _(auto)_ | `zapLogLevel` | Log verbosity: `0` = INFO, `1` = DEBUG, `2` = TRACE. In dev mode, V(1) is visible by default; V(2) requires `--zap-log-level=2`. In production mode, only INFO is visible by default. |
| `--ssl-certificate-authority` | _(empty)_ | `sslCertificateAuthority` | Default CA for new CHs (`lets_encrypt`, `google`, `ssl_com`). Empty = CF default |
| `--ssl-min-tls-version` | _(empty)_ | `sslMinTLSVersion` | Default min TLS version for new CHs (`1.0`-`1.3`). Empty = CF default |
| `--ssl-method` | _(empty)_ | `sslMethod` | Default DCV method for new CHs (`http`, `txt`, `email`). Empty = `http` |
| `--ssl-type` | _(empty)_ | `sslType` | Default validation type for new CHs (`dv`). Empty = `dv` |

### CF API timeout and retry budget

Single CF API calls and paginated bulk list have separate timeout and retry settings.

**Single calls** (`--cf-api-timeout`, `--cf-api-max-retries`): zone lookup, CH get/create/update/delete. Uses the operator's own retry loop with immediate retry (no backoff). Skips retry on 429 (rate limit). Default: 3s timeout, 1 retry. Worst case: 2 x 3s = 6s.

**Bulk drift list** (`--cf-api-bulk-timeout`, `--cf-api-bulk-max-retries`): paginated CH list during drift detection. Uses SDK-level per-page retries (~2s backoff). Only the failed page is retried, not the whole list. Default: 5s timeout, 0 retries. Worst case with 172 CHs at 50/page (5 HTTP calls): 5 x 5s = 25s.

**Total worst case per drift cycle:** single call worst case + bulk list worst case. With defaults: 6s + 25s = 31s (just over the 30s drift interval, extreme case).

**Caution when increasing `--cf-api-bulk-max-retries`:** the SDK uses exponential backoff (base 2s, max 30s) between retries. Setting bulk retries to 1 adds ~2s per failed page. Setting to 2 adds ~6s (2s + 4s) per failed page. Keep the total budget well under `--drift-interval`.

---

## Log Levels

The operator uses structured logging with four verbosity levels:

| Level | Flag | What's logged |
|-------|------|---------------|
| ERROR | _(always visible)_ | API failures: lookup, create, update, delete, list errors; zone-level failures (token fetch, CF zone lookup) |
| INFO | _(default)_ | Operational events: creates, deletes, drift corrections, policy skips, SSL provisioned, drift detection summaries (when drifted > 0) |
| V(1) / DEBUG | `--zap-log-level=1` | Confirmations and heartbeats: finalizer added, status.ssl refreshed, drift detection complete (when drifted = 0), drift detection failed (summary), duplicate CR skip during drift detection |
| V(2) / TRACE | `--zap-log-level=2` | Per-item verbose: orphan CF hostnames (no associated CR), dry-run per-CR "no drift detected" |

In development mode (`--zap-devel=true`, the default), V(1)/DEBUG is visible by default; V(2)/TRACE requires `--zap-log-level=2`. In production mode (`--zap-devel=false`), only INFO is visible by default; set `--zap-log-level` to increase verbosity.

Log messages use the format: `<resource> - <action>, <context> (<reason>)`. Resource prefixes: `custom hostname` for CH operations, `zone` for zone-level infrastructure.

---

## Performance Tuning

### Drift Detection Interval (`--drift-interval`)

Controls how often each Zone CR triggers a bulk list of Cloudflare custom hostnames to detect external changes (dashboard edits, other automation).

| Interval | CF API list calls/hour (per zone) | External drift window |
|----------|----------------------------------|-----------------------|
| `30s` | ~120 | <=30s |
| `1m` *(default)* | ~60 | <=1m |
| `2m` | ~30 | <=2m |
| `5m` | ~12 | <=5m |

**Cloudflare API rate limit:** 1200 requests/5min = 240/min. At `1m` with 5 zones of 1000 hostnames each (~10 list calls per zone per cycle = 50 calls/min), you are well within limits.

The actual wall-clock gap between cycles is `drift-interval + reconcile_execution_time`. Execution time is dominated by Cloudflare API latency (~1-5s for a paginated list). At the default `1m`, expect ~62-65s between cycle starts.

**Recommendation:** Keep the default `1m` unless you have a specific SLA requirement for external drift detection. Only reduce below `1m` if you operate in an environment with frequent external CF changes and need sub-minute detection.

### Drift Event Buffer (`--drift-buffer`)

Size of the internal Go channel used by the zone controller to signal drifted CustomHostname CRs to the CustomHostname controller. If the channel is full, the zone controller **blocks** until space is available, delaying the remainder of that drift detection cycle.

**Sizing guidance:**

| Scenario | Recommended buffer |
|----------|--------------------|
| 1 zone, <=100 CHs | 128 |
| 1-3 zones, <=500 CHs each | 1024 *(default)* |
| 5+ zones, 1000+ CHs each | 4096+ |

In practice the buffer rarely fills -- drifted CRs are consumed quickly by the CustomHostname controller (~200-500ms per CR). Only increase if you see zone reconcile cycles taking longer than expected at high drift volumes.

If the buffer is full, the zone controller blocks on send but respects context cancellation for graceful shutdown. Blocked events are dropped on shutdown and re-detected on the next drift cycle.

### Reconcile Error Backoff (fixed at 30s max)

When a reconcile fails (e.g. Cloudflare API error, invalid credentials), controller-runtime applies exponential backoff before retrying. The operator caps this at **30 seconds** -- after any number of failures, the maximum wait before the next retry is 30s.

This is intentionally not configurable:
- Raising it recreates the original controller-runtime default of ~16 minutes, causing slow recovery after fixing a configuration error
- Lowering it risks hammering the Cloudflare API on persistent errors

The 30s cap means any CR recovers within <=30s of the underlying issue being fixed -- either via its own backoff retry, or via the zone controller's drift detection cycle (<=`--drift-interval`), whichever fires first.

### Memory and CPU Resources

The operator's memory footprint is dominated by the controller-runtime informer cache, which holds all Zone and CustomHostname CR objects in memory.

**Rough sizing:**

| Scale | Approximate memory | CPU |
|-------|--------------------|-----|
| <=100 CHs | 64Mi | minimal |
| ~500 CHs | 80-100Mi | minimal |
| ~1000 CHs | 100-128Mi *(default limit)* | minimal |
| 5000+ CHs | 256Mi+ | low |

The default `128Mi` limit is appropriate for most deployments. Increase if you observe OOMKilled events at scale. CPU usage is low -- the operator is I/O-bound (Cloudflare API calls) rather than CPU-bound.

```yaml
# values.yaml -- example for large deployments
resources:
  limits:
    cpu: 500m
    memory: 256Mi
  requests:
    cpu: 10m
    memory: 128Mi
```

---

## High Availability

### Multiple Replicas

Running more than one replica requires leader election to prevent split-brain. Only the leader runs reconcile loops; followers stand by and take over within ~15s of a leader failure.

```yaml
# values.yaml
replicaCount: 2
leaderElect: true
```

### PodDisruptionBudget

Enable the PodDisruptionBudget (PDB) to ensure at least one replica remains available during voluntary disruptions (node drains, rolling updates). Has no effect with `replicaCount: 1`.

```yaml
# values.yaml
replicaCount: 2
leaderElect: true
podDisruptionBudget:
  enabled: true
  minAvailable: 1
```

---

## Uninstallation

**Important:** Delete CRs before removing the operator. The operator must be running to process finalizers and clean up Cloudflare resources. Removing the operator first leaves CRs stuck with finalizers that can only be removed manually.

### Helm

```bash
# 1. Delete all CustomHostname CRs (operator removes finalizers + deletes from CF)
kubectl delete customhostnames --all -A

# 2. Delete all Zone CRs
kubectl delete zones --all -A

# 3. Uninstall the operator
helm uninstall cf-edge-operator

# 4. Delete CRDs (Helm does not remove CRDs on uninstall)
kubectl delete -f charts/cf-edge-operator/crds/
```

### Kustomize

```bash
# 1. Delete all CustomHostname CRs first
kubectl delete customhostnames --all -A

# 2. Delete all Zone CRs
kubectl delete zones --all -A

# 3. Remove operator, CRDs, RBAC, and namespace
make undeploy
```

**Do not run `make undeploy` before deleting CRs.** It removes CRDs, which triggers cascading deletion of all CRs. CRs with finalizers will block CRD deletion because the operator (already removed) cannot process them. Recovery requires manually patching finalizers off each stuck CR.

---


## Production Checklist

Before going live:

- [ ] Zone CR `spec.id` matches the actual Cloudflare zone ID (verify in CF dashboard)
- [ ] Secret referenced by `spec.credentialsRef` exists and contains a valid API token with `Zone: Read` and `SSL and Certificates: Edit` permissions
- [ ] `--management-policy` set appropriately -- use `create` during migration to prevent update loops (see [migration.md](migration.md))
- [ ] `--delete-policy` set appropriately -- use `own-only` or `never` during any migration window where another tool may manage the same zone (see [migration.md](migration.md))
- [ ] `replicaCount >= 2` and `leaderElect: true` for HA
- [ ] `podDisruptionBudget.enabled: true` if availability during node drains is required
- [ ] `serviceMonitor.enabled: true` and `prometheusRule.enabled: true` if using Prometheus Operator
- [ ] `prometheusRule.runbookUrl` set to point to your copy of [runbook.md](runbook.md)
- [ ] Resource limits reviewed for your scale (see sizing table above)
- [ ] CRD upgrade procedure understood: `helm upgrade` does NOT update CRDs; apply them manually before upgrading the chart (`kubectl apply -f charts/cf-edge-operator/crds/`)
