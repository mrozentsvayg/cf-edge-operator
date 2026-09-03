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
| `--enable-customhostname` | `true` | `features.customhostname.enabled` | Enable the CustomHostname controller (and the shared Zone controller). On by default -- the operator's original role |
| `--enable-loadbalancing` | `false` | `features.loadBalancing.enabled` | Enable the load-balancing control-plane role (Account + the LoadBalancer/LoadBalancerPool/LoadBalancerMonitor controllers). Off by default |
| `--enable-pool-health` | `false` | `features.loadBalancing.poolHealth` | Opt-in pool-health metrics: one extra CF read per Pool per reconcile publishes the pool/origin health gauges. Ignored unless load balancing is enabled (the operator warns if set while it is off). Off by default -- see [Pool health monitoring](#pool-health-monitoring) |
| `--drift-interval` | `1m` | `driftInterval` | How often the zone controller bulk-lists CF hostnames; also how often the load-balancing controllers (Account/LoadBalancer/LoadBalancerPool/LoadBalancerMonitor) self-requeue |
| `--drift-buffer` | `1024` | `driftBuffer` | Internal channel buffer for drift events |
| `--cf-api-timeout` | `5s` | `cfAPITimeout` | Per-request timeout for single CF API read calls (zone lookup, CH and load-balancing get/list) |
| `--cf-api-write-timeout` | `15s` | `cfAPIWriteTimeout` | Per-request timeout for single CF API write calls (CH and load-balancing create/update/delete) |
| `--cf-api-max-retries` | `1` | `cfAPIMaxRetries` | Retries for single CF API calls (immediate, no backoff). Skips retry on 429 |
| `--cf-api-bulk-timeout` | `5s` | `cfAPIBulkTimeout` | Per-page timeout for paginated CF API calls (bulk drift list) |
| `--cf-api-bulk-max-retries` | `0` | `cfAPIBulkMaxRetries` | Per-page retries for paginated CF API calls (SDK-level, ~2s backoff). Only the failed page is retried |
| `--cf-api-write-delay` | `250ms` | `cfAPIWriteDelay` | Pause after each successful CF write (create/edit/delete). Paces sequential writes to avoid CF API throttling |
| `--leader-elect` | `true` | `leaderElect` | Required when running multiple replicas. Binary default is `false`; Helm default is `true` (safe for production) |
| `--zap-devel` | `true` | `zapDevel` | Development logger: human-readable console format, DPanic panics. Set `false` for JSON output (recommended for production log aggregation). |
| `--zap-log-level` | _(auto)_ | `zapLogLevel` | Log verbosity: `0` = INFO, `1` = DEBUG, `2` = TRACE. In dev mode, V(1) is visible by default; V(2) requires `--zap-log-level=2`. In production mode, only INFO is visible by default. |
| `--ssl-certificate-authority` | _(empty)_ | `sslCertificateAuthority` | Default CA for new CHs (`lets_encrypt`, `google`, `ssl_com`). Empty = CF default |
| `--ssl-min-tls-version` | _(empty)_ | `sslMinTLSVersion` | Default min TLS version for new CHs (`1.0`-`1.3`). Empty = CF default |
| `--ssl-method` | _(empty)_ | `sslMethod` | Default DCV method for new CHs (`http`, `txt`, `email`). Empty = `http` |
| `--ssl-type` | _(empty)_ | `sslType` | Default validation type for new CHs (`dv`). Empty = `dv` |

### CF API timeout and retry budget

Single CF API calls and paginated bulk list have separate timeout and retry settings.

**Read calls** (`--cf-api-timeout`, `--cf-api-max-retries`): zone lookup, CH get/findByHostname. Uses the operator's own retry loop with immediate retry (no backoff). Skips retry on 429 (rate limit). Default: 5s timeout, 1 retry. Worst case: 2 x 5s = 10s.

**Write calls** (`--cf-api-write-timeout`, `--cf-api-max-retries`): CH create/update/delete. Same retry loop as reads. Default: 15s timeout, 1 retry. Worst case: 2 x 15s = 30s. Longer timeout reduces false timeouts during CF degradation -- a slow success is better than a timeout that leaves stale errors.

**Bulk drift list** (`--cf-api-bulk-timeout`, `--cf-api-bulk-max-retries`): paginated CH list during drift detection. Uses SDK-level per-page retries (~2s backoff). Only the failed page is retried, not the whole list. Default: 5s timeout, 0 retries. Worst case with 172 CHs at 50/page (5 HTTP calls): 5 x 5s = 25s.

**Total worst case per drift cycle:** single call worst case + bulk list worst case. With defaults: 10s + 25s = 35s (just over the 30s drift interval, extreme case).

**Write pacing** (`--cf-api-write-delay`): pause after each successful CF write operation. Prevents burst writes from triggering CF API throttling during bulk spec changes (e.g., originSNI rollout across many CRs). Default: 250ms. Set to 0 for no delay. For N writes, total pacing overhead is N x delay (e.g., 24 writes x 250ms = 6s).

**Caution when increasing `--cf-api-bulk-max-retries`:** the SDK uses exponential backoff (base 2s, max 30s) between retries. Setting bulk retries to 1 adds ~2s per failed page. Setting to 2 adds ~6s (2s + 4s) per failed page. Keep the total budget well under `--drift-interval`.

---

## Log Levels

The operator uses structured logging with four verbosity levels:

| Level | Flag | What's logged |
|-------|------|---------------|
| ERROR | _(always visible)_ | API failures: lookup, create, update, delete, list errors; zone-level failures (token fetch, CF zone lookup) |
| INFO | _(default)_ | Operational events: creates, deletes, drift corrections, policy skips, SSL provisioned, drift detection summaries (when drifted > 0) |
| V(1) / DEBUG | `--zap-log-level=1` | Confirmations and heartbeats: finalizer added, hostnameStatus refreshed, status.ssl refreshed, drift detection complete (when drifted = 0), drift detection failed (summary), duplicate CR skip during drift detection |
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

## Load Balancing

These notes apply only when the load-balancing control-plane role is enabled
(`features.loadBalancing.enabled=true`). See
[architecture.md](architecture.md#load-balancing-reconciliation-model) for the
reconciliation model.

### Pool health monitoring

The operator's sync-state metrics track whether it has reconciled each CR to
Cloudflare; they say nothing about what Cloudflare's health checks observe. To
also publish CF-side pool and origin health, enable the opt-in axis:

```yaml
# values.yaml
features:
  loadBalancing:
    enabled: true
    poolHealth: true   # --enable-pool-health
```

When on, every LoadBalancerPool reconcile makes **one extra Cloudflare read**
(`PoolHealth.Get`, recorded under `operation="health"`) inside the existing
per-pool loop -- no new controller or bulk poll, and the poll is isolated: a
failure records a CF API error and leaves the health series stale, but never sets
an error or flips a pool's `Ready`. The four gauges
(`loadbalancerpool_health`, `loadbalancerpool_health_region`,
`loadbalancerpool_origin_health`, `loadbalancerpool_origin_health_region`) carry
raw region-status counts, so you derive fully/partial/down thresholds in PromQL.

**Cardinality caveat.** Health series scale with pools, origins, and regions:

- Every polled pool always gets the summarized per-pool and per-origin counts
  (3 status series each).
- Per-region series (`*_health_region`) are emitted **only** for pools that set
  `spec.checkRegions`, a CR-declared dimension bounded to at most 13 regions.
- A pool with `checkRegions` **unset** health-checks from *every* Cloudflare data
  center (heavier probes and an unbounded region set), so it gets **no per-region
  metric** -- only the summarized counts.

Set `spec.checkRegions` on pools you want per-region health for: it both bounds
the probe fan-out and makes the per-region series coherent. Leaving it unset on
many pools means heavier CF probing with no per-region visibility.

**Alerting.** There is deliberately no operator alert on pool health. The
operator's health metrics are poll-stale (refreshed only each `--drift-interval`)
and are best for dashboards and self-service PromQL, not time-sensitive paging.
For real-time paging on origin/pool health, use Cloudflare's native **Load
Balancing Health Alert** notification (stateful, per pool/origin, integrates with
PagerDuty and other destinations). The split is deliberate: the operator alerts on
sync (its domain); Cloudflare alerts on health (its domain).

### Coexisting with out-of-band configuration

The operator manages only the fields the CRDs model and leaves the rest of a
Cloudflare load balancer, pool, or monitor intact (see the curated-subset model in
[architecture.md](architecture.md#load-balancing-reconciliation-model)). A few
fields warrant explicit notes:

- **Pool `monitor` (monitor groups deferred).** A pool the operator manages
  cannot use a Cloudflare **monitor group** out-of-band: the pool edit always
  sends the single `monitor` field, so an externally-attached monitor group would
  be overwritten on the next reconcile. Monitor groups need a future
  `LoadBalancerMonitorGroup` CRD (a reusable account-scoped resource; pools would
  reference it via `monitorGroupRef`). Until it lands, keep operator-managed pools
  on a single `monitorRef`.
- **Pool `notification_filter` (omitted).** This alerting-plane field is not
  modeled. It is pool-level, so the operator never sends it -- an out-of-band
  `notification_filter` is coexist-safe (never clobbered). Cloudflare is
  deprecating it in favor of centralized Notifications.
- **Pool `notification_email` (retained for compatibility).** `spec.notificationEmail`
  is modeled and enforced even though Cloudflare has deprecated it, for backward
  compatibility with existing pools. `notification_filter` (its replacement) is
  omitted per the note above -- the two are intentionally treated differently.
- **Origin `port` and `virtualNetworkID` (now modeled).** Both are modeled on
  `spec.origins[]`, fixing an earlier version that would clobber an out-of-band
  port or virtual-network setting on edit. Set `virtualNetworkID` when an origin
  `address` is an internal/reserved address.

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

## CRD Management

All CRDs -- `CustomHostname`, `Zone`, and the four `loadbalancing.cf-edge.io`
kinds -- are rendered by the chart from templates, gated by the same feature
switches as the controllers. Which CRDs a release installs follows the enabled
features:

| Features enabled | CRDs installed |
|------------------|----------------|
| `customhostname` only *(default)* | `customhostnames`, `zones` |
| `loadBalancing` only | `zones`, `accounts`, `loadbalancers`, `loadbalancerpools`, `loadbalancermonitors` |
| both | all six |
| neither | none |

`Zone` is shared substrate (it supplies zone identity and credentials to both
CustomHostname and LoadBalancer), so it installs whenever either feature is on.

Two values control CRD lifecycle, mirroring cert-manager:

- **`crds.enabled`** *(default `true`)* -- the chart installs and manages the CRDs.
  Set `false` to manage them out-of-band (a cluster admin applies `config/crd`
  separately, or another chart owns them); the chart then renders no CRDs.
- **`crds.keep`** *(default `true`)* -- stamps `helm.sh/resource-policy: keep` and
  `argocd.argoproj.io/sync-options: Prune=false` onto every CRD so a `helm
  uninstall` or a GitOps prune never deletes them. Deleting a CRD cascade-deletes
  every CR of that kind, so keeping them is the safe default.

Because the CRDs are chart-managed (not in Helm's install-only `crds/`
directory), `helm upgrade` updates them in place -- no manual `kubectl apply` step
is needed before upgrading, unlike the pre-consolidation chart.

### Migrating an existing install to chart-managed CRDs

Earlier chart versions shipped the CustomHostname and Zone CRDs in Helm's special
`crds/` directory, which installs them once and never tracks them in the release.
This version renders all CRDs as normal templates so Helm (and ArgoCD) own them.

- **ArgoCD:** the CRD names are unchanged, so this is an in-place update, not a
  delete/recreate -- ArgoCD simply starts tracking the existing objects. The
  `argocd.argoproj.io/sync-options: Prune=false` annotation (from `crds.keep`)
  guards against an accidental prune removing a CRD, which would cascade-delete
  every CR of that kind.
- **Plain `helm upgrade`:** Helm refuses to adopt a resource it did not create
  ("... exists and cannot be imported into the current release"). Adopt the two
  pre-existing CRDs once before upgrading:

  ```bash
  for crd in customhostnames.saas.cf-edge.io zones.domains.cf-edge.io; do
    kubectl annotate crd "$crd" \
      meta.helm.sh/release-name=cf-edge-operator \
      meta.helm.sh/release-namespace=<release-namespace> --overwrite
    kubectl label crd "$crd" app.kubernetes.io/managed-by=Helm --overwrite
  done
  ```

  (The `loadbalancing.cf-edge.io` CRDs were already chart-templated, so they need
  no adoption.) This is the same adoption cert-manager documents for its own
  `crds/`-to-chart migration.

---

## Uninstallation

**Important -- delete in dependency order.** A resource CR's finalizer is released
only after the operator has deleted the backing Cloudflare resource, and that
requires three things to still be present: the operator itself, the CR's
referenced config CR (Zone for CustomHostname/LoadBalancer, Account for
LoadBalancerPool/LoadBalancerMonitor), and that config's credentials Secret --
the operator builds a Cloudflare client from the Zone/Account + Secret. So delete
the resource CRs *first*, then their Zone/Account CRs, the Secret, and finally the
operator. Removing any of those before the resource CRs leaves the CRs stuck in
`Terminating` -- see [CR stuck in Terminating](#cr-stuck-in-terminating).

The `loadbalancing.cf-edge.io` CRs (LoadBalancer, LoadBalancerPool,
LoadBalancerMonitor, Account) exist only on a control cluster
(`features.loadBalancing.enabled=true`); the `--ignore-not-found` flags below are no-ops
elsewhere.

### Helm

```bash
# 1. Delete resource CRs first (operator removes finalizers + deletes from CF)
kubectl delete loadbalancers,loadbalancerpools,loadbalancermonitors --all -A --ignore-not-found
kubectl delete customhostnames --all -A

# 2. Delete the config CRs they reference (now that no dependents remain)
kubectl delete accounts --all -A --ignore-not-found
kubectl delete zones --all -A

# 3. Uninstall the operator (removes the Deployment, RBAC, Service, etc.). With
#    crds.keep=true (the default) every CRD carries helm.sh/resource-policy:keep,
#    so the CRDs SURVIVE the uninstall -- and with them any CRs not deleted above.
helm uninstall cf-edge-operator

# 4. (Optional) Remove the CRDs. This cascade-deletes every remaining CR of each
#    kind, so only do this once all CRs are gone (steps 1-2) -- no operator is left
#    to finalize CRs, so any that remain would first need their finalizers stripped.
#    --ignore-not-found skips the loadbalancing CRDs on non-control clusters.
kubectl delete crd \
  customhostnames.saas.cf-edge.io \
  zones.domains.cf-edge.io \
  accounts.accounts.cf-edge.io \
  loadbalancers.loadbalancing.cf-edge.io \
  loadbalancerpools.loadbalancing.cf-edge.io \
  loadbalancermonitors.loadbalancing.cf-edge.io \
  --ignore-not-found
```

### Kustomize

> **Note.** The kubectl/Kustomize install path (`make install`, or the bundled
> `dist/install.yaml`) applies **all** CRDs raw -- it has neither the Helm
> chart's per-feature CRD gating (`crds.enabled`) nor the keep/Prune annotations
> (`crds.keep`). Every CRD is installed regardless of which features run, and
> `make undeploy` (below) removes every CRD, cascade-deleting all CRs. Use the
> Helm chart for per-feature CRD selection and uninstall-safety.

```bash
# 1. Delete resource CRs first
kubectl delete loadbalancers,loadbalancerpools,loadbalancermonitors --all -A --ignore-not-found
kubectl delete customhostnames --all -A

# 2. Delete the config CRs they reference
kubectl delete accounts --all -A --ignore-not-found
kubectl delete zones --all -A

# 3. Remove operator, CRDs, RBAC, and namespace
make undeploy
```

**Do not run `make undeploy` (or `helm uninstall`) before deleting the resource CRs.**
Removing the operator or CRDs first triggers cascading deletion of all CRs, but
CRs with finalizers cannot be processed once the operator is gone, so they block
CRD deletion. Recovery then requires manually patching finalizers off each stuck
CR.

### GitOps pruning

When an app that bundles both resource CRs and their Zone/Account/Secret is
pruned, deletion order is not guaranteed. If your GitOps tool supports deletion
ordering, put the operator, Zone/Account CRs, and the Secret in an *earlier* wave
than the resource CRs so they finalize while their credentials are still
reachable (with ArgoCD, `argocd.argoproj.io/sync-wave`). If they do wedge, the
recovery below is non-destructive.

The CRDs themselves carry `argocd.argoproj.io/sync-options: Prune=false` (from
`crds.keep=true`), so a prune never removes a CRD -- which would cascade-delete
every CR of that kind. Remove CRDs deliberately (see step 4 above), never via a
prune.

---

## Troubleshooting

### CR stuck in Terminating

A CustomHostname, LoadBalancer, LoadBalancerPool, or LoadBalancerMonitor CR sits
in `Terminating` and its finalizer never clears.

**Cause.** Releasing the finalizer requires the operator to delete the backing
Cloudflare resource first (for `deletePolicy: always`/`own-only`), which needs a
Cloudflare client. The client can't be built if any of these was removed before
the CR:

- the operator itself (nothing is reconciling the CR),
- the referenced Zone/Account CR, or
- the credentials Secret.

This is intentional: the operator will not release a CR while its Cloudflare
resource may still exist, so a CR is never removed out from under a live
Cloudflare resource. (`deletePolicy: never` and `managementPolicy: observe`
release the finalizer without a client and are never affected.)

**Recovery (non-destructive, self-healing).** Restore whatever was removed --
re-apply the Zone/Account CR, re-create the Secret, or re-deploy the operator.
The stuck CR is still being retried (controller-runtime backoff, capped at 30s),
so on the next attempt it builds a client, deletes the Cloudflare resource, and
clears its own finalizer. No manual finalizer editing is needed.

**Last resort.** If the Cloudflare resource is already gone and you just need the
CR removed, strip the finalizer manually (this leaves any surviving Cloudflare
resource orphaned):

```bash
kubectl patch <kind> <name> -n <ns> --type=merge -p '{"metadata":{"finalizers":[]}}'
```

---


## Production Checklist

Before going live:

- [ ] Zone CR `spec.id` matches the actual Cloudflare zone ID (verify in CF dashboard)
- [ ] Secret referenced by `spec.credentialsRef` exists and contains a valid API token with `Zone: Read` and `SSL and Certificates: Edit` permissions
- [ ] For a Zone used only as a `LoadBalancer.zoneRef` and backed by an LB-scoped token (one that cannot read `custom_hostnames`), set `spec.manageCustomHostnames: false` so the operator skips the CustomHostname drift pass and does not accrue `custom_hostnames` 403s / `cf_edge_operator_drift_detection_errors_total` noise
- [ ] `--management-policy` set appropriately -- use `create` during migration to prevent update loops (see [migration.md](migration.md))
- [ ] `--delete-policy` set appropriately -- use `own-only` or `never` during any migration window where another tool may manage the same zone (see [migration.md](migration.md))
- [ ] `replicaCount >= 2` and `leaderElect: true` for HA
- [ ] `podDisruptionBudget.enabled: true` if availability during node drains is required
- [ ] `serviceMonitor.enabled: true` and `prometheusRule.enabled: true` if using Prometheus Operator
- [ ] `prometheusRule.runbookUrl` set to point to your copy of [runbook.md](runbook.md)
- [ ] Resource limits reviewed for your scale (see sizing table above)
- [ ] CRD lifecycle understood: CRDs are chart-managed and update in place on `helm upgrade` (`crds.enabled=true`); they survive `helm uninstall`/prune (`crds.keep=true`). See [CRD Management](#crd-management), and adopt pre-existing CRDs first when upgrading from a pre-consolidation chart.
