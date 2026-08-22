#!/usr/bin/env bash
#
# Full Helm chart verification -- the SINGLE SOURCE OF TRUTH shared by the "Helm"
# CI job (.github/workflows/helm.yml) and local `make helm-verify`, so local == CI
# mechanically. Run from the repo root. Requires `helm` on PATH.
#
# Covers: helm lint, all-features render smokes, flag-rendering assertions,
# CRD-lifecycle (crds.enabled / crds.keep / per-feature gating) assertions, and
# the chart-CRDs-match-generated-CRDs diff.
#
# Assertions use here-strings (grep -q PATTERN <<<"$var") rather than
# `echo "$var" | grep -q` so that grep exiting early on a match never leaves echo
# writing to a closed pipe (the "write error: Broken pipe" noise). set -e aborts on
# a failed assertion (the `|| { echo FAIL; exit 1; }` / `&& { echo FAIL; exit 1; }`
# guards drive the exit explicitly).
set -e

C=charts/cf-edge-operator

echo "== helm lint =="
helm lint "$C"/

echo "== render smoke (all features) =="
helm template test "$C"/ \
  --set podDisruptionBudget.enabled=true \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.namespace=monitoring \
  --set prometheusRule.enabled=true \
  --set prometheusRule.namespace=monitoring \
  >/dev/null

echo "== render smoke (all features + runbook URL) =="
helm template test "$C"/ \
  --set podDisruptionBudget.enabled=true \
  --set serviceMonitor.enabled=true \
  --set prometheusRule.enabled=true \
  --set prometheusRule.runbookUrl=https://example.com/runbooks \
  >/dev/null

echo "== verify rendered flags match values =="
# Boolean flags: string and boolean values (ApplicationSet passes strings)
t=$(helm template test "$C"/ --set-string dryRun=false)
grep -q "\-\-dry-run" <<<"$t" && { echo "FAIL: --dry-run present with string false"; exit 1; }
t=$(helm template test "$C"/ --set-string dryRun=true)
grep -q "\-\-dry-run" <<<"$t" || { echo "FAIL: --dry-run absent with string true"; exit 1; }
t=$(helm template test "$C"/ --set dryRun=false)
grep -q "\-\-dry-run" <<<"$t" && { echo "FAIL: --dry-run present with bool false"; exit 1; }
t=$(helm template test "$C"/ --set dryRun=true)
grep -q "\-\-dry-run" <<<"$t" || { echo "FAIL: --dry-run absent with bool true"; exit 1; }
t=$(helm template test "$C"/ --set-string leaderElect=false)
grep -q "\-\-leader-elect$" <<<"$t" && { echo "FAIL: --leader-elect present with string false"; exit 1; }
t=$(helm template test "$C"/ --set-string leaderElect=true)
grep -q "\-\-leader-elect$" <<<"$t" || { echo "FAIL: --leader-elect absent with string true"; exit 1; }

# Operator flags render with correct values
out=$(helm template test "$C"/ \
  --set podLabels.export-logs=true \
  --set managementPolicy=create \
  --set deletePolicy=never \
  --set driftInterval=30s \
  --set cfAPITimeout=5s \
  --set cfAPIWriteTimeout=20s \
  --set cfAPIMaxRetries=2 \
  --set cfAPIBulkTimeout=10s \
  --set cfAPIBulkMaxRetries=1 \
  --set cfAPIWriteDelay=500ms \
  --set sslCertificateAuthority=lets_encrypt \
  --set sslMinTLSVersion=1.2 \
  --set zapDevel=false \
  --set zapLogLevel=2)
grep -q "\-\-management-policy=create"               <<<"$out" || { echo "FAIL: managementPolicy"; exit 1; }
grep -q "\-\-delete-policy=never"                    <<<"$out" || { echo "FAIL: deletePolicy"; exit 1; }
grep -q "\-\-drift-interval=30s"                     <<<"$out" || { echo "FAIL: driftInterval"; exit 1; }
grep -q "\-\-cf-api-timeout=5s"                      <<<"$out" || { echo "FAIL: cfAPITimeout"; exit 1; }
grep -q "\-\-cf-api-write-timeout=20s"              <<<"$out" || { echo "FAIL: cfAPIWriteTimeout"; exit 1; }
grep -q "\-\-cf-api-max-retries=2"                   <<<"$out" || { echo "FAIL: cfAPIMaxRetries"; exit 1; }
grep -q "\-\-cf-api-bulk-timeout=10s"               <<<"$out" || { echo "FAIL: cfAPIBulkTimeout"; exit 1; }
grep -q "\-\-cf-api-bulk-max-retries=1"             <<<"$out" || { echo "FAIL: cfAPIBulkMaxRetries"; exit 1; }
grep -q "\-\-cf-api-write-delay=500ms"              <<<"$out" || { echo "FAIL: cfAPIWriteDelay"; exit 1; }
grep -q "\-\-ssl-certificate-authority=lets_encrypt" <<<"$out" || { echo "FAIL: sslCertificateAuthority"; exit 1; }
grep -q "\-\-ssl-min-tls-version=1.2"               <<<"$out" || { echo "FAIL: sslMinTLSVersion"; exit 1; }
grep -q "\-\-zap-devel=false"                        <<<"$out" || { echo "FAIL: zapDevel"; exit 1; }
grep -q "\-\-zap-log-level=2"                        <<<"$out" || { echo "FAIL: zapLogLevel"; exit 1; }
grep -q "export-logs"                                <<<"$out" || { echo "FAIL: podLabels"; exit 1; }

# Optional flags absent when empty (defaults). Match the "=" (flag) form:
# crds.yaml renders CRDs into `helm template` output, and some CRD field
# descriptions name operator flags (e.g. "use --ssl-method operator default"),
# so a bare "--ssl-method" grep would match description prose. A real rendered
# flag is always "--<name>=<value>", so "=" scopes the check to actual flags.
out=$(helm template test "$C"/)
grep -q "\-\-ssl-certificate-authority=" <<<"$out" && { echo "FAIL: sslCA should be absent by default"; exit 1; }
grep -q "\-\-ssl-min-tls-version="       <<<"$out" && { echo "FAIL: sslMinTLS should be absent by default"; exit 1; }
grep -q "\-\-ssl-method="                <<<"$out" && { echo "FAIL: sslMethod should be absent by default"; exit 1; }
grep -q "\-\-ssl-type="                  <<<"$out" && { echo "FAIL: sslType should be absent by default"; exit 1; }
grep -q "\-\-zap-log-level="             <<<"$out" && { echo "FAIL: zapLogLevel should be absent by default"; exit 1; }
grep -q "export-logs"                    <<<"$out" && { echo "FAIL: podLabels should be absent by default"; exit 1; }

# Feature gating. Both flags always render as --enable-<feature>=<bool>.
# Defaults: customhostname on, loadBalancing off. CRDs render as regular
# templates (crds.yaml), so no --include-crds is needed.
def=$(helm template test "$C"/)
grep -q "\-\-enable-customhostname=true" <<<"$def" || { echo "FAIL: --enable-customhostname=true absent by default"; exit 1; }
grep -q "\-\-enable-loadbalancing=false" <<<"$def" || { echo "FAIL: --enable-loadbalancing=false absent by default"; exit 1; }
grep -q "resources: \[customhostnames\]" <<<"$def" || { echo "FAIL: CH RBAC absent by default"; exit 1; }
grep -q "resources: \[zones\]"           <<<"$def" || { echo "FAIL: Zone RBAC absent by default"; exit 1; }
grep -q "loadbalancing.cf-edge.io"       <<<"$def" && { echo "FAIL: loadbalancing RBAC/CRDs present by default"; exit 1; }
grep -q "accounts.cf-edge.io"            <<<"$def" && { echo "FAIL: accounts RBAC/CRDs present by default"; exit 1; }
grep -q "events.k8s.io"                  <<<"$def" && { echo "FAIL: events.k8s.io RBAC present by default (should be LB-gated)"; exit 1; }
# features.loadBalancing.enabled=true adds the flag value, LB RBAC, and the
# LB CRDs (Account lives in its own accounts.cf-edge.io group, LoadBalancer/
# Pool/Monitor in loadbalancing.cf-edge.io).
cp=$(helm template test "$C"/ --set features.loadBalancing.enabled=true)
grep -q "\-\-enable-loadbalancing=true"      <<<"$cp" || { echo "FAIL: --enable-loadbalancing=true absent when loadBalancing enabled"; exit 1; }
grep -q "resources: \[accounts\]"            <<<"$cp" || { echo "FAIL: LB RBAC absent when loadBalancing enabled"; exit 1; }
grep -q "name: accounts.accounts.cf-edge.io" <<<"$cp" || { echo "FAIL: accounts CRD absent when loadBalancing enabled"; exit 1; }
n=$(grep -cE "name: (loadbalancers|loadbalancerpools|loadbalancermonitors)\.loadbalancing\.cf-edge\.io" <<<"$cp")
[ "$n" = "3" ] || { echo "FAIL: expected 3 loadbalancing CRDs when loadBalancing enabled, got $n"; exit 1; }
# The LB controller emits Events via events.k8s.io/v1 (mgr.GetEventRecorder), so the
# manager ClusterRole must grant events.k8s.io when loadBalancing is enabled.
grep -q "apiGroups: \[events.k8s.io\]" <<<"$cp" || { echo "FAIL: events.k8s.io RBAC absent when loadBalancing enabled"; exit 1; }
# customhostname disabled omits the CH flag value and CH RBAC; with LB also
# off, the shared Zone RBAC is omitted too.
choff=$(helm template test "$C"/ --set features.customhostname.enabled=false)
grep -q "\-\-enable-customhostname=false" <<<"$choff" || { echo "FAIL: --enable-customhostname=false absent when CH disabled"; exit 1; }
grep -q "resources: \[customhostnames\]"  <<<"$choff" && { echo "FAIL: CH RBAC present when CH disabled"; exit 1; }
grep -q "resources: \[zones\]"            <<<"$choff" && { echo "FAIL: Zone RBAC present when both CH and LB disabled"; exit 1; }
# Zone RBAC returns when LB alone is enabled (Zone is shared substrate).
lbonly=$(helm template test "$C"/ --set features.customhostname.enabled=false --set features.loadBalancing.enabled=true)
grep -q "resources: \[customhostnames\]" <<<"$lbonly" && { echo "FAIL: CH RBAC present when CH disabled (LB on)"; exit 1; }
grep -q "resources: \[zones\]"           <<<"$lbonly" || { echo "FAIL: Zone RBAC absent when LB enabled (Zone is shared)"; exit 1; }
echo "All flag rendering tests passed"

echo "== verify CRD lifecycle (crds.enabled / crds.keep / per-feature gating) =="
crd_count() { grep -cE "^  name: \S+\.cf-edge\.io" || true; }
# Defaults (crds.enabled, CH on, LB off): CustomHostname + Zone only.
out=$(helm template test "$C"/)
n=$(crd_count <<<"$out"); [ "$n" = "2" ] || { echo "FAIL: expected 2 CRDs by default, got $n"; exit 1; }
grep -q "^  name: customhostnames.saas.cf-edge.io" <<<"$out" || { echo "FAIL: CH CRD absent by default"; exit 1; }
grep -q "^  name: zones.domains.cf-edge.io"        <<<"$out" || { echo "FAIL: Zone CRD absent by default"; exit 1; }
grep -q "loadbalancing.cf-edge.io"                 <<<"$out" && { echo "FAIL: LB CRDs present by default"; exit 1; }
# keep=true (default) stamps resource-policy:keep + argocd Prune=false on each CRD.
k=$(grep -c "helm.sh/resource-policy: keep" <<<"$out" || true); [ "$k" = "2" ] || { echo "FAIL: expected 2 keep annotations, got $k"; exit 1; }
p=$(grep -c "argocd.argoproj.io/sync-options: Prune=false" <<<"$out" || true); [ "$p" = "2" ] || { echo "FAIL: expected 2 Prune annotations, got $p"; exit 1; }
# CH off + LB on: Zone (shared) + Account + 3 LB CRDs, no CustomHostname.
out=$(helm template test "$C"/ --set features.customhostname.enabled=false --set features.loadBalancing.enabled=true)
n=$(crd_count <<<"$out"); [ "$n" = "5" ] || { echo "FAIL: expected 5 CRDs (Zone+Account+3 LB), got $n"; exit 1; }
grep -q "^  name: customhostnames.saas.cf-edge.io" <<<"$out" && { echo "FAIL: CH CRD present when CH disabled"; exit 1; }
grep -q "^  name: zones.domains.cf-edge.io"        <<<"$out" || { echo "FAIL: Zone CRD absent when LB enabled (shared substrate)"; exit 1; }
grep -q "^  name: accounts.accounts.cf-edge.io"    <<<"$out" || { echo "FAIL: Account CRD absent when LB enabled"; exit 1; }
# Both features on: all 6 CRDs, each stamped with the keep + Prune annotations.
# The keep/Prune counts must equal the CRD count so a missed annotation splice
# (e.g. a future CRD generated without a metadata.annotations block) fails CI.
out=$(helm template test "$C"/ --set features.customhostname.enabled=true --set features.loadBalancing.enabled=true)
n=$(crd_count <<<"$out"); [ "$n" = "6" ] || { echo "FAIL: expected 6 CRDs with both features on, got $n"; exit 1; }
k=$(grep -c "helm.sh/resource-policy: keep" <<<"$out" || true); [ "$k" = "6" ] || { echo "FAIL: expected 6 keep annotations (one per CRD) with both features on, got $k"; exit 1; }
p=$(grep -c "argocd.argoproj.io/sync-options: Prune=false" <<<"$out" || true); [ "$p" = "6" ] || { echo "FAIL: expected 6 Prune annotations (one per CRD) with both features on, got $p"; exit 1; }
# Both features off: no CRDs (Zone is dropped too).
out=$(helm template test "$C"/ --set features.customhostname.enabled=false --set features.loadBalancing.enabled=false)
n=$(crd_count <<<"$out"); [ "$n" = "0" ] || { echo "FAIL: expected 0 CRDs with both features off, got $n"; exit 1; }
# crds.enabled=false: out-of-band mode emits no CRDs even with features on.
out=$(helm template test "$C"/ --set crds.enabled=false --set features.customhostname.enabled=true --set features.loadBalancing.enabled=true)
n=$(crd_count <<<"$out"); [ "$n" = "0" ] || { echo "FAIL: expected 0 CRDs with crds.enabled=false, got $n"; exit 1; }
# crds.keep=false: CRDs render without the keep/Prune annotations.
out=$(helm template test "$C"/ --set crds.keep=false)
n=$(crd_count <<<"$out"); [ "$n" = "2" ] || { echo "FAIL: expected 2 CRDs with crds.keep=false, got $n"; exit 1; }
grep -q "helm.sh/resource-policy: keep"                 <<<"$out" && { echo "FAIL: keep annotation present with crds.keep=false"; exit 1; }
grep -q "argocd.argoproj.io/sync-options: Prune=false" <<<"$out" && { echo "FAIL: Prune annotation present with crds.keep=false"; exit 1; }
echo "All CRD lifecycle tests passed"

echo "== verify chart CRDs match generated CRDs =="
# All CRDs are chart-rendered from crds-render/ (a plain data dir loaded via
# .Files.Glob and emitted per-feature by templates/crds.yaml). The wrapper emits
# each schema body verbatim (crds.keep only splices annotations under metadata),
# so the data files must byte-match the generated bases.
diff -rq config/crd/bases/ "$C"/crds-render/

echo "All Helm verification passed"
