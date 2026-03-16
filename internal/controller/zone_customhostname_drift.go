/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

// CustomHostname-specific drift detection for the zone controller.
// Each Cloudflare resource type (custom hostnames, rulesets, etc.) has its own
// drift detection in a separate file, keeping the zone controller as a thin
// orchestrator that handles credential validation and scheduling.

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

// detectCustomHostnameDrift bulk-lists Cloudflare custom hostnames for the given zone,
// compares them against CustomHostname CRs, and enqueues drifted CRs for the
// CustomHostname worker controller. Also publishes per-zone CR state and orphan metrics.
func (r *ZoneReconciler) detectCustomHostnameDrift(ctx context.Context, cf *cloudflare.Client, zone *domainsv1beta1.Zone) error {
	log := logf.FromContext(ctx)

	cfHostnames, err := r.listCloudflareHostnames(ctx, cf, zone.Spec.ID)
	if err != nil {
		log.Error(err, "failed to list Cloudflare custom hostnames", "zoneID", zone.Spec.ID)
		return err
	}

	// List CustomHostname CRs referencing this zone (indexed by spec.zoneRef.name)
	var chList saasv1beta1.CustomHostnameList
	if err := r.List(ctx, &chList, client.MatchingFields{zoneRefField: zone.Name}); err != nil {
		return fmt.Errorf("failed to list CustomHostname CRs: %w", err)
	}

	// Count CR states for this zone. States are mutually exclusive: conflict > ready > unhealthy > pending.
	// See crState() for the classification logic.
	stateCounts := map[string]int{"ready": 0, "pending": 0, "unhealthy": 0, "conflict": 0}

	// Enqueue CRs that have drifted from Cloudflare state, and build a set of
	// known CR hostnames for orphan detection in the next pass.
	// All CRs are evaluated regardless of managementPolicy — observe-mode CRs
	// need enqueue to refresh status from CF, create-mode CRs need it to detect
	// missing hostnames. The CH controller handles policy-specific behavior.
	crHostnames := make(map[string]bool, len(chList.Items))
	drifted := 0
	for i := range chList.Items {
		ch := &chList.Items[i]
		if !r.refersToZone(ch, zone) {
			continue
		}
		stateCounts[crState(ch)]++
		crHostnames[ch.Spec.Hostname] = true
		cfCH, exists := cfHostnames[ch.Spec.Hostname]
		if !exists {
			// Hostname missing from CF: always enqueue, even if the CR previously had a
			// HostnameConflict condition. This is the self-healing path: when the owning CR
			// is deleted and CF removes the hostname, the previously-conflicted CR can now
			// provision it, clearing the conflict condition on its next successful reconcile.
			log.Info("hostname missing from CF, enqueuing", "hostname", ch.Spec.Hostname)
			r.sendDriftEvent(ctx, ch)
			drifted++
		} else if cfCH.CustomOriginServer != ch.Spec.OriginServer || sniDrifted(cfCH.CustomOriginSNI, ch) || sslDrifted(cfCH.SSL, ch.Spec.SSL) {
			if isHostnameConflict(ch) {
				log.Info("skipping drift enqueue: hostname conflict", "hostname", ch.Spec.Hostname)
			} else {
				log.Info("drift detected, enqueuing CustomHostname", "hostname", ch.Spec.Hostname, "reason", "spec")
				r.sendDriftEvent(ctx, ch)
				drifted++
			}
		} else if changed := sslStatusChangedFields(ch.Status.SSL, cfCH.SSL); changed != nil {
			log.Info("drift detected, enqueuing CustomHostname", "hostname", ch.Spec.Hostname, "reason", "statusSSL", "changed", changed)
			r.sendDriftEvent(ctx, ch)
			drifted++
		} else if r.DryRun {
			log.V(1).Info("dry-run: no changes needed", "hostname", ch.Spec.Hostname)
		}
	}

	// Publish per-zone CR state counts.
	for state, count := range stateCounts {
		customHostnames.WithLabelValues(zone.Name, state).Set(float64(count))
	}

	// Log orphans: custom hostnames in Cloudflare with no corresponding CR.
	// Visible at --zap-log-level=1 (debug). Useful for auditing manual
	// Cloudflare changes or planning migration to CRs.
	managedCount, orphanCount := 0, 0
	for hostname, cfCH := range cfHostnames {
		if crHostnames[hostname] {
			managedCount++
		} else {
			log.V(1).Info("orphan: custom hostname in Cloudflare has no CR",
				"hostname", hostname, "origin", cfCH.CustomOriginServer)
			orphanCount++
		}
	}
	zoneCustomHostnames.WithLabelValues(zone.Name, "managed").Set(float64(managedCount))
	zoneCustomHostnames.WithLabelValues(zone.Name, "orphan").Set(float64(orphanCount))
	zoneCustomHostnames.WithLabelValues(zone.Name, "drifted").Set(float64(drifted))
	zoneCustomHostnames.WithLabelValues(zone.Name, "total").Set(float64(len(cfHostnames)))

	driftBufferDepth.WithLabelValues(cfResourceCustomHostname).Set(float64(len(r.CustomHostnameEvents)))

	if drifted > 0 {
		log.Info("drift detection complete", "zoneID", zone.Spec.ID, "drifted", drifted, "total", len(cfHostnames))
	} else {
		log.V(1).Info("drift detection complete", "zoneID", zone.Spec.ID, "drifted", 0, "total", len(cfHostnames))
	}

	return nil
}

// sendDriftEvent sends a drift event to the CustomHostname controller via the shared channel.
// Uses a non-blocking attempt first to detect and log buffer overflow, then blocks if needed.
func (r *ZoneReconciler) sendDriftEvent(ctx context.Context, ch *saasv1beta1.CustomHostname) {
	ev := event.GenericEvent{Object: ch}
	select {
	case r.CustomHostnameEvents <- ev:
	default:
		driftBufferOverflowTotal.WithLabelValues(cfResourceCustomHostname).Inc()
		logf.FromContext(ctx).Info("drift buffer full, blocking", "hostname", ch.Spec.Hostname)
		select {
		case r.CustomHostnameEvents <- ev:
		case <-ctx.Done():
			logf.FromContext(ctx).Info("drift event dropped due to shutdown", "hostname", ch.Spec.Hostname)
		}
	}
}

func (r *ZoneReconciler) listCloudflareHostnames(ctx context.Context, cf *cloudflare.Client, zoneID string) (map[string]custom_hostnames.CustomHostnameListResponse, error) {
	start := time.Now()
	result := make(map[string]custom_hostnames.CustomHostnameListResponse)
	pager := cf.CustomHostnames.ListAutoPaging(ctx, custom_hostnames.CustomHostnameListParams{
		ZoneID: cloudflare.F(zoneID),
	})
	// NOTE: Map keyed by hostname — if CF returns duplicates (edge case with pending
	// deletions), the later entry wins. CF deduplicates hostnames in practice.
	for pager.Next() {
		ch := pager.Current()
		result[ch.Hostname] = ch
	}
	err := pager.Err()
	recordCFCall(cfResourceCustomHostname, cfOpList, start, &err)
	return result, err
}

func (r *ZoneReconciler) refersToZone(ch *saasv1beta1.CustomHostname, zone *domainsv1beta1.Zone) bool {
	if ch.Spec.ZoneRef.Name != zone.Name {
		return false
	}
	ns := ch.Spec.ZoneRef.Namespace
	return ns == "" || ns == zone.Namespace
}

// statusPair is a status/cf value pair for structured status-refresh logging.
type statusPair struct {
	Status string `json:"status"`
	CF     string `json:"cf"`
}

// sslStatusChangedFields returns a structured map of SSL fields that differ
// between status.ssl and the CF response. Nil map means no changes.
// Field order: CA, minTLS, method, type, sslStatus, id, issuer, serialNumber,
// bundleMethod, wildcard, uploadedOn, expiresOn.
// NOTE: Hosts, ValidationRecords, and ValidationErrors are intentionally excluded —
// they change alongside Status during provisioning and are refreshed when the CR
// is enqueued for any other reason.
func sslStatusChangedFields(status *saasv1beta1.CustomHostnameSSLStatus, cfSSL custom_hostnames.CustomHostnameListResponseSSL) map[string]statusPair {
	if status == nil {
		if string(cfSSL.Status) != "" {
			return map[string]statusPair{"status": {Status: "", CF: string(cfSSL.Status)}}
		}
		return nil
	}
	changed := map[string]statusPair{}
	if status.CertificateAuthority != string(cfSSL.CertificateAuthority) {
		changed["ca"] = statusPair{Status: status.CertificateAuthority, CF: string(cfSSL.CertificateAuthority)}
	}
	if status.MinTLSVersion != string(cfSSL.Settings.MinTLSVersion) {
		changed["minTLS"] = statusPair{Status: status.MinTLSVersion, CF: string(cfSSL.Settings.MinTLSVersion)}
	}
	if status.Method != string(cfSSL.Method) {
		changed["method"] = statusPair{Status: status.Method, CF: string(cfSSL.Method)}
	}
	if status.Type != string(cfSSL.Type) {
		changed["type"] = statusPair{Status: status.Type, CF: string(cfSSL.Type)}
	}
	if status.Status != string(cfSSL.Status) {
		changed["sslStatus"] = statusPair{Status: status.Status, CF: string(cfSSL.Status)}
	}
	if status.ID != cfSSL.ID {
		changed["id"] = statusPair{Status: status.ID, CF: cfSSL.ID}
	}
	if status.Issuer != cfSSL.Issuer {
		changed["issuer"] = statusPair{Status: status.Issuer, CF: cfSSL.Issuer}
	}
	if status.SerialNumber != cfSSL.SerialNumber {
		changed["serialNumber"] = statusPair{Status: status.SerialNumber, CF: cfSSL.SerialNumber}
	}
	if status.BundleMethod != string(cfSSL.BundleMethod) {
		changed["bundleMethod"] = statusPair{Status: status.BundleMethod, CF: string(cfSSL.BundleMethod)}
	}
	if status.Wildcard != cfSSL.Wildcard {
		changed["wildcard"] = statusPair{Status: fmt.Sprintf("%v", status.Wildcard), CF: fmt.Sprintf("%v", cfSSL.Wildcard)}
	}
	if !timePtrEqual(status.UploadedOn, cfSSL.UploadedOn) {
		changed["uploadedOn"] = statusPair{Status: timePtrString(status.UploadedOn), CF: timeString(cfSSL.UploadedOn)}
	}
	if !timePtrEqual(status.ExpiresOn, cfSSL.ExpiresOn) {
		changed["expiresOn"] = statusPair{Status: timePtrString(status.ExpiresOn), CF: timeString(cfSSL.ExpiresOn)}
	}
	if len(changed) == 0 {
		return nil
	}
	return changed
}

func timePtrString(t *metav1.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}

func timeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func timePtrEqual(mt *metav1.Time, gt time.Time) bool {
	if gt.IsZero() {
		return mt == nil
	}
	if mt == nil {
		return false
	}
	return mt.Time.Equal(gt)
}

// crState classifies a CustomHostname CR into one of four mutually exclusive states
// used for the customHostnames gauge. Priority: conflict > ready > unhealthy > pending.
// ready is checked before unhealthy: Ready=True means CF confirmed the hostname active,
// which is more authoritative than the operator-side error counter.
func crState(ch *saasv1beta1.CustomHostname) string {
	if isHostnameConflict(ch) {
		return "conflict"
	}
	for _, cond := range ch.Status.Conditions {
		if cond.Type == conditionReady && cond.Status == metav1.ConditionTrue {
			return "ready"
		}
	}
	if ch.Status.ConsecutiveErrors > 0 {
		return "unhealthy"
	}
	return "pending"
}

// isHostnameConflict reports whether the CR has been marked as a duplicate hostname conflict.
// Such CRs are skipped by drift detection when the hostname already exists in CF,
// preventing back-and-forth updates between two CRs claiming the same hostname.
func isHostnameConflict(ch *saasv1beta1.CustomHostname) bool {
	for _, cond := range ch.Status.Conditions {
		if cond.Type == conditionReady && cond.Reason == reasonHostnameConflict {
			return true
		}
	}
	return false
}
