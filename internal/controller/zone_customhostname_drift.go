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

	// List all CustomHostname CRs referencing this zone (across all namespaces)
	var chList saasv1beta1.CustomHostnameList
	if err := r.List(ctx, &chList); err != nil {
		return fmt.Errorf("failed to list CustomHostname CRs: %w", err)
	}

	// Count CR states for this zone. States are mutually exclusive: conflict > ready > unhealthy > pending.
	// See crState() for the classification logic.
	stateCounts := map[string]int{"ready": 0, "pending": 0, "unhealthy": 0, "conflict": 0}

	// Enqueue CRs that have drifted from Cloudflare state, and build a set of
	// known CR hostnames for orphan detection in the next pass.
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
		} else if hasDrift(ch, cfCH) {
			if isHostnameConflict(ch) {
				// Another CR owns this hostname. Skip to avoid thrashing CF state back and forth.
				log.Info("skipping drift enqueue: hostname conflict", "hostname", ch.Spec.Hostname)
			} else {
				log.Info("drift detected, enqueuing CustomHostname", "hostname", ch.Spec.Hostname)
				r.sendDriftEvent(ctx, ch)
				drifted++
			}
		} else if r.DryRun {
			log.V(1).Info("dry-run: no changes needed", "hostname", ch.Spec.Hostname)
		}
	}

	// Publish per-zone CR state counts.
	for state, count := range stateCounts {
		customHostnames.WithLabelValues(zone.Status.Name, state).Set(float64(count))
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
	zoneCustomHostnames.WithLabelValues(zone.Status.Name, "managed").Set(float64(managedCount))
	zoneCustomHostnames.WithLabelValues(zone.Status.Name, "orphan").Set(float64(orphanCount))

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
		r.CustomHostnameEvents <- ev
	}
}

func (r *ZoneReconciler) listCloudflareHostnames(ctx context.Context, cf *cloudflare.Client, zoneID string) (map[string]custom_hostnames.CustomHostnameListResponse, error) {
	start := time.Now()
	result := make(map[string]custom_hostnames.CustomHostnameListResponse)
	pager := cf.CustomHostnames.ListAutoPaging(ctx, custom_hostnames.CustomHostnameListParams{
		ZoneID: cloudflare.F(zoneID),
	})
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

func hasDrift(ch *saasv1beta1.CustomHostname, cfCH custom_hostnames.CustomHostnameListResponse) bool {
	if cfCH.CustomOriginServer != ch.Spec.OriginServer {
		return true
	}
	if ch.Spec.OriginSNI != nil && cfCH.CustomOriginSNI != *ch.Spec.OriginSNI {
		return true
	}
	if sslDrifted(cfCH.SSL, ch.Spec.SSL) {
		return true
	}
	cfSSLStatus := string(cfCH.SSL.Status)
	crSSLStatus := ""
	if ch.Status.SSL != nil {
		crSSLStatus = ch.Status.SSL.Status
	}
	return cfSSLStatus != crSSLStatus
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
