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

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	"github.com/cloudflare/cloudflare-go/v6/option"
	"github.com/cloudflare/cloudflare-go/v6/zones"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

const driftDetectionInterval = 5 * time.Minute

// ZoneReconciler reconciles a Zone object.
// It acts as the coordinator: periodically bulk-lists Cloudflare custom hostnames
// and enqueues drifted CustomHostname CRs for the worker (CustomHostnameReconciler).
type ZoneReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// CustomHostnameEvents receives CRs that need reconciliation due to drift detected
	// during the bulk Cloudflare list. The CustomHostname controller watches this channel.
	CustomHostnameEvents chan<- event.GenericEvent
	// DryRun mirrors the operator-wide dry-run flag for logging purposes.
	DryRun bool
}

// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones/finalizers,verbs=update
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *ZoneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, req.NamespacedName, &zone); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Fetch and validate API token
	apiToken, err := r.fetchAPIToken(ctx, &zone)
	if err != nil {
		log.Error(err, "failed to fetch API token")
		zoneReady.WithLabelValues(zone.Name).Set(0)
		return ctrl.Result{}, r.setReady(ctx, &zone, metav1.ConditionFalse, "SecretError", err.Error())
	}

	cf := cloudflare.NewClient(option.WithAPIToken(apiToken))

	// Validate credentials and populate zone name
	zoneGetStart := time.Now()
	zoneDetails, err := cf.Zones.Get(ctx, zones.ZoneGetParams{
		ZoneID: cloudflare.F(zone.Spec.ID),
	})
	recordCFCall(cfResourceZone, cfOpGet, zoneGetStart, &err)
	if err != nil {
		log.Error(err, "failed to fetch zone from Cloudflare", "zoneID", zone.Spec.ID)
		zoneReady.WithLabelValues(zone.Name).Set(0)
		return ctrl.Result{}, r.setReady(ctx, &zone, metav1.ConditionFalse, "CloudflareError", err.Error())
	}
	zoneReady.WithLabelValues(zone.Name).Set(1)
	zone.Status.Name = zoneDetails.Name
	if err := r.Status().Update(ctx, &zone); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update zone status: %w", err)
	}
	if err := r.setReady(ctx, &zone, metav1.ConditionTrue, "ZoneReady", "Zone credentials validated"); err != nil {
		return ctrl.Result{}, err
	}

	// Bulk drift detection: list all Cloudflare custom hostnames for this zone
	cfHostnames, err := r.listCloudflareHostnames(ctx, cf, zone.Spec.ID)
	if err != nil {
		log.Error(err, "failed to list Cloudflare custom hostnames", "zoneID", zone.Spec.ID)
		return ctrl.Result{RequeueAfter: driftDetectionInterval}, nil
	}

	// List all CustomHostname CRs referencing this zone (across all namespaces)
	var chList saasv1beta1.CustomHostnameList
	if err := r.List(ctx, &chList); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list CustomHostname CRs: %w", err)
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
		if !r.refersToZone(ch, &zone) {
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
			r.CustomHostnameEvents <- event.GenericEvent{Object: ch}
			drifted++
		} else if hasDrift(ch, cfCH) {
			if isHostnameConflict(ch) {
				// Another CR owns this hostname. Skip to avoid thrashing CF state back and forth.
				log.Info("skipping drift enqueue: hostname conflict", "hostname", ch.Spec.Hostname)
			} else {
				log.Info("drift detected, enqueuing CustomHostname", "hostname", ch.Spec.Hostname)
				r.CustomHostnameEvents <- event.GenericEvent{Object: ch}
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

	if drifted > 0 {
		log.Info("drift detection complete", "zoneID", zone.Spec.ID, "drifted", drifted, "total", len(cfHostnames))
	}

	return ctrl.Result{RequeueAfter: driftDetectionInterval}, nil
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
	return false
}

func (r *ZoneReconciler) fetchAPIToken(ctx context.Context, zone *domainsv1beta1.Zone) (string, error) {
	key := zone.Spec.CredentialsRef.Key
	if key == "" {
		key = "apiToken"
	}
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{
		Name:      zone.Spec.CredentialsRef.Name,
		Namespace: zone.Namespace,
	}, &secret); err != nil {
		return "", fmt.Errorf("secret %q not found: %w", zone.Spec.CredentialsRef.Name, err)
	}
	token, ok := secret.Data[key]
	if !ok {
		return "", fmt.Errorf("key %q not found in secret %q", key, zone.Spec.CredentialsRef.Name)
	}
	return string(token), nil
}

func (r *ZoneReconciler) setReady(ctx context.Context, zone *domainsv1beta1.Zone, status metav1.ConditionStatus, reason, message string) error {
	meta.SetStatusCondition(&zone.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: zone.Generation,
	})
	if err := r.Status().Update(ctx, zone); err != nil {
		return fmt.Errorf("failed to update zone conditions: %w", err)
	}
	return nil
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

// SetupWithManager sets up the controller with the Manager.
func (r *ZoneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&domainsv1beta1.Zone{}).
		Named("zone").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}
