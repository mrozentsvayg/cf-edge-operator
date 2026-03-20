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

// ZoneReconciler is the coordinator: validates zone credentials and orchestrates
// per-resource-type drift detection. Each resource type (custom hostnames, rulesets, etc.)
// has its own drift detection function in a separate file (zone_*_drift.go).

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	"github.com/cloudflare/cloudflare-go/v6/option"
	"github.com/cloudflare/cloudflare-go/v6/zones"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

// ZoneReconciler reconciles a Zone object.
// It acts as the coordinator: validates credentials, then delegates to per-resource-type
// drift detection functions (detectCustomHostnameDrift, etc.).
type ZoneReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// CustomHostnameEvents receives CRs that need reconciliation due to drift detected
	// during the bulk Cloudflare list. The CustomHostname controller watches this channel.
	CustomHostnameEvents chan<- event.GenericEvent
	// DryRun mirrors the operator-wide dry-run flag for logging purposes.
	DryRun bool
	// DriftInterval controls how often the zone controller bulk-lists Cloudflare
	// resources to detect external drift. Set via --drift-interval (default: 1m).
	DriftInterval time.Duration
}

// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones,verbs=get;list;watch
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones/finalizers,verbs=update
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *ZoneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, req.NamespacedName, &zone); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Zone deleted -- remove stale metric series.
			zoneReady.DeleteLabelValues(req.Name)
			customHostnames.DeletePartialMatch(prometheus.Labels{"zone_cr": req.Name})
			zoneCustomHostnames.DeletePartialMatch(prometheus.Labels{"zone_cr": req.Name})
			sslProvisioningDuration.DeletePartialMatch(prometheus.Labels{"zone_cr": req.Name})
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Fetch and validate API token
	apiToken, err := r.fetchAPIToken(ctx, &zone)
	if err != nil {
		log.Error(err, "failed to fetch API token")
		zoneReady.WithLabelValues(zone.Name).Set(0)
		// setReady is best-effort for observability; return the original error
		// so controller-runtime retries with backoff (capped at 30s).
		_ = r.setReady(ctx, &zone, metav1.ConditionFalse, "SecretError", err.Error())
		return ctrl.Result{}, err
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
		_ = r.setReady(ctx, &zone, metav1.ConditionFalse, "CloudflareError", err.Error())
		return ctrl.Result{}, err
	}
	zoneReady.WithLabelValues(zone.Name).Set(1)
	zone.Status.Name = zoneDetails.Name
	if err := r.setReady(ctx, &zone, metav1.ConditionTrue, "ZoneReady", "Zone credentials validated"); err != nil {
		return ctrl.Result{}, err
	}

	// Per-resource drift detection. Each resource type is in its own file
	// (zone_*_drift.go) and can fail independently without affecting others.
	// NOTE: Error is logged and counted (driftDetectionErrorsTotal) but not
	// returned -- the zone requeues on DriftInterval regardless, so controller-runtime
	// error tracking/backoff is unnecessary.
	// When multiple detectors exist, parallelize with errgroup.Group to avoid
	// sequential paginated API calls stretching the reconcile duration.
	if err := r.detectCustomHostnameDrift(ctx, cf, &zone); err != nil {
		log.Error(err, "drift detection failed", "zoneID", zone.Spec.ID)
		driftDetectionErrorsTotal.WithLabelValues(cfResourceCustomHostname).Inc()
	}

	return ctrl.Result{RequeueAfter: r.DriftInterval}, nil
}

// fetchAPIToken reads the CF API token from the secret referenced by the Zone CR.
// Similar secret-fetch logic exists in buildCloudflareClient (customhostname_controller.go).
// Not deduplicated: buildCloudflareClient additionally looks up the Zone CR and constructs the client.
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
		return fmt.Errorf("failed to update zone status: %w", err)
	}
	return nil
}

const zoneRefField = ".spec.zoneRef.name"

// SetupWithManager sets up the controller with the Manager.
func (r *ZoneReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(),
		&saasv1beta1.CustomHostname{}, zoneRefField,
		func(o client.Object) []string {
			ch := o.(*saasv1beta1.CustomHostname)
			return []string{ch.Spec.ZoneRef.Name}
		}); err != nil {
		return fmt.Errorf("failed to create zoneRef index: %w", err)
	}
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
