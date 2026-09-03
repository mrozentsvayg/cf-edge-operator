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
	// EnableCustomHostname gates the custom-hostname-specific work in this controller.
	// The Zone controller always validates credentials and resolves the zone ID (needed
	// by both custom hostname management and load balancing), but it performs the custom
	// hostname drift bulk-list -- and enqueues drifted CustomHostname CRs -- only when
	// custom hostname management is enabled. A pure load-balancing control cluster runs
	// the Zone controller with this false, so it never lists custom hostnames from
	// Cloudflare. Set from --enable-customhostname.
	EnableCustomHostname bool
	// CustomHostnameEvents receives CRs that need reconciliation due to drift detected
	// during the bulk Cloudflare list. The CustomHostname controller watches this channel.
	// Nil when EnableCustomHostname is false (no custom hostname drift is performed).
	CustomHostnameEvents chan<- event.GenericEvent
	// DryRun mirrors the operator-wide dry-run flag for logging purposes.
	DryRun bool
	// DriftInterval controls how often the zone controller bulk-lists Cloudflare
	// resources to detect external drift. Set via --drift-interval (default: 1m).
	DriftInterval time.Duration
	// CFAPITimeout is the per-request timeout for single Cloudflare API calls.
	// Set via --cf-api-timeout (default: 5s).
	CFAPITimeout time.Duration
	// CFAPIMaxRetries is the maximum number of retries for single CF API calls.
	// Set via --cf-api-max-retries (default: 1). Uses our retry loop (immediate, no backoff).
	CFAPIMaxRetries int
	// CFAPIBulkTimeout is the per-page timeout for paginated CF API calls (bulk drift list).
	// Set via --cf-api-bulk-timeout (default: 5s).
	CFAPIBulkTimeout time.Duration
	// CFAPIBulkMaxRetries is the maximum number of per-page retries for paginated CF API calls.
	// Set via --cf-api-bulk-max-retries (default: 0). Uses SDK retry (per-page, ~2s backoff).
	CFAPIBulkMaxRetries int
	// CFBaseURL overrides the Cloudflare API base URL (for integration tests).
	CFBaseURL string
}

// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones,verbs=get;list;watch
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones/finalizers,verbs=update
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// conditionInitialized is the credentials-validated condition type, shared by Zone and Account.
const conditionInitialized = "Initialized"

// defaultAPITokenKey is the default key looked up inside a credentials Secret when a
// Zone or Account CR leaves credentialsRef.key empty -- the fallback used wherever the
// operator builds a Cloudflare client (CustomHostname via a Zone; Account/Pool/Monitor
// via an Account).
const defaultAPITokenKey = "apiToken"

func (r *ZoneReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, req.NamespacedName, &zone); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Zone deleted -- remove stale metric series.
			zoneInitialized.DeleteLabelValues(req.Name)
			clearZoneCustomHostnameMetrics(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Ensure metric series exists so the CfEdgeOperatorZoneNotInitialized alert
	// (expr: == 0) can match before the first successful zone GET.
	if meta.FindStatusCondition(zone.Status.Conditions, conditionInitialized) == nil {
		zoneInitialized.WithLabelValues(zone.Name).Set(0)
	} else {
		zoneInitialized.WithLabelValues(zone.Name).Set(1)
	}

	// Fetch API token every cycle -- fast local K8s call, and the secret could
	// be rotated at any time. Do not touch the Initialized condition on failure:
	// a missing secret is a transient issue, not a zone identity problem.
	apiToken, err := r.fetchAPIToken(ctx, &zone)
	if err != nil {
		log.Error(err, "zone - API token fetch failed")
		return ctrl.Result{}, err
	}

	// Initialize zone on first reconcile, on spec change (credentialsRef rotation),
	// or when the Initialized condition is missing.
	// Capture initRan BEFORE initializeZone: it advances the Initialized condition's
	// ObservedGeneration (via setInitialized), so needsInit would read false afterward.
	// initRan is true on the first reconcile and on the reconcile after any spec change
	// (generation bump) -- i.e. on a discrete transition, which the unmanaged-zone
	// cleanup below keys off of.
	var cf *cloudflare.Client
	initRan := r.needsInit(&zone)
	if initRan {
		var err error
		cf, err = r.initializeZone(ctx, &zone, apiToken)
		if err != nil {
			return ctrl.Result{}, err
		}
		// Fall through to drift detection -- first cycle should also detect drift.
	} else {
		cf = r.buildCFClient(apiToken)
	}

	// Custom hostname drift detection (custom-hostname-specific). Each resource type
	// lives in its own file (zone_*_drift.go) and can fail independently.
	// NOTE: Error is logged and counted (driftDetectionErrorsTotal) inside
	// detectCustomHostnameDrift -- not returned. The zone requeues on DriftInterval
	// regardless, so controller-runtime error tracking/backoff is unnecessary.
	//
	// Gated on two axes:
	//   - operator-wide (r.EnableCustomHostname / --enable-customhostname): a pure
	//     load-balancing control cluster runs the zone controller only as shared
	//     credential/zone-identity substrate and must never list custom hostnames.
	//   - per-zone (spec.manageCustomHostnames): a Zone that exists only to back a
	//     LoadBalancer.zoneRef -- with an LB-scoped token that cannot read
	//     custom_hostnames -- sets manageCustomHostnames=false so the operator skips the
	//     drift bulk-list for that one zone, avoiding recurring custom_hostnames 403s.
	// Zone initialization above is unaffected either way: load balancing still needs
	// status.name / the resolved zone ID.
	if r.EnableCustomHostname {
		if manageCustomHostnames(&zone) {
			if err := r.detectCustomHostnameDrift(ctx, cf, &zone); err != nil {
				log.V(1).Info("custom hostname - drift detection failed", "zoneID", zone.Spec.ID, "reason", err)
			}
		} else if initRan {
			// Zone opted out of custom hostname management. Do the one-time cleanup only
			// on a discrete transition -- initRan is true on the first reconcile and on the
			// reconcile after a spec flip (both bump generation), never on steady-state
			// cycles -- so the else path stays silent and does no work, matching the
			// operator-wide EnableCustomHostname=false path. A soft-fail on the 403 was
			// rejected: it would still call CF every cycle and mask real auth failures on
			// CH-managing zones. The three drift gauges are set only on the managed branch
			// above, so a managed->unmanaged flip is the only way stale series arise, and it
			// always coincides with an initRan cycle; once cleared they cannot reappear
			// while the zone stays unmanaged.
			log.V(1).Info("custom hostname - management disabled for zone (spec.manageCustomHostnames=false), skipping drift", "zone", zone.Name)
			clearZoneCustomHostnameMetrics(zone.Name)
		}

		// Clean up expired SSL provisioning gauge entries once per reconcile, on both
		// branches. The CustomHostname controller sets sslProvisioningDuration without
		// consulting manageCustomHostnames, so an unmanaged zone can still accrue an SSL
		// series out-of-band; running the TTL cleaner here (not only on the managed
		// branch) bounds the per-hostname gauge even for an all-unmanaged operator.
		cleanExpiredSSLProvisioning()
	}

	return ctrl.Result{RequeueAfter: r.DriftInterval}, nil
}

// manageCustomHostnames reports whether the operator should manage CustomHostnames
// for this zone. A nil spec.manageCustomHostnames (field unset) is treated as true
// for back-compat, matching the CRD default.
func manageCustomHostnames(zone *domainsv1beta1.Zone) bool {
	return zone.Spec.ManageCustomHostnames == nil || *zone.Spec.ManageCustomHostnames
}

// needsInit returns true if the zone requires initialization (zone GET to resolve
// the zone name from Cloudflare). This happens on first reconcile, after operator
// restart with empty status, or after a spec change (e.g., credentialsRef rotation).
func (r *ZoneReconciler) needsInit(zone *domainsv1beta1.Zone) bool {
	if zone.Status.Name == "" {
		return true
	}
	cond := meta.FindStatusCondition(zone.Status.Conditions, conditionInitialized)
	if cond == nil {
		return true
	}
	return cond.ObservedGeneration < zone.Generation
}

// buildCFClient creates a Cloudflare API client with the given token and the
// reconciler's timeout/base-URL settings. SDK retries are disabled; single-call
// retries are handled by cfRetry.
func (r *ZoneReconciler) buildCFClient(apiToken string) *cloudflare.Client {
	cfOpts := []option.RequestOption{
		option.WithAPIToken(apiToken),
		option.WithMaxRetries(0),
	}
	if r.CFAPITimeout > 0 {
		cfOpts = append(cfOpts, option.WithRequestTimeout(r.CFAPITimeout))
	}
	if r.CFBaseURL != "" {
		cfOpts = append(cfOpts, option.WithBaseURL(r.CFBaseURL))
	}
	return cloudflare.NewClient(cfOpts...)
}

// initializeZone runs the one-time zone GET to resolve the zone name from Cloudflare
// and sets the Initialized condition. Called only when needsInit returns true.
func (r *ZoneReconciler) initializeZone(ctx context.Context, zone *domainsv1beta1.Zone, apiToken string) (*cloudflare.Client, error) {
	log := logf.FromContext(ctx)
	cf := r.buildCFClient(apiToken)

	var zoneDetails *zones.Zone
	attempts, err := cfRetry(ctx, cfResourceZone, cfOpGet, r.CFAPIMaxRetries, func() error {
		zoneGetStart := time.Now()
		var callErr error
		zoneDetails, callErr = cf.Zones.Get(ctx, zones.ZoneGetParams{
			ZoneID: cloudflare.F(zone.Spec.ID),
		})
		recordCFCall(cfResourceZone, cfOpGet, zoneGetStart, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "zone - initialization failed", "zoneID", zone.Spec.ID, "attempts", attempts)
		return nil, err
	}

	zone.Status.Name = zoneDetails.Name
	if err := r.setInitialized(ctx, zone, "ZoneInitialized",
		fmt.Sprintf("Zone credentials validated, zone: %s", zoneDetails.Name)); err != nil {
		return nil, err
	}
	zoneInitialized.WithLabelValues(zone.Name).Set(1)
	log.Info("zone - initialized", "zone", zoneDetails.Name, "zoneID", zone.Spec.ID)
	return cf, nil
}

// fetchAPIToken reads the CF API token from the secret referenced by the Zone CR.
// Similar secret-fetch logic exists in buildCloudflareClient (customhostname_controller.go).
// Not deduplicated: buildCloudflareClient additionally looks up the Zone CR and constructs the client.
func (r *ZoneReconciler) fetchAPIToken(ctx context.Context, zone *domainsv1beta1.Zone) (string, error) {
	key := zone.Spec.CredentialsRef.Key
	if key == "" {
		key = defaultAPITokenKey
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

func (r *ZoneReconciler) setInitialized(ctx context.Context, zone *domainsv1beta1.Zone, reason, message string) error {
	meta.SetStatusCondition(&zone.Status.Conditions, metav1.Condition{
		Type:               conditionInitialized,
		Status:             metav1.ConditionTrue,
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
	// The zoneRef index -- and the CustomHostname informer it implies -- is only
	// needed for custom hostname drift detection. When custom hostname management is
	// disabled the zone controller runs purely as load-balancing substrate, so it
	// neither indexes nor watches CustomHostname objects.
	if r.EnableCustomHostname {
		if err := mgr.GetFieldIndexer().IndexField(context.Background(),
			&saasv1beta1.CustomHostname{}, zoneRefField,
			func(o client.Object) []string {
				ch := o.(*saasv1beta1.CustomHostname)
				return []string{ch.Spec.ZoneRef.Name}
			}); err != nil {
			return fmt.Errorf("failed to create zoneRef index: %w", err)
		}
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
