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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	"github.com/cloudflare/cloudflare-go/v6/option"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

const (
	finalizerName = "saas.cf-edge.io/customhostname"
	// hostnameField is the field indexer key for spec.hostname.
	// Used to detect duplicate CRs claiming the same Cloudflare custom hostname in O(1).
	hostnameField = "spec.hostname"

	// Shared strings referenced in >1 place; single-use strings stay as literals.
	conditionReady         = "Ready"
	reasonHostnameConflict = "HostnameConflict"
)

const (
	ManagementPolicyManage  = "manage"
	ManagementPolicyCreate  = "create"
	ManagementPolicyObserve = "observe"
)

const (
	DeletePolicyAlways  = "always"
	DeletePolicyOwnOnly = "own-only"
	DeletePolicyNever   = "never"
)

const (
	sslStatusActive = "active"
	// Aliases for readability within the controller package (canonical order: CA, minTLS, method, type).
	// Some are currently used only in tests -- kept here (not in _test.go) to avoid
	// splitting aliases between production and test code, which would complicate promotion.
	sslCALetsEncrypt = saasv1beta1.SSLCALetsEncrypt
	sslCAGoogle      = saasv1beta1.SSLCAGoogle
	sslMinTLS10      = saasv1beta1.SSLMinTLS10
	sslMinTLS12      = saasv1beta1.SSLMinTLS12
	sslMinTLS13      = saasv1beta1.SSLMinTLS13
	sslMethodHTTP    = saasv1beta1.SSLMethodHTTP
	sslMethodTXT     = saasv1beta1.SSLMethodTXT
	sslTypeDV        = saasv1beta1.SSLTypeDV
	// NOTE: SSLSNIHostHeader is an SNI value, not an SSL setting. Kept in this
	// block with the SSL prefix for colocation with related CF constants.
	sslSNIHostHeader = saasv1beta1.SSLSNIHostHeader
)

// CustomHostnameReconciler reconciles a CustomHostname object.
// It acts as the worker: handles individual Cloudflare API writes (create/update/delete).
// Triggered by spec changes and by the Zone coordinator via the event channel on drift detection.
type CustomHostnameReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	OperatorNamespace string
	// ManagementPolicy is the operator-wide default: "manage", "create", or "observe".
	ManagementPolicy string
	// DeletePolicy controls delete behavior: "always" (default), "own-only", or "never".
	DeletePolicy string
	// DryRun skips all Cloudflare write operations and logs what would happen instead.
	DryRun bool
	// SSLDefaults are operator-wide defaults applied on create when the CR field is empty.
	SSLDefaults SSLDefaults
	// CFAPITimeout is the per-request timeout for single Cloudflare API calls.
	// Set via --cf-api-timeout (default: 5s).
	CFAPITimeout time.Duration
	// CFAPIMaxRetries is the maximum number of retries for single CF API calls.
	// Set via --cf-api-max-retries (default: 1). Uses our retry loop (immediate, no backoff).
	CFAPIMaxRetries int
	// CFBaseURL overrides the Cloudflare API base URL (for integration tests).
	CFBaseURL string
}

// SSLDefaults holds operator-wide default SSL settings for new custom hostnames.
// Field order: CA, minTLS, method, type.
type SSLDefaults struct {
	CertificateAuthority string
	MinTLSVersion        string
	Method               string
	Type                 string
}

// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames/finalizers,verbs=update
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *CustomHostnameReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var ch saasv1beta1.CustomHostname
	if err := r.Get(ctx, req.NamespacedName, &ch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Handle deletion -- non-CF paths first (no client needed), then CF paths.
	if !ch.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&ch, finalizerName) && ch.Status.ID == "" {
			log.Info("custom hostname - could not be deleted, finalizer released (association with Cloudflare was never established)",
				"hostname", ch.Spec.Hostname)
			controllerutil.RemoveFinalizer(&ch, finalizerName)
			return ctrl.Result{}, r.Update(ctx, &ch)
		}
		if controllerutil.ContainsFinalizer(&ch, finalizerName) {
			mgmtPolicy := effectiveManagementPolicy(ch.Spec.ManagementPolicy, r.ManagementPolicy)
			if mgmtPolicy == ManagementPolicyObserve {
				log.Info("custom hostname - not deleted, finalizer released (managementPolicy=observe)",
					"hostname", ch.Spec.Hostname, "id", ch.Status.ID)
				controllerutil.RemoveFinalizer(&ch, finalizerName)
				return ctrl.Result{}, r.Update(ctx, &ch)
			}
			policy := effectiveDeletePolicy(ch.Spec.DeletePolicy, r.DeletePolicy)
			if policy == DeletePolicyNever {
				log.Info("custom hostname - not deleted, finalizer released (deletePolicy=never)",
					"hostname", ch.Spec.Hostname, "id", ch.Status.ID)
				controllerutil.RemoveFinalizer(&ch, finalizerName)
				return ctrl.Result{}, r.Update(ctx, &ch)
			}
		}
		// CF-dependent delete paths: own-only lookup + always/own-only delete.
		zi, err := r.buildCloudflareClient(ctx, &ch)
		if err != nil {
			log.Error(err, "custom hostname - client initialization failed")
			return ctrl.Result{}, err // bare error for retry -- don't setError during deletion
		}
		return r.handleDelete(ctx, zi.Client, zi.ID, &ch)
	}

	zi, err := r.buildCloudflareClient(ctx, &ch)
	if err != nil {
		log.Error(err, "custom hostname - client initialization failed")
		return ctrl.Result{}, r.setError(ctx, &ch, "ZoneError", err.Error())
	}

	// Validate that the origin server belongs to the zone -- Cloudflare for SaaS is zone-scoped.
	// NOTE: Skipped when zi.Domain is empty (Zone not yet reconciled). CF API rejects
	// invalid origins anyway; this is a best-effort early check.
	if zi.Domain != "" && !strings.HasSuffix(ch.Spec.OriginServer, "."+zi.Domain) && ch.Spec.OriginServer != zi.Domain {
		return ctrl.Result{}, r.setError(ctx, &ch, "OriginNotInZone",
			fmt.Sprintf("originServer %q must belong to zone %q", ch.Spec.OriginServer, zi.Domain))
	}

	// NOTE: Requeue after finalizer add to get a fresh object with the finalizer persisted.
	// Continuing in the same cycle risks a race where another controller deletes the
	// object between the update and the next operation.
	if !controllerutil.ContainsFinalizer(&ch, finalizerName) {
		controllerutil.AddFinalizer(&ch, finalizerName)
		if err := r.Update(ctx, &ch); err != nil {
			return ctrl.Result{}, err
		}
		log.V(1).Info("custom hostname - finalizer added", "hostname", ch.Spec.Hostname)
		return ctrl.Result{Requeue: true}, nil
	}

	// Conflict detection: reject if another CR already owns this hostname in Cloudflare.
	// O(1) via field index. Returns early without any CF API call; no requeue scheduled
	// (the Zone controller skips drift-enqueue for conflict CRs, so this stays quiet until
	// the conflict resolves -- at which point the Zone controller re-enqueues via the
	// "hostname missing from CF" path).
	if conflicted, err := r.detectConflict(ctx, &ch); err != nil || conflicted {
		return ctrl.Result{}, err
	}

	// Always resolve current state from Cloudflare by hostname
	// This makes reconciliation idempotent across restarts and crash-recovery scenarios
	return r.reconcileCloudflareState(ctx, zi, &ch)
}

func (r *CustomHostnameReconciler) reconcileCloudflareState(ctx context.Context, zi *zoneInfo, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmtPolicy := effectiveManagementPolicy(ch.Spec.ManagementPolicy, r.ManagementPolicy)

	// NOTE: Always resolve current CF state by hostname, even if status.ID exists.
	// The etcd status could be stale (operator crash, manual CF deletion). This
	// single API call makes reconciliation idempotent across restarts.
	// findByHostname calls recordCFCall internally, satisfying cfRetry's per-attempt metrics contract.
	var existing *custom_hostnames.CustomHostnameListResponse
	attempts, err := cfRetry(ctx, cfResourceCustomHostname, cfOpGet, r.CFAPIMaxRetries, func() error {
		var callErr error
		existing, callErr = r.findByHostname(ctx, zi.Client, zi.ID, ch.Spec.Hostname)
		return callErr
	})
	if err != nil {
		log.Error(err, "custom hostname - lookup failed", "hostname", ch.Spec.Hostname, "attempts", attempts)
		return ctrl.Result{}, r.setError(ctx, ch, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmtPolicy == ManagementPolicyObserve {
			// Observe mode: don't create, wait for external provisioning.
			// The zone controller will re-enqueue when the hostname appears in CF.
			log.Info("custom hostname - not creating (managementPolicy=observe)",
				"hostname", ch.Spec.Hostname)
			ch.Status.ConsecutiveErrors = 0 // Clear errors from prior policy (e.g., switched from manage to observe)
			return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionFalse,
				"WaitingForExternal", "Hostname not yet provisioned in Cloudflare")
		}
		// manage or create: provision it
		return r.handleCreate(ctx, zi, ch)
	}

	// NOTE: Adoption continues in the same reconcile cycle (no early return/requeue)
	// to atomically check drift and update SSL status in a single pass. The status
	// is persisted once at the end, avoiding an extra API round-trip.
	// Exists -- adopt the CF ID into status. Only log and count on the first adoption
	// (status.ID is empty); subsequent reconciles skip when the ID is unchanged.
	if ch.Status.ID != existing.ID {
		if ch.Status.ID == "" {
			log.Info("custom hostname - adopted",
				"hostname", ch.Spec.Hostname,
				"id", existing.ID,
				"origin", existing.CustomOriginServer,
				"sni", existing.CustomOriginSNI)
		} else {
			log.Info("custom hostname - readopted (externally recreated)",
				"hostname", ch.Spec.Hostname,
				"previousID", ch.Status.ID,
				"newID", existing.ID,
				"origin", existing.CustomOriginServer,
				"sni", existing.CustomOriginSNI)
		}
		operationsTotal.WithLabelValues(cfResourceCustomHostname, cfOpAdopt).Inc()
		ch.Status.ID = existing.ID
	}

	// Check drift -- only correct it if management policy is "manage"
	edited := false
	originDrift := existing.CustomOriginServer != ch.Spec.OriginServer || sniDrifted(existing.CustomOriginSNI, ch)
	sslDrift := sslDrifted(existing.SSL, ch.Spec.SSL)
	if originDrift || sslDrift {
		di := buildDriftInfo(existing, ch)
		if mgmtPolicy != ManagementPolicyManage {
			log.Info(fmt.Sprintf("custom hostname - not updating, drift detected (managementPolicy=%s)", mgmtPolicy),
				"hostname", ch.Spec.Hostname,
				"drift", di)
		} else if r.DryRun {
			log.Info("custom hostname - not updating, drift detected (dry-run)",
				"hostname", ch.Spec.Hostname,
				"drift", di)
		} else {
			log.Info("custom hostname - updating, drift detected",
				"hostname", ch.Spec.Hostname,
				"drift", di)
			editParams := custom_hostnames.CustomHostnameEditParams{ZoneID: cloudflare.F(zi.ID)}
			opts := []option.RequestOption{option.WithJSONSet("custom_origin_server", ch.Spec.OriginServer)}
			if ch.Spec.OriginSNI != nil {
				opts = append(opts, option.WithJSONSet("custom_origin_sni", *ch.Spec.OriginSNI))
			}
			if ch.Spec.SSL != nil {
				editParams.SSL = cloudflare.F(buildSSLEditParams(ch.Spec.SSL, existing.SSL))
			}
			var editResp *custom_hostnames.CustomHostnameEditResponse
			attempts, editErr := cfRetry(ctx, cfResourceCustomHostname, cfOpUpdate, r.CFAPIMaxRetries, func() error {
				editStart := time.Now()
				var callErr error
				editResp, callErr = zi.Client.CustomHostnames.Edit(ctx, existing.ID, editParams, opts...)
				recordCFCall(cfResourceCustomHostname, cfOpUpdate, editStart, &callErr)
				return callErr
			})
			if editErr != nil {
				log.Error(editErr, "custom hostname - update failed", "id", existing.ID, "attempts", attempts)
				return ctrl.Result{}, r.setError(ctx, ch, "UpdateFailed", editErr.Error())
			}
			operationsTotal.WithLabelValues(cfResourceCustomHostname, cfOpUpdate).Inc()
			// Use post-edit response for status -- reflects the corrected CF state.
			ch.Status.SSL = sslStatusFromEdit(editResp)
			edited = true
		}
	}
	// Set SSL status in memory; persisted by setCondition's r.Status().Update()
	// inside requeueOrReady -- single write covers SSL, conditions, and counters.
	// When an edit occurred, ch.Status.SSL already has the post-edit state (set above);
	// otherwise, refresh from the pre-edit list response to catch external CF changes.
	if !edited {
		newSSL := sslStatusFromList(existing)
		if !sslStatusEqual(ch.Status.SSL, newSSL) {
			log.V(1).Info("custom hostname - status.ssl refreshed", "hostname", ch.Spec.Hostname, "ssl", newSSL)
		}
		ch.Status.SSL = newSSL
	}
	return r.requeueOrReady(ctx, zi.CR, ch)
}

// effectiveManagementPolicy returns the management policy to apply for this CR.
// spec.managementPolicy takes precedence over the operator-wide --management-policy flag,
// allowing per-CR override without restarting the operator.
// NOTE: No runtime validation of crPolicy -- kubebuilder Enum on the CRD rejects invalid
// values at admission. An invalid value bypassing admission would fall through switch
// statements and behave like "manage" (fail-open). Acceptable given admission enforcement.
func effectiveManagementPolicy(crPolicy, operatorDefault string) string {
	if crPolicy != "" {
		return crPolicy
	}
	return operatorDefault
}

func (r *CustomHostnameReconciler) handleCreate(ctx context.Context, zi *zoneInfo, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	params := custom_hostnames.CustomHostnameNewParams{
		ZoneID:   cloudflare.F(zi.ID),
		Hostname: cloudflare.F(ch.Spec.Hostname),
	}
	// SSL is always required for custom hostnames. Empty spec lets buildSSLParams
	// apply the operator defaults (--ssl-*), falling back to http/dv if unset.
	sslSpec := ch.Spec.SSL
	if sslSpec == nil {
		sslSpec = &saasv1beta1.CustomHostnameSSL{}
	}
	params.SSL = cloudflare.F(buildSSLParams(sslSpec, r.SSLDefaults))
	opts := []option.RequestOption{option.WithJSONSet("custom_origin_server", ch.Spec.OriginServer)}
	if ch.Spec.OriginSNI != nil {
		opts = append(opts, option.WithJSONSet("custom_origin_sni", *ch.Spec.OriginSNI))
	}

	if r.DryRun {
		log.Info("custom hostname - not creating (dry-run)", "hostname", ch.Spec.Hostname, "origin", ch.Spec.OriginServer)
		return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionFalse, "DryRun", "not creating (dry-run)")
	}

	var resp *custom_hostnames.CustomHostnameNewResponse
	attempts, err := cfRetry(ctx, cfResourceCustomHostname, cfOpCreate, r.CFAPIMaxRetries, func() error {
		createStart := time.Now()
		var callErr error
		resp, callErr = zi.Client.CustomHostnames.New(ctx, params, opts...)
		recordCFCall(cfResourceCustomHostname, cfOpCreate, createStart, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "custom hostname - create failed", "hostname", ch.Spec.Hostname, "attempts", attempts)
		return ctrl.Result{}, r.setError(ctx, ch, "CreateFailed", err.Error())
	}

	// Distinguish initial create from recreation after external deletion.
	// createCount > 0 means the hostname existed before and was deleted externally.
	isRecreation := ch.Status.CreateCount > 0
	op := cfOpCreate
	if isRecreation {
		op = cfOpRecreate
		log.Info("custom hostname - recreated", "hostname", ch.Spec.Hostname, "id", resp.ID)
	} else {
		log.Info("custom hostname - created", "hostname", ch.Spec.Hostname, "id", resp.ID)
	}
	operationsTotal.WithLabelValues(cfResourceCustomHostname, op).Inc()

	// Reset SSL provisioning timer on every (re)create so the metric reflects the current cycle.
	now := metav1.Now()
	ch.Status.SSLProvisioningStartedAt = &now
	ch.Status.CreateCount++
	ch.Status.ID = resp.ID
	ch.Status.SSL = sslStatusFromNew(resp)
	// Single write: requeueOrReady -> setCondition persists ID, SSL, and condition together.
	return r.requeueOrReady(ctx, zi.CR, ch)
}

// handleDelete performs CF API operations for CR deletion (own-only lookup, delete).
// Non-CF delete paths (no ID, observe, never) are handled in Reconcile before
// buildCloudflareClient, so they work even when Zone/Secret is missing.
// Dry-run is handled here: skip CF writes but release the finalizer.
func (r *CustomHostnameReconciler) handleDelete(ctx context.Context, cf *cloudflare.Client, zoneID string, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Dry-run: skip CF API calls but release the finalizer so the CR can be
	// garbage collected. The CF hostname stays as an orphan (visible in drift
	// detection). Blocking finalizer removal in dry-run would prevent ArgoCD
	// from pruning CRs, causing stuck syncs.
	if r.DryRun {
		log.Info("custom hostname - not deleted, finalizer released (dry-run)", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
		controllerutil.RemoveFinalizer(ch, finalizerName)
		return ctrl.Result{}, r.Update(ctx, ch)
	}

	if controllerutil.ContainsFinalizer(ch, finalizerName) {
		policy := effectiveDeletePolicy(ch.Spec.DeletePolicy, r.DeletePolicy)
		if policy == DeletePolicyOwnOnly {
			// findByHostname calls recordCFCall internally, satisfying cfRetry's per-attempt metrics contract.
			var current *custom_hostnames.CustomHostnameListResponse
			attempts, err := cfRetry(ctx, cfResourceCustomHostname, cfOpGet, r.CFAPIMaxRetries, func() error {
				var callErr error
				current, callErr = r.findByHostname(ctx, cf, zoneID, ch.Spec.Hostname)
				return callErr
			})
			if err != nil {
				log.Error(err, "custom hostname - pre-delete lookup failed (deletePolicy=own-only)", "hostname", ch.Spec.Hostname, "attempts", attempts)
				return ctrl.Result{}, err // Return raw error for controller-runtime backoff retry;
				// don't setError -- CR is being deleted, status updates are pointless and
				// setError returns nil (no requeue), which would delay finalizer removal.
			}
			if !shouldDeleteInCF(ch.Status.ID, current) {
				log.Info("custom hostname - not deleted, finalizer released (deletePolicy=own-only)",
					"hostname", ch.Spec.Hostname, "statusID", ch.Status.ID,
					"currentID", func() string {
						if current != nil {
							return current.ID
						}
						return "<not found>"
					}())
				controllerutil.RemoveFinalizer(ch, finalizerName)
				return ctrl.Result{}, r.Update(ctx, ch)
			}
		}

		attempts, delErr := cfRetry(ctx, cfResourceCustomHostname, cfOpDelete, r.CFAPIMaxRetries, func() error {
			deleteStart := time.Now()
			_, callErr := cf.CustomHostnames.Delete(ctx, ch.Status.ID, custom_hostnames.CustomHostnameDeleteParams{
				ZoneID: cloudflare.F(zoneID),
			})
			recordCFCall(cfResourceCustomHostname, cfOpDelete, deleteStart, &callErr)
			return callErr
		})
		if delErr != nil {
			// 404 means the resource is already gone (e.g. deleted by another entity or stale ID).
			// Treat as success -- our specific resource no longer exists, remove finalizer.
			var cfErr *cloudflare.Error
			if errors.As(delErr, &cfErr) && cfErr.StatusCode == 404 {
				log.Info("custom hostname - could not be deleted, finalizer released (not found in Cloudflare)", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
			} else {
				log.Error(delErr, "custom hostname - delete failed", "id", ch.Status.ID, "attempts", attempts)
				return ctrl.Result{}, delErr
			}
		} else {
			log.Info("custom hostname - deleted, finalizer released", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
			// NOTE: operationsTotal is only incremented on actual deletes, not 404s.
			// A 404 means the CH was already gone -- the operator didn't perform the delete.
			// The 404 is still recorded in api_errors_by_code_total via recordCFCall above.
			operationsTotal.WithLabelValues(cfResourceCustomHostname, cfOpDelete).Inc()
		}
	}

	controllerutil.RemoveFinalizer(ch, finalizerName)
	return ctrl.Result{}, r.Update(ctx, ch)
}

// effectiveDeletePolicy returns the delete policy to apply for this CR.
// spec.deletePolicy takes precedence over the operator-wide --delete-policy flag,
// allowing per-CR override without restarting the operator.
func effectiveDeletePolicy(crPolicy, operatorDefault string) string {
	if crPolicy != "" {
		return crPolicy
	}
	return operatorDefault
}

// shouldDeleteInCF returns true if the hostname should be deleted from Cloudflare.
// Only called for own-only policy: returns true if current CF state exists and has the same ID as statusID.
func shouldDeleteInCF(statusID string, current *custom_hostnames.CustomHostnameListResponse) bool {
	if current == nil {
		return false
	}
	return current.ID == statusID
}

// NOTE: Early return from the pager on first match abandons remaining pages.
// No resource leak -- the pager is GC'd when Reconcile returns (~200-500ms).
func (r *CustomHostnameReconciler) findByHostname(ctx context.Context, cf *cloudflare.Client, zoneID, hostname string) (*custom_hostnames.CustomHostnameListResponse, error) {
	start := time.Now()
	pager := cf.CustomHostnames.ListAutoPaging(ctx, custom_hostnames.CustomHostnameListParams{
		ZoneID:   cloudflare.F(zoneID),
		Hostname: cloudflare.F(hostname),
	})
	var noErr error
	for pager.Next() {
		ch := pager.Current()
		if ch.Hostname == hostname {
			recordCFCall(cfResourceCustomHostname, cfOpGet, start, &noErr)
			return &ch, nil
		}
	}
	err := pager.Err()
	recordCFCall(cfResourceCustomHostname, cfOpGet, start, &err)
	return nil, err
}

func (r *CustomHostnameReconciler) requeueOrReady(ctx context.Context, zoneCR string, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	ch.Status.ConsecutiveErrors = 0
	if ch.Status.SSL != nil && ch.Status.SSL.Status == sslStatusActive {
		// Observe SSL provisioning duration on first transition to active.
		// Guard against double-counting on operator restart: skip if Ready=True is already set.
		// NOTE: If SSL is already active at creation time (pre-validated domains), the
		// observed duration is near-zero. This is technically correct -- not a bug.
		// SSLProvisioningStartedAt is nil for adopted CHs (not created by the operator).
		// Provisioning duration is only measured for operator-created CHs. Adopted CHs
		// that transition to active are tracked via the customhostnames{state} gauge instead.
		if ch.Status.SSLProvisioningStartedAt != nil {
			alreadyReady := false
			for _, cond := range ch.Status.Conditions {
				if cond.Type == conditionReady && cond.Status == metav1.ConditionTrue {
					alreadyReady = true
					break
				}
			}
			if !alreadyReady {
				method := sslMethodHTTP
				if ch.Status.SSL.Method != "" {
					method = ch.Status.SSL.Method
				} else if ch.Spec.SSL != nil && ch.Spec.SSL.Method != "" {
					method = ch.Spec.SSL.Method
				}
				duration := time.Since(ch.Status.SSLProvisioningStartedAt.Time)
				setSSLProvisioningDuration(zoneCR, ch.Spec.Hostname, method, duration)
				log.Info("custom hostname - SSL provisioned", "hostname", ch.Spec.Hostname, "duration", duration.Round(time.Second), "method", method)
			}
		}
		return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionTrue, conditionReady, "Custom hostname is active")
	}
	sslStatus := "unknown"
	if ch.Status.SSL != nil {
		sslStatus = ch.Status.SSL.Status
	}
	if err := r.setCondition(ctx, ch, metav1.ConditionFalse, "SSLPending", fmt.Sprintf("SSL status: %s", sslStatus)); err != nil {
		return ctrl.Result{}, err
	}
	// No self-requeue: the zone controller detects SSL status changes via its bulk
	// list and re-enqueues this CR when ssl.status transitions (e.g. active).
	return ctrl.Result{}, nil
}

// detectConflict checks whether another CR already owns this hostname in Cloudflare (i.e. has a
// CF ID assigned). If so, it marks this CR with a HostnameConflict condition and returns true.
// The caller should return immediately on (true, nil) -- no requeue is scheduled.
// Self-healing: when the owning CR is deleted, the Zone controller re-enqueues this CR via the
// "hostname missing from CF" path, at which point detectConflict finds no peer with an ID and
// returns false, allowing normal provisioning to proceed.
// NOTE: If two CRs with the same hostname are created simultaneously (neither has an ID yet),
// both pass this check and race to CF. CF returns success for one; the other fails or creates
// a duplicate. On the next reconcile, adoption sets Status.ID on both -> conflict detected.
// Self-heals within one drift cycle.
func (r *CustomHostnameReconciler) detectConflict(ctx context.Context, ch *saasv1beta1.CustomHostname) (bool, error) {
	log := logf.FromContext(ctx)
	var peers saasv1beta1.CustomHostnameList
	if err := r.List(ctx, &peers, client.MatchingFields{hostnameField: ch.Spec.Hostname}); err != nil {
		return false, err
	}
	for i := range peers.Items {
		peer := &peers.Items[i]
		if peer.UID == ch.UID {
			continue
		}
		if peer.Status.ID != "" {
			log.Info("custom hostname - not processing duplicate CR, already managed",
				"hostname", ch.Spec.Hostname, "owner", peer.Namespace+"/"+peer.Name)
			err := r.setConflict(ctx, ch,
				fmt.Sprintf("hostname %q already managed by %s/%s", ch.Spec.Hostname, peer.Namespace, peer.Name))
			return true, err
		}
	}
	return false, nil
}

// setConflict sets a HostnameConflict condition on the CR without incrementing ConsecutiveErrors
// and without scheduling a requeue. The condition clears itself when the conflict resolves:
// once the owning CR is deleted and CF removes the hostname, the Zone controller re-enqueues
// this CR via the "hostname missing from CF" path, and the next successful reconcile replaces
// the condition with Ready.
func (r *CustomHostnameReconciler) setConflict(ctx context.Context, ch *saasv1beta1.CustomHostname, message string) error {
	return r.setCondition(ctx, ch, metav1.ConditionFalse, reasonHostnameConflict, message)
}

// setError increments ConsecutiveErrors, sets a Ready=False condition, and updates status.
func (r *CustomHostnameReconciler) setError(ctx context.Context, ch *saasv1beta1.CustomHostname, reason, message string) error {
	ch.Status.ConsecutiveErrors++
	return r.setCondition(ctx, ch, metav1.ConditionFalse, reason, message)
}

func (r *CustomHostnameReconciler) setCondition(ctx context.Context, ch *saasv1beta1.CustomHostname, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&ch.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: ch.Generation,
	})
	if err := r.Status().Update(ctx, ch); err != nil {
		return fmt.Errorf("failed to update conditions: %w", err)
	}
	return nil
}

type zoneInfo struct {
	Client *cloudflare.Client
	ID     string // CF zone ID
	CR     string // K8s CR name (for metrics)
	Domain string // CF domain name (for origin validation)
}

func (r *CustomHostnameReconciler) buildCloudflareClient(ctx context.Context, ch *saasv1beta1.CustomHostname) (*zoneInfo, error) {
	zoneNS := ch.Spec.ZoneRef.Namespace
	if zoneNS == "" {
		zoneNS = r.OperatorNamespace
	}
	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, types.NamespacedName{Name: ch.Spec.ZoneRef.Name, Namespace: zoneNS}, &zone); err != nil {
		return nil, fmt.Errorf("zone %q not found: %w", ch.Spec.ZoneRef.Name, err)
	}
	key := zone.Spec.CredentialsRef.Key
	if key == "" {
		key = "apiToken"
	}
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: zone.Spec.CredentialsRef.Name, Namespace: zone.Namespace}, &secret); err != nil {
		return nil, fmt.Errorf("secret %q not found: %w", zone.Spec.CredentialsRef.Name, err)
	}
	token, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in secret %q", key, zone.Spec.CredentialsRef.Name)
	}
	opts := []option.RequestOption{
		option.WithAPIToken(string(token)),
		option.WithMaxRetries(0), // SDK retries disabled; we handle retries via cfRetry.
	}
	if r.CFAPITimeout > 0 {
		opts = append(opts, option.WithRequestTimeout(r.CFAPITimeout))
	}
	if r.CFBaseURL != "" {
		opts = append(opts, option.WithBaseURL(r.CFBaseURL))
	}
	return &zoneInfo{
		Client: cloudflare.NewClient(opts...),
		ID:     zone.Spec.ID,
		CR:     zone.Name,
		Domain: zone.Status.Name,
	}, nil
}

func sniDrifted(currentSNI string, ch *saasv1beta1.CustomHostname) bool {
	if ch.Spec.OriginSNI == nil {
		// Nil means "don't manage SNI" -- external changes are not corrected.
		// Used by both the CH controller and detectCustomHostnameDrift() in zone drift detection.
		// NOTE: We intentionally do NOT log when CF's SNI differs from the
		// hostname here. CF uses the sentinel ":request_host_header:" as its
		// default, which is an internal value the operator shouldn't assume.
		// If SNI management is desired, set spec.originSNI explicitly.
		return false
	}
	return currentSNI != *ch.Spec.OriginSNI
}

// NOTE: Uses reflect.DeepEqual rather than field-by-field comparison. The struct has
// 14+ fields including slices -- DeepEqual is concise and correct. Called once per
// reconcile (not a hot path), so performance is not a concern.
func sslStatusEqual(a, b *saasv1beta1.CustomHostnameSSLStatus) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return reflect.DeepEqual(a, b)
}

// SSL field order: CA, minTLS, method, type -- maintained across all SSL functions and logs.

func sslDrifted(cfSSL custom_hostnames.CustomHostnameListResponseSSL, spec *saasv1beta1.CustomHostnameSSL) bool {
	if spec == nil {
		return false
	}
	if spec.CertificateAuthority != "" && string(cfSSL.CertificateAuthority) != spec.CertificateAuthority {
		return true
	}
	if spec.MinTLSVersion != "" && string(cfSSL.Settings.MinTLSVersion) != spec.MinTLSVersion {
		return true
	}
	if spec.Method != "" && string(cfSSL.Method) != spec.Method {
		return true
	}
	if spec.Type != "" && string(cfSSL.Type) != spec.Type {
		return true
	}
	return false
}

// driftPair is a cf/spec value pair for drifted fields in structured drift logging.
// NOTE: Field order {CF, Spec} differs from statusPair {Status, CF} in zone_customhostname_drift.go.
// Both read naturally as "what was -> what should be" in their context:
// driftPair: CF has X, spec wants Y. statusPair: status had X, CF now has Y.
type driftPair struct {
	CF   string `json:"cf"`
	Spec string `json:"spec"`
}

// buildDriftInfo builds a structured drift object for logging.
// Three categories: drifted (cf/spec pairs), matched (single value), unmanaged (CF value).
func buildDriftInfo(existing *custom_hostnames.CustomHostnameListResponse, ch *saasv1beta1.CustomHostname) map[string]any {
	drifted := map[string]any{}
	matched := map[string]string{}
	unmanaged := map[string]string{}

	// Origin -- always managed
	if existing.CustomOriginServer != ch.Spec.OriginServer {
		drifted["origin"] = driftPair{CF: existing.CustomOriginServer, Spec: ch.Spec.OriginServer}
	} else {
		matched["origin"] = ch.Spec.OriginServer
	}

	// SNI
	if ch.Spec.OriginSNI != nil {
		if sniDrifted(existing.CustomOriginSNI, ch) {
			drifted["sni"] = driftPair{CF: existing.CustomOriginSNI, Spec: *ch.Spec.OriginSNI}
		} else {
			matched["sni"] = *ch.Spec.OriginSNI
		}
	} else {
		unmanaged["sni"] = existing.CustomOriginSNI
	}

	// SSL fields -- CA, minTLS, method, type
	ssl := existing.SSL
	spec := ch.Spec.SSL
	// CA
	if spec != nil && spec.CertificateAuthority != "" {
		if string(ssl.CertificateAuthority) != spec.CertificateAuthority {
			drifted["ca"] = driftPair{CF: string(ssl.CertificateAuthority), Spec: spec.CertificateAuthority}
		} else {
			matched["ca"] = spec.CertificateAuthority
		}
	} else {
		unmanaged["ca"] = string(ssl.CertificateAuthority)
	}
	// minTLS
	if spec != nil && spec.MinTLSVersion != "" {
		if string(ssl.Settings.MinTLSVersion) != spec.MinTLSVersion {
			drifted["minTLS"] = driftPair{CF: string(ssl.Settings.MinTLSVersion), Spec: spec.MinTLSVersion}
		} else {
			matched["minTLS"] = spec.MinTLSVersion
		}
	} else {
		unmanaged["minTLS"] = string(ssl.Settings.MinTLSVersion)
	}
	// method
	if spec != nil && spec.Method != "" {
		if string(ssl.Method) != spec.Method {
			drifted["method"] = driftPair{CF: string(ssl.Method), Spec: spec.Method}
		} else {
			matched["method"] = spec.Method
		}
	} else {
		unmanaged["method"] = string(ssl.Method)
	}
	// type
	if spec != nil && spec.Type != "" {
		if string(ssl.Type) != spec.Type {
			drifted["type"] = driftPair{CF: string(ssl.Type), Spec: spec.Type}
		} else {
			matched["type"] = spec.Type
		}
	} else {
		unmanaged["type"] = string(ssl.Type)
	}

	di := map[string]any{"drifted": drifted}
	if len(matched) > 0 {
		di["matched"] = matched
	}
	if len(unmanaged) > 0 {
		di["unmanaged"] = unmanaged
	}
	return di
}

// buildSSLEditParams builds SSL params for a drift-correction edit.
// CF re-provisions the certificate when certain fields change (method, type, CA),
// which resets other fields (e.g. minTLSVersion). To prevent silent resets, we
// always send all four SSL fields: spec value if set, otherwise the current CF
// value (preserving what's already in Cloudflare).
func buildSSLEditParams(ssl *saasv1beta1.CustomHostnameSSL, cfSSL custom_hostnames.CustomHostnameListResponseSSL) custom_hostnames.CustomHostnameEditParamsSSL {
	p := custom_hostnames.CustomHostnameEditParamsSSL{}

	// certificateAuthority
	ca := ssl.CertificateAuthority
	if ca == "" {
		ca = string(cfSSL.CertificateAuthority)
	}
	if ca != "" {
		p.CertificateAuthority = cloudflare.F(cloudflare.CertificateCA(ca))
	}

	// minTLSVersion
	minTLS := ssl.MinTLSVersion
	if minTLS == "" {
		minTLS = string(cfSSL.Settings.MinTLSVersion)
	}
	if minTLS != "" {
		p.Settings = cloudflare.F(custom_hostnames.CustomHostnameEditParamsSSLSettings{
			MinTLSVersion: cloudflare.F(custom_hostnames.CustomHostnameEditParamsSSLSettingsMinTLSVersion(minTLS)),
		})
	}

	// method + type -- CF requires both when either is sent.
	method := ssl.Method
	if method == "" {
		method = string(cfSSL.Method)
	}
	if method == "" {
		method = sslMethodHTTP
	}
	t := ssl.Type
	if t == "" {
		t = string(cfSSL.Type)
	}
	if t == "" {
		t = sslTypeDV
	}
	p.Method = cloudflare.F(custom_hostnames.DCVMethod(method))
	p.Type = cloudflare.F(custom_hostnames.DomainValidationType(t))

	return p
}

func buildSSLParams(ssl *saasv1beta1.CustomHostnameSSL, defaults SSLDefaults) custom_hostnames.CustomHostnameNewParamsSSL {
	p := custom_hostnames.CustomHostnameNewParamsSSL{}
	ca := ssl.CertificateAuthority
	if ca == "" {
		ca = defaults.CertificateAuthority
	}
	if ca != "" {
		p.CertificateAuthority = cloudflare.F(cloudflare.CertificateCA(ca))
	}
	minTLS := ssl.MinTLSVersion
	if minTLS == "" {
		minTLS = defaults.MinTLSVersion
	}
	if minTLS != "" {
		p.Settings = cloudflare.F(custom_hostnames.CustomHostnameNewParamsSSLSettings{
			MinTLSVersion: cloudflare.F(custom_hostnames.CustomHostnameNewParamsSSLSettingsMinTLSVersion(minTLS)),
		})
	}
	method := ssl.Method
	if method == "" {
		method = defaults.Method
	}
	if method == "" {
		method = sslMethodHTTP // CF requires method on create
	}
	p.Method = cloudflare.F(custom_hostnames.DCVMethod(method))
	t := ssl.Type
	if t == "" {
		t = defaults.Type
	}
	if t == "" {
		t = sslTypeDV // CF requires type on create
	}
	p.Type = cloudflare.F(custom_hostnames.DomainValidationType(t))
	return p
}

// sslStatusFromNew maps the CF create response to status.ssl.
// Mirror of sslStatusFromList / sslStatusFromEdit -- if you change one, change the others.
// Not deduplicated because the CF SDK uses separate generated types for
// create, list, and edit responses with no shared interface.
func sslStatusFromNew(resp *custom_hostnames.CustomHostnameNewResponse) *saasv1beta1.CustomHostnameSSLStatus {
	s := &saasv1beta1.CustomHostnameSSLStatus{
		Status:               string(resp.SSL.Status),
		CertificateAuthority: string(resp.SSL.CertificateAuthority),
		MinTLSVersion:        string(resp.SSL.Settings.MinTLSVersion),
		Method:               string(resp.SSL.Method),
		Type:                 string(resp.SSL.Type),
		ID:                   resp.SSL.ID,
		Issuer:               resp.SSL.Issuer,
		SerialNumber:         resp.SSL.SerialNumber,
		BundleMethod:         string(resp.SSL.BundleMethod),
		Wildcard:             resp.SSL.Wildcard,
		Hosts:                resp.SSL.Hosts,
	}
	if !resp.SSL.UploadedOn.IsZero() {
		t := metav1.NewTime(resp.SSL.UploadedOn)
		s.UploadedOn = &t
	}
	if !resp.SSL.ExpiresOn.IsZero() {
		t := metav1.NewTime(resp.SSL.ExpiresOn)
		s.ExpiresOn = &t
	}
	for _, vr := range resp.SSL.ValidationRecords {
		s.ValidationRecords = append(s.ValidationRecords, saasv1beta1.SSLValidationRecord{
			TXTName:  vr.TXTName,
			TXTValue: vr.TXTValue,
			HTTPUrl:  vr.HTTPURL,
			HTTPBody: vr.HTTPBody,
			Emails:   vr.Emails,
		})
	}
	for _, ve := range resp.SSL.ValidationErrors {
		s.ValidationErrors = append(s.ValidationErrors, ve.Message)
	}
	return s
}

// sslStatusFromList maps the CF list/get response to status.ssl.
// Mirror of sslStatusFromNew / sslStatusFromEdit -- if you change one, change the others.
func sslStatusFromList(resp *custom_hostnames.CustomHostnameListResponse) *saasv1beta1.CustomHostnameSSLStatus {
	s := &saasv1beta1.CustomHostnameSSLStatus{
		Status:               string(resp.SSL.Status),
		CertificateAuthority: string(resp.SSL.CertificateAuthority),
		MinTLSVersion:        string(resp.SSL.Settings.MinTLSVersion),
		Method:               string(resp.SSL.Method),
		Type:                 string(resp.SSL.Type),
		ID:                   resp.SSL.ID,
		Issuer:               resp.SSL.Issuer,
		SerialNumber:         resp.SSL.SerialNumber,
		BundleMethod:         string(resp.SSL.BundleMethod),
		Wildcard:             resp.SSL.Wildcard,
		Hosts:                resp.SSL.Hosts,
	}
	if !resp.SSL.UploadedOn.IsZero() {
		t := metav1.NewTime(resp.SSL.UploadedOn)
		s.UploadedOn = &t
	}
	if !resp.SSL.ExpiresOn.IsZero() {
		t := metav1.NewTime(resp.SSL.ExpiresOn)
		s.ExpiresOn = &t
	}
	for _, vr := range resp.SSL.ValidationRecords {
		s.ValidationRecords = append(s.ValidationRecords, saasv1beta1.SSLValidationRecord{
			TXTName:  vr.TXTName,
			TXTValue: vr.TXTValue,
			HTTPUrl:  vr.HTTPURL,
			HTTPBody: vr.HTTPBody,
			Emails:   vr.Emails,
		})
	}
	for _, ve := range resp.SSL.ValidationErrors {
		s.ValidationErrors = append(s.ValidationErrors, ve.Message)
	}
	return s
}

// sslStatusFromEdit maps the CF edit response to status.ssl.
// Mirror of sslStatusFromNew / sslStatusFromList -- if you change one, change the others.
func sslStatusFromEdit(resp *custom_hostnames.CustomHostnameEditResponse) *saasv1beta1.CustomHostnameSSLStatus {
	s := &saasv1beta1.CustomHostnameSSLStatus{
		Status:               string(resp.SSL.Status),
		CertificateAuthority: string(resp.SSL.CertificateAuthority),
		MinTLSVersion:        string(resp.SSL.Settings.MinTLSVersion),
		Method:               string(resp.SSL.Method),
		Type:                 string(resp.SSL.Type),
		ID:                   resp.SSL.ID,
		Issuer:               resp.SSL.Issuer,
		SerialNumber:         resp.SSL.SerialNumber,
		BundleMethod:         string(resp.SSL.BundleMethod),
		Wildcard:             resp.SSL.Wildcard,
		Hosts:                resp.SSL.Hosts,
	}
	if !resp.SSL.UploadedOn.IsZero() {
		t := metav1.NewTime(resp.SSL.UploadedOn)
		s.UploadedOn = &t
	}
	if !resp.SSL.ExpiresOn.IsZero() {
		t := metav1.NewTime(resp.SSL.ExpiresOn)
		s.ExpiresOn = &t
	}
	for _, vr := range resp.SSL.ValidationRecords {
		s.ValidationRecords = append(s.ValidationRecords, saasv1beta1.SSLValidationRecord{
			TXTName:  vr.TXTName,
			TXTValue: vr.TXTValue,
			HTTPUrl:  vr.HTTPURL,
			HTTPBody: vr.HTTPBody,
			Emails:   vr.Emails,
		})
	}
	for _, ve := range resp.SSL.ValidationErrors {
		s.ValidationErrors = append(s.ValidationErrors, ve.Message)
	}
	return s
}

// fastWritePredicate filters informer events for the CustomHostname worker.
//
// On pod restart, the informer emits Create events for all existing CRs.
// We distinguish them from genuinely new CRs using status.ID:
//   - status.ID != "" -> existing CR, already provisioned; drop it and let the
//     Zone coordinator's periodic bulk-list handle drift detection (including
//     SSL status transitions: initializing -> pending_validation -> active).
//   - status.ID == "" -> genuinely new CR (or crash-recovery case); let it through
//     for immediate provisioning.
//   - DeletionTimestamp set -> terminating CR; always let it through regardless of
//     status.ID so the finalizer is removed and the CR can be fully deleted.
//     Without this, a restart with a terminating CR would leave it stuck forever.
//
// NOTE: this predicate is coupled to status.ID as the "seen before" signal.
// If the state model changes (e.g. ID moved to a different field), update this predicate.
func fastWritePredicate() predicate.Predicate {
	// NOTE: GenericFunc is intentionally not set -- defaults to "pass all."
	// Drift events from the zone controller arrive as GenericEvents and must
	// always be processed (they carry CRs needing status refresh or drift correction).
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			ch, ok := e.Object.(*saasv1beta1.CustomHostname)
			if !ok {
				return true
			}
			// Always process CRs with a DeletionTimestamp -- they need finalizer removal.
			// Without this, a terminating CR with status.ID set would be silently skipped
			// on operator restart, leaving the finalizer in place forever.
			if !ch.DeletionTimestamp.IsZero() {
				return true
			}
			return ch.Status.ID == ""
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
	}
}

// SetupWithManager sets up the controller with the Manager.
// driftEvents is the channel through which the Zone coordinator sends CRs needing reconciliation.
func (r *CustomHostnameReconciler) SetupWithManager(mgr ctrl.Manager, driftEvents <-chan event.GenericEvent) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&saasv1beta1.CustomHostname{},
		hostnameField,
		func(o client.Object) []string {
			return []string{o.(*saasv1beta1.CustomHostname).Spec.Hostname}
		},
	); err != nil {
		return fmt.Errorf("failed to index CustomHostname by %s: %w", hostnameField, err)
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&saasv1beta1.CustomHostname{}, builder.WithPredicates(fastWritePredicate())).
		WatchesRawSource(source.Channel(driftEvents, &handler.EnqueueRequestForObject{})).
		Named("customhostname").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}
