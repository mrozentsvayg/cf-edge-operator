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

// CustomHostnameReconciler reconciles a CustomHostname object.
// It acts as the worker: handles individual Cloudflare API writes (create/update/delete).
// Triggered by spec changes and by the Zone coordinator via the event channel on drift detection.
type CustomHostnameReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	OperatorNamespace string
	// DeletePolicy controls delete behavior: "always" (default), "own-only", or "never".
	DeletePolicy string
	// DryRun skips all Cloudflare write operations and logs what would happen instead.
	DryRun bool
}

// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=customhostnames,verbs=get;list;watch;create;update;patch;delete
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

	cf, zoneID, zoneName, err := r.buildCloudflareClient(ctx, &ch)
	if err != nil {
		log.Error(err, "failed to build Cloudflare client")
		return ctrl.Result{}, r.setError(ctx, &ch, "ZoneError", err.Error())
	}

	// Validate that the origin server belongs to the zone — Cloudflare for SaaS is zone-scoped.
	if zoneName != "" && !strings.HasSuffix(ch.Spec.OriginServer, "."+zoneName) && ch.Spec.OriginServer != zoneName {
		return ctrl.Result{}, r.setError(ctx, &ch, "OriginNotInZone",
			fmt.Sprintf("originServer %q must belong to zone %q", ch.Spec.OriginServer, zoneName))
	}

	// Handle deletion
	if !ch.DeletionTimestamp.IsZero() {
		return r.handleDelete(ctx, cf, zoneID, &ch)
	}

	// Ensure finalizer
	if !controllerutil.ContainsFinalizer(&ch, finalizerName) {
		controllerutil.AddFinalizer(&ch, finalizerName)
		if err := r.Update(ctx, &ch); err != nil {
			return ctrl.Result{}, err
		}
		log.V(1).Info("added finalizer", "hostname", ch.Spec.Hostname)
		return ctrl.Result{Requeue: true}, nil
	}

	// Conflict detection: reject if another CR already owns this hostname in Cloudflare.
	// O(1) via field index. Returns early without any CF API call; no requeue scheduled
	// (the Zone controller skips drift-enqueue for conflict CRs, so this stays quiet until
	// the conflict resolves — at which point the Zone controller re-enqueues via the
	// "hostname missing from CF" path).
	if conflicted, err := r.detectConflict(ctx, &ch); err != nil || conflicted {
		return ctrl.Result{}, err
	}

	// Always resolve current state from Cloudflare by hostname
	// This makes reconciliation idempotent across restarts and crash-recovery scenarios
	return r.reconcileCloudflareState(ctx, cf, zoneID, zoneName, &ch)
}

func (r *CustomHostnameReconciler) reconcileCloudflareState(ctx context.Context, cf *cloudflare.Client, zoneID, zoneName string, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmtPolicy := effectiveManagementPolicy(ch.Spec.ManagementPolicy)

	// Single Cloudflare API call: find existing hostname by name
	existing, err := r.findByHostname(ctx, cf, zoneID, ch.Spec.Hostname)
	if err != nil {
		log.Error(err, "failed to look up custom hostname", "hostname", ch.Spec.Hostname)
		return ctrl.Result{}, r.setError(ctx, ch, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmtPolicy == ManagementPolicyObserve {
			// Observe mode: don't create, wait for external provisioning.
			// The zone controller will re-enqueue when the hostname appears in CF.
			log.Info("observe: hostname not found in Cloudflare, waiting for external creation",
				"hostname", ch.Spec.Hostname)
			return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionFalse,
				"WaitingForExternal", "Hostname not yet provisioned in Cloudflare")
		}
		// manage or create: provision it
		return r.handleCreate(ctx, cf, zoneID, zoneName, ch)
	}

	// Exists — adopt ID and sync status
	ch.Status.ID = existing.ID
	log.Info("adopted existing custom hostname",
		"hostname", ch.Spec.Hostname,
		"id", existing.ID,
		"origin", existing.CustomOriginServer,
		"sni", existing.CustomOriginSNI)
	operationsTotal.WithLabelValues(cfResourceCustomHostname, cfOpAdopt).Inc()

	// Check drift — only correct it if management policy is "manage"
	if existing.CustomOriginServer != ch.Spec.OriginServer || sniDrifted(existing.CustomOriginSNI, ch) {
		if mgmtPolicy != ManagementPolicyManage {
			log.Info("drift detected but suppressed by managementPolicy",
				"hostname", ch.Spec.Hostname,
				"policy", mgmtPolicy,
				"currentOrigin", existing.CustomOriginServer,
				"desiredOrigin", ch.Spec.OriginServer,
				"currentSNI", existing.CustomOriginSNI,
				"desiredSNI", ch.Spec.OriginSNI)
		} else {
			log.Info("custom hostname drifted, updating",
				"hostname", ch.Spec.Hostname,
				"currentOrigin", existing.CustomOriginServer,
				"desiredOrigin", ch.Spec.OriginServer,
				"currentSNI", existing.CustomOriginSNI,
				"desiredSNI", ch.Spec.OriginSNI)
			editParams := custom_hostnames.CustomHostnameEditParams{ZoneID: cloudflare.F(zoneID)}
			opts := []option.RequestOption{option.WithJSONSet("custom_origin_server", ch.Spec.OriginServer)}
			if ch.Spec.OriginSNI != nil {
				opts = append(opts, option.WithJSONSet("custom_origin_sni", *ch.Spec.OriginSNI))
			}
			if r.DryRun {
				log.Info("dry-run: would update custom hostname", "hostname", ch.Spec.Hostname,
					"currentOrigin", existing.CustomOriginServer, "desiredOrigin", ch.Spec.OriginServer)
				return ctrl.Result{}, nil
			}
			editStart := time.Now()
			_, editErr := cf.CustomHostnames.Edit(ctx, existing.ID, editParams, opts...)
			recordCFCall(cfResourceCustomHostname, cfOpUpdate, editStart, &editErr)
			if editErr != nil {
				log.Error(editErr, "failed to update custom hostname", "id", existing.ID)
				return ctrl.Result{}, r.setError(ctx, ch, "UpdateFailed", editErr.Error())
			}
			operationsTotal.WithLabelValues(cfResourceCustomHostname, cfOpUpdate).Inc()
		}
	}

	if r.DryRun {
		return ctrl.Result{}, nil
	}
	ch.Status.SSL = sslStatusFromList(existing)
	if err := r.Status().Update(ctx, ch); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	return r.requeueOrReady(ctx, zoneName, ch)
}

// effectiveManagementPolicy returns the management policy, defaulting to "manage".
func effectiveManagementPolicy(policy string) string {
	if policy == "" {
		return ManagementPolicyManage
	}
	return policy
}

func (r *CustomHostnameReconciler) handleCreate(ctx context.Context, cf *cloudflare.Client, zoneID, zoneName string, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	params := custom_hostnames.CustomHostnameNewParams{
		ZoneID:   cloudflare.F(zoneID),
		Hostname: cloudflare.F(ch.Spec.Hostname),
	}
	// Default to DV + HTTP if not specified — SSL is always required for custom hostnames.
	sslSpec := ch.Spec.SSL
	if sslSpec == nil {
		sslSpec = &saasv1beta1.CustomHostnameSSL{Type: "dv", Method: "http"}
	}
	params.SSL = cloudflare.F(buildSSLParams(sslSpec))
	opts := []option.RequestOption{option.WithJSONSet("custom_origin_server", ch.Spec.OriginServer)}
	if ch.Spec.OriginSNI != nil {
		opts = append(opts, option.WithJSONSet("custom_origin_sni", *ch.Spec.OriginSNI))
	}

	if r.DryRun {
		log.Info("dry-run: would create custom hostname", "hostname", ch.Spec.Hostname, "origin", ch.Spec.OriginServer)
		return ctrl.Result{}, nil
	}

	createStart := time.Now()
	resp, err := cf.CustomHostnames.New(ctx, params, opts...)
	recordCFCall(cfResourceCustomHostname, cfOpCreate, createStart, &err)
	if err != nil {
		log.Error(err, "failed to create custom hostname", "hostname", ch.Spec.Hostname)
		return ctrl.Result{}, r.setError(ctx, ch, "CreateFailed", err.Error())
	}

	// Distinguish initial create from recreation after external deletion.
	// createCount > 0 means the hostname existed before and was deleted externally.
	isRecreation := ch.Status.CreateCount > 0
	op := cfOpCreate
	if isRecreation {
		op = cfOpRecreate
		log.Info("custom hostname recreated", "hostname", ch.Spec.Hostname, "id", resp.ID)
	} else {
		log.Info("custom hostname created", "hostname", ch.Spec.Hostname, "id", resp.ID)
	}
	operationsTotal.WithLabelValues(cfResourceCustomHostname, op).Inc()

	// Reset SSL provisioning timer on every (re)create so the metric reflects the current cycle.
	now := metav1.Now()
	ch.Status.SSLProvisioningStartedAt = &now
	ch.Status.CreateCount++
	ch.Status.ID = resp.ID
	ch.Status.SSL = sslStatusFromNew(resp)
	if err := r.Status().Update(ctx, ch); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status after create: %w", err)
	}
	return r.requeueOrReady(ctx, zoneName, ch)
}

func (r *CustomHostnameReconciler) handleDelete(ctx context.Context, cf *cloudflare.Client, zoneID string, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		// Do NOT remove the finalizer — the CR stays in Terminating state while dry-run
		// is active. When the operator restarts without --dry-run, deletion proceeds normally.
		// Returning nil (no requeue) is intentional: the dry-run message fires once per
		// deletion attempt and then stays quiet. The 30 s SSL-pending requeue cycle is
		// also dropped here, so no further reconciles are scheduled for this CR.
		log.Info("dry-run: would delete custom hostname from Cloudflare", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
		return ctrl.Result{}, nil
	}

	if controllerutil.ContainsFinalizer(ch, finalizerName) && ch.Status.ID != "" {
		// Observe mode supersedes deletePolicy: never touch CF, release finalizer unconditionally.
		mgmtPolicy := effectiveManagementPolicy(ch.Spec.ManagementPolicy)
		if mgmtPolicy == ManagementPolicyObserve {
			log.Info("observe: releasing finalizer without deleting from Cloudflare",
				"hostname", ch.Spec.Hostname, "id", ch.Status.ID)
			controllerutil.RemoveFinalizer(ch, finalizerName)
			return ctrl.Result{}, r.Update(ctx, ch)
		}

		// For own-only policy: look up current CF state before deleting.
		// If another entity now owns this hostname (different ID), release the finalizer without deleting.
		// spec.deletePolicy takes precedence over the operator-wide --delete-policy flag.
		policy := effectiveDeletePolicy(ch.Spec.DeletePolicy, r.DeletePolicy)
		if policy == DeletePolicyNever {
			log.Info("never: releasing finalizer without deleting from Cloudflare",
				"hostname", ch.Spec.Hostname, "id", ch.Status.ID)
			controllerutil.RemoveFinalizer(ch, finalizerName)
			return ctrl.Result{}, r.Update(ctx, ch)
		}
		if policy == DeletePolicyOwnOnly {
			current, err := r.findByHostname(ctx, cf, zoneID, ch.Spec.Hostname)
			if err != nil {
				log.Error(err, "failed to look up hostname before delete", "hostname", ch.Spec.Hostname)
				return ctrl.Result{}, err
			}
			if !shouldDeleteInCF(ch.Status.ID, current) {
				log.Info("own-only: releasing finalizer without deleting",
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

		deleteStart := time.Now()
		_, delErr := cf.CustomHostnames.Delete(ctx, ch.Status.ID, custom_hostnames.CustomHostnameDeleteParams{
			ZoneID: cloudflare.F(zoneID),
		})
		recordCFCall(cfResourceCustomHostname, cfOpDelete, deleteStart, &delErr)
		if delErr != nil {
			// 404 means the resource is already gone (e.g. deleted by another entity or stale ID).
			// Treat as success — our specific resource no longer exists, remove finalizer.
			if cfErr, ok := delErr.(*cloudflare.Error); ok && cfErr.StatusCode == 404 {
				log.Info("custom hostname already gone from Cloudflare, releasing finalizer", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
			} else {
				log.Error(delErr, "failed to delete custom hostname", "id", ch.Status.ID)
				return ctrl.Result{}, delErr
			}
		} else {
			log.Info("custom hostname deleted from Cloudflare", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
		}
		operationsTotal.WithLabelValues(cfResourceCustomHostname, cfOpDelete).Inc()
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

func (r *CustomHostnameReconciler) findByHostname(ctx context.Context, cf *cloudflare.Client, zoneID, hostname string) (*custom_hostnames.CustomHostnameListResponse, error) {
	start := time.Now()
	pager := cf.CustomHostnames.ListAutoPaging(ctx, custom_hostnames.CustomHostnameListParams{
		ZoneID:   cloudflare.F(zoneID),
		Hostname: cloudflare.F(hostname),
	})
	for pager.Next() {
		ch := pager.Current()
		if ch.Hostname == hostname {
			err := pager.Err()
			recordCFCall(cfResourceCustomHostname, cfOpGet, start, &err)
			return &ch, err
		}
	}
	err := pager.Err()
	recordCFCall(cfResourceCustomHostname, cfOpGet, start, &err)
	return nil, err
}

func (r *CustomHostnameReconciler) requeueOrReady(ctx context.Context, zoneName string, ch *saasv1beta1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	ch.Status.ConsecutiveErrors = 0
	if ch.Status.SSL != nil && ch.Status.SSL.Status == "active" {
		// Observe SSL provisioning duration on first transition to active.
		// Guard against double-counting on operator restart: skip if Ready=True is already set.
		if ch.Status.SSLProvisioningStartedAt != nil {
			alreadyReady := false
			for _, cond := range ch.Status.Conditions {
				if cond.Type == conditionReady && cond.Status == metav1.ConditionTrue {
					alreadyReady = true
					break
				}
			}
			if !alreadyReady {
				method := "http"
				if ch.Spec.SSL != nil && ch.Spec.SSL.Method != "" {
					method = ch.Spec.SSL.Method
				}
				duration := time.Since(ch.Status.SSLProvisioningStartedAt.Time)
				sslProvisioningDuration.WithLabelValues(zoneName, ch.Spec.Hostname, method).Observe(duration.Seconds())
				log.Info("SSL provisioning complete", "hostname", ch.Spec.Hostname, "duration", duration.Round(time.Second), "method", method)
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
// The caller should return immediately on (true, nil) — no requeue is scheduled.
// Self-healing: when the owning CR is deleted, the Zone controller re-enqueues this CR via the
// "hostname missing from CF" path, at which point detectConflict finds no peer with an ID and
// returns false, allowing normal provisioning to proceed.
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
			log.Info("hostname conflict: already managed by another CR",
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

func (r *CustomHostnameReconciler) buildCloudflareClient(ctx context.Context, ch *saasv1beta1.CustomHostname) (*cloudflare.Client, string, string, error) {
	zoneNS := ch.Spec.ZoneRef.Namespace
	if zoneNS == "" {
		zoneNS = r.OperatorNamespace
	}
	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, types.NamespacedName{Name: ch.Spec.ZoneRef.Name, Namespace: zoneNS}, &zone); err != nil {
		return nil, "", "", fmt.Errorf("zone %q not found: %w", ch.Spec.ZoneRef.Name, err)
	}
	key := zone.Spec.CredentialsRef.Key
	if key == "" {
		key = "apiToken"
	}
	var secret corev1.Secret
	if err := r.Get(ctx, types.NamespacedName{Name: zone.Spec.CredentialsRef.Name, Namespace: zone.Namespace}, &secret); err != nil {
		return nil, "", "", fmt.Errorf("secret %q not found: %w", zone.Spec.CredentialsRef.Name, err)
	}
	token, ok := secret.Data[key]
	if !ok {
		return nil, "", "", fmt.Errorf("key %q not found in secret %q", key, zone.Spec.CredentialsRef.Name)
	}
	return cloudflare.NewClient(option.WithAPIToken(string(token))), zone.Spec.ID, zone.Status.Name, nil
}

func sniDrifted(currentSNI string, ch *saasv1beta1.CustomHostname) bool {
	if ch.Spec.OriginSNI == nil {
		// Nil means "don't manage SNI" — external changes are not corrected.
		// Matches hasDrift() in the zone controller.
		return false
	}
	return currentSNI != *ch.Spec.OriginSNI
}

func buildSSLParams(ssl *saasv1beta1.CustomHostnameSSL) custom_hostnames.CustomHostnameNewParamsSSL {
	p := custom_hostnames.CustomHostnameNewParamsSSL{}
	if ssl.Type != "" {
		p.Type = cloudflare.F(custom_hostnames.DomainValidationType(ssl.Type))
	}
	if ssl.Method != "" {
		p.Method = cloudflare.F(custom_hostnames.DCVMethod(ssl.Method))
	}
	if ssl.CertificateAuthority != "" {
		p.CertificateAuthority = cloudflare.F(cloudflare.CertificateCA(ssl.CertificateAuthority))
	}
	if ssl.MinTLSVersion != "" {
		p.Settings = cloudflare.F(custom_hostnames.CustomHostnameNewParamsSSLSettings{
			MinTLSVersion: cloudflare.F(custom_hostnames.CustomHostnameNewParamsSSLSettingsMinTLSVersion(ssl.MinTLSVersion)),
		})
	}
	return p
}

func sslStatusFromNew(resp *custom_hostnames.CustomHostnameNewResponse) *saasv1beta1.CustomHostnameSSLStatus {
	s := &saasv1beta1.CustomHostnameSSLStatus{Status: string(resp.SSL.Status)}
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

func sslStatusFromList(resp *custom_hostnames.CustomHostnameListResponse) *saasv1beta1.CustomHostnameSSLStatus {
	s := &saasv1beta1.CustomHostnameSSLStatus{Status: string(resp.SSL.Status)}
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
//   - status.ID != "" → existing CR, already provisioned; drop it and let the
//     Zone coordinator's periodic bulk-list handle drift detection (including
//     SSL status transitions: initializing → pending_validation → active).
//   - status.ID == "" → genuinely new CR (or crash-recovery case); let it through
//     for immediate provisioning.
//   - DeletionTimestamp set → terminating CR; always let it through regardless of
//     status.ID so the finalizer is removed and the CR can be fully deleted.
//     Without this, a restart with a terminating CR would leave it stuck forever.
//
// NOTE: this predicate is coupled to status.ID as the "seen before" signal.
// If the state model changes (e.g. ID moved to a different field), update this predicate.
func fastWritePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			ch, ok := e.Object.(*saasv1beta1.CustomHostname)
			if !ok {
				return true
			}
			// Always process CRs with a DeletionTimestamp — they need finalizer removal.
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
