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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	"github.com/cloudflare/cloudflare-go/v6/option"

	cfv1alpha1 "github.com/mrozentsvayg/cf-edge-operator/api/v1alpha1"
)

const (
	finalizerName     = "cf.cf-edge.io/customhostname"
	requeuePendingSSL = 30 * time.Second
)

// CustomHostnameReconciler reconciles a CustomHostname object.
// It acts as the worker: handles individual Cloudflare API writes (create/update/delete).
// Triggered by spec changes and by the Zone coordinator via the event channel on drift detection.
type CustomHostnameReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	OperatorNamespace string
}

// +kubebuilder:rbac:groups=cf.cf-edge.io,resources=customhostnames,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cf.cf-edge.io,resources=customhostnames/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cf.cf-edge.io,resources=customhostnames/finalizers,verbs=update
// +kubebuilder:rbac:groups=cf.cf-edge.io,resources=zones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *CustomHostnameReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var ch cfv1alpha1.CustomHostname
	if err := r.Get(ctx, req.NamespacedName, &ch); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cf, zoneID, zoneName, err := r.buildCloudflareClient(ctx, &ch)
	if err != nil {
		log.Error(err, "failed to build Cloudflare client")
		return ctrl.Result{}, r.setCondition(ctx, &ch, metav1.ConditionFalse, "ZoneError", err.Error())
	}

	// Validate that the origin server belongs to the zone — Cloudflare for SaaS is zone-scoped.
	if zoneName != "" && !strings.HasSuffix(ch.Spec.OriginServer, "."+zoneName) && ch.Spec.OriginServer != zoneName {
		return ctrl.Result{}, r.setCondition(ctx, &ch, metav1.ConditionFalse, "OriginNotInZone",
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
		return ctrl.Result{Requeue: true}, nil
	}

	// Always resolve current state from Cloudflare by hostname
	// This makes reconciliation idempotent across restarts and crash-recovery scenarios
	return r.reconcileCloudflareState(ctx, cf, zoneID, &ch)
}

func (r *CustomHostnameReconciler) reconcileCloudflareState(ctx context.Context, cf *cloudflare.Client, zoneID string, ch *cfv1alpha1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	// Single Cloudflare API call: find existing hostname by name
	existing, err := r.findByHostname(ctx, cf, zoneID, ch.Spec.Hostname)
	if err != nil {
		log.Error(err, "failed to look up custom hostname", "hostname", ch.Spec.Hostname)
		return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionFalse, "LookupFailed", err.Error())
	}

	if existing == nil {
		// Not in Cloudflare — create it
		return r.handleCreate(ctx, cf, zoneID, ch)
	}

	// Exists — adopt ID and sync if drifted
	ch.Status.ID = existing.ID
	if existing.CustomOriginServer != ch.Spec.OriginServer || sniDrifted(existing.CustomOriginSNI, ch) {
		log.Info("custom hostname drifted, updating",
			"hostname", ch.Spec.Hostname,
			"currentOrigin", existing.CustomOriginServer,
			"desiredOrigin", ch.Spec.OriginServer,
			"currentSNI", existing.CustomOriginSNI,
			"desiredSNI", ch.Spec.OriginSNI)
		editParams := custom_hostnames.CustomHostnameEditParams{ZoneID: cloudflare.F(zoneID)}
		opts := []option.RequestOption{option.WithJSONSet("custom_origin_server", ch.Spec.OriginServer)}
		if ch.Spec.OriginSNI != nil && *ch.Spec.OriginSNI != ch.Spec.OriginServer {
			opts = append(opts, option.WithJSONSet("custom_origin_sni", *ch.Spec.OriginSNI))
		}
		if _, err := cf.CustomHostnames.Edit(ctx, existing.ID, editParams, opts...); err != nil {
			log.Error(err, "failed to update custom hostname", "id", existing.ID)
			return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionFalse, "UpdateFailed", err.Error())
		}
	}

	ch.Status.SSL = sslStatusFromList(existing)
	if err := r.Status().Update(ctx, ch); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status: %w", err)
	}
	return r.requeueOrReady(ctx, ch)
}

func (r *CustomHostnameReconciler) handleCreate(ctx context.Context, cf *cloudflare.Client, zoneID string, ch *cfv1alpha1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	params := custom_hostnames.CustomHostnameNewParams{
		ZoneID:   cloudflare.F(zoneID),
		Hostname: cloudflare.F(ch.Spec.Hostname),
	}
	// Default to DV + HTTP if not specified — SSL is always required for custom hostnames.
	sslSpec := ch.Spec.SSL
	if sslSpec == nil {
		sslSpec = &cfv1alpha1.CustomHostnameSSL{Type: "dv", Method: "http"}
	}
	params.SSL = cloudflare.F(buildSSLParams(sslSpec))
	opts := []option.RequestOption{option.WithJSONSet("custom_origin_server", ch.Spec.OriginServer)}
	if ch.Spec.OriginSNI != nil && *ch.Spec.OriginSNI != ch.Spec.OriginServer {
		opts = append(opts, option.WithJSONSet("custom_origin_sni", *ch.Spec.OriginSNI))
	}

	resp, err := cf.CustomHostnames.New(ctx, params, opts...)
	if err != nil {
		log.Error(err, "failed to create custom hostname", "hostname", ch.Spec.Hostname)
		return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionFalse, "CreateFailed", err.Error())
	}

	log.Info("custom hostname created", "hostname", ch.Spec.Hostname, "id", resp.ID)
	ch.Status.ID = resp.ID
	ch.Status.SSL = sslStatusFromNew(resp)
	if err := r.Status().Update(ctx, ch); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update status after create: %w", err)
	}
	return r.requeueOrReady(ctx, ch)
}

func (r *CustomHostnameReconciler) handleDelete(ctx context.Context, cf *cloudflare.Client, zoneID string, ch *cfv1alpha1.CustomHostname) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if controllerutil.ContainsFinalizer(ch, finalizerName) && ch.Status.ID != "" {
		_, err := cf.CustomHostnames.Delete(ctx, ch.Status.ID, custom_hostnames.CustomHostnameDeleteParams{
			ZoneID: cloudflare.F(zoneID),
		})
		if err != nil {
			// 404 means the resource is already gone (e.g. deleted by another entity or stale ID).
			// Treat as success — our specific resource no longer exists, remove finalizer.
			if cfErr, ok := err.(*cloudflare.Error); ok && cfErr.StatusCode == 404 {
				log.Info("custom hostname already gone from Cloudflare, releasing finalizer", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
			} else {
				log.Error(err, "failed to delete custom hostname", "id", ch.Status.ID)
				return ctrl.Result{}, err
			}
		} else {
			log.Info("custom hostname deleted from Cloudflare", "hostname", ch.Spec.Hostname, "id", ch.Status.ID)
		}
	}

	controllerutil.RemoveFinalizer(ch, finalizerName)
	return ctrl.Result{}, r.Update(ctx, ch)
}

func (r *CustomHostnameReconciler) findByHostname(ctx context.Context, cf *cloudflare.Client, zoneID, hostname string) (*custom_hostnames.CustomHostnameListResponse, error) {
	pager := cf.CustomHostnames.ListAutoPaging(ctx, custom_hostnames.CustomHostnameListParams{
		ZoneID:   cloudflare.F(zoneID),
		Hostname: cloudflare.F(hostname),
	})
	for pager.Next() {
		ch := pager.Current()
		if ch.Hostname == hostname {
			return &ch, nil
		}
	}
	return nil, pager.Err()
}

func (r *CustomHostnameReconciler) requeueOrReady(ctx context.Context, ch *cfv1alpha1.CustomHostname) (ctrl.Result, error) {
	if ch.Status.SSL != nil && ch.Status.SSL.Status == "active" {
		return ctrl.Result{}, r.setCondition(ctx, ch, metav1.ConditionTrue, "Ready", "Custom hostname is active")
	}
	sslStatus := "unknown"
	if ch.Status.SSL != nil {
		sslStatus = ch.Status.SSL.Status
	}
	if err := r.setCondition(ctx, ch, metav1.ConditionFalse, "SSLPending", fmt.Sprintf("SSL status: %s", sslStatus)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeuePendingSSL}, nil
}

func (r *CustomHostnameReconciler) setCondition(ctx context.Context, ch *cfv1alpha1.CustomHostname, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&ch.Status.Conditions, metav1.Condition{
		Type:               "Ready",
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

func (r *CustomHostnameReconciler) buildCloudflareClient(ctx context.Context, ch *cfv1alpha1.CustomHostname) (*cloudflare.Client, string, string, error) {
	zoneNS := ch.Spec.ZoneRef.Namespace
	if zoneNS == "" {
		zoneNS = r.OperatorNamespace
	}
	var zone cfv1alpha1.Zone
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

func sniDrifted(currentSNI string, ch *cfv1alpha1.CustomHostname) bool {
	if ch.Spec.OriginSNI == nil {
		return currentSNI != "" && currentSNI != ch.Spec.OriginServer
	}
	return currentSNI != *ch.Spec.OriginSNI
}

func buildSSLParams(ssl *cfv1alpha1.CustomHostnameSSL) custom_hostnames.CustomHostnameNewParamsSSL {
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

func sslStatusFromNew(resp *custom_hostnames.CustomHostnameNewResponse) *cfv1alpha1.CustomHostnameSSLStatus {
	s := &cfv1alpha1.CustomHostnameSSLStatus{Status: string(resp.SSL.Status)}
	if !resp.SSL.ExpiresOn.IsZero() {
		t := metav1.NewTime(resp.SSL.ExpiresOn)
		s.ExpiresOn = &t
	}
	for _, vr := range resp.SSL.ValidationRecords {
		s.ValidationRecords = append(s.ValidationRecords, cfv1alpha1.SSLValidationRecord{
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

func sslStatusFromList(resp *custom_hostnames.CustomHostnameListResponse) *cfv1alpha1.CustomHostnameSSLStatus {
	s := &cfv1alpha1.CustomHostnameSSLStatus{Status: string(resp.SSL.Status)}
	if !resp.SSL.ExpiresOn.IsZero() {
		t := metav1.NewTime(resp.SSL.ExpiresOn)
		s.ExpiresOn = &t
	}
	for _, vr := range resp.SSL.ValidationRecords {
		s.ValidationRecords = append(s.ValidationRecords, cfv1alpha1.SSLValidationRecord{
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
//     Zone coordinator's periodic bulk-list handle drift detection.
//   - status.ID == "" → genuinely new CR (or crash-recovery case); let it through
//     for immediate provisioning.
//
// NOTE: this predicate is coupled to status.ID as the "seen before" signal.
// If the state model changes (e.g. ID moved to a different field), update this predicate.
func fastWritePredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			ch, ok := e.Object.(*cfv1alpha1.CustomHostname)
			if !ok {
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
	return ctrl.NewControllerManagedBy(mgr).
		For(&cfv1alpha1.CustomHostname{}, builder.WithPredicates(fastWritePredicate())).
		WatchesRawSource(source.Channel(driftEvents, &handler.EnqueueRequestForObject{})).
		Named("customhostname").
		Complete(r)
}
