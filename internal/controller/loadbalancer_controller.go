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
	"slices"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/load_balancers"
	"github.com/cloudflare/cloudflare-go/v6/option"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

const finalizerNameLB = "loadbalancing.cf-edge.io/loadbalancer"

// LoadBalancerReconciler reconciles a LoadBalancer object.
//
// LoadBalancers are Cloudflare zone-scoped (the zone provides both the DNS
// record for the LB hostname and the API credentials). Origin pools are
// account-scoped; the LB's DefaultPoolRefs / FallbackPoolRef / RegionPools
// etc. reference LoadBalancerPool CRs by name.
//
// All pools are managed by this operator, so every pool ref resolves from a
// local LoadBalancerPool CR (read its Status.ID). Resolution is best-effort:
// a ref whose CR is absent or not yet provisioned is dropped from the CF LB
// config and recorded on status for observability, so a single unready pool
// doesn't block the whole endpoint. The LB->Pool watch re-triggers the LB as
// each pool becomes ready, so it converges to the full set. Cloudflare's hard
// minimum -- a resolvable fallback pool plus at least one default pool -- is
// still required before the LB is written.
type LoadBalancerReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	OperatorNamespace string
	ManagementPolicy  string
	DeletePolicy      string
	DryRun            bool
	CFAPITimeout      time.Duration
	CFAPIWriteTimeout time.Duration
	CFAPIMaxRetries   int
	CFAPIWriteDelay   time.Duration
	CFBaseURL         string
	// RequeueInterval is how often a reconciled LoadBalancer re-reconciles to
	// re-check for external Cloudflare drift and to retry after transient
	// errors. Unlike CustomHostname there is no Zone-style coordinator driving
	// periodic re-checks, so each controller self-requeues. Set from
	// --drift-interval.
	RequeueInterval time.Duration
}

// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancers/finalizers,verbs=update
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancerpools,verbs=get;list;watch
// +kubebuilder:rbac:groups=domains.cf-edge.io,resources=zones,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *LoadBalancerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var lb lbv1beta1.LoadBalancer
	if err := r.Get(ctx, req.NamespacedName, &lb); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !lb.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&lb, finalizerNameLB) && lb.Status.ID == "" {
			log.Info("loadbalancer - could not be deleted, finalizer released (never established with Cloudflare)",
				"hostname", lb.Spec.Hostname)
			controllerutil.RemoveFinalizer(&lb, finalizerNameLB)
			return ctrl.Result{}, r.Update(ctx, &lb)
		}
		if controllerutil.ContainsFinalizer(&lb, finalizerNameLB) {
			mgmt := effectiveManagementPolicy(lb.Spec.ManagementPolicy, r.ManagementPolicy)
			if mgmt == ManagementPolicyObserve {
				log.Info("loadbalancer - not deleted, finalizer released (managementPolicy=observe)",
					"hostname", lb.Spec.Hostname, "id", lb.Status.ID)
				controllerutil.RemoveFinalizer(&lb, finalizerNameLB)
				return ctrl.Result{}, r.Update(ctx, &lb)
			}
			del := effectiveDeletePolicy(lb.Spec.DeletePolicy, r.DeletePolicy)
			if del == DeletePolicyNever {
				log.Info("loadbalancer - not deleted, finalizer released (deletePolicy=never)",
					"hostname", lb.Spec.Hostname, "id", lb.Status.ID)
				controllerutil.RemoveFinalizer(&lb, finalizerNameLB)
				return ctrl.Result{}, r.Update(ctx, &lb)
			}
		}
		zi, err := r.buildZoneClient(ctx, &lb)
		if err != nil {
			log.Error(err, "loadbalancer - client initialization failed")
			return ctrl.Result{}, err
		}
		return r.handleDelete(ctx, zi, &lb)
	}

	zi, err := r.buildZoneClient(ctx, &lb)
	if err != nil {
		log.Error(err, "loadbalancer - client initialization failed")
		return r.setError(ctx, &lb, "ZoneError", err.Error())
	}

	if !controllerutil.ContainsFinalizer(&lb, finalizerNameLB) {
		controllerutil.AddFinalizer(&lb, finalizerNameLB)
		if err := r.Update(ctx, &lb); err != nil {
			return ctrl.Result{}, err
		}
		log.V(1).Info("loadbalancer - finalizer added", "hostname", lb.Spec.Hostname)
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve referenced pools from local Pool CRs (best-effort: unresolved
	// refs are recorded and dropped, not fatal). The LB->Pool watch re-fires
	// this reconcile as each pool becomes ready.
	resolved, err := r.resolveAllPools(ctx, &lb)
	if err != nil {
		return r.setError(ctx, &lb, "PoolResolutionError", err.Error())
	}
	lb.Status.ResolvedDefaultPoolIDs = resolved.defaultIDs
	lb.Status.ResolvedFallbackPoolID = resolved.fallbackID

	// CF requires a non-empty fallback pool (and at least one default pool,
	// which resolveAllPools guarantees by promoting the fallback when every
	// default ref is unresolved). Until the fallback resolves there is no
	// valid LB to write, so wait for it. This is a wait-for-dependency state,
	// not a reconcile error (mirrors WaitingForMonitor): the pool->LB watch
	// re-drives when the fallback pool becomes ready, backed by the periodic
	// self-requeue.
	if resolved.fallbackID == "" {
		msg := fmt.Sprintf("Fallback pool %q not yet resolved (required by Cloudflare)",
			lb.Spec.FallbackPoolRef.Name)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, &lb, metav1.ConditionFalse, "WaitingForFallbackPool", msg)
	}

	return r.reconcileCloudflareState(ctx, zi, &lb, resolved)
}

func (r *LoadBalancerReconciler) reconcileCloudflareState(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmt := effectiveManagementPolicy(lb.Spec.ManagementPolicy, r.ManagementPolicy)

	var existing *load_balancers.LoadBalancer
	attempts, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpGet, r.CFAPIMaxRetries, func() error {
		var callErr error
		existing, callErr = r.findLoadBalancerByHostname(ctx, zi, lb.Spec.Hostname)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancer - lookup failed", "hostname", lb.Spec.Hostname, "attempts", attempts)
		return r.setError(ctx, lb, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmt == ManagementPolicyObserve {
			log.Info("loadbalancer - not creating (managementPolicy=observe)", "hostname", lb.Spec.Hostname)
			lb.Status.ConsecutiveErrors = 0
			return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, lb, metav1.ConditionFalse,
				"WaitingForExternal", "LoadBalancer not yet provisioned in Cloudflare")
		}
		return r.handleCreate(ctx, zi, lb, resolved)
	}

	if lb.Status.ID != existing.ID {
		if lb.Status.ID == "" {
			log.Info("loadbalancer - adopted", "hostname", lb.Spec.Hostname, "id", existing.ID)
		} else {
			log.Info("loadbalancer - readopted (externally recreated)",
				"hostname", lb.Spec.Hostname, "previousID", lb.Status.ID, "newID", existing.ID)
		}
		operationsTotal.WithLabelValues(cfResourceLoadBalancer, cfOpAdopt).Inc()
		lb.Status.ID = existing.ID
	}

	if lbDrifted(existing, lb, resolved) {
		if mgmt != ManagementPolicyManage {
			log.Info(fmt.Sprintf("loadbalancer - not updating, drift detected (managementPolicy=%s)", mgmt),
				"hostname", lb.Spec.Hostname)
		} else if r.DryRun {
			log.Info("loadbalancer - not updating, drift detected (dry-run)", "hostname", lb.Spec.Hostname)
		} else {
			log.Info("loadbalancer - updating, drift detected", "hostname", lb.Spec.Hostname)
			updated, updErr := r.editLoadBalancer(ctx, zi, lb, existing.ID, resolved)
			if updErr != nil {
				return r.setError(ctx, lb, "UpdateFailed", updErr.Error())
			}
			existing = updated
		}
	}
	_ = existing

	return r.markReady(ctx, lb, resolved)
}

func (r *LoadBalancerReconciler) handleCreate(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancer - not creating (dry-run)", "hostname", lb.Spec.Hostname)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, lb, metav1.ConditionFalse, "DryRun", "not creating (dry-run)")
	}

	params := buildLBNewParams(zi.ID, lb, resolved)
	var resp *load_balancers.LoadBalancer
	attempts, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpCreate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = zi.Client.LoadBalancers.New(ctx, params,
			option.WithRequestTimeout(r.CFAPIWriteTimeout))
		recordCFCall(cfResourceLoadBalancer, cfOpCreate, start, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancer - create failed", "hostname", lb.Spec.Hostname, "attempts", attempts)
		return r.setError(ctx, lb, "CreateFailed", err.Error())
	}
	isRecreation := lb.Status.CreateCount > 0
	op := cfOpCreate
	if isRecreation {
		op = cfOpRecreate
		log.Info("loadbalancer - recreated", "hostname", lb.Spec.Hostname, "id", resp.ID)
	} else {
		log.Info("loadbalancer - created", "hostname", lb.Spec.Hostname, "id", resp.ID)
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancer, op).Inc()
	r.paceWrite()

	lb.Status.ID = resp.ID
	lb.Status.CreateCount++
	return r.markReady(ctx, lb, resolved)
}

func (r *LoadBalancerReconciler) editLoadBalancer(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer, cfID string, resolved *resolvedPools) (*load_balancers.LoadBalancer, error) {
	log := logf.FromContext(ctx)
	// Edit (PATCH, partial): correct only the CRD-modeled fields and leave
	// un-modeled Cloudflare LB config (rules, adaptive_routing, session affinity
	// attributes, ...) intact. The pool topology the CR owns (default/fallback
	// and geo pools) is always sent, so removing pools from the CR clears them.
	params := buildLBEditParams(zi.ID, lb, resolved)
	var resp *load_balancers.LoadBalancer
	attempts, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpUpdate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = zi.Client.LoadBalancers.Edit(ctx, cfID, params,
			option.WithRequestTimeout(r.CFAPIWriteTimeout))
		recordCFCall(cfResourceLoadBalancer, cfOpUpdate, start, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancer - update failed", "id", cfID, "attempts", attempts)
		return nil, err
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancer, cfOpUpdate).Inc()
	r.paceWrite()
	return resp, nil
}

func (r *LoadBalancerReconciler) handleDelete(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancer - not deleted, finalizer released (dry-run)", "hostname", lb.Spec.Hostname, "id", lb.Status.ID)
		controllerutil.RemoveFinalizer(lb, finalizerNameLB)
		return ctrl.Result{}, r.Update(ctx, lb)
	}

	if controllerutil.ContainsFinalizer(lb, finalizerNameLB) {
		policy := effectiveDeletePolicy(lb.Spec.DeletePolicy, r.DeletePolicy)
		if policy == DeletePolicyOwnOnly {
			var current *load_balancers.LoadBalancer
			_, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpGet, r.CFAPIMaxRetries, func() error {
				var callErr error
				current, callErr = r.findLoadBalancerByHostname(ctx, zi, lb.Spec.Hostname)
				return callErr
			})
			if err != nil {
				log.Error(err, "loadbalancer - pre-delete lookup failed (deletePolicy=own-only)", "hostname", lb.Spec.Hostname)
				return ctrl.Result{}, err
			}
			if current == nil || current.ID != lb.Status.ID {
				log.Info("loadbalancer - not deleted, finalizer released (deletePolicy=own-only)",
					"hostname", lb.Spec.Hostname, "statusID", lb.Status.ID)
				controllerutil.RemoveFinalizer(lb, finalizerNameLB)
				return ctrl.Result{}, r.Update(ctx, lb)
			}
		}

		_, delErr := cfRetry(ctx, cfResourceLoadBalancer, cfOpDelete, r.CFAPIMaxRetries, func() error {
			start := time.Now()
			_, callErr := zi.Client.LoadBalancers.Delete(ctx, lb.Status.ID,
				load_balancers.LoadBalancerDeleteParams{ZoneID: cloudflare.F(zi.ID)},
				option.WithRequestTimeout(r.CFAPIWriteTimeout))
			recordCFCall(cfResourceLoadBalancer, cfOpDelete, start, &callErr)
			return callErr
		})
		if delErr != nil {
			var cfErr *cloudflare.Error
			if errors.As(delErr, &cfErr) && cfErr.StatusCode == 404 {
				log.Info("loadbalancer - could not be deleted, finalizer released (not found in Cloudflare)",
					"hostname", lb.Spec.Hostname, "id", lb.Status.ID)
			} else {
				log.Error(delErr, "loadbalancer - delete failed", "id", lb.Status.ID)
				return ctrl.Result{}, delErr
			}
		} else {
			log.Info("loadbalancer - deleted, finalizer released", "hostname", lb.Spec.Hostname, "id", lb.Status.ID)
			operationsTotal.WithLabelValues(cfResourceLoadBalancer, cfOpDelete).Inc()
			r.paceWrite()
		}
	}

	controllerutil.RemoveFinalizer(lb, finalizerNameLB)
	return ctrl.Result{}, r.Update(ctx, lb)
}

// findLoadBalancerByHostname lists LBs in the zone and returns the one whose
// name (which is the DNS hostname) matches this CR. Same idempotency story
// as CustomHostname: reconciliation always resolves current CF state so it
// self-heals across restarts and external deletions.
func (r *LoadBalancerReconciler) findLoadBalancerByHostname(ctx context.Context, zi *zoneInfo, hostname string) (*load_balancers.LoadBalancer, error) {
	start := time.Now()
	pager := zi.Client.LoadBalancers.ListAutoPaging(ctx, load_balancers.LoadBalancerListParams{
		ZoneID: cloudflare.F(zi.ID),
	})
	var noErr error
	for pager.Next() {
		l := pager.Current()
		if l.Name == hostname {
			recordCFCall(cfResourceLoadBalancer, cfOpGet, start, &noErr)
			return &l, nil
		}
	}
	if err := pager.Err(); err != nil {
		recordCFCall(cfResourceLoadBalancer, cfOpGet, start, &err)
		return nil, err
	}
	recordCFCall(cfResourceLoadBalancer, cfOpGet, start, &noErr)
	return nil, nil
}

// buildZoneClient is the LoadBalancer's analog of the CustomHostname's
// buildCloudflareClient. Zone-scoped: we only need the CF zone ID + creds.
func (r *LoadBalancerReconciler) buildZoneClient(ctx context.Context, lb *lbv1beta1.LoadBalancer) (*zoneInfo, error) {
	zoneNS := lb.Spec.ZoneRef.Namespace
	if zoneNS == "" {
		zoneNS = r.OperatorNamespace
	}
	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, types.NamespacedName{Name: lb.Spec.ZoneRef.Name, Namespace: zoneNS}, &zone); err != nil {
		return nil, fmt.Errorf("zone %q not found: %w", lb.Spec.ZoneRef.Name, err)
	}
	key := zone.Spec.CredentialsRef.Key
	if key == "" {
		key = defaultAPITokenKey
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
		option.WithMaxRetries(0),
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

func (r *LoadBalancerReconciler) markReady(ctx context.Context, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	lb.Status.ConsecutiveErrors = 0
	msg := "LoadBalancer is synchronized with Cloudflare"
	if len(resolved.unresolved) > 0 {
		msg = fmt.Sprintf("LoadBalancer synchronized; degraded: %d pool ref(s) unresolved %v",
			len(resolved.unresolved), resolved.unresolved)
	}
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, lb, metav1.ConditionTrue, "Reconciled", msg)
}

// setError records a reconcile failure and schedules a self-requeue so the LB
// re-reconciles after RequeueInterval even without a spec change or watch event.
func (r *LoadBalancerReconciler) setError(ctx context.Context, lb *lbv1beta1.LoadBalancer, reason, message string) (ctrl.Result, error) {
	lb.Status.ConsecutiveErrors++
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, lb, metav1.ConditionFalse, reason, message)
}

func (r *LoadBalancerReconciler) setCondition(ctx context.Context, lb *lbv1beta1.LoadBalancer, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&lb.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: lb.Generation,
	})
	if err := r.Status().Update(ctx, lb); err != nil {
		return fmt.Errorf("failed to update conditions: %w", err)
	}
	return nil
}

func (r *LoadBalancerReconciler) paceWrite() {
	if r.CFAPIWriteDelay > 0 {
		time.Sleep(r.CFAPIWriteDelay)
	}
}

// ---- SetupWithManager --------------------------------------------------

// SetupWithManager wires the LB controller and watches LoadBalancerPool
// status changes so an LB waiting on a pool wakes up automatically when that
// pool's Status.ID lands.
func (r *LoadBalancerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lbv1beta1.LoadBalancer{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&lbv1beta1.LoadBalancerPool{},
			handler.EnqueueRequestsFromMapFunc(r.mapPoolToLBs),
			builder.WithPredicates(statusIDChangedPredicate(func(p *lbv1beta1.LoadBalancerPool) string { return p.Status.ID })),
		).
		Named("loadbalancer").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}

// statusIDChangedPredicate filters a cross-object watch so a dependent only
// re-reconciles when a referenced resource's CF ID (Status.ID) lands, changes,
// or the object is created/deleted -- not on every status write. Dependents
// reference their upstreams purely by CF ID (an LB by its pools' IDs, a pool by
// its monitor's ID), so status-only churn (condition flips, error-counter
// bumps) on the referenced object is irrelevant to them. Without this filter,
// each such write wakes every referencing object and triggers a full CF list,
// multiplying API calls by the fan-in on every drift cycle.
//
// Only UpdateFunc is set; Create/Delete/Generic default to true (a referenced
// object appearing or disappearing must re-drive dependents, including the
// informer's create-replay of ready objects on operator restart).
func statusIDChangedPredicate[T client.Object](statusID func(T) string) predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldObj, ok1 := e.ObjectOld.(T)
			newObj, ok2 := e.ObjectNew.(T)
			if !ok1 || !ok2 {
				return true
			}
			return statusID(oldObj) != statusID(newObj)
		},
	}
}

// mapPoolToLBs enqueues every LoadBalancer that references the given pool, so
// an LB waiting on that pool re-reconciles as soon as its Status.ID lands.
func (r *LoadBalancerReconciler) mapPoolToLBs(ctx context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*lbv1beta1.LoadBalancerPool)
	if !ok {
		return nil
	}
	var lbs lbv1beta1.LoadBalancerList
	if err := r.List(ctx, &lbs); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, lb := range lbs.Items {
		if lbReferencesPool(&lb, pool.Name) {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: lb.Name, Namespace: lb.Namespace},
			})
		}
	}
	return reqs
}

// lbReferencesPool returns true if any of the LB's pool-ref lists mention
// the given pool name. Namespaces on refs are not compared; a pool name
// collision across namespaces would already require chart-level coordination
// to avoid CF-side conflicts (see spec doc).
func lbReferencesPool(lb *lbv1beta1.LoadBalancer, poolName string) bool {
	if lb.Spec.FallbackPoolRef.Name == poolName {
		return true
	}
	for _, r := range lb.Spec.DefaultPoolRefs {
		if r.Name == poolName {
			return true
		}
	}
	for _, refs := range lb.Spec.RegionPools {
		for _, r := range refs {
			if r.Name == poolName {
				return true
			}
		}
	}
	for _, refs := range lb.Spec.CountryPools {
		for _, r := range refs {
			if r.Name == poolName {
				return true
			}
		}
	}
	for _, refs := range lb.Spec.PopPools {
		for _, r := range refs {
			if r.Name == poolName {
				return true
			}
		}
	}
	return false
}

// ---- Pool ref resolution ----------------------------------------------

// resolvedPools bundles the outcome of a pool-ref resolution pass across
// every reference slot on an LB spec. All lists here are CF-side pool IDs;
// unresolved holds the ref names that had no ready Pool CR (surfaced for
// observability -- not a hard failure; see resolve).
type resolvedPools struct {
	defaultIDs []string
	fallbackID string
	regionIDs  map[string][]string
	countryIDs map[string][]string
	popIDs     map[string][]string
	unresolved []string
}

// poolResolver resolves LoadBalancerPool references from local Pool CRs. All
// pools are managed by this (single-owner) operator, so every ref is a local
// CR -- there is no cross-cluster / CF-list fallback.
type poolResolver struct {
	r  *LoadBalancerReconciler
	lb *lbv1beta1.LoadBalancer
}

// resolve resolves a single pool ref to its CF pool ID via the local Pool CR.
// Returns ("", nil) and records the ref in out.unresolved when the CR is
// absent (NotFound) or not yet provisioned (Status.ID empty) -- best-effort,
// so the LB is still reconciled with whatever pools are ready. A transient
// Get error (not NotFound) is propagated so a live pool is never dropped from
// the LB due to an API blip (shrink-safety).
func (pr *poolResolver) resolve(ctx context.Context, ref lbv1beta1.LoadBalancerPoolRef, out *resolvedPools) (string, error) {
	ns := ref.Namespace
	if ns == "" {
		ns = pr.lb.Namespace
	}
	var pool lbv1beta1.LoadBalancerPool
	if err := pr.r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			out.addUnresolved(ref.Name)
			return "", nil
		}
		return "", err
	}
	if pool.Status.ID == "" {
		out.addUnresolved(ref.Name)
		return "", nil
	}
	return pool.Status.ID, nil
}

// addUnresolved records an unresolved pool-ref name once. The same pool can be
// referenced from multiple slots (e.g. fallback plus a default), so dedupe to
// keep the degraded Ready message clean.
func (rp *resolvedPools) addUnresolved(name string) {
	if slices.Contains(rp.unresolved, name) {
		return
	}
	rp.unresolved = append(rp.unresolved, name)
}

// resolveRefList resolves an ordered list of refs, dropping unresolved ones
// (best-effort). Resolved IDs keep the CR's ref order so successive reconciles
// produce byte-identical CF payloads for the same input.
func (pr *poolResolver) resolveRefList(ctx context.Context, refs []lbv1beta1.LoadBalancerPoolRef, out *resolvedPools) ([]string, error) {
	ids := make([]string, 0, len(refs))
	for _, ref := range refs {
		id, err := pr.resolve(ctx, ref, out)
		if err != nil {
			return nil, err
		}
		if id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// sortedKeys returns the keys of m in ascending order. Used to make CF
// payload construction deterministic across reconciles so drift checks
// don't flip-flop.
func sortedKeys(m map[string][]lbv1beta1.LoadBalancerPoolRef) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// resolveKeyedPools resolves a map of pool refs (region/country/pop),
// iterating in sorted key order so successive reconciles produce the
// same output for identical input.
func (pr *poolResolver) resolveKeyedPools(ctx context.Context, in map[string][]lbv1beta1.LoadBalancerPoolRef, out *resolvedPools) (map[string][]string, error) {
	result := map[string][]string{}
	for _, k := range sortedKeys(in) {
		ids, err := pr.resolveRefList(ctx, in[k], out)
		if err != nil {
			return nil, err
		}
		if len(ids) > 0 {
			result[k] = ids
		}
	}
	return result, nil
}

// resolveAllPools resolves every pool reference on the LB spec from local Pool
// CRs (best-effort: unresolved refs are recorded and dropped, not fatal).
// Iteration is ordered (slices) or key-sorted (maps) so successive reconciles
// produce byte-identical CF payloads for the same input.
func (r *LoadBalancerReconciler) resolveAllPools(ctx context.Context, lb *lbv1beta1.LoadBalancer) (*resolvedPools, error) {
	pr := &poolResolver{r: r, lb: lb}
	out := &resolvedPools{}

	defaultIDs, err := pr.resolveRefList(ctx, lb.Spec.DefaultPoolRefs, out)
	if err != nil {
		return nil, err
	}
	out.defaultIDs = defaultIDs

	fallbackID, err := pr.resolve(ctx, lb.Spec.FallbackPoolRef, out)
	if err != nil {
		return nil, err
	}
	out.fallbackID = fallbackID

	if out.regionIDs, err = pr.resolveKeyedPools(ctx, lb.Spec.RegionPools, out); err != nil {
		return nil, err
	}
	if out.countryIDs, err = pr.resolveKeyedPools(ctx, lb.Spec.CountryPools, out); err != nil {
		return nil, err
	}
	if out.popIDs, err = pr.resolveKeyedPools(ctx, lb.Spec.PopPools, out); err != nil {
		return nil, err
	}

	// CF requires default_pools to be non-empty. If every default ref is
	// unresolved but the fallback resolved, promote the fallback into
	// defaults so the LB still comes up (best-effort) rather than failing.
	if len(out.defaultIDs) == 0 && out.fallbackID != "" {
		out.defaultIDs = []string{out.fallbackID}
	}

	return out, nil
}

// ---- CF param builders + drift detection -------------------------------

// buildDefaultPools converts the resolved default pool IDs into the CF SDK's
// DefaultPoolsParam list. The list preserves the CR's ordering; missing pools
// (from partial resolution) are already dropped by resolveAllPools.
func buildDefaultPools(ids []string) []load_balancers.DefaultPoolsParam {
	// DefaultPoolsParam is a type alias for string, so we can convert
	// implicitly via the slice element type.
	out := make([]load_balancers.DefaultPoolsParam, len(ids))
	copy(out, ids)
	return out
}

// mapListsEqual compares two map-of-string-list values for equality. Used
// on RegionPools / CountryPools / PopPools drift checks.
func mapListsEqual(a, b map[string][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		// stringSlicesEqual is len-based, so nil and []string{} compare equal --
		// avoids a reflect.DeepEqual nil-vs-empty-slice false mismatch if a keyed
		// pool value is ever empty.
		if !stringSlicesEqual(av, bv) {
			return false
		}
	}
	return true
}

func buildLBNewParams(zoneID string, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) load_balancers.LoadBalancerNewParams {
	p := load_balancers.LoadBalancerNewParams{
		ZoneID:       cloudflare.F(zoneID),
		Name:         cloudflare.F(lb.Spec.Hostname),
		DefaultPools: cloudflare.F(buildDefaultPools(resolved.defaultIDs)),
		FallbackPool: cloudflare.F(resolved.fallbackID),
	}
	if lb.Spec.SteeringPolicy != "" {
		p.SteeringPolicy = cloudflare.F(load_balancers.SteeringPolicy(lb.Spec.SteeringPolicy))
	}
	if lb.Spec.SessionAffinity != "" {
		p.SessionAffinity = cloudflare.F(load_balancers.SessionAffinity(lb.Spec.SessionAffinity))
	}
	proxied := true
	if lb.Spec.Proxied != nil {
		proxied = *lb.Spec.Proxied
	}
	p.Proxied = cloudflare.F(proxied)
	// TTL applies only to DNS-only (grey-cloud) LBs. Cloudflare ignores ttl for
	// proxied LBs and echoes its own value, so sending it there would drift-loop
	// (compared but the corrective write never takes). Manage it only when the
	// LB is not proxied.
	if !proxied && lb.Spec.TTL > 0 {
		p.TTL = cloudflare.F(float64(lb.Spec.TTL))
	}
	if lb.Spec.Description != "" {
		p.Description = cloudflare.F(lb.Spec.Description)
	}
	if len(resolved.regionIDs) > 0 {
		p.RegionPools = cloudflare.F(resolved.regionIDs)
	}
	if len(resolved.countryIDs) > 0 {
		p.CountryPools = cloudflare.F(resolved.countryIDs)
	}
	if len(resolved.popIDs) > 0 {
		p.POPPools = cloudflare.F(resolved.popIDs)
	}
	return p
}

func buildLBEditParams(zoneID string, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) load_balancers.LoadBalancerEditParams {
	p := load_balancers.LoadBalancerEditParams{
		ZoneID:       cloudflare.F(zoneID),
		Name:         cloudflare.F(lb.Spec.Hostname),
		DefaultPools: cloudflare.F(buildDefaultPools(resolved.defaultIDs)),
		FallbackPool: cloudflare.F(resolved.fallbackID),
	}
	if lb.Spec.SteeringPolicy != "" {
		p.SteeringPolicy = cloudflare.F(load_balancers.SteeringPolicy(lb.Spec.SteeringPolicy))
	}
	if lb.Spec.SessionAffinity != "" {
		p.SessionAffinity = cloudflare.F(load_balancers.SessionAffinity(lb.Spec.SessionAffinity))
	}
	proxied := true
	if lb.Spec.Proxied != nil {
		proxied = *lb.Spec.Proxied
	}
	p.Proxied = cloudflare.F(proxied)
	// TTL applies only to DNS-only (grey-cloud) LBs. Cloudflare ignores ttl for
	// proxied LBs and echoes its own value, so sending it there would drift-loop
	// (compared but the corrective write never takes). Manage it only when the
	// LB is not proxied.
	if !proxied && lb.Spec.TTL > 0 {
		p.TTL = cloudflare.F(float64(lb.Spec.TTL))
	}
	if lb.Spec.Description != "" {
		p.Description = cloudflare.F(lb.Spec.Description)
	}
	// Geo pool maps are always sent on edit (structural: the CR owns the LB's
	// pool topology). An empty map clears CF-side geo steering -- Cloudflare does
	// not auto-remove geo pool references, so we must send {} explicitly. This
	// also keeps drift (unconditional mapListsEqual) loop-free under PATCH.
	p.RegionPools = cloudflare.F(resolved.regionIDs)
	p.CountryPools = cloudflare.F(resolved.countryIDs)
	p.POPPools = cloudflare.F(resolved.popIDs)
	return p
}

// lbDrifted reports whether the CF-observed LB diverges from the CR spec
// on any operator-managed field. Pool references drift on resolved-ID
// mismatch, not on CR-name mismatch, since CF stores IDs.
func lbDrifted(cf *load_balancers.LoadBalancer, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) bool {
	if cf.Name != lb.Spec.Hostname {
		return true
	}
	if !stringSlicesEqual(cf.DefaultPools, resolved.defaultIDs) {
		return true
	}
	if cf.FallbackPool != resolved.fallbackID {
		return true
	}
	if lb.Spec.SteeringPolicy != "" && string(cf.SteeringPolicy) != lb.Spec.SteeringPolicy {
		return true
	}
	proxied := true
	if lb.Spec.Proxied != nil {
		proxied = *lb.Spec.Proxied
	}
	if cf.Proxied != proxied {
		return true
	}
	if lb.Spec.Description != "" && cf.Description != lb.Spec.Description {
		return true
	}
	if !mapListsEqual(cf.RegionPools, resolved.regionIDs) {
		return true
	}
	if !mapListsEqual(cf.CountryPools, resolved.countryIDs) {
		return true
	}
	if !mapListsEqual(cf.POPPools, resolved.popIDs) {
		return true
	}
	// SessionAffinity: only enforce when set on the CR (empty = leave
	// whatever CF has). CF returns "" when unset, so an unset CR field
	// matches an unset CF field.
	if lb.Spec.SessionAffinity != "" && string(cf.SessionAffinity) != lb.Spec.SessionAffinity {
		return true
	}
	// TTL: only enforce when the LB is not proxied and TTL is set. Cloudflare
	// ignores ttl for proxied LBs (see buildLBNewParams), so comparing it there
	// would report perpetual drift. CF stores float64; CR is int32 with
	// kubebuilder minimum=30, so an int comparison is safe.
	if !proxied && lb.Spec.TTL > 0 && int32(cf.TTL) != lb.Spec.TTL {
		return true
	}
	return false
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
