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
	"sort"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/load_balancers"
	"github.com/cloudflare/cloudflare-go/v6/option"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

const (
	finalizerNameLB = "saas.cf-edge.io/loadbalancer"

	// defaultAPITokenKey is the fallback key name inside a Zone's
	// credentialsRef secret when the CR doesn't set it explicitly.
	// Extracted as a package-level constant so all three LB controllers
	// (Monitor, Pool, LB) share the same fallback.
	defaultAPITokenKey = "apiToken"
)

// LoadBalancerReconciler reconciles a LoadBalancer object.
//
// LoadBalancers are Cloudflare zone-scoped (the zone provides both the DNS
// record for the LB hostname and the API credentials). Origin pools are
// account-scoped; the LB's DefaultPoolRefs / FallbackPoolRef / RegionPools
// etc. reference LoadBalancerPool CRs by name.
//
// Pool-name resolution has two modes:
//
//  1. Local CR present: look up the LoadBalancerPool CR (same-cluster) and
//     read its Status.ID. Fast, no CF API round-trip.
//  2. Cross-cluster (peer-region) pool: local CR is absent. Fall back to a
//     CF-side pool-list-by-name lookup in the account.
//
// This dual-mode resolution is what enables the multi-region pattern: each
// region's cluster owns its own LoadBalancerPool CR, and the parent-region
// cluster owns the LoadBalancer that stitches them together.
//
// If a referenced pool can't be resolved anywhere, behavior depends on
// spec.minimumPools:
//   - unset (nil): fail-hard. The LB reconcile errors and requeues; the CF
//     LB isn't created until all pools resolve.
//   - set to N: partial. The LB is reconciled with whatever pools resolve,
//     provided at least N of the total refs resolve. Missing pools are
//     recorded on status.unresolvedPoolRefs; they get added on the next
//     reconcile once the corresponding pool appears in CF.
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
}

// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancers/finalizers,verbs=update
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancerpools,verbs=get;list;watch

func (r *LoadBalancerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var lb saasv1beta1.LoadBalancer
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
		return ctrl.Result{}, r.setError(ctx, &lb, "ZoneError", err.Error())
	}

	if !controllerutil.ContainsFinalizer(&lb, finalizerNameLB) {
		controllerutil.AddFinalizer(&lb, finalizerNameLB)
		if err := r.Update(ctx, &lb); err != nil {
			return ctrl.Result{}, err
		}
		log.V(1).Info("loadbalancer - finalizer added", "hostname", lb.Spec.Hostname)
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve all referenced pools before touching CF. Failed resolutions
	// are collected on Status.UnresolvedPoolRefs; the fail-hard vs partial
	// decision keys off spec.minimumPools.
	resolved, err := r.resolveAllPools(ctx, zi, &lb)
	if err != nil {
		return ctrl.Result{}, r.setError(ctx, &lb, "PoolResolutionError", err.Error())
	}
	lb.Status.ResolvedDefaultPoolIDs = resolved.defaultIDs
	lb.Status.ResolvedFallbackPoolID = resolved.fallbackID
	lb.Status.UnresolvedPoolRefs = resolved.unresolved

	if !resolved.satisfiesMinimum(lb.Spec.MinimumPools) {
		msg := fmt.Sprintf("Unresolved pool refs: %v; minimumPools=%v not met",
			resolved.unresolved, describeMinimum(lb.Spec.MinimumPools))
		return ctrl.Result{}, r.setError(ctx, &lb, "WaitingForPools", msg)
	}

	// CF requires a non-empty fallback pool independent of MinimumPools.
	// A partial-mode resolution may satisfy MinimumPools with only default
	// pools resolving while the fallback ref is still unresolved; guard
	// against calling CF with an empty FallbackPool in that case.
	if resolved.fallbackID == "" {
		msg := fmt.Sprintf("Fallback pool %q not yet resolved (required by CF)",
			lb.Spec.FallbackPoolRef.Name)
		return ctrl.Result{}, r.setError(ctx, &lb, "WaitingForFallbackPool", msg)
	}

	return r.reconcileCloudflareState(ctx, zi, &lb, resolved)
}

func (r *LoadBalancerReconciler) reconcileCloudflareState(ctx context.Context, zi *zoneInfo, lb *saasv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
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
		return ctrl.Result{}, r.setError(ctx, lb, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmt == ManagementPolicyObserve {
			log.Info("loadbalancer - not creating (managementPolicy=observe)", "hostname", lb.Spec.Hostname)
			lb.Status.ConsecutiveErrors = 0
			return ctrl.Result{}, r.setCondition(ctx, lb, metav1.ConditionFalse,
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
				return ctrl.Result{}, r.setError(ctx, lb, "UpdateFailed", updErr.Error())
			}
			existing = updated
		}
	}
	_ = existing

	return r.markReady(ctx, lb, resolved)
}

func (r *LoadBalancerReconciler) handleCreate(ctx context.Context, zi *zoneInfo, lb *saasv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancer - not creating (dry-run)", "hostname", lb.Spec.Hostname)
		return ctrl.Result{}, r.setCondition(ctx, lb, metav1.ConditionFalse, "DryRun", "not creating (dry-run)")
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
		return ctrl.Result{}, r.setError(ctx, lb, "CreateFailed", err.Error())
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

func (r *LoadBalancerReconciler) editLoadBalancer(ctx context.Context, zi *zoneInfo, lb *saasv1beta1.LoadBalancer, cfID string, resolved *resolvedPools) (*load_balancers.LoadBalancer, error) {
	log := logf.FromContext(ctx)
	params := buildLBUpdateParams(zi.ID, lb, resolved)
	var resp *load_balancers.LoadBalancer
	attempts, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpUpdate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = zi.Client.LoadBalancers.Update(ctx, cfID, params,
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

func (r *LoadBalancerReconciler) handleDelete(ctx context.Context, zi *zoneInfo, lb *saasv1beta1.LoadBalancer) (ctrl.Result, error) {
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
func (r *LoadBalancerReconciler) buildZoneClient(ctx context.Context, lb *saasv1beta1.LoadBalancer) (*zoneInfo, error) {
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

func (r *LoadBalancerReconciler) markReady(ctx context.Context, lb *saasv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	lb.Status.ConsecutiveErrors = 0
	msg := "LoadBalancer is synchronized with Cloudflare"
	if len(resolved.unresolved) > 0 {
		msg = fmt.Sprintf("LoadBalancer synchronized in partial mode; %d unresolved pool ref(s)", len(resolved.unresolved))
	}
	return ctrl.Result{}, r.setCondition(ctx, lb, metav1.ConditionTrue, "Reconciled", msg)
}

func (r *LoadBalancerReconciler) setError(ctx context.Context, lb *saasv1beta1.LoadBalancer, reason, message string) error {
	lb.Status.ConsecutiveErrors++
	return r.setCondition(ctx, lb, metav1.ConditionFalse, reason, message)
}

func (r *LoadBalancerReconciler) setCondition(ctx context.Context, lb *saasv1beta1.LoadBalancer, status metav1.ConditionStatus, reason, message string) error {
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
// status changes so an LB waiting on peer-pool resolution wakes up
// automatically when a pool comes online (either locally as a CR or
// eventually in CF via cross-cluster).
func (r *LoadBalancerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&saasv1beta1.LoadBalancer{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&saasv1beta1.LoadBalancerPool{},
			handler.EnqueueRequestsFromMapFunc(r.mapPoolToLBs),
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

// mapPoolToLBs enqueues every LoadBalancer that references the given pool.
// Cross-cluster peer pools (referenced-by-name with no local CR) don't
// trigger this hook (no local event); those LBs re-poll on the standard
// reconcile interval and rely on the CF-side pool-list lookup during
// resolution to pick up the peer pool's CF ID.
func (r *LoadBalancerReconciler) mapPoolToLBs(ctx context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*saasv1beta1.LoadBalancerPool)
	if !ok {
		return nil
	}
	var lbs saasv1beta1.LoadBalancerList
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
func lbReferencesPool(lb *saasv1beta1.LoadBalancer, poolName string) bool {
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
// every reference slot on an LB spec. All lists here are CF-side pool IDs
// (except unresolved, which is the ref names we couldn't resolve).
type resolvedPools struct {
	defaultIDs  []string
	fallbackID  string
	regionIDs   map[string][]string
	countryIDs  map[string][]string
	popIDs      map[string][]string
	unresolved  []string
	totalRefs   int
	resolvedNum int
}

// satisfiesMinimum returns true when the current resolution satisfies the
// LB's minimumPools policy. Nil minimumPools => require all (fail-hard).
func (rp *resolvedPools) satisfiesMinimum(min *int32) bool {
	if min == nil {
		return len(rp.unresolved) == 0
	}
	return rp.resolvedNum >= int(*min)
}

// describeMinimum renders the minimumPools policy for status messages.
func describeMinimum(min *int32) string {
	if min == nil {
		return "all (fail-hard)"
	}
	return fmt.Sprintf(">=%d (partial)", *min)
}

// poolResolver resolves LoadBalancerPool references using a two-mode
// strategy: local same-cluster CR (authoritative) → CF pool-list fallback
// (peer clusters). The peer list is fetched at most once per resolver
// instance and cached, so an LB with N unresolved-locally refs makes at
// most 1 additional CF list call.
type poolResolver struct {
	r  *LoadBalancerReconciler
	zi *zoneInfo
	lb *saasv1beta1.LoadBalancer

	peerCache      map[string]string // name -> CF pool ID
	peerListLoaded bool
	peerListErr    error
}

// loadPeerList fetches the CF account's pool list once and caches by name.
// Safe to call repeatedly; a prior error is remembered and re-surfaced
// (callers can treat any peer-list load failure as fatal for this
// reconcile, since it means we can't safely resolve peer-owned refs).
func (pr *poolResolver) loadPeerList(ctx context.Context) {
	if pr.peerListLoaded {
		return
	}
	pr.peerListLoaded = true
	start := time.Now()
	accountID, err := pr.r.zoneAccountID(ctx, pr.lb)
	if err != nil {
		pr.peerListErr = err
		recordCFCall(cfResourceLoadBalancerPool, cfOpList, start, &pr.peerListErr)
		return
	}
	pager := pr.zi.Client.LoadBalancers.Pools.ListAutoPaging(ctx, load_balancers.PoolListParams{
		AccountID: cloudflare.F(accountID),
	})
	for pager.Next() {
		p := pager.Current()
		if _, seen := pr.peerCache[p.Name]; !seen {
			pr.peerCache[p.Name] = p.ID
		}
	}
	if err := pager.Err(); err != nil {
		pr.peerListErr = err
		recordCFCall(cfResourceLoadBalancerPool, cfOpList, start, &pr.peerListErr)
		return
	}
	var noErr error
	recordCFCall(cfResourceLoadBalancerPool, cfOpList, start, &noErr)
}

// resolve resolves a single pool reference. Returns (id, err). A nil err
// with id=="" means the ref couldn't be resolved via either path — the
// caller decides whether that is fatal or partial. A pre-existing local
// CR without a Status.ID (not yet reconciled) is treated as unresolved:
// falling back to CF-list in that case risks adopting a stale orphan
// pool that was left over from a prior reconcile.
func (pr *poolResolver) resolve(ctx context.Context, ref saasv1beta1.LoadBalancerPoolRef, out *resolvedPools) (string, error) {
	out.totalRefs++
	ns := ref.Namespace
	if ns == "" {
		ns = pr.lb.Namespace
	}
	var pool saasv1beta1.LoadBalancerPool
	err := pr.r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, &pool)
	if err == nil {
		// Local CR exists: it is the authoritative source. If it isn't
		// ready (no Status.ID yet), treat as unresolved rather than
		// falling back to CF's list, to avoid adopting an orphan.
		if pool.Status.ID != "" {
			out.resolvedNum++
			return pool.Status.ID, nil
		}
		out.unresolved = append(out.unresolved, ref.Name)
		return "", nil
	}
	// Local CR not found → peer-cluster case. Try CF-side list.
	pr.loadPeerList(ctx)
	if pr.peerListErr != nil {
		return "", pr.peerListErr
	}
	if id, ok := pr.peerCache[ref.Name]; ok && id != "" {
		out.resolvedNum++
		return id, nil
	}
	out.unresolved = append(out.unresolved, ref.Name)
	return "", nil
}

// resolveRefList resolves an ordered list of refs, preserving positional
// stability by inserting the empty string ("") for unresolved refs. The
// caller strips trailing/embedded empties as appropriate for the target
// CF field (default_pools is required non-empty; region/country/pop
// pools tolerate an empty list per region).
func (pr *poolResolver) resolveRefList(ctx context.Context, refs []saasv1beta1.LoadBalancerPoolRef, out *resolvedPools) ([]string, error) {
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
func sortedKeys(m map[string][]saasv1beta1.LoadBalancerPoolRef) []string {
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
func (pr *poolResolver) resolveKeyedPools(ctx context.Context, in map[string][]saasv1beta1.LoadBalancerPoolRef, out *resolvedPools) (map[string][]string, error) {
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

// resolveAllPools resolves every pool reference on the LB spec.
//
// Resolution strategy per ref:
//  1. Look up the local LoadBalancerPool CR (same-cluster) if it exists
//     AND has Status.ID populated.
//  2. Fall back to a CF pool-list-by-name lookup if the CR isn't found
//     locally (i.e. it's owned by a peer cluster).
//
// Iteration is ordered (slices) or sorted (maps) so successive
// reconciles produce byte-identical CF payloads for the same input.
func (r *LoadBalancerReconciler) resolveAllPools(ctx context.Context, zi *zoneInfo, lb *saasv1beta1.LoadBalancer) (*resolvedPools, error) {
	pr := &poolResolver{r: r, zi: zi, lb: lb, peerCache: map[string]string{}}
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

	// Guarantee at least one entry in defaultIDs — CF's default_pools
	// field is required non-empty. If every default ref failed to
	// resolve but the fallback did, promote fallback into defaults so
	// the LB can still be created / updated in partial mode. This is
	// preferable to failing the reconcile outright, because the fallback
	// pool represents the "last-resort" region and is a sensible default.
	if len(out.defaultIDs) == 0 && out.fallbackID != "" {
		out.defaultIDs = []string{out.fallbackID}
	}

	return out, nil
}

// zoneAccountID reads the Zone CR's status.accountID (populated by the
// Zone controller). Extracted here so pool listing works even when the
// LB CR itself doesn't hold the account (LBs are zone-scoped).
func (r *LoadBalancerReconciler) zoneAccountID(ctx context.Context, lb *saasv1beta1.LoadBalancer) (string, error) {
	zoneNS := lb.Spec.ZoneRef.Namespace
	if zoneNS == "" {
		zoneNS = r.OperatorNamespace
	}
	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, types.NamespacedName{Name: lb.Spec.ZoneRef.Name, Namespace: zoneNS}, &zone); err != nil {
		return "", err
	}
	if zone.Status.AccountID == "" {
		return "", fmt.Errorf("zone %q status.accountID not yet populated", zone.Name)
	}
	return zone.Status.AccountID, nil
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
		if !reflect.DeepEqual(av, bv) {
			return false
		}
	}
	return true
}

func buildLBNewParams(zoneID string, lb *saasv1beta1.LoadBalancer, resolved *resolvedPools) load_balancers.LoadBalancerNewParams {
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
	if lb.Spec.Proxied != nil {
		p.Proxied = cloudflare.F(*lb.Spec.Proxied)
	} else {
		p.Proxied = cloudflare.F(true)
	}
	if lb.Spec.TTL > 0 {
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

func buildLBUpdateParams(zoneID string, lb *saasv1beta1.LoadBalancer, resolved *resolvedPools) load_balancers.LoadBalancerUpdateParams {
	p := load_balancers.LoadBalancerUpdateParams{
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
	if lb.Spec.Proxied != nil {
		p.Proxied = cloudflare.F(*lb.Spec.Proxied)
	} else {
		p.Proxied = cloudflare.F(true)
	}
	if lb.Spec.TTL > 0 {
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

// lbDrifted reports whether the CF-observed LB diverges from the CR spec
// on any operator-managed field. Pool references drift on resolved-ID
// mismatch, not on CR-name mismatch, since CF stores IDs.
func lbDrifted(cf *load_balancers.LoadBalancer, lb *saasv1beta1.LoadBalancer, resolved *resolvedPools) bool {
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
	// TTL: only enforce when set on the CR. CF stores as float64; CR is
	// int32 with kubebuilder minimum=30, so an int comparison is safe.
	if lb.Spec.TTL > 0 && int32(cf.TTL) != lb.Spec.TTL {
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
