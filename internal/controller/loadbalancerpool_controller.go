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
	"strconv"
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
	finalizerNameLBPool = "saas.cf-edge.io/loadbalancerpool"
)

// LoadBalancerPoolReconciler reconciles a LoadBalancerPool object.
//
// Pools are Cloudflare account-scoped. The CR references a Zone as its
// account credential carrier (see LoadBalancerMonitorSpec.AccountRef doc)
// and optionally a LoadBalancerMonitor whose CF ID gets threaded into the
// pool's monitor field. If the referenced monitor isn't yet ready
// (status.ID empty), the pool reconcile requeues.
//
// CR name is used verbatim as the CF pool "name" tag; ensure uniqueness at
// the chart layer. Cross-CR-lookup by name is used both here (for monitor
// resolution) and in the LoadBalancer controller (for peer-pool resolution).
type LoadBalancerPoolReconciler struct {
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

// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancerpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancerpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancerpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=saas.cf-edge.io,resources=loadbalancermonitors,verbs=get;list;watch

func (r *LoadBalancerPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pool saasv1beta1.LoadBalancerPool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !pool.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&pool, finalizerNameLBPool) && pool.Status.ID == "" {
			log.Info("loadbalancerpool - could not be deleted, finalizer released (never established with Cloudflare)",
				"name", pool.Name)
			controllerutil.RemoveFinalizer(&pool, finalizerNameLBPool)
			return ctrl.Result{}, r.Update(ctx, &pool)
		}
		if controllerutil.ContainsFinalizer(&pool, finalizerNameLBPool) {
			mgmt := effectiveManagementPolicy(pool.Spec.ManagementPolicy, r.ManagementPolicy)
			if mgmt == ManagementPolicyObserve {
				log.Info("loadbalancerpool - not deleted, finalizer released (managementPolicy=observe)",
					"name", pool.Name, "id", pool.Status.ID)
				controllerutil.RemoveFinalizer(&pool, finalizerNameLBPool)
				return ctrl.Result{}, r.Update(ctx, &pool)
			}
			del := effectiveDeletePolicy(pool.Spec.DeletePolicy, r.DeletePolicy)
			if del == DeletePolicyNever {
				log.Info("loadbalancerpool - not deleted, finalizer released (deletePolicy=never)",
					"name", pool.Name, "id", pool.Status.ID)
				controllerutil.RemoveFinalizer(&pool, finalizerNameLBPool)
				return ctrl.Result{}, r.Update(ctx, &pool)
			}
		}
		ai, err := r.buildAccountClient(ctx, &pool)
		if err != nil {
			log.Error(err, "loadbalancerpool - client initialization failed")
			return ctrl.Result{}, err
		}
		return r.handleDelete(ctx, ai, &pool)
	}

	ai, err := r.buildAccountClient(ctx, &pool)
	if err != nil {
		log.Error(err, "loadbalancerpool - client initialization failed")
		return ctrl.Result{}, r.setError(ctx, &pool, "AccountError", err.Error())
	}

	if !controllerutil.ContainsFinalizer(&pool, finalizerNameLBPool) {
		controllerutil.AddFinalizer(&pool, finalizerNameLBPool)
		if err := r.Update(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		log.V(1).Info("loadbalancerpool - finalizer added", "name", pool.Name)
		return ctrl.Result{Requeue: true}, nil
	}

	// Resolve the referenced monitor to its CF ID. Missing monitor is not
	// a hard error; we surface it as a WaitingForMonitor condition and
	// requeue on the next monitor status change (watch-driven).
	monitorID, err := r.resolveMonitorID(ctx, &pool)
	if err != nil {
		return ctrl.Result{}, r.setError(ctx, &pool, "MonitorRefError", err.Error())
	}
	pool.Status.MonitorID = monitorID
	if pool.Spec.MonitorRef != nil && monitorID == "" {
		// Monitor CR exists but hasn't reconciled yet, or doesn't exist. Set
		// condition and requeue; the LoadBalancerMonitor watch handler will
		// re-enqueue us once the monitor comes online.
		return ctrl.Result{}, r.setCondition(ctx, &pool, metav1.ConditionFalse,
			"WaitingForMonitor",
			fmt.Sprintf("Monitor %q not ready yet", pool.Spec.MonitorRef.Name))
	}

	return r.reconcileCloudflareState(ctx, ai, &pool, monitorID)
}

func (r *LoadBalancerPoolReconciler) reconcileCloudflareState(ctx context.Context, ai *accountInfo, pool *saasv1beta1.LoadBalancerPool, monitorID string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmt := effectiveManagementPolicy(pool.Spec.ManagementPolicy, r.ManagementPolicy)

	var existing *load_balancers.Pool
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpGet, r.CFAPIMaxRetries, func() error {
		var callErr error
		existing, callErr = r.findPoolByCRName(ctx, ai, pool)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancerpool - lookup failed", "name", pool.Name, "attempts", attempts)
		return ctrl.Result{}, r.setError(ctx, pool, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmt == ManagementPolicyObserve {
			log.Info("loadbalancerpool - not creating (managementPolicy=observe)", "name", pool.Name)
			pool.Status.ConsecutiveErrors = 0
			return ctrl.Result{}, r.setCondition(ctx, pool, metav1.ConditionFalse,
				"WaitingForExternal", "Pool not yet provisioned in Cloudflare")
		}
		return r.handleCreate(ctx, ai, pool, monitorID)
	}

	if pool.Status.ID != existing.ID {
		if pool.Status.ID == "" {
			log.Info("loadbalancerpool - adopted", "name", pool.Name, "id", existing.ID)
		} else {
			log.Info("loadbalancerpool - readopted (externally recreated)",
				"name", pool.Name, "previousID", pool.Status.ID, "newID", existing.ID)
		}
		operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, cfOpAdopt).Inc()
		pool.Status.ID = existing.ID
	}
	pool.Status.Healthy = poolHealthyFromCF(existing)

	if poolDrifted(existing, pool, monitorID) {
		if mgmt != ManagementPolicyManage {
			log.Info(fmt.Sprintf("loadbalancerpool - not updating, drift detected (managementPolicy=%s)", mgmt),
				"name", pool.Name)
		} else if r.DryRun {
			log.Info("loadbalancerpool - not updating, drift detected (dry-run)", "name", pool.Name)
		} else {
			log.Info("loadbalancerpool - updating, drift detected", "name", pool.Name)
			updated, updErr := r.editPool(ctx, ai, pool, existing.ID, monitorID)
			if updErr != nil {
				return ctrl.Result{}, r.setError(ctx, pool, "UpdateFailed", updErr.Error())
			}
			existing = updated
			pool.Status.Healthy = poolHealthyFromCF(existing)
		}
	}

	return r.markReady(ctx, pool)
}

func (r *LoadBalancerPoolReconciler) handleCreate(ctx context.Context, ai *accountInfo, pool *saasv1beta1.LoadBalancerPool, monitorID string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancerpool - not creating (dry-run)", "name", pool.Name)
		return ctrl.Result{}, r.setCondition(ctx, pool, metav1.ConditionFalse, "DryRun", "not creating (dry-run)")
	}

	params := buildPoolNewParams(ai.AccountID, pool, monitorID)
	var resp *load_balancers.Pool
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpCreate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = ai.Client.LoadBalancers.Pools.New(ctx, params,
			option.WithRequestTimeout(r.CFAPIWriteTimeout))
		recordCFCall(cfResourceLoadBalancerPool, cfOpCreate, start, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancerpool - create failed", "name", pool.Name, "attempts", attempts)
		return ctrl.Result{}, r.setError(ctx, pool, "CreateFailed", err.Error())
	}
	isRecreation := pool.Status.CreateCount > 0
	op := cfOpCreate
	if isRecreation {
		op = cfOpRecreate
		log.Info("loadbalancerpool - recreated", "name", pool.Name, "id", resp.ID)
	} else {
		log.Info("loadbalancerpool - created", "name", pool.Name, "id", resp.ID)
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, op).Inc()
	r.paceWrite()

	pool.Status.ID = resp.ID
	pool.Status.CreateCount++
	pool.Status.Healthy = poolHealthyFromCF(resp)
	return r.markReady(ctx, pool)
}

func (r *LoadBalancerPoolReconciler) editPool(ctx context.Context, ai *accountInfo, pool *saasv1beta1.LoadBalancerPool, cfID, monitorID string) (*load_balancers.Pool, error) {
	log := logf.FromContext(ctx)
	params := buildPoolUpdateParams(ai.AccountID, pool, monitorID)
	var resp *load_balancers.Pool
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpUpdate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = ai.Client.LoadBalancers.Pools.Update(ctx, cfID, params,
			option.WithRequestTimeout(r.CFAPIWriteTimeout))
		recordCFCall(cfResourceLoadBalancerPool, cfOpUpdate, start, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancerpool - update failed", "id", cfID, "attempts", attempts)
		return nil, err
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, cfOpUpdate).Inc()
	r.paceWrite()
	return resp, nil
}

func (r *LoadBalancerPoolReconciler) handleDelete(ctx context.Context, ai *accountInfo, pool *saasv1beta1.LoadBalancerPool) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancerpool - not deleted, finalizer released (dry-run)", "name", pool.Name, "id", pool.Status.ID)
		controllerutil.RemoveFinalizer(pool, finalizerNameLBPool)
		return ctrl.Result{}, r.Update(ctx, pool)
	}

	if controllerutil.ContainsFinalizer(pool, finalizerNameLBPool) {
		policy := effectiveDeletePolicy(pool.Spec.DeletePolicy, r.DeletePolicy)
		if policy == DeletePolicyOwnOnly {
			var current *load_balancers.Pool
			_, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpGet, r.CFAPIMaxRetries, func() error {
				var callErr error
				current, callErr = r.findPoolByCRName(ctx, ai, pool)
				return callErr
			})
			if err != nil {
				log.Error(err, "loadbalancerpool - pre-delete lookup failed (deletePolicy=own-only)", "name", pool.Name)
				return ctrl.Result{}, err
			}
			if current == nil || current.ID != pool.Status.ID {
				log.Info("loadbalancerpool - not deleted, finalizer released (deletePolicy=own-only)",
					"name", pool.Name, "statusID", pool.Status.ID)
				controllerutil.RemoveFinalizer(pool, finalizerNameLBPool)
				return ctrl.Result{}, r.Update(ctx, pool)
			}
		}

		_, delErr := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpDelete, r.CFAPIMaxRetries, func() error {
			start := time.Now()
			_, callErr := ai.Client.LoadBalancers.Pools.Delete(ctx, pool.Status.ID,
				load_balancers.PoolDeleteParams{AccountID: cloudflare.F(ai.AccountID)},
				option.WithRequestTimeout(r.CFAPIWriteTimeout))
			recordCFCall(cfResourceLoadBalancerPool, cfOpDelete, start, &callErr)
			return callErr
		})
		if delErr != nil {
			var cfErr *cloudflare.Error
			if errors.As(delErr, &cfErr) && cfErr.StatusCode == 404 {
				log.Info("loadbalancerpool - could not be deleted, finalizer released (not found in Cloudflare)",
					"name", pool.Name, "id", pool.Status.ID)
			} else {
				log.Error(delErr, "loadbalancerpool - delete failed", "id", pool.Status.ID)
				return ctrl.Result{}, delErr
			}
		} else {
			log.Info("loadbalancerpool - deleted, finalizer released", "name", pool.Name, "id", pool.Status.ID)
			operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, cfOpDelete).Inc()
			r.paceWrite()
		}
	}

	controllerutil.RemoveFinalizer(pool, finalizerNameLBPool)
	return ctrl.Result{}, r.Update(ctx, pool)
}

// findPoolByCRName lists pools in the account and returns the one whose
// name matches this CR. Unlike Monitors, Pools have a first-class Name
// field in the CF API -- we key off that directly (CR name == CF pool name).
func (r *LoadBalancerPoolReconciler) findPoolByCRName(ctx context.Context, ai *accountInfo, pool *saasv1beta1.LoadBalancerPool) (*load_balancers.Pool, error) {
	start := time.Now()
	pager := ai.Client.LoadBalancers.Pools.ListAutoPaging(ctx, load_balancers.PoolListParams{
		AccountID: cloudflare.F(ai.AccountID),
	})
	var noErr error
	for pager.Next() {
		p := pager.Current()
		if p.Name == pool.Name {
			recordCFCall(cfResourceLoadBalancerPool, cfOpGet, start, &noErr)
			return &p, nil
		}
	}
	if err := pager.Err(); err != nil {
		recordCFCall(cfResourceLoadBalancerPool, cfOpGet, start, &err)
		return nil, err
	}
	recordCFCall(cfResourceLoadBalancerPool, cfOpGet, start, &noErr)
	return nil, nil
}

// resolveMonitorID resolves spec.monitorRef to the referenced monitor's
// status.ID. Returns "" if MonitorRef is unset (pool has no monitor).
// Returns "" + nil error if the monitor CR exists but isn't ready yet
// (status.ID empty) -- the caller treats this as "requeue and wait".
func (r *LoadBalancerPoolReconciler) resolveMonitorID(ctx context.Context, pool *saasv1beta1.LoadBalancerPool) (string, error) {
	if pool.Spec.MonitorRef == nil {
		return "", nil
	}
	ns := pool.Spec.MonitorRef.Namespace
	if ns == "" {
		ns = pool.Namespace
	}
	var mon saasv1beta1.LoadBalancerMonitor
	if err := r.Get(ctx, types.NamespacedName{Name: pool.Spec.MonitorRef.Name, Namespace: ns}, &mon); err != nil {
		return "", fmt.Errorf("resolve monitor %q in ns %q: %w", pool.Spec.MonitorRef.Name, ns, err)
	}
	return mon.Status.ID, nil
}

func (r *LoadBalancerPoolReconciler) buildAccountClient(ctx context.Context, pool *saasv1beta1.LoadBalancerPool) (*accountInfo, error) {
	zoneNS := pool.Spec.AccountRef.Namespace
	if zoneNS == "" {
		zoneNS = r.OperatorNamespace
	}
	var zone domainsv1beta1.Zone
	if err := r.Get(ctx, types.NamespacedName{Name: pool.Spec.AccountRef.Name, Namespace: zoneNS}, &zone); err != nil {
		return nil, fmt.Errorf("zone %q not found: %w", pool.Spec.AccountRef.Name, err)
	}
	if zone.Status.AccountID == "" {
		return nil, fmt.Errorf("zone %q status.accountID not yet populated; wait for zone reconcile", zone.Name)
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
	return &accountInfo{
		Client:    cloudflare.NewClient(opts...),
		AccountID: zone.Status.AccountID,
		ZoneCR:    zone.Name,
	}, nil
}

func (r *LoadBalancerPoolReconciler) markReady(ctx context.Context, pool *saasv1beta1.LoadBalancerPool) (ctrl.Result, error) {
	pool.Status.ConsecutiveErrors = 0
	return ctrl.Result{}, r.setCondition(ctx, pool, metav1.ConditionTrue, "Reconciled", "Pool is synchronized with Cloudflare")
}

func (r *LoadBalancerPoolReconciler) setError(ctx context.Context, pool *saasv1beta1.LoadBalancerPool, reason, message string) error {
	pool.Status.ConsecutiveErrors++
	return r.setCondition(ctx, pool, metav1.ConditionFalse, reason, message)
}

func (r *LoadBalancerPoolReconciler) setCondition(ctx context.Context, pool *saasv1beta1.LoadBalancerPool, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pool.Generation,
	})
	if err := r.Status().Update(ctx, pool); err != nil {
		return fmt.Errorf("failed to update conditions: %w", err)
	}
	return nil
}

func (r *LoadBalancerPoolReconciler) paceWrite() {
	if r.CFAPIWriteDelay > 0 {
		time.Sleep(r.CFAPIWriteDelay)
	}
}

// ---- SetupWithManager --------------------------------------------------

// SetupWithManager wires in the pool controller. In addition to Pool CRs, we
// watch LoadBalancerMonitor status changes so pools waiting on a monitor
// wake up automatically when the monitor's Status.ID lands.
func (r *LoadBalancerPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&saasv1beta1.LoadBalancerPool{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&saasv1beta1.LoadBalancerMonitor{},
			handler.EnqueueRequestsFromMapFunc(r.mapMonitorToPools),
		).
		Named("loadbalancerpool").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}

// mapMonitorToPools returns reconcile requests for every LoadBalancerPool
// that references the given LoadBalancerMonitor. Enqueued on any monitor
// change so pools blocked on WaitingForMonitor pick up the monitor's new
// Status.ID as soon as it appears.
func (r *LoadBalancerPoolReconciler) mapMonitorToPools(ctx context.Context, obj client.Object) []reconcile.Request {
	mon, ok := obj.(*saasv1beta1.LoadBalancerMonitor)
	if !ok {
		return nil
	}
	var pools saasv1beta1.LoadBalancerPoolList
	// List across the whole cluster; the monitor could be referenced from
	// any namespace. This is called once per monitor-change event so the
	// wide list scope isn't a hot path.
	if err := r.List(ctx, &pools); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, p := range pools.Items {
		if p.Spec.MonitorRef == nil {
			continue
		}
		refNS := p.Spec.MonitorRef.Namespace
		if refNS == "" {
			refNS = p.Namespace
		}
		if p.Spec.MonitorRef.Name == mon.Name && refNS == mon.Namespace {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: p.Name, Namespace: p.Namespace},
			})
		}
	}
	return reqs
}

// ---- Helpers -----------------------------------------------------------

// poolHealthyFromCF derives the pool-level health signal we surface on
// status.healthy from a CF Pool response. CF only exposes per-origin health
// via a separate PoolHealth endpoint; the Pool struct itself doesn't carry
// a rollup health flag. For status.healthy we surface pool.Enabled as a
// coarse "is this pool eligible to receive traffic" proxy. If deep health
// is needed later, extend this to call PoolHealth and combine.
func poolHealthyFromCF(cf *load_balancers.Pool) bool {
	if cf == nil {
		return false
	}
	return cf.Enabled
}

// buildOriginParams translates each origin from the CRD into CF's OriginParam.
func buildOriginParams(origins []saasv1beta1.LoadBalancerPoolOrigin) []load_balancers.OriginParam {
	out := make([]load_balancers.OriginParam, 0, len(origins))
	for _, o := range origins {
		op := load_balancers.OriginParam{
			Name:    cloudflare.F(o.Name),
			Address: cloudflare.F(o.Address),
		}
		if o.Enabled != nil {
			op.Enabled = cloudflare.F(*o.Enabled)
		} else {
			op.Enabled = cloudflare.F(true) // preserve CRD default when unset
		}
		if o.Weight != "" {
			if w, err := strconv.ParseFloat(o.Weight, 64); err == nil {
				op.Weight = cloudflare.F(w)
			}
		}
		if len(o.Header) > 0 {
			op.Header = cloudflare.F(load_balancers.HeaderParam{
				Host: cloudflare.F(o.Header["Host"]),
			})
		}
		out = append(out, op)
	}
	return out
}

func buildPoolNewParams(accountID string, pool *saasv1beta1.LoadBalancerPool, monitorID string) load_balancers.PoolNewParams {
	p := load_balancers.PoolNewParams{
		AccountID: cloudflare.F(accountID),
		Name:      cloudflare.F(pool.Name),
		Origins:   cloudflare.F(buildOriginParams(pool.Spec.Origins)),
	}
	if pool.Spec.Enabled != nil {
		p.Enabled = cloudflare.F(*pool.Spec.Enabled)
	} else {
		p.Enabled = cloudflare.F(true)
	}
	if monitorID != "" {
		p.Monitor = cloudflare.F(monitorID)
	}
	if pool.Spec.MinimumOrigins > 0 {
		p.MinimumOrigins = cloudflare.F(int64(pool.Spec.MinimumOrigins))
	}
	if pool.Spec.NotificationEmail != "" {
		p.NotificationEmail = cloudflare.F(pool.Spec.NotificationEmail)
	}
	if pool.Spec.Latitude != nil && pool.Spec.Longitude != nil {
		if lat, err := strconv.ParseFloat(*pool.Spec.Latitude, 64); err == nil {
			p.Latitude = cloudflare.F(lat)
		}
		if lon, err := strconv.ParseFloat(*pool.Spec.Longitude, 64); err == nil {
			p.Longitude = cloudflare.F(lon)
		}
	}
	if pool.Spec.Description != "" {
		p.Description = cloudflare.F(pool.Spec.Description)
	}
	return p
}

func buildPoolUpdateParams(accountID string, pool *saasv1beta1.LoadBalancerPool, monitorID string) load_balancers.PoolUpdateParams {
	p := load_balancers.PoolUpdateParams{
		AccountID: cloudflare.F(accountID),
		Name:      cloudflare.F(pool.Name),
		Origins:   cloudflare.F(buildOriginParams(pool.Spec.Origins)),
	}
	if pool.Spec.Enabled != nil {
		p.Enabled = cloudflare.F(*pool.Spec.Enabled)
	} else {
		p.Enabled = cloudflare.F(true)
	}
	if monitorID != "" {
		p.Monitor = cloudflare.F(monitorID)
	}
	if pool.Spec.MinimumOrigins > 0 {
		p.MinimumOrigins = cloudflare.F(int64(pool.Spec.MinimumOrigins))
	}
	if pool.Spec.NotificationEmail != "" {
		p.NotificationEmail = cloudflare.F(pool.Spec.NotificationEmail)
	}
	if pool.Spec.Latitude != nil && pool.Spec.Longitude != nil {
		if lat, err := strconv.ParseFloat(*pool.Spec.Latitude, 64); err == nil {
			p.Latitude = cloudflare.F(lat)
		}
		if lon, err := strconv.ParseFloat(*pool.Spec.Longitude, 64); err == nil {
			p.Longitude = cloudflare.F(lon)
		}
	}
	if pool.Spec.Description != "" {
		p.Description = cloudflare.F(pool.Spec.Description)
	}
	return p
}

// poolDrifted compares the CF pool state to the CRD spec and reports true
// if the two diverge on any operator-managed field.
func poolDrifted(cf *load_balancers.Pool, pool *saasv1beta1.LoadBalancerPool, monitorID string) bool {
	if cf.Name != pool.Name {
		return true
	}
	enabled := true
	if pool.Spec.Enabled != nil {
		enabled = *pool.Spec.Enabled
	}
	if cf.Enabled != enabled {
		return true
	}
	if monitorID != "" && cf.Monitor != monitorID {
		return true
	}
	if pool.Spec.MinimumOrigins > 0 && int32(cf.MinimumOrigins) != pool.Spec.MinimumOrigins {
		return true
	}
	if pool.Spec.NotificationEmail != "" && cf.NotificationEmail != pool.Spec.NotificationEmail {
		return true
	}
	if pool.Spec.Description != "" && cf.Description != pool.Spec.Description {
		return true
	}
	// Compare origins by (name, address, enabled, weight) tuple. Header
	// comparison isn't done here yet -- header shape has a strict Host-only
	// mapping and drift detection would need to unwrap CF's HeaderParam.
	if originsDrifted(cf.Origins, pool.Spec.Origins) {
		return true
	}
	return false
}

// originsDrifted returns true if the observed CF origins differ from the
// CRD spec on any field the operator manages.
func originsDrifted(cf []load_balancers.Origin, spec []saasv1beta1.LoadBalancerPoolOrigin) bool {
	if len(cf) != len(spec) {
		return true
	}
	// CF preserves origin order on write, so we can compare positionally.
	for i, s := range spec {
		c := cf[i]
		if c.Name != s.Name || c.Address != s.Address {
			return true
		}
		enabled := true
		if s.Enabled != nil {
			enabled = *s.Enabled
		}
		if c.Enabled != enabled {
			return true
		}
		if s.Weight != "" {
			wantW, err := strconv.ParseFloat(s.Weight, 64)
			if err == nil && c.Weight != wantW {
				return true
			}
		}
	}
	return false
}
