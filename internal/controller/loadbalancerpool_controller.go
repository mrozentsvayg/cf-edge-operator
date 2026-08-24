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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
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

	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

const (
	finalizerNameLBPool = "loadbalancing.cf-edge.io/loadbalancerpool"
)

// LoadBalancerPoolReconciler reconciles a LoadBalancerPool object.
//
// Pools are Cloudflare account-scoped. The CR references an Account (via
// spec.accountRef) for the account ID + credentials, and optionally a
// LoadBalancerMonitor whose CF ID gets threaded into the pool's monitor field.
// If the referenced monitor isn't ready yet (CR absent or status.ID empty), the
// pool waits (WaitingForMonitor) and the monitor->pool watch re-drives it.
//
// CR name is used verbatim as the CF pool "name" tag; ensure uniqueness at the
// chart layer. The CR name is also how the LoadBalancer controller resolves
// this pool's refs to a CF pool ID (via status.ID).
type LoadBalancerPoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Recorder emits Kubernetes Events on failure transitions (nil disables Events).
	Recorder          events.EventRecorder
	OperatorNamespace string
	ManagementPolicy  string
	DeletePolicy      string
	DryRun            bool
	CFAPITimeout      time.Duration
	CFAPIWriteTimeout time.Duration
	CFAPIMaxRetries   int
	CFAPIWriteDelay   time.Duration
	CFBaseURL         string
	// RequeueInterval is how often a reconciled pool re-reconciles to re-check
	// for external Cloudflare drift and to retry after transient errors. There
	// is no Zone-style coordinator for pools, so the controller self-requeues.
	// Set from --drift-interval.
	RequeueInterval time.Duration
	// EnablePoolHealth turns on the opt-in pool-health axis (--enable-pool-health):
	// after the normal sync, each reconcile polls Cloudflare for the pool's health
	// (an extra read per pool per reconcile) and publishes the loadbalancerpool
	// health gauges. Off by default; when off no health call is made and no health
	// series are emitted. The health poll never affects the sync state (see
	// maybePollPoolHealth).
	EnablePoolHealth bool
}

// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancerpools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancerpools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancerpools/finalizers,verbs=update
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancermonitors,verbs=get;list;watch
// +kubebuilder:rbac:groups=accounts.cf-edge.io,resources=accounts,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile drives a LoadBalancerPool to its desired Cloudflare state, then
// rebuilds the per-account state gauge. The recompute is deferred here --
// wrapping the inner reconcile -- so it runs on every path (including the
// not-found/deletion path, so a deleted CR or the last CR for an account leaves
// no stale series) while keeping the inner reconcile's tail-returns intact. Load
// balancing has no Zone-style coordinator, so each controller recomputes its own
// aggregate; see lbStateGauge.
func (r *LoadBalancerPoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer r.recomputeStateGauge(ctx)
	return r.reconcile(ctx, req)
}

func (r *LoadBalancerPoolReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pool lbv1beta1.LoadBalancerPool
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
		return r.setError(ctx, &pool, "AccountError", err.Error())
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
		return r.setError(ctx, &pool, "MonitorRefError", err.Error())
	}
	pool.Status.MonitorID = monitorID
	if pool.Spec.MonitorRef != nil && monitorID == "" {
		// Monitor CR exists but hasn't reconciled yet, or doesn't exist. Set
		// condition and requeue; the LoadBalancerMonitor watch handler will
		// re-enqueue us once the monitor comes online, backed by the periodic
		// self-requeue.
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, &pool, metav1.ConditionFalse,
			reasonWaitingForMonitor,
			fmt.Sprintf("Monitor %q not ready yet", pool.Spec.MonitorRef.Name))
	}

	return r.reconcileCloudflareState(ctx, ai, &pool, monitorID)
}

func (r *LoadBalancerPoolReconciler) reconcileCloudflareState(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool, monitorID string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmt := effectiveManagementPolicy(pool.Spec.ManagementPolicy, r.ManagementPolicy)

	var existing *load_balancers.Pool
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpList, r.CFAPIMaxRetries, func() error {
		var callErr error
		existing, callErr = r.findPoolByCRName(ctx, ai, pool)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancerpool - lookup failed", "name", pool.Name, "attempts", attempts)
		return r.setError(ctx, pool, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmt == ManagementPolicyObserve {
			log.Info("loadbalancerpool - not creating (managementPolicy=observe)", "name", pool.Name)
			pool.Status.ConsecutiveErrors = 0
			return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, pool, metav1.ConditionFalse,
				reasonWaitingForExternal, "Pool not yet provisioned in Cloudflare")
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
	pool.Status.Enabled = poolEnabledFromCF(existing)

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
				return r.setError(ctx, pool, "UpdateFailed", updErr.Error())
			}
			existing = updated
			pool.Status.Enabled = poolEnabledFromCF(existing)
		}
	}

	// Opt-in pool-health poll (independent of the sync above; see
	// maybePollPoolHealth). Runs on the existing-pool path only -- a pool created
	// this reconcile has no Cloudflare health yet and gets polled on its next
	// self-requeue, once it has settled.
	r.maybePollPoolHealth(ctx, ai, pool)

	return r.markReady(ctx, pool)
}

func (r *LoadBalancerPoolReconciler) handleCreate(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool, monitorID string) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancerpool - not creating (dry-run)", "name", pool.Name)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, pool, metav1.ConditionFalse, reasonDryRun, "not creating (dry-run)")
	}

	params := buildPoolNewParams(ai.AccountID, pool, monitorID)
	// Guard against duplicate creates: CF pools have no name uniqueness, so a
	// timed-out-but-succeeded create must be adopted on retry, not re-created.
	resp, adopted, attempts, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, r.CFAPIMaxRetries,
		func() (*load_balancers.Pool, error) { return r.findPoolByCRName(ctx, ai, pool) },
		func() (*load_balancers.Pool, error) {
			start := time.Now()
			p, callErr := ai.Client.LoadBalancers.Pools.New(ctx, params,
				option.WithRequestTimeout(r.CFAPIWriteTimeout))
			recordCFCall(cfResourceLoadBalancerPool, cfOpCreate, start, &callErr)
			return p, callErr
		})
	if err != nil {
		log.Error(err, "loadbalancerpool - create failed", "name", pool.Name, "attempts", attempts)
		return r.setError(ctx, pool, "CreateFailed", err.Error())
	}
	isRecreation := pool.Status.CreateCount > 0
	op := cfOpCreate
	verb := "created"
	if isRecreation {
		op = cfOpRecreate
		verb = "recreated"
	}
	if adopted {
		log.Info(fmt.Sprintf("loadbalancerpool - %s (recovered timed-out create)", verb), "name", pool.Name, "id", resp.ID)
	} else {
		log.Info(fmt.Sprintf("loadbalancerpool - %s", verb), "name", pool.Name, "id", resp.ID)
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, op).Inc()
	r.paceWrite()

	pool.Status.ID = resp.ID
	pool.Status.CreateCount++
	pool.Status.Enabled = poolEnabledFromCF(resp)
	return r.markReady(ctx, pool)
}

func (r *LoadBalancerPoolReconciler) editPool(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool, cfID, monitorID string) (*load_balancers.Pool, error) {
	log := logf.FromContext(ctx)
	// Edit (PATCH, partial): correct only the fields the CRD models and leave
	// un-modeled Cloudflare pool config (load_shedding, origin_steering, ...)
	// intact. Structural fields the CR owns (origins, monitor) are still sent.
	params := buildPoolEditParams(ai.AccountID, pool, monitorID)
	var resp *load_balancers.Pool
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpUpdate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = ai.Client.LoadBalancers.Pools.Edit(ctx, cfID, params,
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

// maybePollPoolHealth polls Cloudflare pool health and publishes the health
// gauges when the opt-in axis is enabled and the pool has a Cloudflare ID. It is
// the gate that keeps the off path truly zero-cost: with EnablePoolHealth false
// no PoolHealth.Get call is made and no health series is emitted.
func (r *LoadBalancerPoolReconciler) maybePollPoolHealth(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool) {
	if !r.EnablePoolHealth || pool.Status.ID == "" {
		return
	}
	r.pollPoolHealth(ctx, ai, pool)
}

// pollPoolHealth fetches the pool's Cloudflare health and publishes the four
// loadbalancerpool health gauges. It is an INDEPENDENT observability axis from
// the sync reconcile: a failed poll records an api error (operation="health") and
// returns without touching the sync state -- no setError, no Ready flip, no
// consecutiveErrors bump. The health gauges are left at their last value (stale);
// staleness is visible via api_errors_by_code_total{operation="health"}, so they
// are deliberately not zeroed on a transient failure.
//
// The cloudflare-go v6.8.0 SDK mis-flattens pop_health, so the poll decodes the
// raw JSON (resp.JSON.RawJSON) itself; see cfPoolHealth in pool_health.go.
func (r *LoadBalancerPoolReconciler) pollPoolHealth(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool) {
	log := logf.FromContext(ctx)

	var resp *load_balancers.PoolHealthGetResponse
	_, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpHealth, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = ai.Client.LoadBalancers.Pools.Health.Get(ctx, pool.Status.ID,
			load_balancers.PoolHealthGetParams{AccountID: cloudflare.F(ai.AccountID)},
			option.WithRequestTimeout(r.CFAPITimeout))
		recordCFCall(cfResourceLoadBalancerPool, cfOpHealth, start, &callErr)
		return callErr
	})
	if err != nil {
		// Poll-failure isolation: the error is already recorded on the api metrics;
		// do not disturb the sync state or the last-known health gauges.
		log.V(1).Info("loadbalancerpool - health poll failed, sync state unaffected (gauges left stale)",
			"name", pool.Name, "id", pool.Status.ID, "reason", err)
		return
	}

	health, err := decodePoolHealth([]byte(resp.JSON.RawJSON()))
	if err != nil {
		log.V(1).Info("loadbalancerpool - health decode failed, sync state unaffected (gauges left stale)",
			"name", pool.Name, "id", pool.Status.ID, "reason", err)
		return
	}

	tally := tallyPoolHealth(health, pool.Spec.CheckRegions)
	poolHealthGaugeSet.publish(pool.Spec.AccountRef.Name, pool.Name, tally)
}

func (r *LoadBalancerPoolReconciler) handleDelete(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool) (ctrl.Result, error) {
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
			_, err := cfRetry(ctx, cfResourceLoadBalancerPool, cfOpList, r.CFAPIMaxRetries, func() error {
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
func (r *LoadBalancerPoolReconciler) findPoolByCRName(ctx context.Context, ai *accountInfo, pool *lbv1beta1.LoadBalancerPool) (*load_balancers.Pool, error) {
	start := time.Now()
	pager := ai.Client.LoadBalancers.Pools.ListAutoPaging(ctx, load_balancers.PoolListParams{
		AccountID: cloudflare.F(ai.AccountID),
	})
	var noErr error
	for pager.Next() {
		p := pager.Current()
		if p.Name == pool.Name {
			recordCFCall(cfResourceLoadBalancerPool, cfOpList, start, &noErr)
			return &p, nil
		}
	}
	if err := pager.Err(); err != nil {
		recordCFCall(cfResourceLoadBalancerPool, cfOpList, start, &err)
		return nil, err
	}
	recordCFCall(cfResourceLoadBalancerPool, cfOpList, start, &noErr)
	return nil, nil
}

// resolveMonitorID resolves spec.monitorRef to the referenced monitor's
// status.ID. Returns "" if MonitorRef is unset (pool has no monitor).
// Returns "" + nil error when the monitor CR doesn't exist yet (NotFound) or
// exists but isn't ready (status.ID empty) -- both are soft "wait for the
// monitor" states the caller surfaces as WaitingForMonitor, not hard errors.
// A non-NotFound Get error is returned so it surfaces as a reconcile error.
func (r *LoadBalancerPoolReconciler) resolveMonitorID(ctx context.Context, pool *lbv1beta1.LoadBalancerPool) (string, error) {
	if pool.Spec.MonitorRef == nil {
		return "", nil
	}
	ns := pool.Spec.MonitorRef.Namespace
	if ns == "" {
		ns = pool.Namespace
	}
	var mon lbv1beta1.LoadBalancerMonitor
	if err := r.Get(ctx, types.NamespacedName{Name: pool.Spec.MonitorRef.Name, Namespace: ns}, &mon); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("resolve monitor %q in ns %q: %w", pool.Spec.MonitorRef.Name, ns, err)
	}
	return mon.Status.ID, nil
}

func (r *LoadBalancerPoolReconciler) buildAccountClient(ctx context.Context, pool *lbv1beta1.LoadBalancerPool) (*accountInfo, error) {
	return buildAccountClientFromRef(ctx, r.Client, r.OperatorNamespace, pool.Spec.AccountRef, r.CFAPITimeout, r.CFBaseURL)
}

func (r *LoadBalancerPoolReconciler) markReady(ctx context.Context, pool *lbv1beta1.LoadBalancerPool) (ctrl.Result, error) {
	pool.Status.ConsecutiveErrors = 0
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, pool, metav1.ConditionTrue, reasonReconciled, "Pool is synchronized with Cloudflare")
}

// setError records a reconcile failure and schedules a self-requeue so the pool
// re-reconciles after RequeueInterval even without a spec change or watch event.
func (r *LoadBalancerPoolReconciler) setError(ctx context.Context, pool *lbv1beta1.LoadBalancerPool, reason, message string) (ctrl.Result, error) {
	pool.Status.ConsecutiveErrors++
	recordFailureEvent(r.Recorder, pool, pool.Status.Conditions, conditionReady, reason, message)
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, pool, metav1.ConditionFalse, reason, message)
}

func (r *LoadBalancerPoolReconciler) setCondition(ctx context.Context, pool *lbv1beta1.LoadBalancerPool, status metav1.ConditionStatus, reason, message string) error {
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

// recomputeStateGauge rebuilds the loadbalancerpools state gauge from every
// LoadBalancerPool CR in the cache, keyed by owning account CR. Called (deferred)
// on every reconcile so the per-account state counts stay current and a deleted
// CR -- or the last CR for an account -- leaves no stale series. Best-effort: a
// cache-list error is logged at V(1) and retried on the next reconcile. See
// lbStateGauge for the in-place-rebuild mechanism.
func (r *LoadBalancerPoolReconciler) recomputeStateGauge(ctx context.Context) {
	var list lbv1beta1.LoadBalancerPoolList
	if err := r.List(ctx, &list); err != nil {
		logf.FromContext(ctx).V(1).Info("loadbalancerpool - state gauge recompute skipped, list failed", "reason", err)
		return
	}
	counts := make(map[string]map[string]int, len(list.Items))
	// liveKeys is the set of pool-health owner keys that still have a CR, used to
	// prune health series for deleted pools. Only built (and pruned) under the
	// opt-in flag, so the off path stays free of any pool-health bookkeeping.
	var liveKeys map[string]bool
	if r.EnablePoolHealth {
		liveKeys = make(map[string]bool, len(list.Items))
	}
	for i := range list.Items {
		pool := &list.Items[i]
		owner := pool.Spec.AccountRef.Name
		if counts[owner] == nil {
			counts[owner] = make(map[string]int, len(lbStateLabels))
		}
		counts[owner][lbReadyState(pool.Status.Conditions)]++
		if liveKeys != nil {
			liveKeys[poolHealthKey(owner, pool.Name)] = true
		}
	}
	lbGaugePool.set(counts)
	if liveKeys != nil {
		poolHealthGaugeSet.prune(liveKeys)
	}
}

// ---- SetupWithManager --------------------------------------------------

// SetupWithManager wires in the pool controller. In addition to Pool CRs, we
// watch LoadBalancerMonitor status changes so pools waiting on a monitor
// wake up automatically when the monitor's Status.ID lands.
func (r *LoadBalancerPoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lbv1beta1.LoadBalancerPool{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(
			&lbv1beta1.LoadBalancerMonitor{},
			handler.EnqueueRequestsFromMapFunc(r.mapMonitorToPools),
			builder.WithPredicates(statusIDChangedPredicate(func(m *lbv1beta1.LoadBalancerMonitor) string { return m.Status.ID })),
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
	mon, ok := obj.(*lbv1beta1.LoadBalancerMonitor)
	if !ok {
		return nil
	}
	var pools lbv1beta1.LoadBalancerPoolList
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

// poolEnabledFromCF reports the pool's administrative enabled state from a CF
// Pool response, surfaced on status.enabled. CF exposes per-origin health only
// via a separate PoolHealth endpoint; the Pool struct itself carries no rollup
// health flag, so we surface the enabled flag (is this pool eligible to receive
// traffic) rather than pretending to report health. If deep health is needed
// later, add a distinct status field sourced from PoolHealth.
func poolEnabledFromCF(cf *load_balancers.Pool) bool {
	if cf == nil {
		return false
	}
	return cf.Enabled
}

// buildOriginParams translates each origin from the CRD into CF's OriginParam.
func buildOriginParams(origins []lbv1beta1.LoadBalancerPoolOrigin) []load_balancers.OriginParam {
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
		// Port is managed only when the CR specifies a non-zero value. 0 means "use
		// the protocol default"; Cloudflare then resolves and echoes the actual port,
		// so sending/comparing 0 would drift-loop. Leave-alone when 0.
		if o.Port > 0 {
			op.Port = cloudflare.F(int64(o.Port))
		}
		// VirtualNetworkID is managed only when set (required for internal/reserved
		// addresses). Empty = leave Cloudflare's value.
		if o.VirtualNetworkID != "" {
			op.VirtualNetworkID = cloudflare.F(o.VirtualNetworkID)
		}
		// Header is managed only when set (non-nil). When managed, always send
		// the Host list -- even if empty (which clears the override) -- so the
		// value written matches what originsDrifted compares against. Cloudflare
		// supports only the Host header override per origin.
		if o.Header != nil {
			op.Header = cloudflare.F(load_balancers.HeaderParam{
				Host: cloudflare.F(append([]string{}, o.Header.Host...)),
			})
		}
		out = append(out, op)
	}
	return out
}

// buildOriginSteering returns the CF origin_steering param and true when the CR
// expresses a policy. Leave-alone: unset (nil, or empty policy) keeps Cloudflare's
// value, so it is sent -- and drift-checked -- only when set.
func buildOriginSteering(pool *lbv1beta1.LoadBalancerPool) (load_balancers.OriginSteeringParam, bool) {
	os := pool.Spec.OriginSteering
	if os == nil || os.Policy == "" {
		return load_balancers.OriginSteeringParam{}, false
	}
	return load_balancers.OriginSteeringParam{
		Policy: cloudflare.F(load_balancers.OriginSteeringPolicy(os.Policy)),
	}, true
}

// buildLoadShedding returns the CF load_shedding param and true when the CR
// expresses at least one subfield. load_shedding is an optional pointer: nil means
// the operator does not manage shedding, so an incident-time out-of-band shed
// survives. When set, each subfield is leave-alone -- sent, and drift-checked, only
// when expressed (non-empty percent string or non-empty policy) -- matching the
// operator's per-subfield coexist convention (see buildLocationStrategy). Percent
// strings are CRD-pattern-validated floats, so ParseFloat cannot fail (the error is
// intentionally discarded).
func buildLoadShedding(ls *lbv1beta1.LoadBalancerPoolLoadShedding) (load_balancers.LoadSheddingParam, bool) {
	if ls == nil {
		return load_balancers.LoadSheddingParam{}, false
	}
	var p load_balancers.LoadSheddingParam
	set := false
	if ls.DefaultPercent != "" {
		v, _ := strconv.ParseFloat(ls.DefaultPercent, 64)
		p.DefaultPercent = cloudflare.F(v)
		set = true
	}
	if ls.DefaultPolicy != "" {
		p.DefaultPolicy = cloudflare.F(load_balancers.LoadSheddingDefaultPolicy(ls.DefaultPolicy))
		set = true
	}
	if ls.SessionPercent != "" {
		v, _ := strconv.ParseFloat(ls.SessionPercent, 64)
		p.SessionPercent = cloudflare.F(v)
		set = true
	}
	if ls.SessionPolicy != "" {
		p.SessionPolicy = cloudflare.F(load_balancers.LoadSheddingSessionPolicy(ls.SessionPolicy))
		set = true
	}
	return p, set
}

// buildCheckRegions converts the CR's check-region codes to the CF enum slice.
func buildCheckRegions(regions []string) []load_balancers.CheckRegion {
	out := make([]load_balancers.CheckRegion, 0, len(regions))
	for _, r := range regions {
		out = append(out, load_balancers.CheckRegion(r))
	}
	return out
}

func buildPoolNewParams(accountID string, pool *lbv1beta1.LoadBalancerPool, monitorID string) load_balancers.PoolNewParams {
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
	if os, ok := buildOriginSteering(pool); ok {
		p.OriginSteering = cloudflare.F(os)
	}
	if ls, ok := buildLoadShedding(pool.Spec.LoadShedding); ok {
		p.LoadShedding = cloudflare.F(ls)
	}
	// check_regions is create-then-edit: Cloudflare rejects it on create, so it is
	// applied by buildPoolEditParams only (see poolDrifted).
	return p
}

func buildPoolEditParams(accountID string, pool *lbv1beta1.LoadBalancerPool, monitorID string) load_balancers.PoolEditParams {
	p := load_balancers.PoolEditParams{
		AccountID: cloudflare.F(accountID),
		Name:      cloudflare.F(pool.Name),
		Origins:   cloudflare.F(buildOriginParams(pool.Spec.Origins)),
	}
	if pool.Spec.Enabled != nil {
		p.Enabled = cloudflare.F(*pool.Spec.Enabled)
	} else {
		p.Enabled = cloudflare.F(true)
	}
	// Monitor is always sent on edit (structural ref): monitorID is the resolved
	// CF monitor ID, or "" when the CR has no monitorRef -- sending "" detaches a
	// previously-attached monitor. The WaitingForMonitor path returns before any
	// edit, so we never send "" for a ref that is merely unresolved.
	p.Monitor = cloudflare.F(monitorID)
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
	if os, ok := buildOriginSteering(pool); ok {
		p.OriginSteering = cloudflare.F(os)
	}
	if ls, ok := buildLoadShedding(pool.Spec.LoadShedding); ok {
		p.LoadShedding = cloudflare.F(ls)
	}
	// check_regions is edit-only (Cloudflare rejects it on create). Sent when the CR
	// expresses it; empty leaves Cloudflare's default (all regions), matching the
	// leave-alone convention. Paired with the poolDrifted check.
	if len(pool.Spec.CheckRegions) > 0 {
		p.CheckRegions = cloudflare.F(buildCheckRegions(pool.Spec.CheckRegions))
	}
	return p
}

// poolDrifted compares the CF pool state to the CRD spec and reports true
// if the two diverge on any operator-managed field.
func poolDrifted(cf *load_balancers.Pool, pool *lbv1beta1.LoadBalancerPool, monitorID string) bool {
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
	// Monitor is always managed (structural ref); compared unconditionally so
	// that detaching it in the CR (monitorID "") is corrected. See buildPoolEditParams.
	if cf.Monitor != monitorID {
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
	// Latitude/Longitude are managed only when both are set on the CR (the CRD
	// requires them together). Compared as float64 -- CF echoes back what we
	// send, mirroring the origin-weight comparison above. Unparseable values are
	// skipped (the CRD pattern validator rejects them at admission).
	if pool.Spec.Latitude != nil && pool.Spec.Longitude != nil {
		if lat, err := strconv.ParseFloat(*pool.Spec.Latitude, 64); err == nil && cf.Latitude != lat {
			return true
		}
		if lon, err := strconv.ParseFloat(*pool.Spec.Longitude, 64); err == nil && cf.Longitude != lon {
			return true
		}
	}
	if os := pool.Spec.OriginSteering; os != nil && os.Policy != "" {
		if string(cf.OriginSteering.Policy) != os.Policy {
			return true
		}
	}
	// check_regions: enforced when the CR expresses it; compared order-insensitively
	// (Cloudflare may reorder). Empty = leave-alone (matches buildPoolEditParams).
	if len(pool.Spec.CheckRegions) > 0 {
		if !unorderedStringSlicesEqual(checkRegionsToStrings(cf.CheckRegions), pool.Spec.CheckRegions) {
			return true
		}
	}
	if ls := pool.Spec.LoadShedding; ls != nil {
		if loadSheddingDrifted(cf.LoadShedding, ls) {
			return true
		}
	}
	// Compare origins by (name, address, enabled, weight, header) tuple.
	if originsDrifted(cf.Origins, pool.Spec.Origins) {
		return true
	}
	return false
}

// originsDrifted returns true if the observed CF origins differ from the
// CRD spec on any field the operator manages.
func originsDrifted(cf []load_balancers.Origin, spec []lbv1beta1.LoadBalancerPoolOrigin) bool {
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
		// Port managed only when the CR sets a non-zero value (see buildOriginParams).
		if s.Port > 0 && c.Port != int64(s.Port) {
			return true
		}
		// VirtualNetworkID managed only when set.
		if s.VirtualNetworkID != "" && c.VirtualNetworkID != s.VirtualNetworkID {
			return true
		}
		// Header is managed only when set (non-nil); when managed we send and
		// compare the Host list. cf.Header.Host is []string (CF's Host alias).
		if s.Header != nil && !stringSlicesEqual(c.Header.Host, s.Header.Host) {
			return true
		}
	}
	return false
}

// loadSheddingDrifted compares the CF-observed load_shedding against the CR,
// honoring the leave-alone rule: each subfield is compared only when the CR
// expresses it. Percent strings are CRD-pattern-validated floats, so ParseFloat
// cannot fail (the error is intentionally discarded). Callers gate on ls != nil.
func loadSheddingDrifted(cf load_balancers.LoadShedding, ls *lbv1beta1.LoadBalancerPoolLoadShedding) bool {
	if ls.DefaultPercent != "" {
		if v, _ := strconv.ParseFloat(ls.DefaultPercent, 64); cf.DefaultPercent != v {
			return true
		}
	}
	if ls.DefaultPolicy != "" && string(cf.DefaultPolicy) != ls.DefaultPolicy {
		return true
	}
	if ls.SessionPercent != "" {
		if v, _ := strconv.ParseFloat(ls.SessionPercent, 64); cf.SessionPercent != v {
			return true
		}
	}
	if ls.SessionPolicy != "" && string(cf.SessionPolicy) != ls.SessionPolicy {
		return true
	}
	return false
}

// checkRegionsToStrings converts CF check-region enums to plain strings for
// order-insensitive comparison against the CR's spec.checkRegions.
func checkRegionsToStrings(regions []load_balancers.CheckRegion) []string {
	out := make([]string, 0, len(regions))
	for _, r := range regions {
		out = append(out, string(r))
	}
	return out
}
