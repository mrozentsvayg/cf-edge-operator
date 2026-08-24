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
	"net/http"
	"slices"
	"strings"
	"time"

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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/load_balancers"
	"github.com/cloudflare/cloudflare-go/v6/option"

	accountsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/accounts/v1beta1"
	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

const (
	// finalizerNameLBMonitor is the finalizer key for LoadBalancerMonitor CRs.
	// Matches the pattern used by CustomHostname (finalizerName).
	finalizerNameLBMonitor = "loadbalancing.cf-edge.io/loadbalancermonitor"
)

// LoadBalancerMonitorReconciler reconciles a LoadBalancerMonitor object.
//
// Monitors are Cloudflare account-scoped resources; the CR references an Account
// (via spec.accountRef) that supplies the CF account ID and API credentials.
// The controller then talks to the CF Load Balancer Monitors API under that
// account.
//
// The controller name-scopes each monitor: the CR's metadata.name is used
// verbatim as the CF-side monitor description (the CF monitor doesn't have a
// separate "name" field, so we key off status.id + description). List-by-CR
// look-ups query the account's monitors and match by CR-name-in-description
// to make reconciliation idempotent across restarts and CF-side deletions.
type LoadBalancerMonitorReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// Recorder emits Kubernetes Events on failure transitions (nil disables Events).
	Recorder          events.EventRecorder
	OperatorNamespace string
	// ManagementPolicy is the operator-wide default: manage / create / observe.
	ManagementPolicy string
	// DeletePolicy is the operator-wide default: always / own-only / never.
	DeletePolicy string
	// DryRun skips CF write operations.
	DryRun bool
	// CFAPITimeout is the per-request timeout for reads.
	CFAPITimeout time.Duration
	// CFAPIWriteTimeout is the per-request timeout for writes.
	CFAPIWriteTimeout time.Duration
	// CFAPIMaxRetries bounds cfRetry attempts for single API calls.
	CFAPIMaxRetries int
	// CFAPIWriteDelay paces successful writes to avoid CF API throttling.
	CFAPIWriteDelay time.Duration
	// CFBaseURL overrides the CF API base URL (test hook).
	CFBaseURL string
	// RequeueInterval is how often a reconciled monitor re-reconciles to
	// re-check for external Cloudflare drift and to retry after transient
	// errors. There is no Zone-style coordinator for monitors, so the
	// controller self-requeues. Set from --drift-interval.
	RequeueInterval time.Duration
}

// accountInfo bundles a CF client with the account ID it should act against.
// Analogous to zoneInfo for zone-scoped resources.
type accountInfo struct {
	Client    *cloudflare.Client
	AccountID string
	// AccountCR is the K8s Account CR name that supplied the credentials
	// + account ID (for logging labels).
	AccountCR string
}

// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancermonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancermonitors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=loadbalancermonitors/finalizers,verbs=update
// +kubebuilder:rbac:groups=accounts.cf-edge.io,resources=accounts,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile drives a LoadBalancerMonitor to its desired Cloudflare state, then
// rebuilds the per-account state gauge. The recompute is deferred here --
// wrapping the inner reconcile -- so it runs on every path (including the
// not-found/deletion path, so a deleted CR or the last CR for an account leaves
// no stale series) while keeping the inner reconcile's tail-returns intact. Load
// balancing has no Zone-style coordinator, so each controller recomputes its own
// aggregate; see lbStateGauge.
func (r *LoadBalancerMonitorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer r.recomputeStateGauge(ctx)
	return r.reconcile(ctx, req)
}

func (r *LoadBalancerMonitorReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var mon lbv1beta1.LoadBalancerMonitor
	if err := r.Get(ctx, req.NamespacedName, &mon); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion paths that don't need a CF client (no ID recorded yet, or
	// managementPolicy=observe / deletePolicy=never) short-circuit here.
	if !mon.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&mon, finalizerNameLBMonitor) && mon.Status.ID == "" {
			log.Info("loadbalancermonitor - could not be deleted, finalizer released (association with Cloudflare was never established)",
				"name", mon.Name)
			controllerutil.RemoveFinalizer(&mon, finalizerNameLBMonitor)
			return ctrl.Result{}, r.Update(ctx, &mon)
		}
		if controllerutil.ContainsFinalizer(&mon, finalizerNameLBMonitor) {
			mgmt := effectiveManagementPolicy(mon.Spec.ManagementPolicy, r.ManagementPolicy)
			if mgmt == ManagementPolicyObserve {
				log.Info("loadbalancermonitor - not deleted, finalizer released (managementPolicy=observe)",
					"name", mon.Name, "id", mon.Status.ID)
				controllerutil.RemoveFinalizer(&mon, finalizerNameLBMonitor)
				return ctrl.Result{}, r.Update(ctx, &mon)
			}
			del := effectiveDeletePolicy(mon.Spec.DeletePolicy, r.DeletePolicy)
			if del == DeletePolicyNever {
				log.Info("loadbalancermonitor - not deleted, finalizer released (deletePolicy=never)",
					"name", mon.Name, "id", mon.Status.ID)
				controllerutil.RemoveFinalizer(&mon, finalizerNameLBMonitor)
				return ctrl.Result{}, r.Update(ctx, &mon)
			}
		}
		ai, err := r.buildAccountClient(ctx, &mon)
		if err != nil {
			log.Error(err, "loadbalancermonitor - client initialization failed")
			return ctrl.Result{}, err // bare error for retry during deletion
		}
		return r.handleDelete(ctx, ai, &mon)
	}

	ai, err := r.buildAccountClient(ctx, &mon)
	if err != nil {
		log.Error(err, "loadbalancermonitor - client initialization failed")
		return r.setError(ctx, &mon, "AccountError", err.Error())
	}

	if !controllerutil.ContainsFinalizer(&mon, finalizerNameLBMonitor) {
		controllerutil.AddFinalizer(&mon, finalizerNameLBMonitor)
		if err := r.Update(ctx, &mon); err != nil {
			return ctrl.Result{}, err
		}
		log.V(1).Info("loadbalancermonitor - finalizer added", "name", mon.Name)
		return ctrl.Result{Requeue: true}, nil
	}

	return r.reconcileCloudflareState(ctx, ai, &mon)
}

func (r *LoadBalancerMonitorReconciler) reconcileCloudflareState(ctx context.Context, ai *accountInfo, mon *lbv1beta1.LoadBalancerMonitor) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmt := effectiveManagementPolicy(mon.Spec.ManagementPolicy, r.ManagementPolicy)

	// Always resolve current CF state -- the CR name is the source of truth,
	// so if status.ID is stale (deleted externally) we still find the current
	// monitor. Empty description filter falls through to list; we match by
	// CR name (see findMonitorByCRName).
	var existing *load_balancers.Monitor
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerMon, cfOpList, r.CFAPIMaxRetries, func() error {
		var callErr error
		existing, callErr = r.findMonitorByCRName(ctx, ai, mon)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancermonitor - lookup failed", "name", mon.Name, "attempts", attempts)
		return r.setError(ctx, mon, "LookupFailed", err.Error())
	}

	if existing == nil {
		if mgmt == ManagementPolicyObserve {
			log.Info("loadbalancermonitor - not creating (managementPolicy=observe)", "name", mon.Name)
			mon.Status.ConsecutiveErrors = 0
			return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, mon, metav1.ConditionFalse,
				reasonWaitingForExternal, "Monitor not yet provisioned in Cloudflare")
		}
		return r.handleCreate(ctx, ai, mon)
	}

	// Adopt: record the CF ID on first sight (or re-adopt after external recreate).
	if mon.Status.ID != existing.ID {
		if mon.Status.ID == "" {
			log.Info("loadbalancermonitor - adopted", "name", mon.Name, "id", existing.ID)
		} else {
			log.Info("loadbalancermonitor - readopted (externally recreated)",
				"name", mon.Name, "previousID", mon.Status.ID, "newID", existing.ID)
		}
		operationsTotal.WithLabelValues(cfResourceLoadBalancerMon, cfOpAdopt).Inc()
		mon.Status.ID = existing.ID
	}

	// Drift detection: only correct if managementPolicy=manage.
	if monitorDrifted(existing, mon) {
		if mgmt != ManagementPolicyManage {
			log.Info(fmt.Sprintf("loadbalancermonitor - not updating, drift detected (managementPolicy=%s)", mgmt),
				"name", mon.Name)
		} else if r.DryRun {
			log.Info("loadbalancermonitor - not updating, drift detected (dry-run)", "name", mon.Name)
		} else {
			log.Info("loadbalancermonitor - updating, drift detected", "name", mon.Name)
			updated, updErr := r.editMonitor(ctx, ai, mon, existing.ID)
			if updErr != nil {
				return r.setError(ctx, mon, "UpdateFailed", updErr.Error())
			}
			existing = updated
		}
	}

	// (Optional) refresh any status fields sourced from the CF response
	// once we have a fresher representation. Monitors are simple structs,
	// so there's nothing to mirror beyond ID -- keep this hook for parity.
	_ = existing

	return r.markReady(ctx, mon)
}

func (r *LoadBalancerMonitorReconciler) handleCreate(ctx context.Context, ai *accountInfo, mon *lbv1beta1.LoadBalancerMonitor) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancermonitor - not creating (dry-run)", "name", mon.Name)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, mon, metav1.ConditionFalse, reasonDryRun, "not creating (dry-run)")
	}

	params := buildMonitorNewParams(ai.AccountID, mon)
	// Guard against duplicate creates: CF monitors have no name uniqueness, so a
	// timed-out-but-succeeded create must be adopted on retry (found by marker),
	// not re-created.
	resp, adopted, attempts, err := cfCreateGuarded(ctx, cfResourceLoadBalancerMon, r.CFAPIMaxRetries,
		func() (*load_balancers.Monitor, error) { return r.findMonitorByCRName(ctx, ai, mon) },
		func() (*load_balancers.Monitor, error) {
			start := time.Now()
			m, callErr := ai.Client.LoadBalancers.Monitors.New(ctx, params,
				option.WithRequestTimeout(r.CFAPIWriteTimeout))
			recordCFCall(cfResourceLoadBalancerMon, cfOpCreate, start, &callErr)
			return m, callErr
		})
	if err != nil {
		log.Error(err, "loadbalancermonitor - create failed", "name", mon.Name, "attempts", attempts)
		return r.setError(ctx, mon, "CreateFailed", err.Error())
	}

	isRecreation := mon.Status.CreateCount > 0
	op := cfOpCreate
	verb := "created"
	if isRecreation {
		op = cfOpRecreate
		verb = "recreated"
	}
	if adopted {
		log.Info(fmt.Sprintf("loadbalancermonitor - %s (recovered timed-out create)", verb), "name", mon.Name, "id", resp.ID)
	} else {
		log.Info(fmt.Sprintf("loadbalancermonitor - %s", verb), "name", mon.Name, "id", resp.ID)
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancerMon, op).Inc()
	r.paceWrite()

	mon.Status.ID = resp.ID
	mon.Status.CreateCount++
	return r.markReady(ctx, mon)
}

func (r *LoadBalancerMonitorReconciler) editMonitor(ctx context.Context, ai *accountInfo, mon *lbv1beta1.LoadBalancerMonitor, cfID string) (*load_balancers.Monitor, error) {
	log := logf.FromContext(ctx)

	// Use Edit (PATCH, partial) so the operator corrects only the fields the
	// CRD models and leaves everything else on the monitor intact. The CRD
	// covers every Cloudflare monitor field today, but PATCH keeps the whole
	// family (monitor/pool/LB) on one model where un-modeled Cloudflare config
	// is never clobbered. Every field the CR expresses is still sent, so the CR
	// stays authoritative for what it manages.
	params := buildMonitorEditParams(ai.AccountID, mon)
	var resp *load_balancers.Monitor
	attempts, err := cfRetry(ctx, cfResourceLoadBalancerMon, cfOpUpdate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = ai.Client.LoadBalancers.Monitors.Edit(ctx, cfID, params,
			option.WithRequestTimeout(r.CFAPIWriteTimeout))
		recordCFCall(cfResourceLoadBalancerMon, cfOpUpdate, start, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancermonitor - update failed", "id", cfID, "attempts", attempts)
		return nil, err
	}
	operationsTotal.WithLabelValues(cfResourceLoadBalancerMon, cfOpUpdate).Inc()
	r.paceWrite()
	return resp, nil
}

func (r *LoadBalancerMonitorReconciler) handleDelete(ctx context.Context, ai *accountInfo, mon *lbv1beta1.LoadBalancerMonitor) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancermonitor - not deleted, finalizer released (dry-run)", "name", mon.Name, "id", mon.Status.ID)
		controllerutil.RemoveFinalizer(mon, finalizerNameLBMonitor)
		return ctrl.Result{}, r.Update(ctx, mon)
	}

	if controllerutil.ContainsFinalizer(mon, finalizerNameLBMonitor) {
		policy := effectiveDeletePolicy(mon.Spec.DeletePolicy, r.DeletePolicy)
		if policy == DeletePolicyOwnOnly {
			var current *load_balancers.Monitor
			_, err := cfRetry(ctx, cfResourceLoadBalancerMon, cfOpList, r.CFAPIMaxRetries, func() error {
				var callErr error
				current, callErr = r.findMonitorByCRName(ctx, ai, mon)
				return callErr
			})
			if err != nil {
				log.Error(err, "loadbalancermonitor - pre-delete lookup failed (deletePolicy=own-only)", "name", mon.Name)
				return ctrl.Result{}, err
			}
			if current == nil || current.ID != mon.Status.ID {
				log.Info("loadbalancermonitor - not deleted, finalizer released (deletePolicy=own-only)",
					"name", mon.Name, "statusID", mon.Status.ID)
				controllerutil.RemoveFinalizer(mon, finalizerNameLBMonitor)
				return ctrl.Result{}, r.Update(ctx, mon)
			}
		}

		_, delErr := cfRetry(ctx, cfResourceLoadBalancerMon, cfOpDelete, r.CFAPIMaxRetries, func() error {
			start := time.Now()
			_, callErr := ai.Client.LoadBalancers.Monitors.Delete(ctx, mon.Status.ID,
				load_balancers.MonitorDeleteParams{AccountID: cloudflare.F(ai.AccountID)},
				option.WithRequestTimeout(r.CFAPIWriteTimeout))
			recordCFCall(cfResourceLoadBalancerMon, cfOpDelete, start, &callErr)
			return callErr
		})
		if delErr != nil {
			var cfErr *cloudflare.Error
			if errors.As(delErr, &cfErr) && cfErr.StatusCode == 404 {
				log.Info("loadbalancermonitor - could not be deleted, finalizer released (not found in Cloudflare)",
					"name", mon.Name, "id", mon.Status.ID)
			} else {
				log.Error(delErr, "loadbalancermonitor - delete failed", "id", mon.Status.ID)
				return ctrl.Result{}, delErr
			}
		} else {
			log.Info("loadbalancermonitor - deleted, finalizer released", "name", mon.Name, "id", mon.Status.ID)
			operationsTotal.WithLabelValues(cfResourceLoadBalancerMon, cfOpDelete).Inc()
			r.paceWrite()
		}
	}

	controllerutil.RemoveFinalizer(mon, finalizerNameLBMonitor)
	return ctrl.Result{}, r.Update(ctx, mon)
}

// findMonitorByCRName lists monitors in the account and returns the one whose
// description matches the CR's namespace/name marker. Matching by description
// is the only cross-restart identity we have -- CF monitors don't have a
// user-provided "name" field, and status.ID may be stale.
//
// The description marker format is: "[cf-edge-operator:<namespace>/<name>]"
// (see monitorMarker / buildMonitorDescription). Any user-provided description in
// spec.description is preserved -- we prefix the marker so lookups by CR
// identity are unambiguous.
func (r *LoadBalancerMonitorReconciler) findMonitorByCRName(ctx context.Context, ai *accountInfo, mon *lbv1beta1.LoadBalancerMonitor) (*load_balancers.Monitor, error) {
	marker := monitorMarker(mon)
	start := time.Now()
	pager := ai.Client.LoadBalancers.Monitors.ListAutoPaging(ctx, load_balancers.MonitorListParams{
		AccountID: cloudflare.F(ai.AccountID),
	})
	var noErr error
	for pager.Next() {
		m := pager.Current()
		if descriptionHasMarker(m.Description, marker) {
			recordCFCall(cfResourceLoadBalancerMon, cfOpList, start, &noErr)
			return &m, nil
		}
	}
	if err := pager.Err(); err != nil {
		recordCFCall(cfResourceLoadBalancerMon, cfOpList, start, &err)
		return nil, err
	}
	recordCFCall(cfResourceLoadBalancerMon, cfOpList, start, &noErr)
	return nil, nil
}

func (r *LoadBalancerMonitorReconciler) buildAccountClient(ctx context.Context, mon *lbv1beta1.LoadBalancerMonitor) (*accountInfo, error) {
	return buildAccountClientFromRef(ctx, r.Client, r.OperatorNamespace, mon.Spec.AccountRef, r.CFAPITimeout, r.CFBaseURL)
}

// buildAccountClientFromRef resolves an AccountRef to a Cloudflare client and
// account ID. Shared by the Pool and Monitor reconcilers, which are both
// account-scoped. The account ID is declared on the Account CR (spec.id), so
// no CF discovery is required here.
func buildAccountClientFromRef(ctx context.Context, c client.Client, operatorNS string, ref lbv1beta1.AccountRef, cfTimeout time.Duration, cfBaseURL string) (*accountInfo, error) {
	ns := ref.Namespace
	if ns == "" {
		ns = operatorNS
	}
	var account accountsv1beta1.Account
	if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: ns}, &account); err != nil {
		return nil, fmt.Errorf("account %q not found: %w", ref.Name, err)
	}
	cf, err := cfClientFromSecret(ctx, c, account.Spec.CredentialsRef, account.Namespace, cfTimeout, cfBaseURL)
	if err != nil {
		return nil, err
	}
	return &accountInfo{
		Client:    cf,
		AccountID: account.Spec.ID,
		AccountCR: account.Name,
	}, nil
}

func (r *LoadBalancerMonitorReconciler) markReady(ctx context.Context, mon *lbv1beta1.LoadBalancerMonitor) (ctrl.Result, error) {
	mon.Status.ConsecutiveErrors = 0
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, mon, metav1.ConditionTrue, reasonReconciled, "Monitor is synchronized with Cloudflare")
}

// setError records a reconcile failure and schedules a self-requeue so the
// monitor re-reconciles after RequeueInterval even without a spec change.
func (r *LoadBalancerMonitorReconciler) setError(ctx context.Context, mon *lbv1beta1.LoadBalancerMonitor, reason, message string) (ctrl.Result, error) {
	mon.Status.ConsecutiveErrors++
	recordFailureEvent(r.Recorder, mon, mon.Status.Conditions, conditionReady, reason, message)
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, mon, metav1.ConditionFalse, reason, message)
}

func (r *LoadBalancerMonitorReconciler) setCondition(ctx context.Context, mon *lbv1beta1.LoadBalancerMonitor, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&mon.Status.Conditions, metav1.Condition{
		Type:               conditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: mon.Generation,
	})
	if err := r.Status().Update(ctx, mon); err != nil {
		return fmt.Errorf("failed to update conditions: %w", err)
	}
	return nil
}

// paceWrite mirrors CustomHostname's pacing: sleeps CFAPIWriteDelay after
// each successful write to reduce CF API throttling risk under bulk changes.
func (r *LoadBalancerMonitorReconciler) paceWrite() {
	if r.CFAPIWriteDelay > 0 {
		time.Sleep(r.CFAPIWriteDelay)
	}
}

// recomputeStateGauge rebuilds the loadbalancermonitors state gauge from every
// LoadBalancerMonitor CR in the cache, keyed by owning account CR. Called
// (deferred) on every reconcile so the per-account state counts stay current and
// a deleted CR -- or the last CR for an account -- leaves no stale series.
// Monitors are leaf resources -- they do not wait on another CR -- but the
// "waiting" series can still be non-zero under managementPolicy=observe
// (WaitingForExternal). Best-effort: a cache-list error is logged at V(1) and
// retried on the next reconcile. See lbStateGauge for the in-place-rebuild mechanism.
func (r *LoadBalancerMonitorReconciler) recomputeStateGauge(ctx context.Context) {
	var list lbv1beta1.LoadBalancerMonitorList
	if err := r.List(ctx, &list); err != nil {
		logf.FromContext(ctx).V(1).Info("loadbalancermonitor - state gauge recompute skipped, list failed", "reason", err)
		return
	}
	counts := make(map[string]map[string]int, len(list.Items))
	for i := range list.Items {
		mon := &list.Items[i]
		owner := mon.Spec.AccountRef.Name
		if counts[owner] == nil {
			counts[owner] = make(map[string]int, len(lbStateLabels))
		}
		counts[owner][lbReadyState(mon.Status.Conditions)]++
	}
	lbGaugeMonitor.set(counts)
}

// ---- SetupWithManager --------------------------------------------------

func (r *LoadBalancerMonitorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lbv1beta1.LoadBalancerMonitor{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("loadbalancermonitor").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}

// ---- Helpers -----------------------------------------------------------

// monitorMarker returns the identity marker stamped into the CF monitor's
// description so we can look the monitor up by CR identity across restarts.
func monitorMarker(mon *lbv1beta1.LoadBalancerMonitor) string {
	return fmt.Sprintf("[cf-edge-operator:%s/%s]", mon.Namespace, mon.Name)
}

// descriptionHasMarker reports whether a CF description string contains the
// given operator marker. Matching is a plain substring test so users can
// add their own description prefix / suffix.
func descriptionHasMarker(desc, marker string) bool {
	if marker == "" {
		return false
	}
	return strings.Contains(desc, marker)
}

// buildMonitorDescription composes the CF monitor description: user-provided
// prefix (if any) then the operator identity marker. The marker must always
// be present so findMonitorByCRName can locate the resource.
func buildMonitorDescription(mon *lbv1beta1.LoadBalancerMonitor) string {
	marker := monitorMarker(mon)
	if mon.Spec.Description == "" {
		return marker
	}
	return mon.Spec.Description + " " + marker
}

// buildMonitorNewParams translates a MonitorSpec into a CF create request.
// Nil / zero-value spec fields are omitted from the CF params so the CF-side
// defaults apply.
func buildMonitorNewParams(accountID string, mon *lbv1beta1.LoadBalancerMonitor) load_balancers.MonitorNewParams {
	p := load_balancers.MonitorNewParams{
		AccountID:   cloudflare.F(accountID),
		Description: cloudflare.F(buildMonitorDescription(mon)),
	}
	if mon.Spec.Type != "" {
		p.Type = cloudflare.F(load_balancers.MonitorNewParamsType(mon.Spec.Type))
	}
	if mon.Spec.Method != "" {
		p.Method = cloudflare.F(mon.Spec.Method)
	}
	if mon.Spec.Path != "" {
		p.Path = cloudflare.F(mon.Spec.Path)
	}
	if mon.Spec.Port > 0 {
		p.Port = cloudflare.F(int64(mon.Spec.Port))
	}
	if len(mon.Spec.Header) > 0 {
		p.Header = cloudflare.F(mon.Spec.Header)
	}
	if mon.Spec.ExpectedCodes != "" {
		p.ExpectedCodes = cloudflare.F(mon.Spec.ExpectedCodes)
	}
	if mon.Spec.ExpectedBody != "" {
		p.ExpectedBody = cloudflare.F(mon.Spec.ExpectedBody)
	}
	// FollowRedirects / AllowInsecure are plain bools with a well-defined false
	// default: always send them so the value we write matches what monitorDrifted
	// compares against. Emitting them only when true would drift-loop forever
	// when the CR sets false but CF has true (drift detected, but the corrective
	// update omits the field, so CF never changes).
	p.FollowRedirects = cloudflare.F(mon.Spec.FollowRedirects)
	p.AllowInsecure = cloudflare.F(mon.Spec.AllowInsecure)
	if mon.Spec.Interval > 0 {
		p.Interval = cloudflare.F(int64(mon.Spec.Interval))
	}
	// Retries is always sent: it is CRD-defaulted (so always populated) and its
	// valid range includes 0 (fail on the first probe). A ">0" guard would drop
	// an explicit 0, and Cloudflare would apply its own default instead of the
	// requested value.
	p.Retries = cloudflare.F(int64(mon.Spec.Retries))
	if mon.Spec.Timeout > 0 {
		p.Timeout = cloudflare.F(int64(mon.Spec.Timeout))
	}
	if mon.Spec.ConsecutiveUp > 0 {
		p.ConsecutiveUp = cloudflare.F(int64(mon.Spec.ConsecutiveUp))
	}
	if mon.Spec.ConsecutiveDown > 0 {
		p.ConsecutiveDown = cloudflare.F(int64(mon.Spec.ConsecutiveDown))
	}
	if mon.Spec.ProbeZone != "" {
		p.ProbeZone = cloudflare.F(mon.Spec.ProbeZone)
	}
	return p
}

// buildMonitorEditParams is the edit-side (PATCH) twin of buildMonitorNewParams.
// The CF SDK's MonitorEditParams shape mirrors MonitorNewParams but has a
// distinct Type enum, so the two builders can't share code without an
// enum-conversion detour that would obscure the intent. Only the fields the CR
// expresses are sent; PATCH leaves every other Cloudflare setting untouched.
func buildMonitorEditParams(accountID string, mon *lbv1beta1.LoadBalancerMonitor) load_balancers.MonitorEditParams {
	p := load_balancers.MonitorEditParams{
		AccountID:   cloudflare.F(accountID),
		Description: cloudflare.F(buildMonitorDescription(mon)),
	}
	if mon.Spec.Type != "" {
		p.Type = cloudflare.F(load_balancers.MonitorEditParamsType(mon.Spec.Type))
	}
	if mon.Spec.Method != "" {
		p.Method = cloudflare.F(mon.Spec.Method)
	}
	if mon.Spec.Path != "" {
		p.Path = cloudflare.F(mon.Spec.Path)
	}
	if mon.Spec.Port > 0 {
		p.Port = cloudflare.F(int64(mon.Spec.Port))
	}
	if len(mon.Spec.Header) > 0 {
		p.Header = cloudflare.F(mon.Spec.Header)
	}
	if mon.Spec.ExpectedCodes != "" {
		p.ExpectedCodes = cloudflare.F(mon.Spec.ExpectedCodes)
	}
	if mon.Spec.ExpectedBody != "" {
		p.ExpectedBody = cloudflare.F(mon.Spec.ExpectedBody)
	}
	// Always sent -- see buildMonitorNewParams (avoids a bool drift-loop).
	p.FollowRedirects = cloudflare.F(mon.Spec.FollowRedirects)
	p.AllowInsecure = cloudflare.F(mon.Spec.AllowInsecure)
	if mon.Spec.Interval > 0 {
		p.Interval = cloudflare.F(int64(mon.Spec.Interval))
	}
	// Retries is always sent: it is CRD-defaulted (so always populated) and its
	// valid range includes 0 (fail on the first probe). A ">0" guard would drop
	// an explicit 0, and Cloudflare would apply its own default instead of the
	// requested value.
	p.Retries = cloudflare.F(int64(mon.Spec.Retries))
	if mon.Spec.Timeout > 0 {
		p.Timeout = cloudflare.F(int64(mon.Spec.Timeout))
	}
	if mon.Spec.ConsecutiveUp > 0 {
		p.ConsecutiveUp = cloudflare.F(int64(mon.Spec.ConsecutiveUp))
	}
	if mon.Spec.ConsecutiveDown > 0 {
		p.ConsecutiveDown = cloudflare.F(int64(mon.Spec.ConsecutiveDown))
	}
	if mon.Spec.ProbeZone != "" {
		p.ProbeZone = cloudflare.F(mon.Spec.ProbeZone)
	}
	return p
}

// monitorDrifted returns true when any CF-observable field differs from the
// CR spec. Reflects intent: fields the CR doesn't manage (empty spec value)
// aren't compared. Kept explicit rather than reflection-driven because the
// field-by-field comparison also documents what the operator manages.
func monitorDrifted(cf *load_balancers.Monitor, mon *lbv1beta1.LoadBalancerMonitor) bool {
	if mon.Spec.Type != "" && string(cf.Type) != mon.Spec.Type {
		return true
	}
	if mon.Spec.Method != "" && cf.Method != mon.Spec.Method {
		return true
	}
	if mon.Spec.Path != "" && cf.Path != mon.Spec.Path {
		return true
	}
	if mon.Spec.Port > 0 && int32(cf.Port) != mon.Spec.Port {
		return true
	}
	if len(mon.Spec.Header) > 0 && monitorHeaderDrifted(cf.Header, mon.Spec.Header) {
		return true
	}
	if mon.Spec.ExpectedCodes != "" && cf.ExpectedCodes != mon.Spec.ExpectedCodes {
		return true
	}
	if mon.Spec.ExpectedBody != "" && cf.ExpectedBody != mon.Spec.ExpectedBody {
		return true
	}
	if cf.FollowRedirects != mon.Spec.FollowRedirects {
		return true
	}
	if cf.AllowInsecure != mon.Spec.AllowInsecure {
		return true
	}
	if mon.Spec.Interval > 0 && int32(cf.Interval) != mon.Spec.Interval {
		return true
	}
	// Retries is always managed (CRD-defaulted; 0 is a valid value), so it is
	// compared unconditionally -- see buildMonitorNewParams.
	if int32(cf.Retries) != mon.Spec.Retries {
		return true
	}
	if mon.Spec.Timeout > 0 && int32(cf.Timeout) != mon.Spec.Timeout {
		return true
	}
	if mon.Spec.ConsecutiveUp > 0 && int32(cf.ConsecutiveUp) != mon.Spec.ConsecutiveUp {
		return true
	}
	if mon.Spec.ConsecutiveDown > 0 && int32(cf.ConsecutiveDown) != mon.Spec.ConsecutiveDown {
		return true
	}
	if mon.Spec.ProbeZone != "" && cf.ProbeZone != mon.Spec.ProbeZone {
		return true
	}
	// Description drift includes marker check: if CF description lost our
	// marker (e.g. user edited it via dashboard), we need to reassert.
	if cf.Description != buildMonitorDescription(mon) {
		return true
	}
	return false
}

// monitorHeaderDrifted reports whether the CF probe headers diverge from the
// CR's. Header names are case-insensitive per the HTTP spec, so keys are
// canonicalized on both sides before comparison -- a case-sensitive compare
// would drift-loop forever if the CR uses "host" while Cloudflare returns the
// normalized "Host". Called only when the CR sets headers (len > 0); when it
// does, the full set is enforced (a header the CR dropped, or one Cloudflare has
// that the CR doesn't, counts as drift), matching the map-drift convention used
// for keyed pools (mapListsEqual). Values are compared in order.
func monitorHeaderDrifted(cf, spec map[string][]string) bool {
	if len(cf) != len(spec) {
		return true
	}
	canonicalCF := make(map[string][]string, len(cf))
	for k, v := range cf {
		canonicalCF[http.CanonicalHeaderKey(k)] = v
	}
	for k, want := range spec {
		got, ok := canonicalCF[http.CanonicalHeaderKey(k)]
		if !ok || !slices.Equal(got, want) {
			return true
		}
	}
	return false
}
