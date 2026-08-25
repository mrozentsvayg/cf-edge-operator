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
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
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

// conditionNetworksSynced is a secondary status condition on a LoadBalancer that
// reports whether spec.networks matches the Cloudflare-observed networks.
// Networks are enforced only at create (Cloudflare's Edit API does not accept
// networks), so this condition surfaces post-create drift without the operator
// auto-correcting it. It does not affect the Ready condition -- a networks drift
// is observability-only. Absent or True means in sync (or unmanaged: spec.networks
// empty); False means drifted.
const conditionNetworksSynced = "NetworksSynced"

// NetworksSynced condition reasons.
const (
	reasonNetworksInSync    = "InSync"    // spec.networks matches Cloudflare
	reasonNetworksDrifted   = "Drifted"   // spec.networks differs from Cloudflare (surface-only)
	reasonNetworksUnmanaged = "Unmanaged" // spec.networks is empty; networks are not managed
)

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

// Reconcile drives a LoadBalancer to its desired Cloudflare state, then rebuilds
// the per-zone state gauge. The recompute is deferred here -- wrapping the inner
// reconcile -- so it runs on every path (including the not-found/deletion path,
// so a deleted CR or the last CR for a zone leaves no stale series) while keeping
// the inner reconcile's tail-returns intact. Load balancing has no Zone-style
// coordinator, so each controller recomputes its own aggregate; see lbStateGauge.
func (r *LoadBalancerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	defer r.recomputeStateGauge(ctx)
	return r.reconcile(ctx, req)
}

func (r *LoadBalancerReconciler) reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
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
		// Fall through to reconcile in the same pass rather than scheduling a requeue.
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
	// Record which pool refs (from any slot -- default, fallback, geo, or
	// random-steering weights) had no ready Pool CR this reconcile. A non-empty
	// list drives the partial state in markReady and the Unresolved printcolumn.
	// nil when everything resolved (clears a prior partial).
	lb.Status.UnresolvedPoolRefs = resolved.unresolved

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
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, &lb, metav1.ConditionFalse, reasonWaitingForFallbackPool, msg)
	}

	return r.reconcileCloudflareState(ctx, zi, &lb, resolved)
}

func (r *LoadBalancerReconciler) reconcileCloudflareState(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	mgmt := effectiveManagementPolicy(lb.Spec.ManagementPolicy, r.ManagementPolicy)

	var existing *load_balancers.LoadBalancer
	attempts, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpList, r.CFAPIMaxRetries, func() error {
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
				reasonWaitingForExternal, "LoadBalancer not yet provisioned in Cloudflare")
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
		// Surface-on-adopt: enabled is always-managed (unset spec => default true),
		// so an adopted LB whose CF-side enabled differs from the CR's desired value
		// is about to be (re-)enforced by the drift correction below. Warn so an
		// operator sees the operator is taking over an out-of-band enable/disable.
		if existing.Enabled != lbEnabled(lb) {
			log.Info("loadbalancer - adopted Cloudflare load balancer has enabled differing from the CR; enforcing the CR value",
				"hostname", lb.Spec.Hostname, "observedEnabled", existing.Enabled, "desiredEnabled", lbEnabled(lb))
			if r.Recorder != nil {
				r.Recorder.Eventf(lb, nil, corev1.EventTypeWarning, "EnabledEnforced", "Enforcing",
					"Adopted Cloudflare load balancer had enabled=%t; enforcing the CR's value enabled=%t",
					existing.Enabled, lbEnabled(lb))
			}
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
			updated, updErr := r.editLoadBalancer(ctx, zi, lb, existing, resolved)
			if updErr != nil {
				return r.setError(ctx, lb, "UpdateFailed", updErr.Error())
			}
			existing = updated
		}
	}

	// Surface (but do not correct) any divergence between spec.networks and the
	// Cloudflare-observed networks. Networks are create-only-write, so this is
	// observability-only and never flips Ready.
	r.surfaceNetworksDrift(ctx, lb, existing.Networks)

	return r.markReady(ctx, lb, resolved)
}

func (r *LoadBalancerReconciler) handleCreate(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if r.DryRun {
		log.Info("loadbalancer - not creating (dry-run)", "hostname", lb.Spec.Hostname)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, lb, metav1.ConditionFalse, reasonDryRun, "not creating (dry-run)")
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
	// Networks are written on create, so record whether Cloudflare accepted them
	// as requested (surface-only; keeps the NetworksSynced condition current from
	// the first reconcile).
	r.surfaceNetworksDrift(ctx, lb, resp.Networks)
	return r.markReady(ctx, lb, resolved)
}

func (r *LoadBalancerReconciler) editLoadBalancer(ctx context.Context, zi *zoneInfo, lb *lbv1beta1.LoadBalancer, existing *load_balancers.LoadBalancer, resolved *resolvedPools) (*load_balancers.LoadBalancer, error) {
	log := logf.FromContext(ctx)
	// Edit (PATCH, partial): correct only the fields the CR expresses and leave
	// everything else intact -- both genuinely un-modeled Cloudflare LB config
	// (e.g. rules) and modeled-but-optional fields the CR does not set (adaptive
	// routing, location strategy, random steering, session-affinity attributes/ttl).
	// The default/fallback pool topology the CR owns is always sent.
	params := buildLBEditParams(zi.ID, lb, resolved)

	// The geo pool maps (region/country/pop) and random_steering.pool_weights are
	// map properties Cloudflare DEEP-MERGES on PATCH, so the typed params can never
	// remove a key the CR dropped. For each map the CR manages (the field is set --
	// presence, not emptiness), inject it as raw JSON: resolved keys plus an explicit
	// null for every key Cloudflare still has that the CR no longer declares, forcing
	// REPLACE over the deep-merge. An unset (nil) map is left untouched on Cloudflare.
	opts := []option.RequestOption{option.WithRequestTimeout(r.CFAPIWriteTimeout)}
	if lb.Spec.RegionPools != nil {
		opts = append(opts, option.WithJSONSet("region_pools", mapWithNulls(resolved.regionIDs, mapKeys(existing.RegionPools))))
	}
	if lb.Spec.CountryPools != nil {
		opts = append(opts, option.WithJSONSet("country_pools", mapWithNulls(resolved.countryIDs, mapKeys(existing.CountryPools))))
	}
	if lb.Spec.PopPools != nil {
		opts = append(opts, option.WithJSONSet("pop_pools", mapWithNulls(resolved.popIDs, mapKeys(existing.POPPools))))
	}
	if lb.Spec.RandomSteering != nil {
		opts = append(opts, option.WithJSONSet("random_steering.pool_weights",
			mapWithNulls(resolvedWeightsToFloat(resolved.randomSteeringWeights), mapKeys(existing.RandomSteering.PoolWeights))))
	}

	var resp *load_balancers.LoadBalancer
	attempts, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpUpdate, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		var callErr error
		resp, callErr = zi.Client.LoadBalancers.Edit(ctx, existing.ID, params, opts...)
		recordCFCall(cfResourceLoadBalancer, cfOpUpdate, start, &callErr)
		return callErr
	})
	if err != nil {
		log.Error(err, "loadbalancer - update failed", "id", existing.ID, "attempts", attempts)
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
			_, err := cfRetry(ctx, cfResourceLoadBalancer, cfOpList, r.CFAPIMaxRetries, func() error {
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
			recordCFCall(cfResourceLoadBalancer, cfOpList, start, &noErr)
			return &l, nil
		}
	}
	if err := pager.Err(); err != nil {
		recordCFCall(cfResourceLoadBalancer, cfOpList, start, &err)
		return nil, err
	}
	recordCFCall(cfResourceLoadBalancer, cfOpList, start, &noErr)
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

// markReady marks the LoadBalancer Ready=True. When every referenced pool
// resolved, the reason is Reconciled (fully synchronized). When one or more pool
// refs are unresolved, the reason is Partial: the LB is still serving (Ready stays
// True so kubectl-wait / GitOps are not blocked) but with a reduced pool set --
// the incompleteness is surfaced via the reason, status.unresolvedPoolRefs, the
// loadbalancers{state="partial"} gauge, and (on the transition into partial) a log
// line plus a Kubernetes Event. status.unresolvedPoolRefs itself is set in
// reconcile before this call.
func (r *LoadBalancerReconciler) markReady(ctx context.Context, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) (ctrl.Result, error) {
	lb.Status.ConsecutiveErrors = 0
	reason := reasonReconciled
	msg := "LoadBalancer is synchronized with Cloudflare"
	if len(resolved.unresolved) > 0 {
		reason = reasonPartial
		msg = fmt.Sprintf("LoadBalancer synchronized; degraded: %d pool ref(s) unresolved %v",
			len(resolved.unresolved), resolved.unresolved)
		// Log + emit an Event only on the transition into partial (the previously
		// persisted Ready reason was not already Partial), not on every reconcile.
		if !readyReasonIs(lb.Status.Conditions, reasonPartial) {
			logf.FromContext(ctx).Info("loadbalancer - serving in a degraded (partial) state, some pool refs unresolved",
				"hostname", lb.Spec.Hostname, "unresolvedPoolRefs", resolved.unresolved)
			if r.Recorder != nil {
				r.Recorder.Eventf(lb, nil, corev1.EventTypeWarning, "Partial", "Degraded",
					"LoadBalancer serving in a degraded state; %d pool ref(s) unresolved: %v",
					len(resolved.unresolved), resolved.unresolved)
			}
		}
	}
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, r.setCondition(ctx, lb, metav1.ConditionTrue, reason, msg)
}

// readyReasonIs reports whether the current (persisted) Ready condition is
// True with the given reason. Used to fire transition-only side effects (log +
// Event) exactly once when the LB enters a state, rather than on every reconcile.
func readyReasonIs(conds []metav1.Condition, reason string) bool {
	cond := apimeta.FindStatusCondition(conds, conditionReady)
	return cond != nil && cond.Status == metav1.ConditionTrue && cond.Reason == reason
}

// lbEnabled returns the LB's desired enabled value. enabled is always-managed:
// unset defaults to true (matching the CRD default), so the operator enforces
// enabled=true unless the CR explicitly sets it false.
func lbEnabled(lb *lbv1beta1.LoadBalancer) bool {
	if lb.Spec.Enabled != nil {
		return *lb.Spec.Enabled
	}
	return true
}

// surfaceNetworksDrift compares the CR's desired spec.networks against the
// Cloudflare-observed networks and surfaces any divergence three ways: the
// NetworksSynced status condition (set here), the loadbalancer_networks_drift
// gauge (recomputed from that condition in recomputeStateGauge), and a log line
// on the transition into drift. Networks are create-only-write -- Cloudflare's
// Edit API does not accept networks -- so drift is surfaced, never auto-corrected,
// and Ready is unaffected. The condition is set in memory; the caller's status
// update (markReady / setCondition) persists it.
//
// When spec.networks is empty (unmanaged), no condition is written for a fresh LB
// (absent == in sync); an existing NetworksSynced condition is flipped to
// True/Unmanaged so a previously-managed-then-cleared LB does not leave a stale
// drift condition.
func (r *LoadBalancerReconciler) surfaceNetworksDrift(ctx context.Context, lb *lbv1beta1.LoadBalancer, observed []string) {
	if len(lb.Spec.Networks) == 0 {
		if apimeta.FindStatusCondition(lb.Status.Conditions, conditionNetworksSynced) != nil {
			apimeta.SetStatusCondition(&lb.Status.Conditions, metav1.Condition{
				Type:               conditionNetworksSynced,
				Status:             metav1.ConditionTrue,
				Reason:             reasonNetworksUnmanaged,
				Message:            "spec.networks is empty; networks are not managed",
				ObservedGeneration: lb.Generation,
			})
		}
		return
	}
	if unorderedStringSlicesEqual(lb.Spec.Networks, observed) {
		apimeta.SetStatusCondition(&lb.Status.Conditions, metav1.Condition{
			Type:               conditionNetworksSynced,
			Status:             metav1.ConditionTrue,
			Reason:             reasonNetworksInSync,
			Message:            "spec.networks matches Cloudflare",
			ObservedGeneration: lb.Generation,
		})
		return
	}
	// Drifted. Log once, on the transition into drift (the persisted condition was
	// not already False).
	if !networksDrifted(lb.Status.Conditions) {
		logf.FromContext(ctx).Info("loadbalancer - networks drift detected (create-enforced; not auto-corrected)",
			"hostname", lb.Spec.Hostname, "desiredNetworks", lb.Spec.Networks, "observedNetworks", observed)
	}
	apimeta.SetStatusCondition(&lb.Status.Conditions, metav1.Condition{
		Type:   conditionNetworksSynced,
		Status: metav1.ConditionFalse,
		Reason: reasonNetworksDrifted,
		Message: fmt.Sprintf("spec.networks differs from Cloudflare: desired %v, observed %v",
			lb.Spec.Networks, observed),
		ObservedGeneration: lb.Generation,
	})
}

// networksDrifted reports whether the NetworksSynced condition is currently False
// (spec.networks diverges from Cloudflare). Used both to log the drift transition
// only once and to recompute the loadbalancer_networks_drift gauge from the CR set.
func networksDrifted(conds []metav1.Condition) bool {
	cond := apimeta.FindStatusCondition(conds, conditionNetworksSynced)
	return cond != nil && cond.Status == metav1.ConditionFalse
}

// setError records a reconcile failure and schedules a self-requeue so the LB
// re-reconciles after RequeueInterval even without a spec change or watch event.
func (r *LoadBalancerReconciler) setError(ctx context.Context, lb *lbv1beta1.LoadBalancer, reason, message string) (ctrl.Result, error) {
	lb.Status.ConsecutiveErrors++
	recordFailureEvent(r.Recorder, lb, lb.Status.Conditions, conditionReady, reason, message)
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

// recomputeStateGauge rebuilds the loadbalancers state gauge from every
// LoadBalancer CR in the cache, keyed by owning zone CR. Called (deferred) on
// every reconcile so the per-zone state counts stay current and a deleted CR --
// or the last CR for a zone -- leaves no stale series. Best-effort: a cache-list
// error is logged at V(1) and retried on the next reconcile. See lbStateGauge
// for the in-place-rebuild mechanism.
func (r *LoadBalancerReconciler) recomputeStateGauge(ctx context.Context) {
	var list lbv1beta1.LoadBalancerList
	if err := r.List(ctx, &list); err != nil {
		logf.FromContext(ctx).V(1).Info("loadbalancer - state gauge recompute skipped, list failed", "reason", err)
		return
	}
	counts := make(map[string]map[string]int, len(list.Items))
	drift := make(map[string]int, len(list.Items))
	for i := range list.Items {
		lb := &list.Items[i]
		owner := lb.Spec.ZoneRef.Name
		if counts[owner] == nil {
			counts[owner] = make(map[string]int, len(lbStateLabelsLB))
			// Seed a 0 drift series for every zone that has LBs so a synced zone
			// reads 0 rather than leaving no series (and no stale one on cleanup).
			drift[owner] = 0
		}
		counts[owner][lbReadyState(lb.Status.Conditions)]++
		if networksDrifted(lb.Status.Conditions) {
			drift[owner]++
		}
	}
	lbGaugeLoadBalancer.set(counts)
	setNetworksDriftGauge(drift)
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
	for _, m := range []*map[string][]lbv1beta1.LoadBalancerPoolRef{lb.Spec.RegionPools, lb.Spec.CountryPools, lb.Spec.PopPools} {
		if m == nil {
			continue
		}
		for _, refs := range *m {
			for _, r := range refs {
				if r.Name == poolName {
					return true
				}
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
	// randomSteeringWeights maps resolved CF pool IDs to their random-steering
	// weight strings (from spec.randomSteering.poolWeights). Populated only when
	// spec.randomSteering is set. An unresolved weighted pool is dropped here and
	// recorded in unresolved (feeding the partial state), same as the other pool
	// slots. Weight is kept as the CR's string form to preserve fractional
	// precision; buildRandomSteering parses each to float64 for the CF payload.
	randomSteeringWeights map[string]string
	unresolved            []string
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

// resolveWeightedPools resolves random-steering pool weights to a map of resolved
// CF pool ID -> weight string. Refs are resolved via the same path as the other
// pool slots, so an unresolved weighted pool is recorded in out.unresolved
// (feeding the partial state) and dropped from the result. Iterates in ref order.
func (pr *poolResolver) resolveWeightedPools(ctx context.Context, weights []lbv1beta1.LoadBalancerPoolWeight, out *resolvedPools) (map[string]string, error) {
	result := make(map[string]string, len(weights))
	for _, w := range weights {
		id, err := pr.resolve(ctx, w.PoolRef, out)
		if err != nil {
			return nil, err
		}
		if id != "" {
			result[id] = w.Weight
		}
	}
	return result, nil
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

// derefKeyedPools returns the pointed-to keyed-pool map, or nil when the field is
// unset. resolveKeyedPools handles a nil map (yields an empty result), so an unset
// map resolves to "nothing" while the caller still checks spec.<X>Pools != nil for
// the presence-not-emptiness management decision (send/drift only when managed).
func derefKeyedPools(p *map[string][]lbv1beta1.LoadBalancerPoolRef) map[string][]lbv1beta1.LoadBalancerPoolRef {
	if p == nil {
		return nil
	}
	return *p
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

	if out.regionIDs, err = pr.resolveKeyedPools(ctx, derefKeyedPools(lb.Spec.RegionPools), out); err != nil {
		return nil, err
	}
	if out.countryIDs, err = pr.resolveKeyedPools(ctx, derefKeyedPools(lb.Spec.CountryPools), out); err != nil {
		return nil, err
	}
	if out.popIDs, err = pr.resolveKeyedPools(ctx, derefKeyedPools(lb.Spec.PopPools), out); err != nil {
		return nil, err
	}

	// Random-steering pool weights reference pools too; resolve them so an
	// unresolved weighted pool feeds the partial state like any other slot.
	if lb.Spec.RandomSteering != nil {
		if out.randomSteeringWeights, err = pr.resolveWeightedPools(ctx, lb.Spec.RandomSteering.PoolWeights, out); err != nil {
			return nil, err
		}
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

// mapKeys returns the keys of m (any value type). Used to feed mapWithNulls the
// set of keys Cloudflare currently has for a map property.
func mapKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// mapWithNulls builds the raw object for a PATCH that REPLACES a Cloudflare map
// property despite Cloudflare deep-merging map properties: every desired key
// carries its value, and every key Cloudflare still has (observedKeys) that is
// absent from desired is set to nil, which marshals to JSON null -- Cloudflare
// treats a null map value as a key removal. An empty desired map nulls every
// observed key (clear all). This is injected via option.WithJSONSet because the
// typed param.Field[map[...]] cannot express a null value (a nil slice marshals
// as [], and float64 has no nil).
func mapWithNulls[V any](desired map[string]V, observedKeys []string) map[string]any {
	out := make(map[string]any, len(desired)+len(observedKeys))
	for k, v := range desired {
		out[k] = v
	}
	for _, k := range observedKeys {
		if _, ok := desired[k]; !ok {
			out[k] = nil
		}
	}
	return out
}

// resolvedWeightsToFloat parses the resolver's weight strings (CRD-pattern-validated
// floats, so ParseFloat cannot fail -- the error is intentionally discarded) into the
// float64 map Cloudflare's pool_weights expects. Shared by buildRandomSteering (the
// create/edit payload) and editLoadBalancer (the null-removal override).
func resolvedWeightsToFloat(weights map[string]string) map[string]float64 {
	pw := make(map[string]float64, len(weights))
	for id, w := range weights {
		f, _ := strconv.ParseFloat(w, 64)
		pw[id] = f
	}
	return pw
}

// sessionAffinityActive reports whether session affinity is set to a real mode
// (not empty, not "none"). SessionAffinityAttributes and SessionAffinityTtl are
// only meaningful -- so only sent and drift-checked -- when affinity is active,
// mirroring the proxied-ttl gating: Cloudflare ignores these when affinity is off,
// so sending them would drift-loop.
func sessionAffinityActive(lb *lbv1beta1.LoadBalancer) bool {
	return lb.Spec.SessionAffinity != "" && lb.Spec.SessionAffinity != "none"
}

// buildAdaptiveRouting returns the CF adaptive_routing param and true when the CR
// expresses it. failover_across_pools is a leave-alone *bool: unset (nil) keeps
// Cloudflare's value, so it is sent -- and drift-checked -- only when non-nil.
func buildAdaptiveRouting(lb *lbv1beta1.LoadBalancer) (load_balancers.AdaptiveRoutingParam, bool) {
	ar := lb.Spec.AdaptiveRouting
	if ar == nil || ar.FailoverAcrossPools == nil {
		return load_balancers.AdaptiveRoutingParam{}, false
	}
	return load_balancers.AdaptiveRoutingParam{
		FailoverAcrossPools: cloudflare.F(*ar.FailoverAcrossPools),
	}, true
}

// buildLocationStrategy returns the CF location_strategy param and true when the
// CR expresses at least one subfield. mode / prefer_ecs are leave-alone strings:
// each is sent -- and drift-checked -- only when non-empty.
func buildLocationStrategy(lb *lbv1beta1.LoadBalancer) (load_balancers.LocationStrategyParam, bool) {
	ls := lb.Spec.LocationStrategy
	if ls == nil {
		return load_balancers.LocationStrategyParam{}, false
	}
	var p load_balancers.LocationStrategyParam
	set := false
	if ls.Mode != "" {
		p.Mode = cloudflare.F(load_balancers.LocationStrategyMode(ls.Mode))
		set = true
	}
	if ls.PreferECS != "" {
		p.PreferECS = cloudflare.F(load_balancers.LocationStrategyPreferECS(ls.PreferECS))
		set = true
	}
	return p, set
}

// buildSessionAffinityAttributes returns the CF session_affinity_attributes param
// and true when the CR expresses at least one subfield. Callers gate on
// sessionAffinityActive first. Each subfield is a leave-alone value: sent -- and
// drift-checked -- only when set (int32>0, non-empty slice/string, or non-nil *bool).
func buildSessionAffinityAttributes(attrs *lbv1beta1.LoadBalancerSessionAffinityAttributes) (load_balancers.SessionAffinityAttributesParam, bool) {
	var p load_balancers.SessionAffinityAttributesParam
	if attrs == nil {
		return p, false
	}
	set := false
	if attrs.DrainDuration > 0 {
		p.DrainDuration = cloudflare.F(float64(attrs.DrainDuration))
		set = true
	}
	if len(attrs.Headers) > 0 {
		p.Headers = cloudflare.F(attrs.Headers)
		set = true
	}
	if attrs.RequireAllHeaders != nil {
		p.RequireAllHeaders = cloudflare.F(*attrs.RequireAllHeaders)
		set = true
	}
	if attrs.Samesite != "" {
		p.Samesite = cloudflare.F(load_balancers.SessionAffinityAttributesSamesite(attrs.Samesite))
		set = true
	}
	if attrs.Secure != "" {
		p.Secure = cloudflare.F(load_balancers.SessionAffinityAttributesSecure(attrs.Secure))
		set = true
	}
	if attrs.ZeroDowntimeFailover != "" {
		p.ZeroDowntimeFailover = cloudflare.F(load_balancers.SessionAffinityAttributesZeroDowntimeFailover(attrs.ZeroDowntimeFailover))
		set = true
	}
	return p, set
}

// buildRandomSteering builds the CF random_steering param from the CR's default
// weight and the resolved per-pool weights (CF pool ID -> weight string from the
// resolver). pool_weights is always set when random steering is expressed (the CR
// owns the weighted-pool topology, like the geo pool maps); an unresolved weighted
// pool is already absent from weights (dropped by the resolver and recorded in
// unresolvedPoolRefs), so it is naturally omitted. default_weight is sent only when
// set. Weight strings are CRD-pattern-validated floats, so ParseFloat cannot fail
// (the error is intentionally discarded).
func buildRandomSteering(rs *lbv1beta1.LoadBalancerRandomSteering, weights map[string]string) load_balancers.RandomSteeringParam {
	var p load_balancers.RandomSteeringParam
	if rs.DefaultWeight != "" {
		dw, _ := strconv.ParseFloat(rs.DefaultWeight, 64)
		p.DefaultWeight = cloudflare.F(dw)
	}
	// pool_weights is a map property Cloudflare deep-merges on PATCH, so this typed
	// value is authoritative on create but is overridden on edit by editLoadBalancer
	// (via option.WithJSONSet) to add explicit nulls for weighted pools the CR
	// dropped -- the typed float64 map cannot express a null value.
	p.PoolWeights = cloudflare.F(resolvedWeightsToFloat(weights))
	return p
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
	// Geo pool maps are presence-managed (a non-nil map is owned in full), matching
	// editLoadBalancer, lbDrifted, and the RandomSteering gate below. On create there
	// is no CF-side map to prune, so the resolved map is sent as-is; nil is omitted.
	if lb.Spec.RegionPools != nil {
		p.RegionPools = cloudflare.F(resolved.regionIDs)
	}
	if lb.Spec.CountryPools != nil {
		p.CountryPools = cloudflare.F(resolved.countryIDs)
	}
	if lb.Spec.PopPools != nil {
		p.POPPools = cloudflare.F(resolved.popIDs)
	}
	// Networks are create-only-write: Cloudflare's create API accepts networks, but
	// its Edit API does not, so they are sent here and excluded from buildLBEditParams
	// and lbDrifted (post-create drift is surfaced, not auto-corrected). enabled is
	// the opposite -- absent from the create API, so it is applied via edit only.
	if len(lb.Spec.Networks) > 0 {
		p.Networks = cloudflare.F(lb.Spec.Networks)
	}
	// Optional nested features. Each is sent IFF the CR expresses it IFF it is
	// drift-checked in lbDrifted (PATCH/coexist invariant). The same block is
	// mirrored in buildLBEditParams -- keep the two in lockstep.
	if ar, ok := buildAdaptiveRouting(lb); ok {
		p.AdaptiveRouting = cloudflare.F(ar)
	}
	if ls, ok := buildLocationStrategy(lb); ok {
		p.LocationStrategy = cloudflare.F(ls)
	}
	if lb.Spec.RandomSteering != nil {
		p.RandomSteering = cloudflare.F(buildRandomSteering(lb.Spec.RandomSteering, resolved.randomSteeringWeights))
	}
	// session_affinity_attributes / session_affinity_ttl only apply when affinity is
	// active (set and not "none"); Cloudflare ignores them otherwise, so gating here
	// (like proxied-ttl) prevents a drift-loop against fields CF will not honor.
	if sessionAffinityActive(lb) {
		if attrs, ok := buildSessionAffinityAttributes(lb.Spec.SessionAffinityAttributes); ok {
			p.SessionAffinityAttributes = cloudflare.F(attrs)
		}
		if lb.Spec.SessionAffinityTtl > 0 {
			p.SessionAffinityTTL = cloudflare.F(float64(lb.Spec.SessionAffinityTtl))
		}
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
	// NOTE: region_pools / country_pools / pop_pools are intentionally NOT set
	// here. They are top-level map properties that Cloudflare DEEP-MERGES on PATCH,
	// so a key the CR dropped cannot be removed via the typed param (a nil slice
	// value marshals as [], not null). editLoadBalancer injects each managed map --
	// resolved keys plus explicit nulls for keys the CR no longer declares -- via
	// option.WithJSONSet to force REPLACE semantics. An unset (nil) map is left
	// untouched on Cloudflare: presence, not emptiness, decides management.
	// enabled is always-managed and edit-only (Cloudflare's create API does not
	// accept it, so it is applied via a follow-up edit -- create-then-edit). Unset
	// spec defaults to true, so an out-of-band disable is corrected back to the CR
	// value. Paired with the lbDrifted check below.
	p.Enabled = cloudflare.F(lbEnabled(lb))
	// Optional nested features. Each is sent IFF the CR expresses it IFF it is
	// drift-checked in lbDrifted (PATCH/coexist invariant). The same block is
	// mirrored in buildLBNewParams -- keep the two in lockstep.
	if ar, ok := buildAdaptiveRouting(lb); ok {
		p.AdaptiveRouting = cloudflare.F(ar)
	}
	if ls, ok := buildLocationStrategy(lb); ok {
		p.LocationStrategy = cloudflare.F(ls)
	}
	if lb.Spec.RandomSteering != nil {
		p.RandomSteering = cloudflare.F(buildRandomSteering(lb.Spec.RandomSteering, resolved.randomSteeringWeights))
	}
	// session_affinity_attributes / session_affinity_ttl only apply when affinity is
	// active (set and not "none"); Cloudflare ignores them otherwise, so gating here
	// (like proxied-ttl) prevents a drift-loop against fields CF will not honor.
	if sessionAffinityActive(lb) {
		if attrs, ok := buildSessionAffinityAttributes(lb.Spec.SessionAffinityAttributes); ok {
			p.SessionAffinityAttributes = cloudflare.F(attrs)
		}
		if lb.Spec.SessionAffinityTtl > 0 {
			p.SessionAffinityTTL = cloudflare.F(float64(lb.Spec.SessionAffinityTtl))
		}
	}
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
	// enabled is always-managed (unset spec => default true), so an out-of-band
	// disable/enable drifts and is corrected via edit. networks are deliberately
	// NOT compared here: they are create-only-write, so a networks divergence is
	// surfaced (NetworksSynced / loadbalancer_networks_drift) but never triggers a
	// corrective edit -- comparing them would drift-loop against an un-writable field.
	if cf.Enabled != lbEnabled(lb) {
		return true
	}
	// Geo pool maps drift only when the CR manages them (the field is set --
	// presence, not emptiness). An unset (nil) map is left alone: never compared,
	// never corrected, matching editLoadBalancer's send-only-when-managed rule.
	if lb.Spec.RegionPools != nil && !mapListsEqual(cf.RegionPools, resolved.regionIDs) {
		return true
	}
	if lb.Spec.CountryPools != nil && !mapListsEqual(cf.CountryPools, resolved.countryIDs) {
		return true
	}
	if lb.Spec.PopPools != nil && !mapListsEqual(cf.POPPools, resolved.popIDs) {
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
	// Optional nested features (adaptive routing, location strategy, random steering,
	// session affinity) are drift-checked in a helper to keep this function's
	// cyclomatic complexity bounded. Each is compared IFF the CR expresses it IFF it
	// is sent by the build funcs (PATCH/coexist invariant), so an unset feature is
	// never compared and cannot drift-loop.
	return lbNestedFeaturesDrifted(cf, lb, resolved)
}

// lbNestedFeaturesDrifted reports whether any operator-managed optional nested LB
// feature diverges from the CR. Split out of lbDrifted to bound its complexity; the
// send-IFF-drift-checked invariant is preserved (mirrors buildLBEditParams).
func lbNestedFeaturesDrifted(cf *load_balancers.LoadBalancer, lb *lbv1beta1.LoadBalancer, resolved *resolvedPools) bool {
	if ar := lb.Spec.AdaptiveRouting; ar != nil && ar.FailoverAcrossPools != nil {
		if cf.AdaptiveRouting.FailoverAcrossPools != *ar.FailoverAcrossPools {
			return true
		}
	}
	if ls := lb.Spec.LocationStrategy; ls != nil {
		if ls.Mode != "" && string(cf.LocationStrategy.Mode) != ls.Mode {
			return true
		}
		if ls.PreferECS != "" && string(cf.LocationStrategy.PreferECS) != ls.PreferECS {
			return true
		}
	}
	if rs := lb.Spec.RandomSteering; rs != nil {
		if randomSteeringDrifted(cf.RandomSteering, rs, resolved.randomSteeringWeights) {
			return true
		}
	}
	if sessionAffinityActive(lb) {
		if sessionAffinityAttributesDrifted(cf.SessionAffinityAttributes, lb.Spec.SessionAffinityAttributes) {
			return true
		}
		if lb.Spec.SessionAffinityTtl > 0 && int32(cf.SessionAffinityTTL) != lb.Spec.SessionAffinityTtl {
			return true
		}
	}
	return false
}

// sessionAffinityAttributesDrifted compares the CF-observed attributes against the
// CR field-by-field, honoring the leave-alone rule: a subfield is compared only when
// the CR expresses it. Callers gate on sessionAffinityActive first.
func sessionAffinityAttributesDrifted(cf load_balancers.SessionAffinityAttributes, attrs *lbv1beta1.LoadBalancerSessionAffinityAttributes) bool {
	if attrs == nil {
		return false
	}
	if attrs.DrainDuration > 0 && int32(cf.DrainDuration) != attrs.DrainDuration {
		return true
	}
	if len(attrs.Headers) > 0 && !stringSlicesEqual(cf.Headers, attrs.Headers) {
		return true
	}
	if attrs.RequireAllHeaders != nil && cf.RequireAllHeaders != *attrs.RequireAllHeaders {
		return true
	}
	if attrs.Samesite != "" && string(cf.Samesite) != attrs.Samesite {
		return true
	}
	if attrs.Secure != "" && string(cf.Secure) != attrs.Secure {
		return true
	}
	if attrs.ZeroDowntimeFailover != "" && string(cf.ZeroDowntimeFailover) != attrs.ZeroDowntimeFailover {
		return true
	}
	return false
}

// randomSteeringDrifted compares the CF-observed random_steering against the CR.
// pool_weights is structural (the CR owns the weighted-pool topology, like the geo
// pool maps), so the full resolved map is compared; default_weight is leave-alone
// (compared only when set). Weight strings are CRD-pattern-validated floats, so the
// ParseFloat calls cannot fail (the error is intentionally discarded), and the same
// parse feeds both the build payload and this check, so equal input never drifts.
func randomSteeringDrifted(cf load_balancers.RandomSteering, rs *lbv1beta1.LoadBalancerRandomSteering, weights map[string]string) bool {
	if rs.DefaultWeight != "" {
		dw, _ := strconv.ParseFloat(rs.DefaultWeight, 64)
		if cf.DefaultWeight != dw {
			return true
		}
	}
	if len(cf.PoolWeights) != len(weights) {
		return true
	}
	for id, w := range weights {
		f, _ := strconv.ParseFloat(w, 64)
		cfw, ok := cf.PoolWeights[id]
		if !ok || cfw != f {
			return true
		}
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

// unorderedStringSlicesEqual compares two string slices as sets (order- and
// duplicate-insensitive within the multiset). Used for networks drift surfacing,
// where Cloudflare may return spec.networks in a different order than submitted --
// an order-only difference is not a real divergence.
func unorderedStringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		if seen[v] == 0 {
			return false
		}
		seen[v]--
	}
	return true
}
