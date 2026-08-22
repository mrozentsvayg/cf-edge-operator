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
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Ready condition reasons shared by the load-balancing controllers
// (LoadBalancer / LoadBalancerPool / LoadBalancerMonitor). Centralized because
// they are referenced in more than one place -- the controllers write them and
// lbReadyState classifies the aggregate state gauges from them, so the mapping
// cannot silently drift away from the reasons the controllers actually set.
// Single-use failure reasons (AccountError, ZoneError, MonitorRefError,
// PoolResolutionError, LookupFailed, CreateFailed, UpdateFailed) stay as literals
// at their call sites; lbReadyState folds every Ready=False reason it does not
// recognize into the "error" state.
const (
	reasonReconciled             = "Reconciled"             // Ready=True: synchronized with Cloudflare
	reasonPartial                = "Partial"                // Ready=True: synchronized but serving degraded (LB-only; >=1 pool ref unresolved)
	reasonDryRun                 = "DryRun"                 // Ready=False: writes suppressed (--dry-run)
	reasonWaitingForMonitor      = "WaitingForMonitor"      // Ready=False: pool waiting on its monitor ref
	reasonWaitingForFallbackPool = "WaitingForFallbackPool" // Ready=False: LB waiting on its fallback pool ref
	reasonWaitingForExternal     = "WaitingForExternal"     // Ready=False: observe mode, resource not yet in Cloudflare
)

// lbReadyState classifies a load-balancing CR into one of five mutually
// exclusive states from its Ready condition -- the load-balancing analog of
// crState for CustomHostname. The states partition every CR (their sum equals
// the total), so each CR maps to exactly one bucket:
//
//   - ready:   Ready=True (reason Reconciled): fully synchronized with Cloudflare.
//   - partial: Ready=True, reason Partial (LB-only): synchronized and serving,
//     but >=1 referenced pool is unresolved so the LB runs with a reduced pool
//     set. Pools and monitors never enter this state (only the LoadBalancer
//     controller sets reason Partial), so their gauges never emit a partial
//     series.
//   - dryrun:  Ready=False, reason DryRun (writes suppressed).
//   - waiting: Ready=False, a soft wait on a dependency that increments no error
//     counter -- WaitingForMonitor / WaitingForFallbackPool (a dangling or
//     not-yet-ready ref) or WaitingForExternal (observe mode).
//   - error:   any other Ready=False reason -- the setError failures
//     (AccountError, ZoneError, MonitorRefError, PoolResolutionError,
//     LookupFailed, CreateFailed, UpdateFailed).
//
// A CR with no Ready condition yet (finalizer just added, not reconciled) is
// counted as waiting: it is neither ready nor failed, and the state is transient
// (the next reconcile writes a condition). Deriving from the reason -- not from
// status.consecutiveErrors -- is deliberate: some soft-wait paths leave the error
// counter untouched rather than resetting it, so the counter is not a reliable
// signal.
func lbReadyState(conds []metav1.Condition) string {
	cond := apimeta.FindStatusCondition(conds, conditionReady)
	if cond == nil {
		return lbStateWaiting
	}
	if cond.Status == metav1.ConditionTrue {
		if cond.Reason == reasonPartial {
			return lbStatePartial
		}
		return lbStateReady
	}
	switch cond.Reason {
	case reasonDryRun:
		return lbStateDryRun
	case reasonWaitingForMonitor, reasonWaitingForFallbackPool, reasonWaitingForExternal:
		return lbStateWaiting
	default:
		return lbStateError
	}
}
