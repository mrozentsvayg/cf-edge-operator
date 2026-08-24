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
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/accounts"
	"github.com/cloudflare/cloudflare-go/v6/option"

	accountsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/accounts/v1beta1"
)

// AccountReconciler validates an Account: it confirms the referenced credentials
// can reach the Cloudflare account (accounts.Get) and records the account name
// and an Initialized condition. It creates no Cloudflare resources -- Pool and
// Monitor reconcilers read the Account's spec directly for their account ID and
// credentials. Validation is a fail-fast convenience (mirrors the Zone
// controller); a bad account ID or token surfaces here instead of only later on
// a Pool/Monitor reconcile.
type AccountReconciler struct {
	client.Client
	Scheme            *runtime.Scheme
	OperatorNamespace string
	CFAPITimeout      time.Duration
	CFAPIMaxRetries   int
	CFBaseURL         string
	// RequeueInterval is how often a reconciled Account re-validates its
	// credentials against Cloudflare (and retries after a transient validation
	// failure); set from --drift-interval. Periodic re-validation keeps
	// account_initialized fresh so a revoked token surfaces without a spec change;
	// a re-validation that finds no change is silent and does not re-write status,
	// so steady state is quiet.
	RequeueInterval time.Duration
}

// +kubebuilder:rbac:groups=accounts.cf-edge.io,resources=accounts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=accounts.cf-edge.io,resources=accounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *AccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var account accountsv1beta1.Account
	if err := r.Get(ctx, req.NamespacedName, &account); err != nil {
		if client.IgnoreNotFound(err) == nil {
			// Account deleted -- remove the stale metric series.
			accountInitialized.DeleteLabelValues(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Ensure the metric series exists so the CfEdgeOperatorAccountNotInitialized
	// alert (expr: == 0) can match before the first validation completes. Mirrors
	// the Zone controller's zoneInitialized handling; setInitialized keeps it
	// current thereafter.
	if cond := apimeta.FindStatusCondition(account.Status.Conditions, conditionInitialized); cond != nil && cond.Status == metav1.ConditionTrue {
		accountInitialized.WithLabelValues(account.Name).Set(1)
	} else {
		accountInitialized.WithLabelValues(account.Name).Set(0)
	}

	cf, err := cfClientFromSecret(ctx, r.Client, account.Spec.CredentialsRef, account.Namespace, r.CFAPITimeout, r.CFBaseURL)
	if err != nil {
		// A missing/rotating secret is transient, not a definitive credential
		// problem. Leave the Initialized condition + metric intact (sticky) --
		// mirroring the Zone controller, which never touches Initialized on a token
		// fetch failure -- and requeue. Flipping to False here would false-page the
		// critical AccountNotInitialized alert on a routine secret rotation.
		log.Error(err, "account - client initialization failed (transient, keeping last-known Initialized state)", "account", account.Name)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
	}

	// Validate the credentials can reach the account, and record its name.
	var name string
	attempts, err := cfRetry(ctx, cfResourceAccount, cfOpGet, r.CFAPIMaxRetries, func() error {
		start := time.Now()
		resp, callErr := cf.Accounts.Get(ctx, accounts.AccountGetParams{AccountID: cloudflare.F(account.Spec.ID)},
			option.WithRequestTimeout(r.CFAPITimeout))
		recordCFCall(cfResourceAccount, cfOpGet, start, &callErr)
		if callErr == nil && resp != nil {
			name = resp.Name
		}
		return callErr
	})
	if err != nil {
		if isDefinitiveValidationFailure(err) {
			// Definitive failure (a 4xx other than rate limiting -- invalid/revoked
			// token, missing account, malformed request): the credentials genuinely
			// cannot validate, so flip Initialized=False + metric=0 to fire the alert.
			log.Error(err, "account - validation failed (definitive)", "account", account.Name, "attempts", attempts)
			return r.setInitialized(ctx, &account, metav1.ConditionFalse, "ValidationFailed", err.Error())
		}
		// Transient failure (timeout, 5xx, network error, or 429 rate limit): leave
		// the Initialized condition + metric intact (sticky) so a blip does not flip
		// a previously-validated Account and false-page the critical alert. Requeue.
		log.Error(err, "account - validation failed (transient, keeping last-known Initialized state)", "account", account.Name, "attempts", attempts)
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
	}

	// Only write status + log on a real change: the first successful validation, or
	// a changed account name. A steady-state re-validation (already Initialized,
	// same name) is fully silent -- no log line at any level and no no-op status
	// write every RequeueInterval -- matching the sibling controllers on a no-op
	// reconcile (Zone's init-once guard; the LoadBalancer controllers' readyReasonIs).
	// The top-of-reconcile block keeps account_initialized fresh, and a definitive
	// failure above already flipped the condition + metric, so there is nothing to
	// do on a no-op re-validation.
	if apimeta.IsStatusConditionTrue(account.Status.Conditions, conditionInitialized) && account.Status.Name == name {
		return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
	}

	account.Status.Name = name
	log.Info("account - initialized", "account", account.Name, "accountID", account.Spec.ID, "name", name)
	return r.setInitialized(ctx, &account, metav1.ConditionTrue, "AccountValidated",
		fmt.Sprintf("Account credentials validated (%s)", name))
}

// setInitialized records the Account's validation outcome and schedules the next
// re-validation via RequeueInterval so revoked credentials or account changes
// surface without waiting for a spec change. Called only on a real change (first
// init, a changed account name, or a definitive failure) -- steady-state
// re-validations return earlier without re-writing status.
func (r *AccountReconciler) setInitialized(ctx context.Context, account *accountsv1beta1.Account, status metav1.ConditionStatus, reason, message string) (ctrl.Result, error) {
	apimeta.SetStatusCondition(&account.Status.Conditions, metav1.Condition{
		Type:               conditionInitialized,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: account.Generation,
	})
	if err := r.Status().Update(ctx, account); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to update account status: %w", err)
	}
	if status == metav1.ConditionTrue {
		accountInitialized.WithLabelValues(account.Name).Set(1)
	} else {
		accountInitialized.WithLabelValues(account.Name).Set(0)
	}
	return ctrl.Result{RequeueAfter: r.RequeueInterval}, nil
}

// isDefinitiveValidationFailure reports whether err is a definitive Cloudflare
// client error -- a 4xx status other than 429 rate limiting -- as opposed to a
// transient failure (429, 5xx, timeout, cancellation, or a network error). A
// definitive failure (401/403 invalid or unauthorized token, 404 missing account,
// 400 malformed request) will not self-heal, so the Account controller flips
// Initialized=False + metric=0 on it to fire the AccountNotInitialized alert.
// Transient failures are sticky: a previously-validated Account keeps its
// condition and metric so a blip does not false-page the critical alert.
func isDefinitiveValidationFailure(err error) bool {
	var cfErr *cloudflare.Error
	if errors.As(err, &cfErr) {
		return cfErr.StatusCode >= 400 && cfErr.StatusCode < 500 && cfErr.StatusCode != 429
	}
	return false
}

// cfClientFromSecret builds a Cloudflare API client from a credentials secret.
// Shared by the Account, Pool, and Monitor reconcilers.
func cfClientFromSecret(ctx context.Context, c client.Client, credRef accountsv1beta1.SecretRef, secretNS string, cfTimeout time.Duration, cfBaseURL string) (*cloudflare.Client, error) {
	key := credRef.Key
	if key == "" {
		key = defaultAPITokenKey
	}
	var secret corev1.Secret
	if err := c.Get(ctx, types.NamespacedName{Name: credRef.Name, Namespace: secretNS}, &secret); err != nil {
		return nil, fmt.Errorf("secret %q not found: %w", credRef.Name, err)
	}
	token, ok := secret.Data[key]
	if !ok {
		return nil, fmt.Errorf("key %q not found in secret %q", key, credRef.Name)
	}
	opts := []option.RequestOption{
		option.WithAPIToken(string(token)),
		option.WithMaxRetries(0),
	}
	if cfTimeout > 0 {
		opts = append(opts, option.WithRequestTimeout(cfTimeout))
	}
	if cfBaseURL != "" {
		opts = append(opts, option.WithBaseURL(cfBaseURL))
	}
	return cloudflare.NewClient(opts...), nil
}

// SetupWithManager wires the Account controller. Accounts are passive config
// (no CF resources created), so it only reconciles on spec changes.
func (r *AccountReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&accountsv1beta1.Account{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("account").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}
