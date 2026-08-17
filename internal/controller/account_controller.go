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

	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
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
}

// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=accounts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=loadbalancing.cf-edge.io,resources=accounts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *AccountReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var account lbv1beta1.Account
	if err := r.Get(ctx, req.NamespacedName, &account); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cf, err := cfClientFromSecret(ctx, r.Client, account.Spec.CredentialsRef, account.Namespace, r.CFAPITimeout, r.CFBaseURL)
	if err != nil {
		log.Error(err, "account - client initialization failed", "account", account.Name)
		return ctrl.Result{}, r.setInitialized(ctx, &account, metav1.ConditionFalse, "CredentialsError", err.Error())
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
		log.Error(err, "account - validation failed", "account", account.Name, "attempts", attempts)
		return ctrl.Result{}, r.setInitialized(ctx, &account, metav1.ConditionFalse, "ValidationFailed", err.Error())
	}

	account.Status.Name = name
	log.Info("account - initialized", "account", account.Name, "accountID", account.Spec.ID, "name", name)
	return ctrl.Result{}, r.setInitialized(ctx, &account, metav1.ConditionTrue, "AccountValidated",
		fmt.Sprintf("Account credentials validated (%s)", name))
}

func (r *AccountReconciler) setInitialized(ctx context.Context, account *lbv1beta1.Account, status metav1.ConditionStatus, reason, message string) error {
	apimeta.SetStatusCondition(&account.Status.Conditions, metav1.Condition{
		Type:               conditionInitialized,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: account.Generation,
	})
	if err := r.Status().Update(ctx, account); err != nil {
		return fmt.Errorf("failed to update account status: %w", err)
	}
	return nil
}

// cfClientFromSecret builds a Cloudflare API client from a credentials secret.
// Shared by the Account, Pool, and Monitor reconcilers.
func cfClientFromSecret(ctx context.Context, c client.Client, credRef lbv1beta1.SecretRef, secretNS string, cfTimeout time.Duration, cfBaseURL string) (*cloudflare.Client, error) {
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
		For(&lbv1beta1.Account{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Named("account").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedWithMaxWaitRateLimiter(
				workqueue.DefaultTypedControllerRateLimiter[reconcile.Request](),
				30*time.Second,
			),
		}).
		Complete(r)
}
