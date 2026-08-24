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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	accountsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/accounts/v1beta1"
	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

// --- recordFailureEvent: the helper's decision logic, in isolation ---

func TestRecordFailureEvent_TransitionOnly(t *testing.T) {
	failing := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, Reason: "CreateFailed"}}
	ready := []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Reconciled"}}

	cases := []struct {
		name   string
		conds  []metav1.Condition
		reason string
		want   bool
	}{
		{"no prior condition -> emit", nil, "CreateFailed", true},
		{"already failing, same reason -> suppress", failing, "CreateFailed", false},
		{"already failing, new reason -> emit", failing, "UpdateFailed", true},
		{"transition from Ready=True -> emit", ready, "CreateFailed", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := events.NewFakeRecorder(4)
			recordFailureEvent(rec, &corev1.ConfigMap{}, tc.conds, "Ready", tc.reason, "boom")
			got := len(rec.Events) == 1
			if got != tc.want {
				t.Fatalf("emitted=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestRecordFailureEvent_NilRecorderNoPanic(t *testing.T) {
	recordFailureEvent(nil, &corev1.ConfigMap{}, nil, "Ready", "CreateFailed", "boom")
}

func TestRecordFailureEvent_TruncatesLongNote(t *testing.T) {
	rec := events.NewFakeRecorder(2)
	recordFailureEvent(rec, &corev1.ConfigMap{}, nil, "Ready", "CreateFailed", strings.Repeat("x", 4000))
	msg := <-rec.Events
	if len(msg) > maxEventNote+100 {
		t.Fatalf("note not truncated: len=%d", len(msg))
	}
}

// --- funnel tests: the controllers' setError / setInitialized route through the
// shared helper, so an actual failing call emits (transition-only) and a success
// does not. Uses a fake client + FakeRecorder: no API server, no RBAC, no cluster,
// so these are deterministic regression guards for the wiring (the RBAC and real
// Event persistence are verified separately on a live cluster). ---

func newEventTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := runtime.NewScheme()
	if err := lbv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("add lb scheme: %v", err)
	}
	if err := accountsv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("add accounts scheme: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).WithStatusSubresource(objs...).Build()
}

// TestMonitorSetError_EmitsWarningEventTransitionOnly guards the Ready-condition
// funnel (shared by LB / Pool / Monitor / CH via setError).
func TestMonitorSetError_EmitsWarningEventTransitionOnly(t *testing.T) {
	ctx := context.Background()
	mon := &lbv1beta1.LoadBalancerMonitor{ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"}}
	cl := newEventTestClient(t, mon)
	rec := events.NewFakeRecorder(8)
	r := &LoadBalancerMonitorReconciler{Client: cl, Recorder: rec, RequeueInterval: time.Second}
	key := client.ObjectKeyFromObject(mon)
	reget := func() {
		if err := cl.Get(ctx, key, mon); err != nil {
			t.Fatalf("get: %v", err)
		}
	}

	reget()
	if _, err := r.setError(ctx, mon, "CreateFailed", "boom"); err != nil {
		t.Fatalf("setError: %v", err)
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("first failure: want 1 event, got %d", got)
	}

	reget()
	if _, err := r.setError(ctx, mon, "CreateFailed", "boom again"); err != nil {
		t.Fatalf("setError: %v", err)
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("repeat same reason: want no new event (still 1), got %d", got)
	}

	reget()
	if _, err := r.setError(ctx, mon, "UpdateFailed", "changed"); err != nil {
		t.Fatalf("setError: %v", err)
	}
	if got := len(rec.Events); got != 2 {
		t.Fatalf("reason change: want 2 events, got %d", got)
	}
}

// TestAccountSetInitialized_EmitsOnFailureNotSuccess guards the Initialized-condition
// funnel (Account) and that a successful validation emits nothing.
func TestAccountSetInitialized_EmitsOnFailureNotSuccess(t *testing.T) {
	ctx := context.Background()
	acct := &accountsv1beta1.Account{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "ns"}}
	cl := newEventTestClient(t, acct)
	rec := events.NewFakeRecorder(8)
	r := &AccountReconciler{Client: cl, Recorder: rec, RequeueInterval: time.Second}
	key := client.ObjectKeyFromObject(acct)
	reget := func() {
		if err := cl.Get(ctx, key, acct); err != nil {
			t.Fatalf("get: %v", err)
		}
	}

	reget()
	if _, err := r.setInitialized(ctx, acct, metav1.ConditionFalse, "ValidationFailed", "bad token"); err != nil {
		t.Fatalf("setInitialized: %v", err)
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("failure: want 1 event, got %d", got)
	}

	reget()
	if _, err := r.setInitialized(ctx, acct, metav1.ConditionFalse, "ValidationFailed", "still bad"); err != nil {
		t.Fatalf("setInitialized: %v", err)
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("repeat failure: want still 1, got %d", got)
	}

	reget()
	if _, err := r.setInitialized(ctx, acct, metav1.ConditionTrue, "AccountValidated", "ok"); err != nil {
		t.Fatalf("setInitialized: %v", err)
	}
	if got := len(rec.Events); got != 1 {
		t.Fatalf("success must not emit: want still 1, got %d", got)
	}
}
