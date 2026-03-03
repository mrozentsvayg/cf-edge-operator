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
	"reflect"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	saasv1alpha1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1alpha1"
)

func TestDetectConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = saasv1alpha1.AddToScheme(scheme)

	indexer := func(o client.Object) []string {
		return []string{o.(*saasv1alpha1.CustomHostname).Spec.Hostname}
	}
	makeReconciler := func(objs ...client.Object) *CustomHostnameReconciler {
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&saasv1alpha1.CustomHostname{}, hostnameField, indexer).
			WithStatusSubresource(&saasv1alpha1.CustomHostname{}).
			WithObjects(objs...).
			Build()
		return &CustomHostnameReconciler{Client: c, Scheme: scheme}
	}

	owner := &saasv1alpha1.CustomHostname{
		ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default", UID: types.UID("uid-owner")},
		Spec:       saasv1alpha1.CustomHostnameSpec{Hostname: "api.acme.com"},
		Status:     saasv1alpha1.CustomHostnameStatus{ID: "cf-id-abc"},
	}
	ctx := context.Background()

	t.Run("no conflict: sole CR for hostname", func(t *testing.T) {
		r := makeReconciler(owner)
		conflicted, err := r.detectConflict(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if conflicted {
			t.Error("expected no conflict for sole CR")
		}
	})

	t.Run("conflict detected: peer has CF ID", func(t *testing.T) {
		duplicate := &saasv1alpha1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "duplicate", Namespace: "default", UID: types.UID("uid-dup")},
			Spec:       saasv1alpha1.CustomHostnameSpec{Hostname: "api.acme.com"},
		}
		r := makeReconciler(owner, duplicate)

		conflicted, err := r.detectConflict(ctx, duplicate)
		if err != nil {
			t.Fatal(err)
		}
		if !conflicted {
			t.Error("expected conflict to be detected")
		}
		// HostnameConflict condition must be set on the duplicate
		var got saasv1alpha1.CustomHostname
		if err := r.Get(ctx, client.ObjectKeyFromObject(duplicate), &got); err != nil {
			t.Fatal(err)
		}
		if !isHostnameConflict(&got) {
			t.Errorf("expected HostnameConflict condition, got: %v", got.Status.Conditions)
		}
		// Duplicate must not have adopted the CF ID
		if got.Status.ID != "" {
			t.Errorf("expected empty status.id on duplicate, got %q", got.Status.ID)
		}
	})

	t.Run("no conflict: peer has no CF ID yet", func(t *testing.T) {
		// Both CRs are brand-new — neither has a CF ID. Let both through;
		// CF will reject one, and the conflict is detected on the next reconcile.
		a := &saasv1alpha1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "cr-a", Namespace: "default", UID: types.UID("uid-a")},
			Spec:       saasv1alpha1.CustomHostnameSpec{Hostname: "api.acme.com"},
		}
		b := &saasv1alpha1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "cr-b", Namespace: "default", UID: types.UID("uid-b")},
			Spec:       saasv1alpha1.CustomHostnameSpec{Hostname: "api.acme.com"},
		}
		r := makeReconciler(a, b)

		conflicted, err := r.detectConflict(ctx, b)
		if err != nil {
			t.Fatal(err)
		}
		if conflicted {
			t.Error("expected no conflict when no peer has a CF ID")
		}
	})

	t.Run("no conflict: different hostname", func(t *testing.T) {
		other := &saasv1alpha1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", UID: types.UID("uid-other")},
			Spec:       saasv1alpha1.CustomHostnameSpec{Hostname: "other.acme.com"},
			Status:     saasv1alpha1.CustomHostnameStatus{ID: "cf-id-xyz"},
		}
		r := makeReconciler(owner, other)

		conflicted, err := r.detectConflict(ctx, owner)
		if err != nil {
			t.Fatal(err)
		}
		if conflicted {
			t.Error("expected no conflict for different hostname")
		}
	})
}

func TestFastWritePredicateTerminating(t *testing.T) {
	pred := fastWritePredicate()
	now := metav1.Now()

	tests := []struct {
		name string
		ch   saasv1alpha1.CustomHostname
		want bool
	}{
		{
			name: "new CR, no ID → let through",
			ch:   saasv1alpha1.CustomHostname{},
			want: true,
		},
		{
			name: "existing CR, has ID → block (Zone controller handles drift)",
			ch:   saasv1alpha1.CustomHostname{Status: saasv1alpha1.CustomHostnameStatus{ID: "cf-id-abc"}},
			want: false,
		},
		{
			name: "terminating CR with ID → let through (must remove finalizer)",
			ch: saasv1alpha1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status:     saasv1alpha1.CustomHostnameStatus{ID: "cf-id-abc"},
			},
			want: true,
		},
		{
			name: "terminating CR without ID → let through",
			ch:   saasv1alpha1.CustomHostname{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := pred.Create(event.CreateEvent{Object: &tt.ch})
			if got != tt.want {
				t.Errorf("fastWritePredicate.Create() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestHandleDeleteDryRun(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = saasv1alpha1.AddToScheme(scheme)

	now := metav1.Now()
	ch := &saasv1alpha1.CustomHostname{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-ch",
			Namespace:         "default",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &now,
		},
		Spec:   saasv1alpha1.CustomHostnameSpec{Hostname: "test.example.com"},
		Status: saasv1alpha1.CustomHostnameStatus{ID: "cf-id-123"},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ch).
		Build()

	r := &CustomHostnameReconciler{Client: c, Scheme: scheme, DryRun: true}

	ctx := context.Background()
	result, err := r.handleDelete(ctx, nil, "zone-id", ch)
	if err != nil {
		t.Fatalf("handleDelete returned error: %v", err)
	}
	if result.Requeue || result.RequeueAfter != 0 {
		t.Errorf("expected no requeue in dry-run, got %+v", result)
	}

	// The finalizer must still be present — dry-run must not mutate K8s.
	// If this fails it means dry-run removed the finalizer, which would
	// silently orphan the CF hostname when the CR disappears from K8s.
	var got saasv1alpha1.CustomHostname
	if err := c.Get(ctx, client.ObjectKeyFromObject(ch), &got); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	for _, f := range got.Finalizers {
		if f == finalizerName {
			return // finalizer present — correct
		}
	}
	t.Error("dry-run: finalizer was removed; CR would disappear from K8s while CF hostname stays (orphan bug)")
}

func TestShouldDeleteInCF(t *testing.T) {
	matched := &custom_hostnames.CustomHostnameListResponse{ID: "abc123"}
	different := &custom_hostnames.CustomHostnameListResponse{ID: "xyz999"}

	tests := []struct {
		name     string
		policy   string
		statusID string
		current  *custom_hostnames.CustomHostnameListResponse
		want     bool
	}{
		{
			name:     "always: always deletes regardless",
			policy:   DeletePolicyAlways,
			statusID: "abc123",
			current:  different,
			want:     true,
		},
		{
			name:     "always: deletes even when not found",
			policy:   DeletePolicyAlways,
			statusID: "abc123",
			current:  nil,
			want:     true,
		},
		{
			name:     "own-only: not found in CF, do not delete",
			policy:   DeletePolicyOwnOnly,
			statusID: "abc123",
			current:  nil,
			want:     false,
		},
		{
			name:     "own-only: IDs match, delete",
			policy:   DeletePolicyOwnOnly,
			statusID: "abc123",
			current:  matched,
			want:     true,
		},
		{
			name:     "own-only: IDs differ, do not delete",
			policy:   DeletePolicyOwnOnly,
			statusID: "abc123",
			current:  different,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDeleteInCF(tt.policy, tt.statusID, tt.current); got != tt.want {
				t.Errorf("shouldDeleteInCF() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSSLStatusFromNew(t *testing.T) {
	expires := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		resp custom_hostnames.CustomHostnameNewResponse
		want saasv1alpha1.CustomHostnameSSLStatus
	}{
		{
			name: "status only",
			resp: custom_hostnames.CustomHostnameNewResponse{
				SSL: custom_hostnames.CustomHostnameNewResponseSSL{Status: "pending_validation"},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{Status: "pending_validation"},
		},
		{
			name: "expires on set",
			resp: custom_hostnames.CustomHostnameNewResponse{
				SSL: custom_hostnames.CustomHostnameNewResponseSSL{
					Status:    "active",
					ExpiresOn: expires,
				},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{
				Status:    "active",
				ExpiresOn: func() *metav1.Time { t := metav1.NewTime(expires); return &t }(),
			},
		},
		{
			name: "validation record fields mapped",
			resp: custom_hostnames.CustomHostnameNewResponse{
				SSL: custom_hostnames.CustomHostnameNewResponseSSL{
					Status: "pending_validation",
					ValidationRecords: []custom_hostnames.CustomHostnameNewResponseSSLValidationRecord{
						{TXTName: "_cf-txt.example.com", TXTValue: "abc123", HTTPURL: "http://example.com/.well-known/pki-validation/x.txt", HTTPBody: "body"},
					},
				},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{
				Status: "pending_validation",
				ValidationRecords: []saasv1alpha1.SSLValidationRecord{
					{TXTName: "_cf-txt.example.com", TXTValue: "abc123", HTTPUrl: "http://example.com/.well-known/pki-validation/x.txt", HTTPBody: "body"},
				},
			},
		},
		{
			name: "validation error mapped",
			resp: custom_hostnames.CustomHostnameNewResponse{
				SSL: custom_hostnames.CustomHostnameNewResponseSSL{
					Status:           "validation_timed_out",
					ValidationErrors: []custom_hostnames.CustomHostnameNewResponseSSLValidationError{{Message: "timed out"}},
				},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{
				Status:           "validation_timed_out",
				ValidationErrors: []string{"timed out"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sslStatusFromNew(&tt.resp)
			if got.Status != tt.want.Status {
				t.Errorf("Status = %q, want %q", got.Status, tt.want.Status)
			}
			if (got.ExpiresOn == nil) != (tt.want.ExpiresOn == nil) {
				t.Errorf("ExpiresOn nil mismatch: got %v, want %v", got.ExpiresOn, tt.want.ExpiresOn)
			} else if got.ExpiresOn != nil && !got.ExpiresOn.Equal(tt.want.ExpiresOn) {
				t.Errorf("ExpiresOn = %v, want %v", got.ExpiresOn, tt.want.ExpiresOn)
			}
			if len(got.ValidationRecords) != len(tt.want.ValidationRecords) {
				t.Fatalf("ValidationRecords len = %d, want %d", len(got.ValidationRecords), len(tt.want.ValidationRecords))
			}
			for i, vr := range got.ValidationRecords {
				if !reflect.DeepEqual(vr, tt.want.ValidationRecords[i]) {
					t.Errorf("ValidationRecords[%d] = %+v, want %+v", i, vr, tt.want.ValidationRecords[i])
				}
			}
			if len(got.ValidationErrors) != len(tt.want.ValidationErrors) {
				t.Fatalf("ValidationErrors len = %d, want %d", len(got.ValidationErrors), len(tt.want.ValidationErrors))
			}
			for i, ve := range got.ValidationErrors {
				if ve != tt.want.ValidationErrors[i] {
					t.Errorf("ValidationErrors[%d] = %q, want %q", i, ve, tt.want.ValidationErrors[i])
				}
			}
		})
	}
}

func TestSSLStatusFromList(t *testing.T) {
	expires := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		resp custom_hostnames.CustomHostnameListResponse
		want saasv1alpha1.CustomHostnameSSLStatus
	}{
		{
			name: "status only",
			resp: custom_hostnames.CustomHostnameListResponse{
				SSL: custom_hostnames.CustomHostnameListResponseSSL{Status: "pending_validation"},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{Status: "pending_validation"},
		},
		{
			name: "expires on set",
			resp: custom_hostnames.CustomHostnameListResponse{
				SSL: custom_hostnames.CustomHostnameListResponseSSL{
					Status:    "active",
					ExpiresOn: expires,
				},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{
				Status:    "active",
				ExpiresOn: func() *metav1.Time { t := metav1.NewTime(expires); return &t }(),
			},
		},
		{
			name: "validation record fields mapped",
			resp: custom_hostnames.CustomHostnameListResponse{
				SSL: custom_hostnames.CustomHostnameListResponseSSL{
					Status: "pending_validation",
					ValidationRecords: []custom_hostnames.CustomHostnameListResponseSSLValidationRecord{
						{TXTName: "_cf-txt.example.com", TXTValue: "abc123", HTTPURL: "http://example.com/.well-known/pki-validation/x.txt", HTTPBody: "body"},
					},
				},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{
				Status: "pending_validation",
				ValidationRecords: []saasv1alpha1.SSLValidationRecord{
					{TXTName: "_cf-txt.example.com", TXTValue: "abc123", HTTPUrl: "http://example.com/.well-known/pki-validation/x.txt", HTTPBody: "body"},
				},
			},
		},
		{
			name: "validation error mapped",
			resp: custom_hostnames.CustomHostnameListResponse{
				SSL: custom_hostnames.CustomHostnameListResponseSSL{
					Status:           "validation_timed_out",
					ValidationErrors: []custom_hostnames.CustomHostnameListResponseSSLValidationError{{Message: "timed out"}},
				},
			},
			want: saasv1alpha1.CustomHostnameSSLStatus{
				Status:           "validation_timed_out",
				ValidationErrors: []string{"timed out"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sslStatusFromList(&tt.resp)
			if got.Status != tt.want.Status {
				t.Errorf("Status = %q, want %q", got.Status, tt.want.Status)
			}
			if (got.ExpiresOn == nil) != (tt.want.ExpiresOn == nil) {
				t.Errorf("ExpiresOn nil mismatch: got %v, want %v", got.ExpiresOn, tt.want.ExpiresOn)
			} else if got.ExpiresOn != nil && !got.ExpiresOn.Equal(tt.want.ExpiresOn) {
				t.Errorf("ExpiresOn = %v, want %v", got.ExpiresOn, tt.want.ExpiresOn)
			}
			if len(got.ValidationRecords) != len(tt.want.ValidationRecords) {
				t.Fatalf("ValidationRecords len = %d, want %d", len(got.ValidationRecords), len(tt.want.ValidationRecords))
			}
			for i, vr := range got.ValidationRecords {
				if !reflect.DeepEqual(vr, tt.want.ValidationRecords[i]) {
					t.Errorf("ValidationRecords[%d] = %+v, want %+v", i, vr, tt.want.ValidationRecords[i])
				}
			}
			if len(got.ValidationErrors) != len(tt.want.ValidationErrors) {
				t.Fatalf("ValidationErrors len = %d, want %d", len(got.ValidationErrors), len(tt.want.ValidationErrors))
			}
			for i, ve := range got.ValidationErrors {
				if ve != tt.want.ValidationErrors[i] {
					t.Errorf("ValidationErrors[%d] = %q, want %q", i, ve, tt.want.ValidationErrors[i])
				}
			}
		})
	}
}
