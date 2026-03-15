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
	"slices"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

func TestDetectConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = saasv1beta1.AddToScheme(scheme)

	indexer := func(o client.Object) []string {
		return []string{o.(*saasv1beta1.CustomHostname).Spec.Hostname}
	}
	makeReconciler := func(objs ...client.Object) *CustomHostnameReconciler {
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&saasv1beta1.CustomHostname{}, hostnameField, indexer).
			WithStatusSubresource(&saasv1beta1.CustomHostname{}).
			WithObjects(objs...).
			Build()
		return &CustomHostnameReconciler{Client: c, Scheme: scheme}
	}

	owner := &saasv1beta1.CustomHostname{
		ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: "default", UID: types.UID("uid-owner")},
		Spec:       saasv1beta1.CustomHostnameSpec{Hostname: "api.acme.com"},
		Status:     saasv1beta1.CustomHostnameStatus{ID: "cf-id-abc"},
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
		duplicate := &saasv1beta1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "duplicate", Namespace: "default", UID: types.UID("uid-dup")},
			Spec:       saasv1beta1.CustomHostnameSpec{Hostname: "api.acme.com"},
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
		var got saasv1beta1.CustomHostname
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
		a := &saasv1beta1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "cr-a", Namespace: "default", UID: types.UID("uid-a")},
			Spec:       saasv1beta1.CustomHostnameSpec{Hostname: "api.acme.com"},
		}
		b := &saasv1beta1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "cr-b", Namespace: "default", UID: types.UID("uid-b")},
			Spec:       saasv1beta1.CustomHostnameSpec{Hostname: "api.acme.com"},
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
		other := &saasv1beta1.CustomHostname{
			ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "default", UID: types.UID("uid-other")},
			Spec:       saasv1beta1.CustomHostnameSpec{Hostname: "other.acme.com"},
			Status:     saasv1beta1.CustomHostnameStatus{ID: "cf-id-xyz"},
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
		ch   saasv1beta1.CustomHostname
		want bool
	}{
		{
			name: "new CR, no ID → let through",
			ch:   saasv1beta1.CustomHostname{},
			want: true,
		},
		{
			name: "existing CR, has ID → block (Zone controller handles drift)",
			ch:   saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{ID: "cf-id-abc"}},
			want: false,
		},
		{
			name: "terminating CR with ID → let through (must remove finalizer)",
			ch: saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status:     saasv1beta1.CustomHostnameStatus{ID: "cf-id-abc"},
			},
			want: true,
		},
		{
			name: "terminating CR without ID → let through",
			ch:   saasv1beta1.CustomHostname{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}},
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
	_ = saasv1beta1.AddToScheme(scheme)

	now := metav1.Now()
	ch := &saasv1beta1.CustomHostname{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-ch",
			Namespace:         "default",
			Finalizers:        []string{finalizerName},
			DeletionTimestamp: &now,
		},
		Spec:   saasv1beta1.CustomHostnameSpec{Hostname: "test.example.com"},
		Status: saasv1beta1.CustomHostnameStatus{ID: "cf-id-123"},
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
	if result.RequeueAfter != 0 {
		t.Errorf("expected no requeue in dry-run, got %+v", result)
	}

	// The finalizer must still be present — dry-run must not mutate K8s.
	// If this fails it means dry-run removed the finalizer, which would
	// silently orphan the CF hostname when the CR disappears from K8s.
	var got saasv1beta1.CustomHostname
	if err := c.Get(ctx, client.ObjectKeyFromObject(ch), &got); err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !slices.Contains(got.Finalizers, finalizerName) {
		t.Error("dry-run: finalizer was removed; CR would disappear from K8s while CF hostname stays (orphan bug)")
	}
}

func TestEffectiveManagementPolicy(t *testing.T) {
	tests := []struct {
		name            string
		crPolicy        string
		operatorDefault string
		want            string
	}{
		{"cr not set, uses operator default", "", "manage", "manage"},
		{"cr not set, create default", "", "create", "create"},
		{"cr overrides to create", "create", "manage", "create"},
		{"cr overrides to manage", "manage", "create", "manage"},
		{"cr not set, observe default", "", "observe", "observe"},
		{"cr overrides to observe", "observe", "manage", "observe"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveManagementPolicy(tt.crPolicy, tt.operatorDefault); got != tt.want {
				t.Errorf("effectiveManagementPolicy() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEffectiveDeletePolicy(t *testing.T) {
	tests := []struct {
		name            string
		crPolicy        string
		operatorDefault string
		want            string
	}{
		{"cr not set, uses operator default", "", "always", "always"},
		{"cr not set, own-only default", "", "own-only", "own-only"},
		{"cr overrides to own-only", "own-only", "always", "own-only"},
		{"cr overrides to always", "always", "own-only", "always"},
		{"cr not set, never default", "", "never", "never"},
		{"cr overrides to never", "never", "always", "never"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := effectiveDeletePolicy(tt.crPolicy, tt.operatorDefault); got != tt.want {
				t.Errorf("effectiveDeletePolicy() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestShouldDeleteInCF(t *testing.T) {
	matched := &custom_hostnames.CustomHostnameListResponse{ID: "abc123"}
	different := &custom_hostnames.CustomHostnameListResponse{ID: "xyz999"}

	tests := []struct {
		name     string
		statusID string
		current  *custom_hostnames.CustomHostnameListResponse
		want     bool
	}{
		{
			name:     "not found in CF, do not delete",
			statusID: "abc123",
			current:  nil,
			want:     false,
		},
		{
			name:     "IDs match, delete",
			statusID: "abc123",
			current:  matched,
			want:     true,
		},
		{
			name:     "IDs differ, do not delete",
			statusID: "abc123",
			current:  different,
			want:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDeleteInCF(tt.statusID, tt.current); got != tt.want {
				t.Errorf("shouldDeleteInCF() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSniDrifted(t *testing.T) {
	sni := "sni.example.com"
	hostHeader := ":request_host_header:"
	originServer := "origin.example.com"
	tests := []struct {
		name       string
		currentSNI string
		ch         saasv1beta1.CustomHostname
		want       bool
	}{
		{
			name:       "nil spec, CF empty → no drift (don't manage)",
			currentSNI: "",
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer}},
			want:       false,
		},
		{
			name:       "nil spec, CF has origin server → no drift (don't manage)",
			currentSNI: originServer,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer}},
			want:       false,
		},
		{
			name:       "nil spec, CF has custom SNI → no drift (don't manage)",
			currentSNI: sni,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer}},
			want:       false,
		},
		{
			name:       "nil spec, CF has :request_host_header: → no drift (don't manage)",
			currentSNI: hostHeader,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer}},
			want:       false,
		},
		{
			name:       "spec matches CF → no drift",
			currentSNI: sni,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer, OriginSNI: &sni}},
			want:       false,
		},
		{
			name:       "spec differs from CF → drift",
			currentSNI: "other.example.com",
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer, OriginSNI: &sni}},
			want:       true,
		},
		{
			name:       "spec = origin server, CF empty (no entitlement) → drift",
			currentSNI: "",
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer, OriginSNI: &originServer}},
			want:       true,
		},
		{
			name:       "spec = origin server, CF = origin server → no drift",
			currentSNI: originServer,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer, OriginSNI: &originServer}},
			want:       false,
		},
		{
			name:       "spec = :request_host_header:, CF = :request_host_header: → no drift",
			currentSNI: hostHeader,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer, OriginSNI: &hostHeader}},
			want:       false,
		},
		{
			name:       "spec = :request_host_header:, CF has origin server → drift",
			currentSNI: originServer,
			ch:         saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: originServer, OriginSNI: &hostHeader}},
			want:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sniDrifted(tt.currentSNI, &tt.ch); got != tt.want {
				t.Errorf("sniDrifted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSslDrifted(t *testing.T) {
	tests := []struct {
		name  string
		cfSSL custom_hostnames.CustomHostnameListResponseSSL
		spec  *saasv1beta1.CustomHostnameSSL
		want  bool
	}{
		{
			name: "nil spec → no drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{
				CertificateAuthority: "lets_encrypt",
			},
			spec: nil,
			want: false,
		},
		{
			name:  "empty spec → no drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{CertificateAuthority: "lets_encrypt"},
			spec:  &saasv1beta1.CustomHostnameSSL{},
			want:  false,
		},
		{
			name:  "CA matches → no drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{CertificateAuthority: "google"},
			spec:  &saasv1beta1.CustomHostnameSSL{CertificateAuthority: "google"},
			want:  false,
		},
		{
			name:  "CA differs → drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{CertificateAuthority: "lets_encrypt"},
			spec:  &saasv1beta1.CustomHostnameSSL{CertificateAuthority: "google"},
			want:  true,
		},
		{
			name:  "method differs → drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{Method: sslMethodHTTP},
			spec:  &saasv1beta1.CustomHostnameSSL{Method: "txt"},
			want:  true,
		},
		{
			name:  "type matches → no drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{Type: sslTypeDV},
			spec:  &saasv1beta1.CustomHostnameSSL{Type: sslTypeDV},
			want:  false,
		},
		{
			name: "minTLSVersion differs → drift",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{
				Settings: custom_hostnames.CustomHostnameListResponseSSLSettings{MinTLSVersion: "1.0"},
			},
			spec: &saasv1beta1.CustomHostnameSSL{MinTLSVersion: "1.2"},
			want: true,
		},
		{
			name:  "CA not in spec, CF has CA → no drift (unmanaged field)",
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{CertificateAuthority: "google", Method: sslMethodHTTP},
			spec:  &saasv1beta1.CustomHostnameSSL{Method: sslMethodHTTP},
			want:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sslDrifted(tt.cfSSL, tt.spec); got != tt.want {
				t.Errorf("sslDrifted() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSSLStatusFromNew(t *testing.T) {
	expires := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		resp custom_hostnames.CustomHostnameNewResponse
		want saasv1beta1.CustomHostnameSSLStatus
	}{
		{
			name: "status only",
			resp: custom_hostnames.CustomHostnameNewResponse{
				SSL: custom_hostnames.CustomHostnameNewResponseSSL{Status: "pending_validation"},
			},
			want: saasv1beta1.CustomHostnameSSLStatus{Status: "pending_validation"},
		},
		{
			name: "expires on set",
			resp: custom_hostnames.CustomHostnameNewResponse{
				SSL: custom_hostnames.CustomHostnameNewResponseSSL{
					Status:    sslStatusActive,
					ExpiresOn: expires,
				},
			},
			want: saasv1beta1.CustomHostnameSSLStatus{
				Status:    sslStatusActive,
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
			want: saasv1beta1.CustomHostnameSSLStatus{
				Status: "pending_validation",
				ValidationRecords: []saasv1beta1.SSLValidationRecord{
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
			want: saasv1beta1.CustomHostnameSSLStatus{
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
		want saasv1beta1.CustomHostnameSSLStatus
	}{
		{
			name: "status only",
			resp: custom_hostnames.CustomHostnameListResponse{
				SSL: custom_hostnames.CustomHostnameListResponseSSL{Status: "pending_validation"},
			},
			want: saasv1beta1.CustomHostnameSSLStatus{Status: "pending_validation"},
		},
		{
			name: "expires on set",
			resp: custom_hostnames.CustomHostnameListResponse{
				SSL: custom_hostnames.CustomHostnameListResponseSSL{
					Status:    sslStatusActive,
					ExpiresOn: expires,
				},
			},
			want: saasv1beta1.CustomHostnameSSLStatus{
				Status:    sslStatusActive,
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
			want: saasv1beta1.CustomHostnameSSLStatus{
				Status: "pending_validation",
				ValidationRecords: []saasv1beta1.SSLValidationRecord{
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
			want: saasv1beta1.CustomHostnameSSLStatus{
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

func TestBuildSSLParams(t *testing.T) {
	tests := []struct {
		name       string
		ssl        saasv1beta1.CustomHostnameSSL
		defaults   SSLDefaults
		wantType   bool
		wantCA     bool
		wantMinTLS bool
		wantMethod bool
	}{
		{
			name:       "CR fields set, no defaults",
			ssl:        saasv1beta1.CustomHostnameSSL{Type: sslTypeDV, Method: sslMethodHTTP},
			wantType:   true,
			wantMethod: true,
		},
		{
			name: "all CR fields set",
			ssl: saasv1beta1.CustomHostnameSSL{
				Type:                 sslTypeDV,
				Method:               "txt",
				CertificateAuthority: "google",
				MinTLSVersion:        "1.2",
			},
			wantType:   true,
			wantMethod: true,
			wantCA:     true,
			wantMinTLS: true,
		},
		{
			name: "empty CR fields, no defaults",
			ssl:  saasv1beta1.CustomHostnameSSL{},
		},
		{
			name:       "empty CR fields, defaults applied",
			ssl:        saasv1beta1.CustomHostnameSSL{},
			defaults:   SSLDefaults{Method: "txt", CertificateAuthority: "google", MinTLSVersion: "1.2"},
			wantMethod: true,
			wantCA:     true,
			wantMinTLS: true,
		},
		{
			name:       "CR fields override defaults",
			ssl:        saasv1beta1.CustomHostnameSSL{Method: sslMethodHTTP, CertificateAuthority: "lets_encrypt", MinTLSVersion: "1.3"},
			defaults:   SSLDefaults{Method: "txt", CertificateAuthority: "google", MinTLSVersion: "1.2"},
			wantMethod: true,
			wantCA:     true,
			wantMinTLS: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSSLParams(&tt.ssl, tt.defaults)
			hasType := got.Type.Present
			hasCA := got.CertificateAuthority.Present
			hasMinTLS := got.Settings.Present
			hasMethod := got.Method.Present
			if hasType != tt.wantType {
				t.Errorf("Type.Present = %v, want %v", hasType, tt.wantType)
			}
			if hasCA != tt.wantCA {
				t.Errorf("CertificateAuthority.Present = %v, want %v", hasCA, tt.wantCA)
			}
			if hasMinTLS != tt.wantMinTLS {
				t.Errorf("Settings.Present = %v, want %v", hasMinTLS, tt.wantMinTLS)
			}
			if hasMethod != tt.wantMethod {
				t.Errorf("Method.Present = %v, want %v", hasMethod, tt.wantMethod)
			}
		})
	}
}

func TestSslStatusFromList(t *testing.T) {
	resp := &custom_hostnames.CustomHostnameListResponse{}
	resp.SSL.Status = sslStatusActive
	resp.SSL.Method = sslMethodHTTP
	resp.SSL.Type = sslTypeDV
	resp.SSL.CertificateAuthority = "google"
	resp.SSL.Settings.MinTLSVersion = "1.2"

	s := sslStatusFromList(resp)
	if s.Status != sslStatusActive {
		t.Errorf("Status = %q, want %q", s.Status, sslStatusActive)
	}
	if s.Method != sslMethodHTTP {
		t.Errorf("Method = %q, want %q", s.Method, sslMethodHTTP)
	}
	if s.Type != sslTypeDV {
		t.Errorf("Type = %q, want %q", s.Type, sslTypeDV)
	}
	if s.CertificateAuthority != "google" {
		t.Errorf("CertificateAuthority = %q, want %q", s.CertificateAuthority, "google")
	}
	if s.MinTLSVersion != "1.2" {
		t.Errorf("MinTLSVersion = %q, want %q", s.MinTLSVersion, "1.2")
	}
}

func TestFastWritePredicateUpdate(t *testing.T) {
	pred := fastWritePredicate()

	tests := []struct {
		name   string
		oldGen int64
		newGen int64
		want   bool
	}{
		{
			name:   "generation changed → let through",
			oldGen: 1,
			newGen: 2,
			want:   true,
		},
		{
			name:   "generation unchanged (status-only update) → block",
			oldGen: 1,
			newGen: 1,
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := &saasv1beta1.CustomHostname{}
			old.SetGeneration(tt.oldGen)
			new := &saasv1beta1.CustomHostname{}
			new.SetGeneration(tt.newGen)
			got := pred.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: new})
			if got != tt.want {
				t.Errorf("fastWritePredicate.Update() = %v, want %v", got, tt.want)
			}
		})
	}
}
