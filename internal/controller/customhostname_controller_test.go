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
	"reflect"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	saasv1alpha1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1alpha1"
)

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
