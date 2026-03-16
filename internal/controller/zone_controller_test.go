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
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

func TestIsHostnameConflict(t *testing.T) {
	tests := []struct {
		name string
		ch   saasv1beta1.CustomHostname
		want bool
	}{
		{
			name: "no conditions",
			ch:   saasv1beta1.CustomHostname{},
			want: false,
		},
		{
			name: "Ready=True",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready"},
				},
			}},
			want: false,
		},
		{
			name: "Ready=False, HostnameConflict",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionFalse, Reason: "HostnameConflict"},
				},
			}},
			want: true,
		},
		{
			name: "Ready=False, other reason",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionFalse, Reason: "SSLPending"},
				},
			}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isHostnameConflict(&tt.ch); got != tt.want {
				t.Errorf("isHostnameConflict() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCRState(t *testing.T) {
	tests := []struct {
		name string
		ch   saasv1beta1.CustomHostname
		want string
	}{
		{
			name: "no conditions, no errors → pending",
			ch:   saasv1beta1.CustomHostname{},
			want: "pending",
		},
		{
			name: "Ready=True, no errors → ready",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready"},
				},
			}},
			want: "ready",
		},
		{
			name: "Ready=True with errors → ready (ready beats unhealthy)",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionTrue, Reason: "Ready"},
				},
				ConsecutiveErrors: 3,
			}},
			want: "ready",
		},
		{
			name: "consecutiveErrors > 0, Ready=False → unhealthy",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionFalse, Reason: "CreateFailed"},
				},
				ConsecutiveErrors: 2,
			}},
			want: "unhealthy",
		},
		{
			name: "Ready=False, no errors → pending",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionFalse, Reason: "SSLPending"},
				},
			}},
			want: "pending",
		},
		{
			name: "HostnameConflict → conflict (beats everything)",
			ch: saasv1beta1.CustomHostname{Status: saasv1beta1.CustomHostnameStatus{
				Conditions: []metav1.Condition{
					{Type: "Ready", Status: metav1.ConditionFalse, Reason: "HostnameConflict"},
				},
				ConsecutiveErrors: 5,
			}},
			want: "conflict",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := crState(&tt.ch); got != tt.want {
				t.Errorf("crState() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSslStatusChangedFields(t *testing.T) {
	tests := []struct {
		name        string
		status      *saasv1beta1.CustomHostnameSSLStatus
		cfSSL       custom_hostnames.CustomHostnameListResponseSSL
		wantChanged bool
	}{
		{
			name:        "nil status, CF has status → drift",
			status:      nil,
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive},
			wantChanged: true,
		},
		{
			name:        "nil status, CF empty → no drift",
			status:      nil,
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{},
			wantChanged: false,
		},
		{
			name:        "status matches CF → no drift",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive, CertificateAuthority: sslCAGoogle, Method: sslMethodHTTP, Type: sslTypeDV},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, CertificateAuthority: sslCAGoogle, Method: sslMethodHTTP, Type: sslTypeDV},
			wantChanged: false,
		},
		{
			name:   "minTLS differs → drift (status refresh)",
			status: &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive},
			cfSSL: custom_hostnames.CustomHostnameListResponseSSL{
				Status:   sslStatusActive,
				Settings: custom_hostnames.CustomHostnameListResponseSSLSettings{MinTLSVersion: custom_hostnames.CustomHostnameListResponseSSLSettingsMinTLSVersion(sslMinTLS12)},
			},
			wantChanged: true,
		},
		{
			name:        "cert ID differs → drift (reissue detected)",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive, ID: "old-cert-id"},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, ID: "new-cert-id"},
			wantChanged: true,
		},
		{
			name:        "issuer differs → drift",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive, Issuer: "Let's Encrypt"},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, Issuer: "Google Trust Services"},
			wantChanged: true,
		},
		{
			name:        "serialNumber differs → drift (reissue)",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive, SerialNumber: "old-serial"},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, SerialNumber: "new-serial"},
			wantChanged: true,
		},
		{
			name:        "bundleMethod differs → drift",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive, BundleMethod: "ubiquitous"},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, BundleMethod: "optimal"},
			wantChanged: true,
		},
		{
			name:        "wildcard differs → drift",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive, Wildcard: false},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, Wildcard: true},
			wantChanged: true,
		},
		{
			name:        "expiresOn appears → drift (cert issued)",
			status:      &saasv1beta1.CustomHostnameSSLStatus{Status: sslStatusActive},
			cfSSL:       custom_hostnames.CustomHostnameListResponseSSL{Status: sslStatusActive, ExpiresOn: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)},
			wantChanged: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sslStatusChangedFields(tt.status, tt.cfSSL)
			if (got != nil) != tt.wantChanged {
				t.Errorf("sslStatusChangedFields() changed=%v, wantChanged=%v, fields=%v", got != nil, tt.wantChanged, got)
			}
		})
	}
}

func TestRefersToZone(t *testing.T) {
	r := &ZoneReconciler{}
	zone := &domainsv1beta1.Zone{
		ObjectMeta: metav1.ObjectMeta{Name: "my-zone", Namespace: "operator-ns"},
	}
	tests := []struct {
		name string
		ch   saasv1beta1.CustomHostname
		want bool
	}{
		{
			name: "matching name, empty namespace (defaults to zone ns)",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{ZoneRef: saasv1beta1.ZoneRef{Name: "my-zone"}}},
			want: true,
		},
		{
			name: "matching name and namespace",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{ZoneRef: saasv1beta1.ZoneRef{Name: "my-zone", Namespace: "operator-ns"}}},
			want: true,
		},
		{
			name: "matching name, wrong namespace",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{ZoneRef: saasv1beta1.ZoneRef{Name: "my-zone", Namespace: "other-ns"}}},
			want: false,
		},
		{
			name: "wrong name",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{ZoneRef: saasv1beta1.ZoneRef{Name: "other-zone"}}},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := r.refersToZone(&tt.ch, zone); got != tt.want {
				t.Errorf("refersToZone() = %v, want %v", got, tt.want)
			}
		})
	}
}
