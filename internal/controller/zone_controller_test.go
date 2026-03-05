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

	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

func TestHasDrift(t *testing.T) {
	sni := "sni.example.com"
	tests := []struct {
		name string
		ch   saasv1beta1.CustomHostname
		cfCH custom_hostnames.CustomHostnameListResponse
		want bool
	}{
		{
			name: "no drift",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com"},
			want: false,
		},
		{
			name: "origin server drift",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "new.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "old.example.com"},
			want: true,
		},
		{
			name: "sni set and matches",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com", OriginSNI: &sni}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: sni},
			want: false,
		},
		{
			name: "sni set and differs",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com", OriginSNI: &sni}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: "other.example.com"},
			want: true,
		},
		{
			name: "sni not spec'd, cf has sni set",
			ch:   saasv1beta1.CustomHostname{Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: sni},
			want: false,
		},
		{
			name: "ssl status matches",
			ch: saasv1beta1.CustomHostname{
				Spec:   saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com"},
				Status: saasv1beta1.CustomHostnameStatus{SSL: &saasv1beta1.CustomHostnameSSLStatus{Status: "active"}},
			},
			cfCH: custom_hostnames.CustomHostnameListResponse{
				CustomOriginServer: "origin.example.com",
				SSL:                custom_hostnames.CustomHostnameListResponseSSL{Status: "active"},
			},
			want: false,
		},
		{
			name: "ssl status transition: pending → active",
			ch: saasv1beta1.CustomHostname{
				Spec:   saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com"},
				Status: saasv1beta1.CustomHostnameStatus{SSL: &saasv1beta1.CustomHostnameSSLStatus{Status: "pending_validation"}},
			},
			cfCH: custom_hostnames.CustomHostnameListResponse{
				CustomOriginServer: "origin.example.com",
				SSL:                custom_hostnames.CustomHostnameListResponseSSL{Status: "active"},
			},
			want: true,
		},
		{
			name: "ssl status: cr has nil ssl, cf has status",
			ch: saasv1beta1.CustomHostname{
				Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com"},
			},
			cfCH: custom_hostnames.CustomHostnameListResponse{
				CustomOriginServer: "origin.example.com",
				SSL:                custom_hostnames.CustomHostnameListResponseSSL{Status: "pending_validation"},
			},
			want: true,
		},
		{
			name: "ssl status: both empty (no ssl)",
			ch: saasv1beta1.CustomHostname{
				Spec: saasv1beta1.CustomHostnameSpec{OriginServer: "origin.example.com"},
			},
			cfCH: custom_hostnames.CustomHostnameListResponse{
				CustomOriginServer: "origin.example.com",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasDrift(&tt.ch, tt.cfCH); got != tt.want {
				t.Errorf("hasDrift() = %v, want %v", got, tt.want)
			}
		})
	}
}
