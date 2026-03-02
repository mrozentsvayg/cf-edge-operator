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

	cfv1alpha1 "github.com/mrozentsvayg/cf-edge-operator/api/v1alpha1"
)

func TestHasDrift(t *testing.T) {
	sni := "sni.example.com"
	tests := []struct {
		name string
		ch   cfv1alpha1.CustomHostname
		cfCH custom_hostnames.CustomHostnameListResponse
		want bool
	}{
		{
			name: "no drift",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com"},
			want: false,
		},
		{
			name: "origin server drift",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "new.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "old.example.com"},
			want: true,
		},
		{
			name: "sni set and matches",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com", OriginSNI: &sni}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: sni},
			want: false,
		},
		{
			name: "sni set and differs",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com", OriginSNI: &sni}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: "other.example.com"},
			want: true,
		},
		{
			name: "sni not spec'd, cf has sni set",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: sni},
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

