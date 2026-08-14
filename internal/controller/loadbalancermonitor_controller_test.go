/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/cloudflare/cloudflare-go/v6/load_balancers"

	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

// Focused unit tests for the pure functions in loadbalancermonitor_controller.
// The reconcile-loop tests live alongside CustomHostname's envtest suite --
// this file covers the deterministic helpers (marker composition, drift
// detection, param translation) so regressions surface at `go test` speed
// without needing a running control plane.

func newMonitorCR(name, ns string, spec saasv1beta1.LoadBalancerMonitorSpec) *saasv1beta1.LoadBalancerMonitor {
	return &saasv1beta1.LoadBalancerMonitor{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       spec,
	}
}

func TestMonitorMarker_UsesNamespaceAndName(t *testing.T) {
	mon := newMonitorCR("http-health", "ns", saasv1beta1.LoadBalancerMonitorSpec{})
	got := monitorMarker(mon)
	want := "[cf-edge-operator:ns/http-health]"
	if got != want {
		t.Fatalf("marker mismatch: got %q want %q", got, want)
	}
}

func TestBuildMonitorDescription_PreservesUserPrefix(t *testing.T) {
	tests := []struct {
		name    string
		userDsc string
		want    string
	}{
		{"empty prefix -> marker only",
			"", "[cf-edge-operator:ns/name]"},
		{"user prefix -> prefix + marker",
			"Health check",
			"Health check [cf-edge-operator:ns/name]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mon := newMonitorCR("name", "ns", saasv1beta1.LoadBalancerMonitorSpec{
				Description: tc.userDsc,
			})
			if got := buildMonitorDescription(mon); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestDescriptionHasMarker(t *testing.T) {
	marker := "[cf-edge-operator:ns/name]"
	tests := []struct {
		name string
		desc string
		want bool
	}{
		{"exact match", marker, true},
		{"prefix + marker", "User text " + marker, true},
		{"marker + suffix", marker + " extra", true},
		{"different marker", "[cf-edge-operator:other/name]", false},
		{"unrelated text", "unrelated description", false},
		{"empty", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := descriptionHasMarker(tc.desc, marker); got != tc.want {
				t.Fatalf("got %v want %v (desc=%q)", got, tc.want, tc.desc)
			}
		})
	}
}

func TestDescriptionHasMarker_EmptyMarkerIsAlwaysFalse(t *testing.T) {
	if descriptionHasMarker("anything", "") {
		t.Fatal("empty marker should never match")
	}
}

func TestMonitorDrifted_SpecFieldsCompareOnlyWhenSet(t *testing.T) {
	// If the CR doesn't set a field (empty / zero), CF's current value for
	// that field should not trigger drift -- the operator treats unset
	// fields as "don't manage".
	cf := &load_balancers.Monitor{
		Type:          "https",
		Method:        "GET",
		Path:          "/health",
		Port:          443,
		ExpectedCodes: "200",
		Interval:      60,
	}
	mon := newMonitorCR("m", "ns", saasv1beta1.LoadBalancerMonitorSpec{
		// Only method is managed; all other CR-side spec fields are zero.
		Method: "GET",
	})
	// Marker matches CF -- construct the CF Description so that only method
	// is compared. The description-drift check would otherwise trigger.
	cf.Description = buildMonitorDescription(mon)
	if monitorDrifted(cf, mon) {
		t.Fatalf("unset spec fields should not drift; cf=%+v", cf)
	}

	mon.Spec.Method = "POST"
	if !monitorDrifted(cf, mon) {
		t.Fatal("changing managed field should drift")
	}
}

func TestMonitorDrifted_MarkerLossIsAlwaysDrift(t *testing.T) {
	mon := newMonitorCR("m", "ns", saasv1beta1.LoadBalancerMonitorSpec{})
	cf := &load_balancers.Monitor{
		Description: "user edited via dashboard", // marker stripped
	}
	if !monitorDrifted(cf, mon) {
		t.Fatal("description drift (missing marker) must always drift")
	}
}

func TestBuildMonitorNewParams_OnlyEmitsSetFields(t *testing.T) {
	// The CF SDK's param.Field distinguishes "set" from "not set". A field
	// left unset omits from the JSON body, which is what we want when the
	// CR spec field is zero -- CF applies its own defaults.
	mon := newMonitorCR("m", "ns", saasv1beta1.LoadBalancerMonitorSpec{
		Method: "POST",
		Path:   "/",
	})
	p := buildMonitorNewParams("account-xyz", mon)

	// Sanity: managed fields are Present; unmanaged fields are absent.
	if !p.Method.Present {
		t.Error("Method should be set when spec sets it")
	}
	if !p.Path.Present {
		t.Error("Path should be set when spec sets it")
	}
	if p.Interval.Present {
		t.Error("Interval should NOT be set when spec doesn't set it (spec=0)")
	}
	if p.Timeout.Present {
		t.Error("Timeout should NOT be set when spec doesn't set it (spec=0)")
	}
}

func TestBuildMonitorNewParams_HeaderIsPreserved(t *testing.T) {
	mon := newMonitorCR("m", "ns", saasv1beta1.LoadBalancerMonitorSpec{
		Header: map[string][]string{
			"Content-Type": {"application/json"},
			"Accept":       {"application/json"},
		},
	})
	p := buildMonitorNewParams("account-xyz", mon)
	if !p.Header.Present {
		t.Fatal("Header should be present when spec.Header is non-empty")
	}
	got := p.Header.Value
	if len(got) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(got))
	}
	if got["Content-Type"][0] != "application/json" {
		t.Errorf("Content-Type value lost in translation: %v", got["Content-Type"])
	}
}
