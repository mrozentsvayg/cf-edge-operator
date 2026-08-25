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

	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

// Focused unit tests for the pure functions in loadbalancermonitor_controller.
// The reconcile-loop tests live alongside CustomHostname's envtest suite --
// this file covers the deterministic helpers (marker composition, drift
// detection, param translation) so regressions surface at `go test` speed
// without needing a running control plane.

// newMonitorCR builds a LoadBalancerMonitor CR in namespace "ns"; tests only
// vary the name and spec (mirrors newPoolCR / newLBCR which hard-code metadata).
func newMonitorCR(name string, spec lbv1beta1.LoadBalancerMonitorSpec) *lbv1beta1.LoadBalancerMonitor {
	return &lbv1beta1.LoadBalancerMonitor{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec:       spec,
	}
}

func TestMonitorMarker_UsesNamespaceAndName(t *testing.T) {
	mon := newMonitorCR("http-health", lbv1beta1.LoadBalancerMonitorSpec{})
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
			mon := newMonitorCR("name", lbv1beta1.LoadBalancerMonitorSpec{
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
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{
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
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{})
	cf := &load_balancers.Monitor{
		Description: "user edited via dashboard", // marker stripped
	}
	if !monitorDrifted(cf, mon) {
		t.Fatal("description drift (missing marker) must always drift")
	}
}

func TestMonitorDrifted_HeaderCaseInsensitive(t *testing.T) {
	// Header names are case-insensitive per the HTTP spec, so a CR "host" must
	// not drift against a Cloudflare-normalized "Host" (that would drift-loop).
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{
		Header: &map[string][]string{"host": {"api.internal"}},
	})
	cf := &load_balancers.Monitor{
		Header:      map[string][]string{"Host": {"api.internal"}},
		Description: buildMonitorDescription(mon),
	}
	if monitorDrifted(cf, mon) {
		t.Fatal("case-only key difference must not drift")
	}

	// A genuine value difference on a managed header drifts.
	cf.Header = map[string][]string{"Host": {"other.internal"}}
	if !monitorDrifted(cf, mon) {
		t.Fatal("changed header value should drift")
	}

	// A header the CR manages but Cloudflare is missing drifts.
	cf.Header = map[string][]string{}
	if !monitorDrifted(cf, mon) {
		t.Fatal("missing managed header should drift")
	}

	// A header Cloudflare has that the CR does not set drifts (full-set
	// enforcement when the CR manages headers, matching keyed-pool map drift).
	cf.Header = map[string][]string{"Host": {"api.internal"}, "Accept": {"application/json"}}
	if !monitorDrifted(cf, mon) {
		t.Fatal("extra CF header should drift when the CR manages headers")
	}
}

func TestMonitorDrifted_HeaderUnmanagedWhenUnset(t *testing.T) {
	// When the CR sets no headers, Cloudflare-side headers are left alone.
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{})
	cf := &load_balancers.Monitor{
		Header:      map[string][]string{"Host": {"api.internal"}},
		Description: buildMonitorDescription(mon),
	}
	if monitorDrifted(cf, mon) {
		t.Fatal("unset spec.Header must not drift against CF headers")
	}
}

func TestBuildMonitorNewParams_OnlyEmitsSetFields(t *testing.T) {
	// The CF SDK's param.Field distinguishes "set" from "not set". A field
	// left unset omits from the JSON body, which is what we want when the
	// CR spec field is zero -- CF applies its own defaults.
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{
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

func TestBuildMonitorParams_BoolsAlwaysSent(t *testing.T) {
	// followRedirects / allowInsecure must always be emitted (even when false)
	// so the value written matches monitorDrifted's unconditional comparison.
	// Emitting only-when-true would drift-loop forever when the CR sets false
	// but CF has true.
	monFalse := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{})
	np := buildMonitorNewParams("acct", monFalse)
	if !np.FollowRedirects.Present || np.FollowRedirects.Value {
		t.Errorf("New: FollowRedirects must be present and false; present=%v value=%v", np.FollowRedirects.Present, np.FollowRedirects.Value)
	}
	if !np.AllowInsecure.Present || np.AllowInsecure.Value {
		t.Errorf("New: AllowInsecure must be present and false; present=%v value=%v", np.AllowInsecure.Present, np.AllowInsecure.Value)
	}
	ep := buildMonitorEditParams("acct", monFalse)
	if !ep.FollowRedirects.Present || ep.FollowRedirects.Value {
		t.Errorf("Edit: FollowRedirects must be present and false; present=%v value=%v", ep.FollowRedirects.Present, ep.FollowRedirects.Value)
	}
	if !ep.AllowInsecure.Present || ep.AllowInsecure.Value {
		t.Errorf("Edit: AllowInsecure must be present and false; present=%v value=%v", ep.AllowInsecure.Present, ep.AllowInsecure.Value)
	}

	monTrue := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{FollowRedirects: true, AllowInsecure: true})
	np = buildMonitorNewParams("acct", monTrue)
	if !np.FollowRedirects.Value || !np.AllowInsecure.Value {
		t.Errorf("New: true bools must round-trip; followRedirects=%v allowInsecure=%v", np.FollowRedirects.Value, np.AllowInsecure.Value)
	}
}

func TestBuildMonitorNewParams_HeaderIsPreserved(t *testing.T) {
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{
		Header: &map[string][]string{
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

func TestMonitorDrifted_HeaderEmptyMapIsManaged(t *testing.T) {
	// An explicit empty header map (present, not nil) means "no headers": the
	// operator owns the set, so Cloudflare-side headers must be cleared. This is
	// the presence-not-emptiness contract -- distinct from an unset (nil) Header,
	// which leaves Cloudflare alone (TestMonitorDrifted_HeaderUnmanagedWhenUnset).
	mon := newMonitorCR("m", lbv1beta1.LoadBalancerMonitorSpec{Header: &map[string][]string{}})
	cf := &load_balancers.Monitor{
		Header:      map[string][]string{"Host": {"api.internal"}},
		Description: buildMonitorDescription(mon),
	}
	if !monitorDrifted(cf, mon) {
		t.Fatal("empty (present) spec.Header must drift when Cloudflare still has headers")
	}
	cf.Header = map[string][]string{}
	if monitorDrifted(cf, mon) {
		t.Fatal("empty spec.Header must not drift when Cloudflare already has no headers")
	}
}

func TestHeaderEditOverride(t *testing.T) {
	// Dropped keys become explicit nulls (JSON null -> Cloudflare removes them);
	// kept keys carry their value; keys are canonicalized so a CR "host" does not
	// collide with a null for the Cloudflare-normalized "Host".
	desired := map[string][]string{"host": {"api.internal"}}
	observed := map[string][]string{"Host": {"old.internal"}, "Accept": {"application/json"}}
	got := headerEditOverride(desired, observed)

	if v, ok := got["Host"]; !ok {
		t.Fatal("managed key Host should be present")
	} else if slice, ok := v.([]string); !ok || len(slice) != 1 || slice[0] != "api.internal" {
		t.Fatalf("Host value wrong: %#v", v)
	}
	if v, ok := got["Accept"]; !ok {
		t.Fatal("dropped key Accept should be present as a removal")
	} else if v != nil {
		t.Fatalf("dropped key Accept must be nil (JSON null), got %#v", v)
	}
	if _, ok := got["host"]; ok {
		t.Fatal("keys must be canonicalized; raw 'host' must not be sent alongside 'Host'")
	}

	// An empty desired map nulls every observed key (clear all headers).
	cleared := headerEditOverride(map[string][]string{}, observed)
	if len(cleared) != len(observed) {
		t.Fatalf("clear-all: expected %d nulled keys, got %d", len(observed), len(cleared))
	}
	for k, v := range cleared {
		if v != nil {
			t.Fatalf("clear-all: key %q should be nil, got %#v", k, v)
		}
	}
}
