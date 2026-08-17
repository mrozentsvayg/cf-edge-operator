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

// newLBCR builds a LoadBalancer CR with fixed metadata (Name: "global",
// Namespace: "ns"). Tests only vary the spec, so keeping the metadata
// hard-coded matches how the helper is actually used.
func newLBCR(spec lbv1beta1.LoadBalancerSpec) *lbv1beta1.LoadBalancer {
	return &lbv1beta1.LoadBalancer{
		ObjectMeta: metav1.ObjectMeta{Name: "global", Namespace: "ns"},
		Spec:       spec,
	}
}

func TestLBReferencesPool(t *testing.T) {
	lb := &lbv1beta1.LoadBalancer{
		Spec: lbv1beta1.LoadBalancerSpec{
			DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{
				{Name: "us-pool"},
				{Name: "apac-pool"},
			},
			FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "us-pool"},
			RegionPools: map[string][]lbv1beta1.LoadBalancerPoolRef{
				"WNAM": {{Name: "us-pool"}},
				"APAC": {{Name: "apac-pool"}, {Name: "backup-pool"}},
			},
			CountryPools: map[string][]lbv1beta1.LoadBalancerPoolRef{
				"JP": {{Name: "jp-pool"}},
			},
			PopPools: map[string][]lbv1beta1.LoadBalancerPoolRef{
				"NRT": {{Name: "nrt-pool"}},
			},
		},
	}
	for _, name := range []string{"us-pool", "apac-pool", "backup-pool", "jp-pool", "nrt-pool"} {
		if !lbReferencesPool(lb, name) {
			t.Errorf("%q should be referenced", name)
		}
	}
	if lbReferencesPool(lb, "unrelated") {
		t.Error("unrelated pool must not be referenced")
	}
}

func TestBuildDefaultPools_PreservesOrder(t *testing.T) {
	got := buildDefaultPools([]string{"a", "b", "c"})
	if len(got) != 3 {
		t.Fatalf("len=%d want 3", len(got))
	}
	if got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("order lost: %+v", got)
	}
}

func TestMapListsEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b map[string][]string
		want bool
	}{
		{"both empty", nil, nil, true},
		{"empty vs non-empty", nil, map[string][]string{"k": {"v"}}, false},
		{"same keys and values", map[string][]string{"k": {"v"}}, map[string][]string{"k": {"v"}}, true},
		{"different keys", map[string][]string{"k1": {"v"}}, map[string][]string{"k2": {"v"}}, false},
		{"different values", map[string][]string{"k": {"v1"}}, map[string][]string{"k": {"v2"}}, false},
		{"order matters in slice", map[string][]string{"k": {"a", "b"}}, map[string][]string{"k": {"b", "a"}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mapListsEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestStringSlicesEqual(t *testing.T) {
	if !stringSlicesEqual(nil, nil) {
		t.Error("nil == nil")
	}
	if !stringSlicesEqual([]string{"a", "b"}, []string{"a", "b"}) {
		t.Error("equal slices")
	}
	if stringSlicesEqual([]string{"a"}, []string{"a", "b"}) {
		t.Error("different lengths")
	}
	if stringSlicesEqual([]string{"a", "b"}, []string{"b", "a"}) {
		t.Error("order matters")
	}
}

func TestLBDrifted_NameChange(t *testing.T) {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
	})
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}
	cf := &load_balancers.LoadBalancer{
		Name:         "different.example.com",
		DefaultPools: []string{"a"},
		FallbackPool: "a",
		Proxied:      true,
	}
	if !lbDrifted(cf, lb, resolved) {
		t.Fatal("hostname change should drift")
	}
}

func TestLBDrifted_ResolvedPoolIDChange(t *testing.T) {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
	})
	// resolved.defaultIDs is what we WOULD write to CF now; cf.DefaultPools
	// is what CF currently has. Any divergence is drift.
	resolved := &resolvedPools{defaultIDs: []string{"new-id"}, fallbackID: "new-id"}
	cf := &load_balancers.LoadBalancer{
		Name:         "lb.example.com",
		DefaultPools: []string{"old-id"},
		FallbackPool: "new-id",
		Proxied:      true,
	}
	if !lbDrifted(cf, lb, resolved) {
		t.Fatal("pool ID change should drift")
	}
}

func TestLBDrifted_ProxiedDefaultsMatch(t *testing.T) {
	// CRD default for Proxied is true (via pointer default). If the spec
	// omits it and CF reports true, that must not count as drift.
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
	})
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}
	cf := &load_balancers.LoadBalancer{
		Name:         "lb.example.com",
		DefaultPools: []string{"a"},
		FallbackPool: "a",
		Proxied:      true,
	}
	if lbDrifted(cf, lb, resolved) {
		t.Fatal("proxied defaults should not drift")
	}
}

func TestLBDrifted_UnmanagedFieldsIgnored(t *testing.T) {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
	})
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}
	cf := &load_balancers.LoadBalancer{
		Name:         "lb.example.com",
		DefaultPools: []string{"a"},
		FallbackPool: "a",
		Proxied:      true,
		// Description set on CF side but not managed by CR -- must NOT drift.
		Description: "manually edited",
	}
	if lbDrifted(cf, lb, resolved) {
		t.Fatal("unmanaged description drift should be ignored")
	}
}

func TestLBDrifted_SteeringPolicyChange(t *testing.T) {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname:       "lb.example.com",
		SteeringPolicy: "dynamic_latency",
	})
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}
	cf := &load_balancers.LoadBalancer{
		Name:           "lb.example.com",
		DefaultPools:   []string{"a"},
		FallbackPool:   "a",
		Proxied:        true,
		SteeringPolicy: "off",
	}
	if !lbDrifted(cf, lb, resolved) {
		t.Fatal("steering policy change should drift")
	}
}

func TestLBDrifted_RegionPoolsChange(t *testing.T) {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
	})
	resolved := &resolvedPools{
		defaultIDs: []string{"a"},
		fallbackID: "a",
		regionIDs:  map[string][]string{"WNAM": {"us-id"}, "APAC": {"apac-id"}},
	}
	cf := &load_balancers.LoadBalancer{
		Name:         "lb.example.com",
		DefaultPools: []string{"a"},
		FallbackPool: "a",
		Proxied:      true,
		RegionPools:  map[string][]string{"WNAM": {"us-id"}}, // missing APAC
	}
	if !lbDrifted(cf, lb, resolved) {
		t.Fatal("region pool set mismatch should drift")
	}
}
