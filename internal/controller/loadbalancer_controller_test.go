/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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

// defPools builds an (unweighted) defaultPoolRefs list from plain pool names.
// Weighted default entries are constructed inline where a test needs a weight.
func defPools(names ...string) []lbv1beta1.LoadBalancerDefaultPoolRef {
	out := make([]lbv1beta1.LoadBalancerDefaultPoolRef, 0, len(names))
	for _, n := range names {
		out = append(out, lbv1beta1.LoadBalancerDefaultPoolRef{
			LoadBalancerPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: n},
		})
	}
	return out
}

func TestLBReferencesPool(t *testing.T) {
	lb := &lbv1beta1.LoadBalancer{
		Spec: lbv1beta1.LoadBalancerSpec{
			DefaultPoolRefs: defPools("us-pool", "apac-pool"),
			FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "us-pool"},
			RegionPools: &map[string][]lbv1beta1.LoadBalancerPoolRef{
				"WNAM": {{Name: "us-pool"}},
				"APAC": {{Name: "apac-pool"}, {Name: "backup-pool"}},
			},
			CountryPools: &map[string][]lbv1beta1.LoadBalancerPoolRef{
				"JP": {{Name: "jp-pool"}},
			},
			PopPools: &map[string][]lbv1beta1.LoadBalancerPoolRef{
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
		// enabled is always-managed and defaults to true; CF reporting true must
		// not count as drift when the CR omits it.
		Enabled: true,
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
		Enabled:      true,
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

func TestLBDrifted_EnabledManaged(t *testing.T) {
	base := &load_balancers.LoadBalancer{
		Name:         "lb.example.com",
		DefaultPools: []string{"a"},
		FallbackPool: "a",
		Proxied:      true,
	}
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}

	// Unset spec.enabled defaults to true; CF disabled (false) must drift so the
	// operator re-enables it (always-managed).
	lbDefault := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com"})
	cfDisabled := *base
	cfDisabled.Enabled = false
	if !lbDrifted(&cfDisabled, lbDefault, resolved) {
		t.Fatal("out-of-band disable must drift (enabled defaults to true, always-managed)")
	}
	cfEnabled := *base
	cfEnabled.Enabled = true
	if lbDrifted(&cfEnabled, lbDefault, resolved) {
		t.Fatal("enabled=true matching the default must not drift")
	}

	// Explicit spec.enabled=false: CF enabled (true) must drift back to false.
	lbFalse := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com", Enabled: new(false)})
	if !lbDrifted(&cfEnabled, lbFalse, resolved) {
		t.Fatal("spec.enabled=false vs CF enabled=true must drift")
	}
	if lbDrifted(&cfDisabled, lbFalse, resolved) {
		t.Fatal("spec.enabled=false matching CF disabled must not drift")
	}
}

func TestLBDrifted_NetworksNotDriftChecked(t *testing.T) {
	// networks are create-only-write, so a divergence must NOT trigger a corrective
	// edit (surface-only). lbDrifted must ignore networks entirely.
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
		Networks: []string{"net-a", "net-b"},
	})
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}
	cf := &load_balancers.LoadBalancer{
		Name:         "lb.example.com",
		DefaultPools: []string{"a"},
		FallbackPool: "a",
		Proxied:      true,
		Enabled:      true,
		Networks:     []string{"net-a"}, // differs from spec, but not drift-checked
	}
	if lbDrifted(cf, lb, resolved) {
		t.Fatal("networks divergence must not count as drift (create-only-write)")
	}
}

func TestBuildLBNewParams_NetworksAndNoEnabled(t *testing.T) {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{
		Hostname: "lb.example.com",
		Networks: []string{"net-a", "net-b"},
	})
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}
	p := buildLBNewParams("zone", lb, resolved)
	if !p.Networks.Present {
		t.Fatal("networks should be sent on create when spec.networks is set")
	}
	if len(p.Networks.Value) != 2 || p.Networks.Value[0] != "net-a" || p.Networks.Value[1] != "net-b" {
		t.Fatalf("networks not wired through: %+v", p.Networks.Value)
	}

	// networks omitted when spec is empty.
	lbNoNet := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com"})
	if buildLBNewParams("zone", lbNoNet, resolved).Networks.Present {
		t.Fatal("networks should be absent on create when spec.networks is empty")
	}
}

func TestBuildLBEditParams_EnabledAlwaysSentNoNetworks(t *testing.T) {
	resolved := &resolvedPools{defaultIDs: []string{"a"}, fallbackID: "a"}

	// Unset spec.enabled -> edit sends enabled=true (default, always-managed).
	lbDefault := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com"})
	p := buildLBEditParams("zone", lbDefault, resolved)
	if !p.Enabled.Present || p.Enabled.Value != true {
		t.Fatalf("edit must send enabled=true by default; got present=%v value=%v", p.Enabled.Present, p.Enabled.Value)
	}

	// Explicit false -> edit sends enabled=false.
	lbFalse := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com", Enabled: new(false)})
	if v := buildLBEditParams("zone", lbFalse, resolved).Enabled; !v.Present || v.Value != false {
		t.Fatalf("edit must send enabled=false when set; got present=%v value=%v", v.Present, v.Value)
	}
}

func TestUnorderedStringSlicesEqual(t *testing.T) {
	cases := []struct {
		name string
		a, b []string
		want bool
	}{
		{"both nil", nil, nil, true},
		{"same order", []string{"a", "b"}, []string{"a", "b"}, true},
		{"different order", []string{"a", "b"}, []string{"b", "a"}, true},
		{"length mismatch", []string{"a"}, []string{"a", "b"}, false},
		{"different elements", []string{"a", "b"}, []string{"a", "c"}, false},
		{"duplicate multiset differs", []string{"a", "a", "b"}, []string{"a", "b", "b"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := unorderedStringSlicesEqual(tc.a, tc.b); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}

func TestNetworksDrifted(t *testing.T) {
	if networksDrifted(nil) {
		t.Error("absent NetworksSynced condition must not report drift")
	}
	synced := []metav1.Condition{{Type: conditionNetworksSynced, Status: metav1.ConditionTrue, Reason: reasonNetworksInSync}}
	if networksDrifted(synced) {
		t.Error("NetworksSynced=True must not report drift")
	}
	drifted := []metav1.Condition{{Type: conditionNetworksSynced, Status: metav1.ConditionFalse, Reason: reasonNetworksDrifted}}
	if !networksDrifted(drifted) {
		t.Error("NetworksSynced=False must report drift")
	}
}

// lbFakeScheme builds a scheme with the load-balancing types registered.
func lbFakeScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := lbv1beta1.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return s
}

func TestResolveDefaultPools_WeightsFoldedAndFeedPartial(t *testing.T) {
	scheme := lbFakeScheme(t)
	ctx := context.Background()

	// A weighted default pool resolves and its weight is folded into poolWeights
	// (keyed by CF pool ID); a second weighted default pool is missing, so it feeds
	// unresolved (partial) and is absent from poolWeights.
	readyPool := &lbv1beta1.LoadBalancerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-ready", Namespace: "ns"},
		Status:     lbv1beta1.LoadBalancerPoolStatus{ID: "cf-pool-ready"},
	}
	weightedReady := &lbv1beta1.LoadBalancerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "pool-weighted", Namespace: "ns"},
		Status:     lbv1beta1.LoadBalancerPoolStatus{ID: "cf-pool-weighted"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(readyPool, weightedReady).Build()
	r := &LoadBalancerReconciler{Client: c, Scheme: scheme}

	lb := &lbv1beta1.LoadBalancer{
		ObjectMeta: metav1.ObjectMeta{Name: "lb", Namespace: "ns"},
		Spec: lbv1beta1.LoadBalancerSpec{
			Hostname:       "lb.example.com",
			SteeringPolicy: "random",
			DefaultPoolRefs: []lbv1beta1.LoadBalancerDefaultPoolRef{
				{LoadBalancerPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-ready"}},
				{LoadBalancerPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-weighted"}, Weight: "0.5"},
				{LoadBalancerPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-missing"}, Weight: "0.5"},
			},
			FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-ready"},
		},
	}
	resolved, err := r.resolveAllPools(ctx, lb)
	if err != nil {
		t.Fatal(err)
	}
	// The missing default pool feeds unresolved/partial.
	found := false
	for _, u := range resolved.unresolved {
		if u == "pool-missing" {
			found = true
		}
	}
	if !found {
		t.Fatalf("unresolved default pool must feed partial; got %v", resolved.unresolved)
	}
	// The resolved weighted pool's weight is folded in, keyed by CF pool ID.
	if resolved.poolWeights["cf-pool-weighted"] != "0.5" {
		t.Fatalf("resolved weighted pool not folded into poolWeights: %+v", resolved.poolWeights)
	}
	// An unweighted default pool carries no weight entry.
	if _, ok := resolved.poolWeights["cf-pool-ready"]; ok {
		t.Fatalf("unweighted default pool must not appear in poolWeights: %+v", resolved.poolWeights)
	}
	// The unresolved and unweighted default pools contributed no weight: only the
	// single resolved weighted pool folds in, so poolWeights holds exactly one entry.
	if len(resolved.poolWeights) != 1 {
		t.Fatalf("expected exactly one folded weight (cf-pool-weighted), got %+v", resolved.poolWeights)
	}
}

func TestGeoMapNeedsRemoval(t *testing.T) {
	managed := &map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "p"}}}
	cases := []struct {
		name     string
		spec     *map[string][]lbv1beta1.LoadBalancerPoolRef
		desired  map[string][]string
		observed map[string][]string
		want     bool
	}{
		{
			name: "unmanaged (nil spec) never needs removal even if CF has keys",
			spec: nil,
			// desired is irrelevant when unmanaged; CF still holds a key.
			observed: map[string][]string{"WNAM": {"a"}},
			want:     false,
		},
		{
			name:     "managed, CF key dropped by the CR needs removal",
			spec:     managed,
			desired:  map[string][]string{"WNAM": {"a"}},
			observed: map[string][]string{"WNAM": {"a"}, "ENAM": {"b"}},
			want:     true,
		},
		{
			name:     "managed, exact key set does not need removal",
			spec:     managed,
			desired:  map[string][]string{"WNAM": {"a"}},
			observed: map[string][]string{"WNAM": {"a"}},
			want:     false,
		},
		{
			name:     "managed, pure additions (CF subset of desired) do not need removal",
			spec:     managed,
			desired:  map[string][]string{"WNAM": {"a"}, "ENAM": {"b"}},
			observed: map[string][]string{"WNAM": {"a"}},
			want:     false,
		},
		{
			name:     "managed, empty desired against a non-empty CF map needs removal (clear all)",
			spec:     managed,
			desired:  map[string][]string{},
			observed: map[string][]string{"WNAM": {"a"}},
			want:     true,
		},
		{
			name:     "managed, value change on a shared key is not a removal",
			spec:     managed,
			desired:  map[string][]string{"WNAM": {"b"}},
			observed: map[string][]string{"WNAM": {"a"}},
			want:     false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := geoMapNeedsRemoval(tc.spec, tc.desired, tc.observed); got != tc.want {
				t.Fatalf("geoMapNeedsRemoval = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWeightedSteeringActive(t *testing.T) {
	for policy, want := range map[string]bool{
		"random":                     true,
		"least_outstanding_requests": true,
		"least_connections":          true,
		"off":                        false,
		"geo":                        false,
		"dynamic_latency":            false,
		"proximity":                  false,
		"":                           false,
	} {
		lb := newLBCR(lbv1beta1.LoadBalancerSpec{SteeringPolicy: policy})
		if got := weightedSteeringActive(lb); got != want {
			t.Errorf("weightedSteeringActive(%q) = %v, want %v", policy, got, want)
		}
	}
}

// TestPoolWeightsDrifted covers the exact-match weight comparison: under the folded
// model CF's pool_weights equals the desired set exactly, so drift is any keyset or
// value divergence (including a desired weight CF dropped -- an out-of-band removal to
// re-apply), plus a default_weight change when the CR sets it.
func TestPoolWeightsDrifted(t *testing.T) {
	tests := []struct {
		name          string
		cf            load_balancers.RandomSteering
		defaultWeight string
		weights       map[string]string
		want          bool
	}{
		{
			name:    "exact match does not drift",
			cf:      load_balancers.RandomSteering{PoolWeights: map[string]float64{"a": 0.6}},
			weights: map[string]string{"a": "0.6"},
			want:    false,
		},
		{
			name:    "a desired weight absent from CF drifts (out-of-band removal re-applied)",
			cf:      load_balancers.RandomSteering{PoolWeights: map[string]float64{}},
			weights: map[string]string{"a": "0.6"},
			want:    true,
		},
		{
			name:    "a CF weight the CR dropped drifts (nulled out)",
			cf:      load_balancers.RandomSteering{PoolWeights: map[string]float64{"a": 0.6, "b": 0.4}},
			weights: map[string]string{"a": "0.6"},
			want:    true,
		},
		{
			name:    "a shared key with a changed value drifts",
			cf:      load_balancers.RandomSteering{PoolWeights: map[string]float64{"a": 0.1}},
			weights: map[string]string{"a": "0.6"},
			want:    true,
		},
		{
			name:          "default_weight change drifts",
			cf:            load_balancers.RandomSteering{DefaultWeight: 0.9, PoolWeights: map[string]float64{"a": 0.6}},
			defaultWeight: "0.2",
			weights:       map[string]string{"a": "0.6"},
			want:          true,
		},
		{
			name:          "unset default_weight leaves Cloudflare's value alone",
			cf:            load_balancers.RandomSteering{DefaultWeight: 0.9, PoolWeights: map[string]float64{"a": 0.6}},
			defaultWeight: "",
			weights:       map[string]string{"a": "0.6"},
			want:          false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := poolWeightsDrifted(tt.cf, tt.defaultWeight, tt.weights); got != tt.want {
				t.Fatalf("poolWeightsDrifted = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMarkReady_PartialTransitionOnce(t *testing.T) {
	scheme := lbFakeScheme(t)
	ctx := context.Background()
	lb := &lbv1beta1.LoadBalancer{
		ObjectMeta: metav1.ObjectMeta{Name: "lb", Namespace: "ns"},
		Spec:       lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com"},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&lbv1beta1.LoadBalancer{}).
		WithObjects(lb).Build()
	rec := events.NewFakeRecorder(10)
	r := &LoadBalancerReconciler{Client: c, Scheme: scheme, Recorder: rec}

	// Enter partial: Ready=True + reason Partial, one Event.
	partial := &resolvedPools{unresolved: []string{"pool-missing"}}
	lb.Status.UnresolvedPoolRefs = partial.unresolved
	if _, err := r.markReady(ctx, lb, partial); err != nil {
		t.Fatal(err)
	}
	cond := apimeta.FindStatusCondition(lb.Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != reasonPartial {
		t.Fatalf("expected Ready=True+Partial, got %+v", cond)
	}
	if len(lb.Status.UnresolvedPoolRefs) != 1 || lb.Status.UnresolvedPoolRefs[0] != "pool-missing" {
		t.Fatalf("unresolvedPoolRefs not populated: %v", lb.Status.UnresolvedPoolRefs)
	}

	// Stay partial across a second reconcile: no new Event (transition-only).
	if _, err := r.markReady(ctx, lb, partial); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events); n != 1 {
		t.Fatalf("expected exactly one partial Event across two partial reconciles, got %d", n)
	}

	// Fully resolved: reason Reconciled, unresolvedPoolRefs cleared, no extra Event.
	lb.Status.UnresolvedPoolRefs = nil
	if _, err := r.markReady(ctx, lb, &resolvedPools{}); err != nil {
		t.Fatal(err)
	}
	cond = apimeta.FindStatusCondition(lb.Status.Conditions, conditionReady)
	if cond == nil || cond.Reason != reasonReconciled {
		t.Fatalf("expected Ready reason Reconciled after full resolution, got %+v", cond)
	}
	if n := len(rec.Events); n != 1 {
		t.Fatalf("resolution must not emit an Event; total=%d", n)
	}

	// Re-enter partial: a fresh transition emits another Event.
	lb.Status.UnresolvedPoolRefs = partial.unresolved
	if _, err := r.markReady(ctx, lb, partial); err != nil {
		t.Fatal(err)
	}
	if n := len(rec.Events); n != 2 {
		t.Fatalf("re-entering partial must emit a second Event; total=%d", n)
	}
}

func TestSurfaceNetworksDrift_Conditions(t *testing.T) {
	r := &LoadBalancerReconciler{}
	ctx := context.Background()

	// In sync (order-insensitive) -> NetworksSynced=True/InSync.
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com", Networks: []string{"a", "b"}})
	r.surfaceNetworksDrift(ctx, lb, []string{"b", "a"})
	c := apimeta.FindStatusCondition(lb.Status.Conditions, conditionNetworksSynced)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonNetworksInSync {
		t.Fatalf("expected NetworksSynced=True/InSync, got %+v", c)
	}

	// Drift -> NetworksSynced=False/Drifted; Ready untouched.
	r.surfaceNetworksDrift(ctx, lb, []string{"a"})
	c = apimeta.FindStatusCondition(lb.Status.Conditions, conditionNetworksSynced)
	if c == nil || c.Status != metav1.ConditionFalse || c.Reason != reasonNetworksDrifted {
		t.Fatalf("expected NetworksSynced=False/Drifted, got %+v", c)
	}
	if apimeta.FindStatusCondition(lb.Status.Conditions, conditionReady) != nil {
		t.Fatal("surfaceNetworksDrift must not touch the Ready condition")
	}

	// Empty spec.networks with a pre-existing condition -> flipped to True/Unmanaged
	// (the networks-cleared branch: the operator stops managing networks, so the
	// drift condition/metric must clear rather than stay False forever).
	unmanaged := newLBWithConditions(lb.Status.Conditions)
	r.surfaceNetworksDrift(ctx, unmanaged, nil)
	c = apimeta.FindStatusCondition(unmanaged.Status.Conditions, conditionNetworksSynced)
	if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonNetworksUnmanaged {
		t.Fatalf("expected NetworksSynced=True/Unmanaged when spec.networks is cleared, got %+v", c)
	}
	if networksDrifted(unmanaged.Status.Conditions) {
		t.Fatal("networksDrifted must be false once spec.networks is cleared (Unmanaged)")
	}
}

// newLBWithConditions returns a fresh LB (empty spec.networks) carrying the given
// conditions, to exercise the "networks cleared" branch of surfaceNetworksDrift.
func newLBWithConditions(conds []metav1.Condition) *lbv1beta1.LoadBalancer {
	lb := newLBCR(lbv1beta1.LoadBalancerSpec{Hostname: "lb.example.com"})
	lb.Status.Conditions = conds
	return lb
}
