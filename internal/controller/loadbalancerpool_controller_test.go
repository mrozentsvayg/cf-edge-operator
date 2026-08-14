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

// newPoolCR builds a LoadBalancerPool CR with fixed metadata (Name:
// "us-pool", Namespace: "default"). Tests only vary the spec, so keeping
// the metadata hard-coded matches how the helper is actually used.
func newPoolCR(spec saasv1beta1.LoadBalancerPoolSpec) *saasv1beta1.LoadBalancerPool {
	return &saasv1beta1.LoadBalancerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "us-pool", Namespace: "default"},
		Spec:       spec,
	}
}

func TestPoolHealthyFromCF(t *testing.T) {
	if poolHealthyFromCF(nil) {
		t.Fatal("nil pool must not be healthy")
	}
	if poolHealthyFromCF(&load_balancers.Pool{Enabled: false}) {
		t.Fatal("disabled pool must not be healthy")
	}
	if !poolHealthyFromCF(&load_balancers.Pool{Enabled: true}) {
		t.Fatal("enabled pool should be healthy")
	}
}

func TestBuildOriginParams_DefaultsAndWeight(t *testing.T) {
	origins := []saasv1beta1.LoadBalancerPoolOrigin{
		{Name: "primary", Address: "1.1.1.1"},
		{Name: "explicit-off", Address: "2.2.2.2", Enabled: new(bool), Weight: "0.5"},
	}
	out := buildOriginParams(origins)
	if len(out) != 2 {
		t.Fatalf("expected 2 origins, got %d", len(out))
	}
	// Unset Enabled defaults to true (CRD default), matching spec.
	if !out[0].Enabled.Value {
		t.Errorf("origin[0] Enabled should default to true when unset")
	}
	if out[1].Enabled.Value {
		t.Errorf("origin[1] Enabled=false was not preserved")
	}
	if !out[1].Weight.Present {
		t.Errorf("origin[1] Weight should be set from spec")
	}
	if out[1].Weight.Value != 0.5 {
		t.Errorf("origin[1] Weight = %v, want 0.5", out[1].Weight.Value)
	}
	// Weight left unset produces no Present flag.
	if out[0].Weight.Present {
		t.Errorf("origin[0] Weight should be absent when spec.weight is empty")
	}
}

func TestBuildPoolNewParams_MonitorIDIsOptional(t *testing.T) {
	pool := newPoolCR(saasv1beta1.LoadBalancerPoolSpec{
		Origins: []saasv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}},
	})

	// No monitorID -> Monitor is absent from params (CF will omit / default it).
	p := buildPoolNewParams("acct", pool, "")
	if p.Monitor.Present {
		t.Fatal("Monitor should be absent when monitorID is empty")
	}

	// With monitorID -> Monitor is present with that value.
	p = buildPoolNewParams("acct", pool, "mon-abc")
	if !p.Monitor.Present || p.Monitor.Value != "mon-abc" {
		t.Fatalf("Monitor not wired through; got present=%v value=%q", p.Monitor.Present, p.Monitor.Value)
	}

	// Name is always CR name.
	if !p.Name.Present || p.Name.Value != "us-pool" {
		t.Fatalf("Pool name mismatch: %+v", p.Name)
	}
}

func TestPoolDrifted_RespectsUnmanagedFields(t *testing.T) {
	pool := newPoolCR(saasv1beta1.LoadBalancerPoolSpec{
		Origins: []saasv1beta1.LoadBalancerPoolOrigin{
			{Name: "a", Address: "1.1.1.1"},
		},
	})
	cf := &load_balancers.Pool{
		Name:    "us-pool",
		Enabled: true,
		Origins: []load_balancers.Origin{{Name: "a", Address: "1.1.1.1", Enabled: true}},
		// Description set on CF side but not managed by CR -- must NOT drift.
		Description: "manually added",
	}
	if poolDrifted(cf, pool, "") {
		t.Fatal("unmanaged description drift should be ignored")
	}
}

func TestPoolDrifted_NameChange(t *testing.T) {
	pool := newPoolCR(saasv1beta1.LoadBalancerPoolSpec{
		Origins: []saasv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}},
	})
	cf := &load_balancers.Pool{
		Name:    "different-name",
		Enabled: true,
		Origins: []load_balancers.Origin{{Name: "a", Address: "1.1.1.1", Enabled: true}},
	}
	if !poolDrifted(cf, pool, "") {
		t.Fatal("name mismatch should drift")
	}
}

func TestPoolDrifted_MonitorChange(t *testing.T) {
	pool := newPoolCR(saasv1beta1.LoadBalancerPoolSpec{
		Origins: []saasv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}},
	})
	cf := &load_balancers.Pool{
		Name:    "us-pool",
		Enabled: true,
		Monitor: "old-monitor-id",
		Origins: []load_balancers.Origin{{Name: "a", Address: "1.1.1.1", Enabled: true}},
	}
	if !poolDrifted(cf, pool, "new-monitor-id") {
		t.Fatal("monitor ID change should drift")
	}
	// Passing empty monitorID -> monitor drift is NOT checked (unmanaged).
	if poolDrifted(cf, pool, "") {
		t.Fatal("empty monitorID should not trigger monitor drift check")
	}
}

func TestOriginsDrifted_LengthMismatch(t *testing.T) {
	cf := []load_balancers.Origin{
		{Name: "a", Address: "1.1.1.1", Enabled: true},
	}
	spec := []saasv1beta1.LoadBalancerPoolOrigin{
		{Name: "a", Address: "1.1.1.1"},
		{Name: "b", Address: "2.2.2.2"},
	}
	if !originsDrifted(cf, spec) {
		t.Fatal("origin length mismatch should drift")
	}
}

func TestOriginsDrifted_EnabledDefaultsMatch(t *testing.T) {
	// CRD default for origin.Enabled is true. If the CR omits it and CF
	// reports true, that must not count as drift.
	cf := []load_balancers.Origin{{Name: "a", Address: "1.1.1.1", Enabled: true}}
	spec := []saasv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}}
	if originsDrifted(cf, spec) {
		t.Fatal("CF Enabled=true and spec unset should not drift (both mean 'enabled')")
	}
}

func TestOriginsDrifted_WeightUnparseableIsIgnored(t *testing.T) {
	// If the CR's weight string doesn't parse, we skip the check rather than
	// asserting drift (avoids trying to correct data the CR can't even
	// serialize). The CRD pattern validator catches this at admission.
	cf := []load_balancers.Origin{{Name: "a", Address: "1.1.1.1", Enabled: true, Weight: 0.5}}
	spec := []saasv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1", Weight: "not-a-number"}}
	if originsDrifted(cf, spec) {
		t.Fatal("unparseable weight should not drift (guard-only behavior)")
	}
}
