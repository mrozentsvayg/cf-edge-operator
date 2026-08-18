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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/cloudflare/cloudflare-go/v6/load_balancers"

	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

// newPoolCR builds a LoadBalancerPool CR with fixed metadata (Name:
// "us-pool", Namespace: "default"). Tests only vary the spec, so keeping
// the metadata hard-coded matches how the helper is actually used.
func newPoolCR(spec lbv1beta1.LoadBalancerPoolSpec) *lbv1beta1.LoadBalancerPool {
	return &lbv1beta1.LoadBalancerPool{
		ObjectMeta: metav1.ObjectMeta{Name: "us-pool", Namespace: "default"},
		Spec:       spec,
	}
}

func TestResolveMonitorID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = lbv1beta1.AddToScheme(scheme)
	ctx := context.Background()

	makeReconciler := func(objs ...client.Object) *LoadBalancerPoolReconciler {
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
		return &LoadBalancerPoolReconciler{Client: c, Scheme: scheme}
	}

	t.Run("no monitorRef -> empty, no error", func(t *testing.T) {
		r := makeReconciler()
		id, err := r.resolveMonitorID(ctx, newPoolCR(lbv1beta1.LoadBalancerPoolSpec{}))
		if err != nil || id != "" {
			t.Fatalf("got id=%q err=%v", id, err)
		}
	})

	t.Run("monitor CR missing (NotFound) -> empty, no error (soft wait)", func(t *testing.T) {
		r := makeReconciler()
		pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
			MonitorRef: &lbv1beta1.LoadBalancerMonitorRef{Name: "ghost"},
		})
		id, err := r.resolveMonitorID(ctx, pool)
		if err != nil {
			t.Fatalf("NotFound must be a soft wait, not an error; got %v", err)
		}
		if id != "" {
			t.Fatalf("id=%q want empty", id)
		}
	})

	t.Run("monitor exists but not ready -> empty, no error", func(t *testing.T) {
		mon := &lbv1beta1.LoadBalancerMonitor{
			ObjectMeta: metav1.ObjectMeta{Name: "mon", Namespace: "default"},
		}
		r := makeReconciler(mon)
		pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
			MonitorRef: &lbv1beta1.LoadBalancerMonitorRef{Name: "mon"},
		})
		id, err := r.resolveMonitorID(ctx, pool)
		if err != nil || id != "" {
			t.Fatalf("got id=%q err=%v", id, err)
		}
	})

	t.Run("monitor ready -> returns status.ID", func(t *testing.T) {
		mon := &lbv1beta1.LoadBalancerMonitor{
			ObjectMeta: metav1.ObjectMeta{Name: "mon", Namespace: "default"},
			Status:     lbv1beta1.LoadBalancerMonitorStatus{ID: "cf-mon-123"},
		}
		r := makeReconciler(mon)
		pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
			MonitorRef: &lbv1beta1.LoadBalancerMonitorRef{Name: "mon"},
		})
		id, err := r.resolveMonitorID(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if id != "cf-mon-123" {
			t.Fatalf("id=%q want cf-mon-123", id)
		}
	})
}

func TestPoolEnabledFromCF(t *testing.T) {
	if poolEnabledFromCF(nil) {
		t.Fatal("nil pool must not be enabled")
	}
	if poolEnabledFromCF(&load_balancers.Pool{Enabled: false}) {
		t.Fatal("disabled pool must not report enabled")
	}
	if !poolEnabledFromCF(&load_balancers.Pool{Enabled: true}) {
		t.Fatal("enabled pool should report enabled")
	}
}

func TestBuildOriginParams_DefaultsAndWeight(t *testing.T) {
	origins := []lbv1beta1.LoadBalancerPoolOrigin{
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
	pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
		Origins: []lbv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}},
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
	pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
		Origins: []lbv1beta1.LoadBalancerPoolOrigin{
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
	pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
		Origins: []lbv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}},
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
	pool := newPoolCR(lbv1beta1.LoadBalancerPoolSpec{
		Origins: []lbv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}},
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
	spec := []lbv1beta1.LoadBalancerPoolOrigin{
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
	spec := []lbv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}}
	if originsDrifted(cf, spec) {
		t.Fatal("CF Enabled=true and spec unset should not drift (both mean 'enabled')")
	}
}

func TestOriginsDrifted_WeightUnparseableIsIgnored(t *testing.T) {
	// If the CR's weight string doesn't parse, we skip the check rather than
	// asserting drift (avoids trying to correct data the CR can't even
	// serialize). The CRD pattern validator catches this at admission.
	cf := []load_balancers.Origin{{Name: "a", Address: "1.1.1.1", Enabled: true, Weight: 0.5}}
	spec := []lbv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1", Weight: "not-a-number"}}
	if originsDrifted(cf, spec) {
		t.Fatal("unparseable weight should not drift (guard-only behavior)")
	}
}

func TestBuildOriginParams_HostHeader(t *testing.T) {
	origins := []lbv1beta1.LoadBalancerPoolOrigin{
		{Name: "no-header", Address: "1.1.1.1"},
		{Name: "with-host", Address: "2.2.2.2", Header: &lbv1beta1.LoadBalancerOriginHeader{Host: []string{"api.internal"}}},
	}
	out := buildOriginParams(origins)
	// Header is unmanaged (nil spec) -> not sent.
	if out[0].Header.Present {
		t.Error("origin[0] Header should be absent when spec.Header is nil")
	}
	// Header set -> sent with the Host list preserved.
	if !out[1].Header.Present {
		t.Fatal("origin[1] Header should be present when spec.Header is set")
	}
	host := out[1].Header.Value.Host
	if !host.Present || len(host.Value) != 1 || host.Value[0] != "api.internal" {
		t.Errorf("origin[1] Host header lost in translation: present=%v value=%v", host.Present, host.Value)
	}
}

func TestOriginsDrifted_HostHeader(t *testing.T) {
	cfWithHost := []load_balancers.Origin{
		{Name: "a", Address: "1.1.1.1", Enabled: true, Header: load_balancers.Header{Host: []string{"api.internal"}}},
	}

	// Managed header matches -> no drift.
	specMatch := []lbv1beta1.LoadBalancerPoolOrigin{
		{Name: "a", Address: "1.1.1.1", Header: &lbv1beta1.LoadBalancerOriginHeader{Host: []string{"api.internal"}}},
	}
	if originsDrifted(cfWithHost, specMatch) {
		t.Fatal("matching Host header should not drift")
	}

	// Managed header differs -> drift.
	specDiff := []lbv1beta1.LoadBalancerPoolOrigin{
		{Name: "a", Address: "1.1.1.1", Header: &lbv1beta1.LoadBalancerOriginHeader{Host: []string{"other.internal"}}},
	}
	if !originsDrifted(cfWithHost, specDiff) {
		t.Fatal("differing Host header should drift")
	}

	// Unmanaged header (nil spec.Header) -> CF's header is ignored.
	specUnmanaged := []lbv1beta1.LoadBalancerPoolOrigin{{Name: "a", Address: "1.1.1.1"}}
	if originsDrifted(cfWithHost, specUnmanaged) {
		t.Fatal("unmanaged header (nil spec) should not drift")
	}
}
