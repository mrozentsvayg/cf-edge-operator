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
	"errors"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// guardRes is a stand-in resource type for exercising the generic
// cfCreateGuarded helper without pulling in a real CF SDK response type.
type guardRes struct{ id string }

func TestCFCreateGuarded_FirstAttemptSucceeds(t *testing.T) {
	ctx := context.Background()
	findCalls := 0
	res, adopted, attempts, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 1,
		func() (*guardRes, error) { findCalls++; return nil, nil },
		func() (*guardRes, error) { return &guardRes{id: "new"}, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if adopted {
		t.Error("should not be adopted on first-attempt success")
	}
	if attempts != 1 {
		t.Errorf("attempts=%d want 1", attempts)
	}
	if res == nil || res.id != "new" {
		t.Errorf("unexpected result: %+v", res)
	}
	if findCalls != 0 {
		t.Errorf("find must not run when the first create succeeds; calls=%d", findCalls)
	}
}

func TestCFCreateGuarded_AdoptsAfterTimeout(t *testing.T) {
	// The create "times out" (error) but the resource actually landed on CF; the
	// retry's find discovers it and adopts instead of creating a duplicate.
	ctx := context.Background()
	createCalls := 0
	res, adopted, _, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 2,
		func() (*guardRes, error) { return &guardRes{id: "landed"}, nil },
		func() (*guardRes, error) {
			createCalls++
			return nil, context.DeadlineExceeded
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !adopted {
		t.Error("expected adoption of the timed-out-but-created resource")
	}
	if res == nil || res.id != "landed" {
		t.Errorf("unexpected result: %+v", res)
	}
	if createCalls != 1 {
		t.Errorf("create must run once then adopt on retry; createCalls=%d", createCalls)
	}
}

func TestCFCreateGuarded_RetriesWhenNotCreated(t *testing.T) {
	// First create fails and the resource did NOT land (find returns nil), so the
	// guard retries the create, which then succeeds.
	ctx := context.Background()
	createCalls := 0
	res, adopted, attempts, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 2,
		func() (*guardRes, error) { return nil, nil },
		func() (*guardRes, error) {
			createCalls++
			if createCalls == 1 {
				return nil, errors.New("transient")
			}
			return &guardRes{id: "retry-created"}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if adopted {
		t.Error("should not be adopted when find returns nil")
	}
	if res == nil || res.id != "retry-created" {
		t.Errorf("unexpected result: %+v", res)
	}
	if createCalls != 2 {
		t.Errorf("expected 2 create calls; got %d", createCalls)
	}
	if attempts != 2 {
		t.Errorf("attempts=%d want 2", attempts)
	}
}

func TestCFCreateGuarded_AbandonsWhenFindErrors(t *testing.T) {
	// If find can't confirm existence, abandon the retry (returning the original
	// create error) rather than risk a duplicate.
	ctx := context.Background()
	createCalls := 0
	origErr := errors.New("create failed")
	res, adopted, _, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 2,
		func() (*guardRes, error) { return nil, errors.New("list failed") },
		func() (*guardRes, error) { createCalls++; return nil, origErr },
	)
	if !errors.Is(err, origErr) {
		t.Errorf("want the original create error, got %v", err)
	}
	if adopted || res != nil {
		t.Error("must not adopt or return a resource when find errors")
	}
	if createCalls != 1 {
		t.Errorf("create must run once then abandon; got %d", createCalls)
	}
}

func TestPreInitLoadBalancerMetrics(t *testing.T) {
	// Exercised only via main() behind --enable-loadbalancing, so cover it here.
	// It must not panic (all vectors are registered in init()) and must be safe
	// to call more than once (WithLabelValues is idempotent).
	PreInitLoadBalancerMetrics()
	PreInitLoadBalancerMetrics()
}

func TestPreInitCustomHostnameMetrics(t *testing.T) {
	// Exercised only via main() behind --enable-customhostname, so cover it here.
	// It must not panic (all vectors are registered in init()) and must be safe
	// to call more than once (WithLabelValues is idempotent).
	PreInitCustomHostnameMetrics()
	PreInitCustomHostnameMetrics()
}

func TestLBReadyState(t *testing.T) {
	// One Ready condition per case; verifies the reason -> state mapping the
	// aggregate gauges rely on. Covers all four states across the reasons every
	// load-balancing controller can set (setError reasons collapse to "error").
	ready := func(status metav1.ConditionStatus, reason string) []metav1.Condition {
		return []metav1.Condition{{Type: conditionReady, Status: status, Reason: reason}}
	}
	cases := []struct {
		name  string
		conds []metav1.Condition
		want  string
	}{
		{"reconciled", ready(metav1.ConditionTrue, reasonReconciled), lbStateReady},
		{"partial (Ready=True + Partial)", ready(metav1.ConditionTrue, reasonPartial), lbStatePartial},
		{"ready wins regardless of reason", ready(metav1.ConditionTrue, "AnythingElse"), lbStateReady},
		{"dryrun", ready(metav1.ConditionFalse, reasonDryRun), lbStateDryRun},
		{"waiting for monitor", ready(metav1.ConditionFalse, reasonWaitingForMonitor), lbStateWaiting},
		{"waiting for fallback pool", ready(metav1.ConditionFalse, reasonWaitingForFallbackPool), lbStateWaiting},
		{"waiting for external", ready(metav1.ConditionFalse, reasonWaitingForExternal), lbStateWaiting},
		{"account error", ready(metav1.ConditionFalse, "AccountError"), lbStateError},
		{"zone error", ready(metav1.ConditionFalse, "ZoneError"), lbStateError},
		{"monitor ref error", ready(metav1.ConditionFalse, "MonitorRefError"), lbStateError},
		{"pool resolution error", ready(metav1.ConditionFalse, "PoolResolutionError"), lbStateError},
		{"lookup failed", ready(metav1.ConditionFalse, "LookupFailed"), lbStateError},
		{"create failed", ready(metav1.ConditionFalse, "CreateFailed"), lbStateError},
		{"update failed", ready(metav1.ConditionFalse, "UpdateFailed"), lbStateError},
		{"unknown false reason folds to error", ready(metav1.ConditionFalse, "SomethingNew"), lbStateError},
		{"no ready condition yet", nil, lbStateWaiting},
		{"other condition only", []metav1.Condition{{Type: conditionInitialized, Status: metav1.ConditionTrue}}, lbStateWaiting},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := lbReadyState(tc.conds); got != tc.want {
				t.Errorf("lbReadyState = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLBStateGaugeSetAndStaleCleanup(t *testing.T) {
	// Bind a fresh, unregistered gauge so the test is hermetic (no cross-test
	// contamination via the global registry / prevOwners).
	vec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_lb_state_gauge",
		Help: "test",
	}, []string{"zone_cr", "state"})
	sg := &lbStateGauge{gauge: vec, ownerLabel: "zone_cr", stateLabels: lbStateLabels, prevOwners: map[string]bool{}}

	// First publish: one owner with counts across two states. All four states are
	// seeded for that owner (0 or positive), so the vector has exactly 4 series.
	sg.set(map[string]map[string]int{"z1": {lbStateReady: 2, lbStateError: 1}})
	if got := testutil.ToFloat64(vec.WithLabelValues("z1", lbStateReady)); got != 2 {
		t.Errorf("z1 ready = %v, want 2", got)
	}
	if got := testutil.ToFloat64(vec.WithLabelValues("z1", lbStateError)); got != 1 {
		t.Errorf("z1 error = %v, want 1", got)
	}
	if got := testutil.ToFloat64(vec.WithLabelValues("z1", lbStateWaiting)); got != 0 {
		t.Errorf("z1 waiting = %v, want 0 (seeded)", got)
	}
	if n := testutil.CollectAndCount(vec); n != 4 {
		t.Errorf("series count = %d, want 4 (one owner x four states)", n)
	}

	// Recompute for the same owner after the errored CR is gone: the error series
	// must drop to 0 in place (not left stale), and no extra series appear.
	sg.set(map[string]map[string]int{"z1": {lbStateReady: 3}})
	if got := testutil.ToFloat64(vec.WithLabelValues("z1", lbStateError)); got != 0 {
		t.Errorf("z1 error after recompute = %v, want 0", got)
	}
	if got := testutil.ToFloat64(vec.WithLabelValues("z1", lbStateReady)); got != 3 {
		t.Errorf("z1 ready after recompute = %v, want 3", got)
	}
	if n := testutil.CollectAndCount(vec); n != 4 {
		t.Errorf("series count = %d, want 4 after in-place recompute", n)
	}

	// A different owner appears and z1 has no CRs left: z1's series must be removed
	// entirely (owner cleanup), leaving only z2's four series.
	sg.set(map[string]map[string]int{"z2": {lbStateReady: 1}})
	if n := testutil.CollectAndCount(vec); n != 4 {
		t.Errorf("series count = %d, want 4 (only z2 remains)", n)
	}
	if got := testutil.ToFloat64(vec.WithLabelValues("z2", lbStateReady)); got != 1 {
		t.Errorf("z2 ready = %v, want 1", got)
	}

	// All CRs deleted: the gauge must drop every series (no stale owner).
	sg.set(map[string]map[string]int{})
	if n := testutil.CollectAndCount(vec); n != 0 {
		t.Errorf("series count = %d, want 0 after all owners removed", n)
	}
}

func TestLBStateGauge_PartialIsLBOnly(t *testing.T) {
	// The loadbalancers gauge (stateLabels lbStateLabelsLB) emits a partial series;
	// the pool/monitor gauges (stateLabels lbStateLabels) must never emit partial.
	lbVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_lb_partial_gauge", Help: "test",
	}, []string{"zone_cr", "state"})
	lbSG := &lbStateGauge{gauge: lbVec, ownerLabel: "zone_cr", stateLabels: lbStateLabelsLB, prevOwners: map[string]bool{}}
	lbSG.set(map[string]map[string]int{"z1": {lbStateReady: 1, lbStatePartial: 2}})
	if got := testutil.ToFloat64(lbVec.WithLabelValues("z1", lbStatePartial)); got != 2 {
		t.Errorf("lb partial = %v, want 2", got)
	}
	// Five series: ready, partial, waiting, dryrun, error (all seeded for the owner).
	if n := testutil.CollectAndCount(lbVec); n != 5 {
		t.Errorf("lb series count = %d, want 5 (partial included)", n)
	}

	poolVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "test_pool_partial_gauge", Help: "test",
	}, []string{"account_cr", "state"})
	poolSG := &lbStateGauge{gauge: poolVec, ownerLabel: "account_cr", stateLabels: lbStateLabels, prevOwners: map[string]bool{}}
	// Even if a partial count somehow slipped into the map, a pool gauge must not
	// publish a partial series (its stateLabels omit it).
	poolSG.set(map[string]map[string]int{"a1": {lbStateReady: 1, lbStatePartial: 9}})
	// Four series (ready, waiting, dryrun, error) and no partial series: even a
	// stray partial count in the map is not published, because the pool gauge's
	// stateLabels omit partial.
	if n := testutil.CollectAndCount(poolVec); n != 4 {
		t.Errorf("pool series count = %d, want 4 (no partial series)", n)
	}
}

func TestSetNetworksDriftGauge_SeedAndCleanup(t *testing.T) {
	// Reset the package-level owner tracking so the test is hermetic regardless of
	// order relative to reconcile-driven writers.
	lbNetworksDriftGauge.mu.Lock()
	lbNetworksDriftGauge.prevOwners = map[string]bool{}
	lbNetworksDriftGauge.mu.Unlock()
	loadBalancerNetworksDrift.Reset()

	// A zone with one drifted LB and a zone with none (seeded 0).
	setNetworksDriftGauge(map[string]int{"z1": 1, "z2": 0})
	if got := testutil.ToFloat64(loadBalancerNetworksDrift.WithLabelValues("z1")); got != 1 {
		t.Errorf("z1 drift = %v, want 1", got)
	}
	if got := testutil.ToFloat64(loadBalancerNetworksDrift.WithLabelValues("z2")); got != 0 {
		t.Errorf("z2 drift = %v, want 0 (seeded)", got)
	}
	if n := testutil.CollectAndCount(loadBalancerNetworksDrift); n != 2 {
		t.Errorf("series count = %d, want 2", n)
	}

	// z1's LB is deleted (no longer present); its series must be removed, z2 stays.
	setNetworksDriftGauge(map[string]int{"z2": 0})
	if n := testutil.CollectAndCount(loadBalancerNetworksDrift); n != 1 {
		t.Errorf("series count = %d, want 1 after z1 cleanup", n)
	}

	// All owners gone: no series remain.
	setNetworksDriftGauge(map[string]int{})
	if n := testutil.CollectAndCount(loadBalancerNetworksDrift); n != 0 {
		t.Errorf("series count = %d, want 0 after all owners removed", n)
	}
}

func TestCFCreateGuarded_No429Retry(t *testing.T) {
	ctx := context.Background()
	findCalls := 0
	createCalls := 0
	_, adopted, _, err := cfCreateGuarded(ctx, cfResourceLoadBalancerPool, 3,
		func() (*guardRes, error) { findCalls++; return &guardRes{id: "x"}, nil },
		func() (*guardRes, error) { createCalls++; return nil, &cloudflare.Error{StatusCode: 429} },
	)
	if err == nil {
		t.Fatal("expected the 429 error to propagate")
	}
	if adopted {
		t.Error("429 must not adopt")
	}
	if createCalls != 1 {
		t.Errorf("429 must not retry the create; createCalls=%d", createCalls)
	}
	if findCalls != 0 {
		t.Errorf("429 must not trigger find; findCalls=%d", findCalls)
	}
}

// TestClearZoneCustomHostnameMetrics verifies that clearing a zone's
// CustomHostname-family series removes exactly that zone's series (across all four
// CH gauges) and its SSL provisioning tracking-cache entries, and leaves other zones
// untouched. Uses countSeries (a side-effect-free registry gather) so absence is
// observed as 0 series rather than recreated at 0.
func TestClearZoneCustomHostnameMetrics(t *testing.T) {
	const zoneCR = "clear-ch-metrics-zone"     // unique to this test; no suite uses it
	const otherZone = "clear-ch-metrics-other" // bystander that must survive

	// Populate every CH-family series for the zone under test, plus a bystander.
	customHostnames.WithLabelValues(zoneCR, chStateReady).Set(3)
	hostnameStatusGauge.WithLabelValues(zoneCR, "active").Set(2)
	zoneCustomHostnames.WithLabelValues(zoneCR, "total").Set(5)
	customHostnames.WithLabelValues(otherZone, chStateReady).Set(1)
	// setSSLProvisioningDuration sets the gauge AND registers a tracking-cache entry,
	// so both the gauge series and the cache entry are exercised by the clear.
	setSSLProvisioningDuration(zoneCR, "app.example.com", sslMethodHTTP, 90*time.Second)
	setSSLProvisioningDuration(otherZone, "other.example.com", sslMethodHTTP, 30*time.Second)
	t.Cleanup(func() { clearZoneCustomHostnameMetrics(otherZone) })

	clearZoneCustomHostnameMetrics(zoneCR)

	families := []struct {
		name   string
		family string
	}{
		{"customHostnames", "cf_edge_operator_customhostnames"},
		{"hostnameStatus", "cf_edge_operator_customhostname_status"},
		{"zoneCustomHostnames", "cf_edge_operator_zone_customhostnames"},
		{"sslProvisioningDuration", "cf_edge_operator_ssl_provisioning_duration_seconds"},
	}
	for _, f := range families {
		if n := countSeries(f.family, map[string]string{labelZoneCR: zoneCR}); n != 0 {
			t.Errorf("%s: expected 0 series for zone %q after clear, got %d", f.name, zoneCR, n)
		}
	}

	// The zone's SSL provisioning tracking-cache entries must be gone; the bystander's
	// must survive.
	zoneCached, otherCached := 0, 0
	sslProvisioningMu.Lock()
	for _, entry := range sslProvisioningCache {
		switch entry.zoneCR {
		case zoneCR:
			zoneCached++
		case otherZone:
			otherCached++
		}
	}
	sslProvisioningMu.Unlock()
	if zoneCached != 0 {
		t.Errorf("expected 0 SSL provisioning cache entries for cleared zone %q, got %d", zoneCR, zoneCached)
	}
	if otherCached != 1 {
		t.Errorf("expected bystander zone %q SSL provisioning cache entry to survive, got %d", otherZone, otherCached)
	}

	// The bystander zone's gauge series must be untouched.
	if n := countSeries("cf_edge_operator_customhostnames", map[string]string{labelZoneCR: otherZone}); n != 1 {
		t.Errorf("bystander zone %q series should survive clear, got %d", otherZone, n)
	}
}

// TestSetBuildInfo verifies the build-info gauge is published as a constant 1,
// labeled by the version and commit passed at startup.
func TestSetBuildInfo(t *testing.T) {
	SetBuildInfo("v1.2.3", "abc1234")
	if got := testutil.ToFloat64(buildInfoGauge.WithLabelValues("v1.2.3", "abc1234")); got != 1 {
		t.Fatalf("cf_edge_operator_build_info{version=v1.2.3,commit=abc1234} = %v, want 1", got)
	}
}

// TestMetricsRegistered asserts every custom collector is registered in the
// controller-runtime metrics registry. A collector that is defined but omitted from
// the init() MustRegister block is valid Go and reads back fine in a value test, yet
// never reaches /metrics -- exactly how the build_info gauge regressed. Registering an
// already-registered collector returns prometheus.AlreadyRegisteredError; a nil error
// means it was NOT registered (and we undo the accidental registration). The list
// mirrors the init() MustRegister block: a new metric must be added to both, or this
// test fails.
func TestMetricsRegistered(t *testing.T) {
	// Keyed by Go var name so the list can be diffed against the MustRegister block.
	collectors := map[string]prometheus.Collector{
		"operationsTotal":                    operationsTotal,
		"sslProvisioningDuration":            sslProvisioningDuration,
		"customHostnames":                    customHostnames,
		"hostnameStatusGauge":                hostnameStatusGauge,
		"zoneCustomHostnames":                zoneCustomHostnames,
		"zoneInitialized":                    zoneInitialized,
		"buildInfoGauge":                     buildInfoGauge,
		"accountInitialized":                 accountInitialized,
		"loadBalancers":                      loadBalancers,
		"loadBalancerNetworksDrift":          loadBalancerNetworksDrift,
		"loadBalancerPools":                  loadBalancerPools,
		"loadBalancerMonitors":               loadBalancerMonitors,
		"loadBalancerPoolHealth":             loadBalancerPoolHealth,
		"loadBalancerPoolHealthRegion":       loadBalancerPoolHealthRegion,
		"loadBalancerPoolOriginHealth":       loadBalancerPoolOriginHealth,
		"loadBalancerPoolOriginHealthRegion": loadBalancerPoolOriginHealthRegion,
		"cfAPICallDuration":                  cfAPICallDuration,
		"cfAPIErrorsByCode":                  cfAPIErrorsByCode,
		"cfAPIRetriesTotal":                  cfAPIRetriesTotal,
		"driftBufferDepth":                   driftBufferDepth,
		"driftBufferOverflowTotal":           driftBufferOverflowTotal,
		"driftDetectionErrorsTotal":          driftDetectionErrorsTotal,
	}
	for name, c := range collectors {
		err := crtlmetrics.Registry.Register(c)
		if err == nil {
			// Not previously registered: undo our accidental registration, then fail.
			crtlmetrics.Registry.Unregister(c)
			t.Errorf("%s: collector is not registered (missing from init() MustRegister -> absent from /metrics)", name)
			continue
		}
		if _, ok := errors.AsType[prometheus.AlreadyRegisteredError](err); !ok {
			t.Errorf("%s: unexpected registry error: %v", name, err)
		}
	}
}
