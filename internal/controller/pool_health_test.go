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
	"maps"
	"testing"
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/cloudflare/cloudflare-go/v6/option"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

// poolHealthFixture is a raw pool-health GET result: three reported regions
// (WNAM healthy, ENAM unhealthy, WEU with no region-level healthy flag -> unknown)
// across two origins. 2.2.2.2 is absent from WEU's origins -> unknown there.
const poolHealthFixture = `{
  "pool_id": "pool-1",
  "pop_health": {
    "WNAM": {
      "healthy": true,
      "origins": [
        {"1.1.1.1": {"healthy": true, "rtt": "12ms", "failure_reason": "No failures", "response_code": 200}},
        {"2.2.2.2": {"healthy": false, "failure_reason": "timeout"}}
      ]
    },
    "ENAM": {
      "healthy": false,
      "origins": [
        {"1.1.1.1": {"healthy": true}},
        {"2.2.2.2": {"healthy": false}}
      ]
    },
    "WEU": {
      "origins": [
        {"1.1.1.1": {"healthy": true}}
      ]
    }
  }
}`

// --- registry gather helpers (side-effect free, unlike testutil.ToFloat64) ---

func metricLabelsMatch(got []*dto.LabelPair, want map[string]string) bool {
	m := make(map[string]string, len(got))
	for _, l := range got {
		m[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if m[k] != v {
			return false
		}
	}
	return true
}

// gaugeVal returns the value of the single gauge series matching want, and
// whether such a series exists. Gathering (rather than testutil.ToFloat64) means
// a missing series stays missing instead of being recreated at 0.
func gaugeVal(t *testing.T, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := crtlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if metricLabelsMatch(m.GetLabel(), want) {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// countSeries is the *testing.T-free twin of seriesCount: it counts series of the
// named metric family matching want, so the Ginkgo integration suite (which has no
// *testing.T) can poll it from inside an Eventually. On a gather error it reports
// zero rather than failing.
func countSeries(name string, want map[string]string) int {
	families, err := crtlmetrics.Registry.Gather()
	if err != nil {
		return 0
	}
	n := 0
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if metricLabelsMatch(m.GetLabel(), want) {
				n++
			}
		}
	}
	return n
}

// gaugeValue is the *testing.T-free twin of gaugeVal: it returns the value of the
// single gauge series matching want (0 if none), so the Ginkgo integration suite
// can poll a published gauge from inside an Eventually.
func gaugeValue(name string, want map[string]string) float64 {
	families, err := crtlmetrics.Registry.Gather()
	if err != nil {
		return 0
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if metricLabelsMatch(m.GetLabel(), want) {
				return m.GetGauge().GetValue()
			}
		}
	}
	return 0
}

// seriesCount counts series of the named metric family matching want.
func seriesCount(t *testing.T, name string, want map[string]string) int {
	t.Helper()
	families, err := crtlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	n := 0
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if metricLabelsMatch(m.GetLabel(), want) {
				n++
			}
		}
	}
	return n
}

// counterVal returns the value of the single counter series matching want.
func counterVal(t *testing.T, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := crtlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if metricLabelsMatch(m.GetLabel(), want) {
				return m.GetCounter().GetValue(), true
			}
		}
	}
	return 0, false
}

func wantGauge(t *testing.T, name string, labels map[string]string, want float64) {
	t.Helper()
	got, ok := gaugeVal(t, name, labels)
	if !ok {
		t.Fatalf("%s%v: series missing, want %v", name, labels, want)
	}
	if got != want {
		t.Errorf("%s%v = %v, want %v", name, labels, got, want)
	}
}

// --- decode + tally ---

func TestDecodeAndTallyPoolHealth(t *testing.T) {
	h, err := decodePoolHealth([]byte(poolHealthFixture))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(h.PopHealth) != 3 {
		t.Fatalf("pop_health regions = %d, want 3", len(h.PopHealth))
	}

	// Without checkRegions: pool + origin counts over the reported regions only;
	// no per-region breakdown.
	tally := tallyPoolHealth(h, nil)
	if tally.regionStatus != nil || tally.originRegionStatus != nil {
		t.Errorf("per-region maps must be nil when checkRegions unset")
	}
	if len(tally.regions) != 0 {
		t.Errorf("regions = %v, want empty when checkRegions unset", tally.regions)
	}
	// WNAM healthy, ENAM unhealthy, WEU unknown (no region healthy flag).
	if got := tally.poolStatusCounts; got[poolHealthStatusHealthy] != 1 || got[poolHealthStatusUnhealthy] != 1 || got[poolHealthStatusUnknown] != 1 {
		t.Errorf("poolStatusCounts = %v, want 1/1/1", got)
	}
	// 1.1.1.1 healthy in all 3 regions.
	if got := tally.originStatusCounts["1.1.1.1"]; got[poolHealthStatusHealthy] != 3 || got[poolHealthStatusUnhealthy] != 0 || got[poolHealthStatusUnknown] != 0 {
		t.Errorf("origin 1.1.1.1 counts = %v, want 3/0/0", got)
	}
	// 2.2.2.2 unhealthy in WNAM+ENAM, unknown in WEU (absent from its origins).
	if got := tally.originStatusCounts["2.2.2.2"]; got[poolHealthStatusHealthy] != 0 || got[poolHealthStatusUnhealthy] != 2 || got[poolHealthStatusUnknown] != 1 {
		t.Errorf("origin 2.2.2.2 counts = %v, want 0/2/1", got)
	}

	// With checkRegions: declared regions drive the per-region breakdown; a
	// declared region not in the response (SEAS) is unknown.
	tally = tallyPoolHealth(h, []string{"WNAM", "ENAM", "SEAS"})
	if tally.regionStatus["WNAM"] != poolHealthStatusHealthy {
		t.Errorf("region WNAM = %q, want healthy", tally.regionStatus["WNAM"])
	}
	if tally.regionStatus["ENAM"] != poolHealthStatusUnhealthy {
		t.Errorf("region ENAM = %q, want unhealthy", tally.regionStatus["ENAM"])
	}
	if tally.regionStatus["SEAS"] != poolHealthStatusUnknown {
		t.Errorf("region SEAS = %q, want unknown (declared, not reported)", tally.regionStatus["SEAS"])
	}
	if got := tally.originRegionStatus["2.2.2.2"]; got["WNAM"] != poolHealthStatusUnhealthy || got["SEAS"] != poolHealthStatusUnknown {
		t.Errorf("origin 2.2.2.2 per-region = %v, want WNAM unhealthy / SEAS unknown", got)
	}
	if got := tally.originRegionStatus["1.1.1.1"]; got["WNAM"] != poolHealthStatusHealthy || got["SEAS"] != poolHealthStatusUnknown {
		t.Errorf("origin 1.1.1.1 per-region = %v, want WNAM healthy / SEAS unknown", got)
	}
}

// --- gauge publish (all four gauges, checkRegions-set) ---

func TestPoolHealthGaugesPublish(t *testing.T) {
	const acct, pool = "acct-pub", "pool-pub"
	g := &poolHealthGauges{prev: map[string]poolHealthPrev{}}
	h, err := decodePoolHealth([]byte(poolHealthFixture))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	g.publish(acct, pool, tallyPoolHealth(h, []string{"WNAM", "ENAM", "SEAS"}))

	base := map[string]string{"account_cr": acct, "pool_cr": pool}
	// #1 pool region counts.
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health", merge(base, "status", "healthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health", merge(base, "status", "unhealthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health", merge(base, "status", "unknown"), 1)
	// #3 origin region counts.
	wantGauge(t, "cf_edge_operator_loadbalancerpool_origin_health", merge(base, "origin", "1.1.1.1", "status", "healthy"), 3)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_origin_health", merge(base, "origin", "2.2.2.2", "status", "unhealthy"), 2)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_origin_health", merge(base, "origin", "2.2.2.2", "status", "unknown"), 1)
	// #2 per-region: one status holds 1, others 0 (sum per region == 1).
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "WNAM", "status", "healthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "WNAM", "status", "unhealthy"), 0)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "SEAS", "status", "unknown"), 1)
	// #4 per-origin-per-region.
	wantGauge(t, "cf_edge_operator_loadbalancerpool_origin_health_region", merge(base, "origin", "2.2.2.2", "region", "WNAM", "status", "unhealthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_origin_health_region", merge(base, "origin", "1.1.1.1", "region", "ENAM", "status", "healthy"), 1)

	// cleanup so the shared registry does not carry these series into other tests.
	g.prune(map[string]bool{})
}

// --- per-region gauges only emitted when checkRegions set ---

func TestPoolHealthPerRegionOnlyWhenCheckRegionsSet(t *testing.T) {
	const acct, pool = "acct-noreg", "pool-noreg"
	g := &poolHealthGauges{prev: map[string]poolHealthPrev{}}
	h, err := decodePoolHealth([]byte(poolHealthFixture))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	g.publish(acct, pool, tallyPoolHealth(h, nil)) // checkRegions unset

	base := map[string]string{"account_cr": acct, "pool_cr": pool}
	// Summarized gauges present.
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_health", base); n == 0 {
		t.Errorf("expected summarized pool health series, got none")
	}
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_origin_health", base); n == 0 {
		t.Errorf("expected summarized origin health series, got none")
	}
	// Per-region gauges absent.
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_health_region", base); n != 0 {
		t.Errorf("per-region gauge emitted for checkRegions-unset pool: %d series", n)
	}
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_origin_health_region", base); n != 0 {
		t.Errorf("per-origin-per-region gauge emitted for checkRegions-unset pool: %d series", n)
	}

	g.prune(map[string]bool{})
}

// --- stale cleanup on origin removal ---

func TestPoolHealthStaleCleanupOnOriginRemoval(t *testing.T) {
	const acct, pool = "acct-stale", "pool-stale"
	g := &poolHealthGauges{prev: map[string]poolHealthPrev{}}

	twoOrigins := `{"pool_id":"p","pop_health":{"WNAM":{"healthy":true,"origins":[
		{"1.1.1.1":{"healthy":true}},{"2.2.2.2":{"healthy":true}}]}}}`
	oneOrigin := `{"pool_id":"p","pop_health":{"WNAM":{"healthy":true,"origins":[
		{"1.1.1.1":{"healthy":true}}]}}}`

	h2, _ := decodePoolHealth([]byte(twoOrigins))
	g.publish(acct, pool, tallyPoolHealth(h2, []string{"WNAM"}))
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_origin_health", map[string]string{"account_cr": acct, "pool_cr": pool, "origin": "2.2.2.2"}); n == 0 {
		t.Fatalf("origin 2.2.2.2 series should exist after first publish")
	}

	h1, _ := decodePoolHealth([]byte(oneOrigin))
	g.publish(acct, pool, tallyPoolHealth(h1, []string{"WNAM"}))
	// 2.2.2.2 removed -> its series in #3 and #4 gone.
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_origin_health", map[string]string{"account_cr": acct, "pool_cr": pool, "origin": "2.2.2.2"}); n != 0 {
		t.Errorf("origin 2.2.2.2 #3 series not cleaned: %d remain", n)
	}
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_origin_health_region", map[string]string{"account_cr": acct, "pool_cr": pool, "origin": "2.2.2.2"}); n != 0 {
		t.Errorf("origin 2.2.2.2 #4 series not cleaned: %d remain", n)
	}
	// 1.1.1.1 still present.
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_origin_health", map[string]string{"account_cr": acct, "pool_cr": pool, "origin": "1.1.1.1"}); n == 0 {
		t.Errorf("origin 1.1.1.1 series should remain")
	}

	g.prune(map[string]bool{})
}

// --- prune on pool delete ---

func TestPoolHealthPruneOnDelete(t *testing.T) {
	const acct, pool = "acct-del", "pool-del"
	g := &poolHealthGauges{prev: map[string]poolHealthPrev{}}
	h, _ := decodePoolHealth([]byte(poolHealthFixture))
	g.publish(acct, pool, tallyPoolHealth(h, []string{"WNAM", "ENAM"}))

	base := map[string]string{"account_cr": acct, "pool_cr": pool}
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_health", base); n == 0 {
		t.Fatalf("expected series before prune")
	}
	// Prune with the pool absent from liveKeys (CR deleted).
	g.prune(map[string]bool{})
	for _, name := range []string{
		"cf_edge_operator_loadbalancerpool_health",
		"cf_edge_operator_loadbalancerpool_health_region",
		"cf_edge_operator_loadbalancerpool_origin_health",
		"cf_edge_operator_loadbalancerpool_origin_health_region",
	} {
		if n := seriesCount(t, name, base); n != 0 {
			t.Errorf("%s: %d series remain after delete-prune, want 0", name, n)
		}
	}
	// A live pool key must be preserved by prune.
	g.publish(acct, pool, tallyPoolHealth(h, []string{"WNAM"}))
	g.prune(map[string]bool{poolHealthKey(acct, pool): true})
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_health", base); n == 0 {
		t.Errorf("live pool series pruned in error")
	}
	g.prune(map[string]bool{})
}

// --- end-to-end poll through the SDK (proves RawJSON decode of the mis-flattened
// pop_health map) ---

func newPoolHealthReconciler(baseURL string, enable bool) *LoadBalancerPoolReconciler {
	return &LoadBalancerPoolReconciler{
		CFAPITimeout:     5 * time.Second,
		CFAPIMaxRetries:  0,
		CFBaseURL:        baseURL,
		EnablePoolHealth: enable,
	}
}

func newMockAccountInfo(baseURL string) *accountInfo {
	return &accountInfo{
		Client: cloudflare.NewClient(
			option.WithAPIToken("test-token"),
			option.WithMaxRetries(0),
			option.WithBaseURL(baseURL),
		),
		AccountID: lbAccountID,
		AccountCR: "acct-e2e",
	}
}

func TestPollPoolHealthThroughSDK(t *testing.T) {
	mock := newLBMockServer()
	defer mock.Close()

	const cfID = "cf-pool-e2e"
	mock.seedPoolHealth(cfID, map[string]any{
		"pool_id": cfID,
		"pop_health": map[string]any{
			"WNAM": map[string]any{
				"healthy": true,
				"origins": []any{
					map[string]any{"1.1.1.1": map[string]any{"healthy": true}},
				},
			},
			"ENAM": map[string]any{
				"healthy": false,
				"origins": []any{
					map[string]any{"1.1.1.1": map[string]any{"healthy": false}},
				},
			},
		},
	})

	r := newPoolHealthReconciler(mock.URL(), true)
	ai := newMockAccountInfo(mock.URL())
	pool := &lbv1beta1.LoadBalancerPool{
		Spec:   lbv1beta1.LoadBalancerPoolSpec{AccountRef: lbv1beta1.AccountRef{Name: "acct-e2e"}, CheckRegions: []string{"WNAM", "ENAM"}},
		Status: lbv1beta1.LoadBalancerPoolStatus{ID: cfID},
	}
	pool.Name = "pool-e2e"

	r.maybePollPoolHealth(context.Background(), ai, pool)

	if got := mock.poolHealthGets(); got != 1 {
		t.Fatalf("health GETs = %d, want 1", got)
	}
	base := map[string]string{"account_cr": "acct-e2e", "pool_cr": "pool-e2e"}
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health", merge(base, "status", "healthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health", merge(base, "status", "unhealthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "WNAM", "status", "healthy"), 1)
	wantGauge(t, "cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "ENAM", "status", "unhealthy"), 1)

	poolHealthGaugeSet.prune(map[string]bool{})
}

// --- off path: no call, no series ---

func TestPoolHealthOffPath(t *testing.T) {
	mock := newLBMockServer()
	defer mock.Close()
	const cfID = "cf-pool-off"
	mock.seedPoolHealth(cfID, map[string]any{"pool_id": cfID, "pop_health": map[string]any{
		"WNAM": map[string]any{"healthy": true, "origins": []any{}}}})

	ai := newMockAccountInfo(mock.URL())
	base := map[string]string{"account_cr": "acct-off", "pool_cr": "pool-off"}
	pool := &lbv1beta1.LoadBalancerPool{
		Spec:   lbv1beta1.LoadBalancerPoolSpec{AccountRef: lbv1beta1.AccountRef{Name: "acct-off"}},
		Status: lbv1beta1.LoadBalancerPoolStatus{ID: cfID},
	}
	pool.Name = "pool-off"

	// Flag off -> no call, no series.
	newPoolHealthReconciler(mock.URL(), false).maybePollPoolHealth(context.Background(), ai, pool)
	if got := mock.poolHealthGets(); got != 0 {
		t.Errorf("off path made %d health GETs, want 0", got)
	}
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_health", base); n != 0 {
		t.Errorf("off path emitted %d health series, want 0", n)
	}

	// Flag on but no CF ID yet -> nothing to poll.
	poolNoID := pool.DeepCopy()
	poolNoID.Status.ID = ""
	newPoolHealthReconciler(mock.URL(), true).maybePollPoolHealth(context.Background(), ai, poolNoID)
	if got := mock.poolHealthGets(); got != 0 {
		t.Errorf("no-ID poll made %d health GETs, want 0", got)
	}

	// Flag on with a CF ID -> exactly one call.
	newPoolHealthReconciler(mock.URL(), true).maybePollPoolHealth(context.Background(), ai, pool)
	if got := mock.poolHealthGets(); got != 1 {
		t.Errorf("on path made %d health GETs, want 1", got)
	}

	poolHealthGaugeSet.prune(map[string]bool{})
}

// --- poll-failure isolation ---

func TestPollPoolHealthFailureIsolation(t *testing.T) {
	mock := newLBMockServer()
	defer mock.Close()
	const cfID = "cf-pool-fail"
	mock.setPoolHealthFail(cfID, 500)

	r := newPoolHealthReconciler(mock.URL(), true)
	ai := newMockAccountInfo(mock.URL())
	pool := &lbv1beta1.LoadBalancerPool{
		Spec:   lbv1beta1.LoadBalancerPoolSpec{AccountRef: lbv1beta1.AccountRef{Name: "acct-fail"}},
		Status: lbv1beta1.LoadBalancerPoolStatus{ID: cfID},
	}
	pool.Name = "pool-fail"
	// Pre-set a Ready condition + error counter so we can prove the poll leaves
	// the sync state untouched.
	pool.Status.ConsecutiveErrors = 0
	pool.Status.Conditions = []metav1.Condition{{
		Type: conditionReady, Status: metav1.ConditionTrue, Reason: reasonReconciled, Message: "ok",
	}}

	errLabels := map[string]string{"resource": cfResourceLoadBalancerPool, "operation": cfOpHealth, "status_code": "500"}
	before, _ := counterVal(t, "cf_edge_operator_api_errors_by_code_total", errLabels)

	r.maybePollPoolHealth(context.Background(), ai, pool)

	// The failed poll recorded an api error under operation="health"...
	after, ok := counterVal(t, "cf_edge_operator_api_errors_by_code_total", errLabels)
	if !ok || after != before+1 {
		t.Errorf("api error counter = %v (ok=%v), want %v", after, ok, before+1)
	}
	// ...made the call...
	if got := mock.poolHealthGets(); got != 1 {
		t.Errorf("health GETs = %d, want 1", got)
	}
	// ...did NOT touch the sync state...
	if pool.Status.ConsecutiveErrors != 0 {
		t.Errorf("consecutiveErrors = %d, want 0 (poll must not bump it)", pool.Status.ConsecutiveErrors)
	}
	cond := apimetaFind(pool.Status.Conditions, conditionReady)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != reasonReconciled {
		t.Errorf("Ready condition changed by failed poll: %+v", cond)
	}
	// ...and emitted no health gauge series (nothing was ever published).
	base := map[string]string{"account_cr": "acct-fail", "pool_cr": "pool-fail"}
	if n := seriesCount(t, "cf_edge_operator_loadbalancerpool_health", base); n != 0 {
		t.Errorf("failed poll emitted %d health series, want 0", n)
	}
}

func merge(base map[string]string, kv ...string) map[string]string {
	out := make(map[string]string, len(base)+len(kv)/2)
	maps.Copy(out, base)
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i]] = kv[i+1]
	}
	return out
}

func apimetaFind(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}
	return nil
}
