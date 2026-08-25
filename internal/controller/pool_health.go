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
	"encoding/json"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// Pool-health status label values for the loadBalancerPoolHealth family. A
// region (or an origin within a region) is healthy when Cloudflare reports
// healthy=true, unhealthy when it reports healthy=false, and unknown when
// Cloudflare returns no determinate health for it -- an indeterminate/absent
// healthy flag, or a CR-declared check region missing from the response.
const (
	poolHealthStatusHealthy   = "healthy"
	poolHealthStatusUnhealthy = "unhealthy"
	poolHealthStatusUnknown   = "unknown"
)

// poolHealthStatuses is the fixed status set every gauge in the pool-health
// family publishes for a polled pool, so a status that empties reads 0 rather
// than leaving a stale series (mirroring lbStateGauge's always-write-every-state
// approach). It is the status dimension for all four pool-health gauges.
var poolHealthStatuses = []string{poolHealthStatusHealthy, poolHealthStatusUnhealthy, poolHealthStatusUnknown}

// cfPoolHealth is the operator's own decode of the pool-health GET response.
//
// The cloudflare-go v6.8.0 SDK mis-flattens pop_health -- it models the field as
// a single object (PoolHealthGetResponsePOPHealth) instead of a per-region map --
// so the typed response is unusable and we decode the raw JSON ourselves. The
// shape matches the Cloudflare API: pop_health is keyed by region code; each
// region carries an overall healthy flag and an origins list whose entries are
// each a single-key map of origin address -> per-origin health. Only the healthy
// flags are consumed. They are *bool so an absent/indeterminate flag decodes to
// nil (tallied as unknown) rather than a misleading false.
type cfPoolHealth struct {
	PopHealth map[string]cfPoolHealthRegion `json:"pop_health"`
}

type cfPoolHealthRegion struct {
	Healthy *bool `json:"healthy"`
	// Origins is a list of single-key maps: origin address -> origin health.
	Origins []map[string]cfPoolHealthOrigin `json:"origins"`
}

type cfPoolHealthOrigin struct {
	Healthy *bool `json:"healthy"`
}

// decodePoolHealth parses the raw JSON of a pool-health GET result (obtained via
// the SDK response's JSON.RawJSON accessor) into cfPoolHealth.
func decodePoolHealth(raw []byte) (cfPoolHealth, error) {
	var h cfPoolHealth
	if err := json.Unmarshal(raw, &h); err != nil {
		return cfPoolHealth{}, err
	}
	return h, nil
}

// classifyHealthy maps a Cloudflare healthy flag to a pool-health status: nil
// (absent/indeterminate) -> unknown, true -> healthy, false -> unhealthy.
func classifyHealthy(h *bool) string {
	switch {
	case h == nil:
		return poolHealthStatusUnknown
	case *h:
		return poolHealthStatusHealthy
	default:
		return poolHealthStatusUnhealthy
	}
}

// poolHealthTally is the computed input for the four pool-health gauges.
//
//   - poolStatusCounts: status -> count of Cloudflare-checked regions in that
//     status (gauge #1, all polled pools).
//   - originStatusCounts: origin address -> status -> region count (gauge #3, all
//     polled pools).
//   - regionStatus / originRegionStatus: the per-region breakdowns (gauges #2/#4),
//     populated only for pools with spec.checkRegions set (nil otherwise).
//   - origins / regions: the label sets emitted this cycle, tracked so
//     poolHealthGaugeSet can clean up regions/origins that later vanish.
type poolHealthTally struct {
	poolStatusCounts   map[string]int
	originStatusCounts map[string]map[string]int
	regionStatus       map[string]string
	originRegionStatus map[string]map[string]string
	origins            []string
	regions            []string
}

// tallyPoolHealth reduces a decoded pool-health response into the gauge inputs.
//
// The pool- and origin-level counts (gauges #1/#3) are computed over the regions
// Cloudflare actually reported (the pop_health keys), so their per-status sum is
// the number of regions checked. The per-region breakdowns (gauges #2/#4) are
// computed over the CR-declared checkRegions, which is the bounded, coherent
// region dimension: a declared region absent from the response (or with an
// indeterminate flag) is unknown. tallyPoolHealth READS checkRegions only to decide
// whether to emit the per-region gauges and to enumerate their regions; the pool
// controller sends check_regions to Cloudflare on the follow-up edit, so once a pool
// has reconciled Cloudflare probes exactly the CR-declared regions.
func tallyPoolHealth(h cfPoolHealth, checkRegions []string) poolHealthTally {
	// Collect the origin address set and a per-region origin -> healthy lookup.
	originSet := map[string]struct{}{}
	regionOrigins := make(map[string]map[string]*bool, len(h.PopHealth))
	for region, rh := range h.PopHealth {
		om := map[string]*bool{}
		for _, entry := range rh.Origins {
			for addr, oh := range entry {
				om[addr] = oh.Healthy
				originSet[addr] = struct{}{}
			}
		}
		regionOrigins[region] = om
	}

	// Gauge #1: pool status counts over the reported regions.
	poolCounts := newStatusCounts()
	for _, rh := range h.PopHealth {
		poolCounts[classifyHealthy(rh.Healthy)]++
	}

	// Gauge #3: per-origin status counts over the reported regions. An origin
	// absent from a region's origins list is unknown for that region, so each
	// origin's per-status sum also equals the number of regions checked.
	origins := make([]string, 0, len(originSet))
	originCounts := make(map[string]map[string]int, len(originSet))
	for addr := range originSet {
		origins = append(origins, addr)
		c := newStatusCounts()
		for region := range h.PopHealth {
			oh, ok := regionOrigins[region][addr]
			if !ok {
				c[poolHealthStatusUnknown]++
				continue
			}
			c[classifyHealthy(oh)]++
		}
		originCounts[addr] = c
	}

	tally := poolHealthTally{
		poolStatusCounts:   poolCounts,
		originStatusCounts: originCounts,
		origins:            origins,
	}

	// Gauges #2/#4: per-region breakdowns, only for checkRegions-set pools.
	if len(checkRegions) > 0 {
		tally.regions = append([]string(nil), checkRegions...)
		regionStatus := make(map[string]string, len(checkRegions))
		originRegionStatus := make(map[string]map[string]string, len(origins))
		for _, addr := range origins {
			originRegionStatus[addr] = make(map[string]string, len(checkRegions))
		}
		for _, region := range checkRegions {
			rh, present := h.PopHealth[region]
			if !present {
				// Declared region not reported by Cloudflare -> unknown for the
				// region and for every origin in it.
				regionStatus[region] = poolHealthStatusUnknown
				for _, addr := range origins {
					originRegionStatus[addr][region] = poolHealthStatusUnknown
				}
				continue
			}
			regionStatus[region] = classifyHealthy(rh.Healthy)
			for _, addr := range origins {
				oh, ok := regionOrigins[region][addr]
				if !ok {
					originRegionStatus[addr][region] = poolHealthStatusUnknown
					continue
				}
				originRegionStatus[addr][region] = classifyHealthy(oh)
			}
		}
		tally.regionStatus = regionStatus
		tally.originRegionStatus = originRegionStatus
	}

	return tally
}

func newStatusCounts() map[string]int {
	return map[string]int{
		poolHealthStatusHealthy:   0,
		poolHealthStatusUnhealthy: 0,
		poolHealthStatusUnknown:   0,
	}
}

// poolHealthGauges owns the four pool-health gauge vectors and the bookkeeping to
// publish a pool's tally in place and drop series for regions/origins/pools that
// vanish. It is the pool-health analog of lbStateGauge.
//
// Every pool-health gauge is written by exactly one controller (the pool
// reconciler), whose reconciles are serialized (MaxConcurrentReconciles defaults
// to 1), so the only concurrent access is the /metrics scrape goroutine, which
// never touches prev. The mutex guards prev for defense in depth and documents
// the shared state; the Set/DeletePartialMatch calls are individually
// scrape-safe. Series are only ever written here, and this is only reached under
// --enable-pool-health, so the off path emits nothing.
type poolHealthGauges struct {
	mu sync.Mutex
	// prev tracks, per pool key (account_cr\x00pool_cr), the origin and region
	// label sets last published, so the next publish (or a prune) can remove the
	// series for any that disappeared.
	prev map[string]poolHealthPrev
}

type poolHealthPrev struct {
	origins map[string]bool
	regions map[string]bool
}

// poolHealthGaugeSet is the process-wide pool-health gauge manager.
var poolHealthGaugeSet = &poolHealthGauges{prev: map[string]poolHealthPrev{}}

// poolHealthKey joins the owner labels into the map key used for prev tracking.
// The NUL separator cannot appear in a Kubernetes object name, so the join is
// unambiguous.
func poolHealthKey(accountCR, poolCR string) string {
	return accountCR + "\x00" + poolCR
}

// publish writes a pool's tally into the four gauges and removes series for any
// origin or region that was published on the previous cycle but is absent now
// (origins removed from the pool, checkRegions narrowed or cleared). Status
// transitions need no cleanup: every status series for a present region/origin is
// (re)written each cycle, so a stale status is overwritten to 0.
func (g *poolHealthGauges) publish(accountCR, poolCR string, t poolHealthTally) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Gauge #1: pool region counts (always all three statuses).
	for _, s := range poolHealthStatuses {
		loadBalancerPoolHealth.WithLabelValues(accountCR, poolCR, s).Set(float64(t.poolStatusCounts[s]))
	}
	// Gauge #3: per-origin region counts.
	for addr, counts := range t.originStatusCounts {
		for _, s := range poolHealthStatuses {
			loadBalancerPoolOriginHealth.WithLabelValues(accountCR, poolCR, addr, s).Set(float64(counts[s]))
		}
	}
	// Gauge #2: per-region status (1 for the holding status, 0 for the rest ->
	// sum per region == 1). Empty for checkRegions-unset pools.
	for region, status := range t.regionStatus {
		for _, s := range poolHealthStatuses {
			loadBalancerPoolHealthRegion.WithLabelValues(accountCR, poolCR, region, s).Set(boolToFloat(s == status))
		}
	}
	// Gauge #4: per-origin-per-region status. Empty for checkRegions-unset pools.
	for addr, byRegion := range t.originRegionStatus {
		for region, status := range byRegion {
			for _, s := range poolHealthStatuses {
				loadBalancerPoolOriginHealthRegion.WithLabelValues(accountCR, poolCR, addr, region, s).Set(boolToFloat(s == status))
			}
		}
	}

	newOrigins := sliceToBoolSet(t.origins)
	newRegions := sliceToBoolSet(t.regions)
	key := poolHealthKey(accountCR, poolCR)
	if p, ok := g.prev[key]; ok {
		for addr := range p.origins {
			if !newOrigins[addr] {
				loadBalancerPoolOriginHealth.DeletePartialMatch(prometheus.Labels{labelAccountCR: accountCR, labelPoolCR: poolCR, labelOrigin: addr})
				loadBalancerPoolOriginHealthRegion.DeletePartialMatch(prometheus.Labels{labelAccountCR: accountCR, labelPoolCR: poolCR, labelOrigin: addr})
			}
		}
		for region := range p.regions {
			if !newRegions[region] {
				loadBalancerPoolHealthRegion.DeletePartialMatch(prometheus.Labels{labelAccountCR: accountCR, labelPoolCR: poolCR, labelRegion: region})
				loadBalancerPoolOriginHealthRegion.DeletePartialMatch(prometheus.Labels{labelAccountCR: accountCR, labelPoolCR: poolCR, labelRegion: region})
			}
		}
	}
	g.prev[key] = poolHealthPrev{origins: newOrigins, regions: newRegions}
}

// prune drops all four gauges' series for every previously-published pool whose
// key is not in liveKeys -- i.e. pools whose CR has been deleted. It is called
// (under the flag) from the pool controller's deferred state-gauge recompute,
// which already lists every pool CR, so a deleted pool's health series are cleaned
// on the same reconcile that cleans its sync-state series. This mirrors
// lbStateGauge's vanished-owner cleanup.
func (g *poolHealthGauges) prune(liveKeys map[string]bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for key := range g.prev {
		if liveKeys[key] {
			continue
		}
		accountCR, poolCR := splitPoolHealthKey(key)
		labels := prometheus.Labels{labelAccountCR: accountCR, labelPoolCR: poolCR}
		loadBalancerPoolHealth.DeletePartialMatch(labels)
		loadBalancerPoolHealthRegion.DeletePartialMatch(labels)
		loadBalancerPoolOriginHealth.DeletePartialMatch(labels)
		loadBalancerPoolOriginHealthRegion.DeletePartialMatch(labels)
		delete(g.prev, key)
	}
}

func splitPoolHealthKey(key string) (accountCR, poolCR string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '\x00' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

func sliceToBoolSet(s []string) map[string]bool {
	m := make(map[string]bool, len(s))
	for _, v := range s {
		m[v] = true
	}
	return m
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
