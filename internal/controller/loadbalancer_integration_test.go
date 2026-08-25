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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	accountsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/accounts/v1beta1"
	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	lbv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
)

// lbMockServer is an in-memory Cloudflare Load Balancing API mock. It is kept
// independent from cfMockServer (the custom-hostname mock) to avoid coupling:
// the two suites exercise disjoint CF endpoints. State is protected by a mutex
// and keyed by CF id; ids are handed out from a single monotonic counter.
type lbMockServer struct {
	mu        sync.Mutex
	accountID string
	zoneID    string
	zoneName  string
	monitors  map[string]mockMonitor // keyed by CF id
	pools     map[string]mockPool    // keyed by CF id
	lbs       map[string]mockLB      // keyed by CF id
	idCounter int
	server    *httptest.Server

	// failMonitorCreateOnce, when set, makes the next monitor create store the
	// record server-side but respond 500 (once), simulating a timed-out-but-
	// -succeeded create. Exercises cfCreateGuarded's adopt-on-retry path.
	failMonitorCreateOnce bool

	// accountStatusOverride maps an account ID to a forced HTTP status for
	// handleAccountGet: a value >= 400 responds with that error, a value < 400
	// (e.g. 200) is treated as a valid account. Used by the account_initialized
	// sticky tests to simulate transient (5xx) vs definitive (403) validation
	// failures against an otherwise-valid account, isolated from the shared
	// account ID.
	accountStatusOverride map[string]int

	// lbUpdateCount counts LoadBalancer PUT (update) calls. Used by the TTL
	// regression test to assert an LB does not drift-loop (repeated PUTs) after
	// it has settled.
	lbUpdateCount int

	// poolHealth maps a pool CF id to the pool-health GET result object (pool_id +
	// pop_health), served RawJSON-shaped so the operator decodes the region map the
	// v6.8.0 SDK mis-flattens. A pool with no seeded entry responds with an empty
	// pop_health.
	poolHealth map[string]map[string]any
	// poolHealthFail forces handlePoolHealth to respond with the given HTTP status
	// (>= 400) for a pool id, exercising poll-failure isolation.
	poolHealthFail map[string]int
	// poolHealthGetCount counts pool-health GET calls, used to prove the off path
	// makes no health call.
	poolHealthGetCount int
}

// mockMonitor mirrors the CF monitor fields the reconciler reads back. JSON
// tags are snake_case to match the CF API on both decode (request body) and
// encode (response), so create/update round-trips echo what was submitted.
type mockMonitor struct {
	ID              string              `json:"id"`
	Type            string              `json:"type"`
	Method          string              `json:"method"`
	Path            string              `json:"path"`
	Port            int64               `json:"port"`
	Header          map[string][]string `json:"header"`
	ExpectedCodes   string              `json:"expected_codes"`
	ExpectedBody    string              `json:"expected_body"`
	FollowRedirects bool                `json:"follow_redirects"`
	AllowInsecure   bool                `json:"allow_insecure"`
	Interval        int64               `json:"interval"`
	Retries         int64               `json:"retries"`
	Timeout         int64               `json:"timeout"`
	ConsecutiveUp   int64               `json:"consecutive_up"`
	ConsecutiveDown int64               `json:"consecutive_down"`
	ProbeZone       string              `json:"probe_zone"`
	Description     string              `json:"description"`
}

type mockPool struct {
	ID                string       `json:"id"`
	Name              string       `json:"name"`
	Enabled           bool         `json:"enabled"`
	Monitor           string       `json:"monitor"`
	MinimumOrigins    int64        `json:"minimum_origins"`
	NotificationEmail string       `json:"notification_email"`
	Description       string       `json:"description"`
	Latitude          float64      `json:"latitude"`
	Longitude         float64      `json:"longitude"`
	Origins           []mockOrigin `json:"origins"`
	// CheckRegions round-trips the edit-only check_regions field so a pool that
	// declares spec.checkRegions converges instead of drift-looping: the operator
	// rejects it on create and sends it via a follow-up edit (see
	// buildPoolEditParams), which handlePoolUpdate must then echo back.
	CheckRegions []string `json:"check_regions,omitempty"`
	// LoadShedding is an example of a Cloudflare-side field the operator does
	// NOT model. Under PATCH (partial edit) it must survive an operator update
	// that touches only the modeled fields. The operator never sends it.
	LoadShedding string `json:"load_shedding,omitempty"`
}

type mockOrigin struct {
	Name    string           `json:"name"`
	Address string           `json:"address"`
	Enabled bool             `json:"enabled"`
	Weight  float64          `json:"weight"`
	Header  mockOriginHeader `json:"header"`
}

type mockOriginHeader struct {
	Host []string `json:"Host"`
}

type mockLB struct {
	ID              string              `json:"id"`
	Name            string              `json:"name"`
	DefaultPools    []string            `json:"default_pools"`
	FallbackPool    string              `json:"fallback_pool"`
	Proxied         bool                `json:"proxied"`
	Enabled         bool                `json:"enabled"`
	SteeringPolicy  string              `json:"steering_policy"`
	SessionAffinity string              `json:"session_affinity"`
	TTL             float64             `json:"ttl"`
	Description     string              `json:"description"`
	Networks        []string            `json:"networks"`
	RegionPools     map[string][]string `json:"region_pools"`
	CountryPools    map[string][]string `json:"country_pools"`
	PopPools        map[string][]string `json:"pop_pools"`
	// Optional nested features. Pointers with omitempty so an LB that never
	// expresses them round-trips as absent (the SDK then decodes CF zero-values,
	// mirroring real Cloudflare's "unset" reply).
	AdaptiveRouting           *mockAdaptiveRouting           `json:"adaptive_routing,omitempty"`
	LocationStrategy          *mockLocationStrategy          `json:"location_strategy,omitempty"`
	RandomSteering            *mockRandomSteering            `json:"random_steering,omitempty"`
	SessionAffinityAttributes *mockSessionAffinityAttributes `json:"session_affinity_attributes,omitempty"`
	SessionAffinityTTL        float64                        `json:"session_affinity_ttl,omitempty"`
	// Rules is an example of a Cloudflare-side field the operator does NOT model.
	// Under PATCH (partial edit) it must survive an operator update that touches
	// only the modeled fields. The operator never sends it.
	Rules json.RawMessage `json:"rules,omitempty"`
}

type mockAdaptiveRouting struct {
	FailoverAcrossPools bool `json:"failover_across_pools"`
}

type mockLocationStrategy struct {
	Mode      string `json:"mode,omitempty"`
	PreferECS string `json:"prefer_ecs,omitempty"`
}

type mockRandomSteering struct {
	DefaultWeight float64            `json:"default_weight,omitempty"`
	PoolWeights   map[string]float64 `json:"pool_weights,omitempty"`
}

type mockSessionAffinityAttributes struct {
	DrainDuration        float64  `json:"drain_duration,omitempty"`
	Headers              []string `json:"headers,omitempty"`
	RequireAllHeaders    *bool    `json:"require_all_headers,omitempty"`
	Samesite             string   `json:"samesite,omitempty"`
	Secure               string   `json:"secure,omitempty"`
	ZeroDowntimeFailover string   `json:"zero_downtime_failover,omitempty"`
}

// newLBMockServer builds the load-balancing API mock configured for the shared
// test account and zone (every caller uses the same fixtures, so these are taken
// from the package constants rather than parameters).
func newLBMockServer() *lbMockServer {
	m := &lbMockServer{
		accountID:             lbAccountID,
		zoneID:                lbZoneID,
		zoneName:              lbZoneName,
		monitors:              make(map[string]mockMonitor),
		pools:                 make(map[string]mockPool),
		lbs:                   make(map[string]mockLB),
		accountStatusOverride: make(map[string]int),
		poolHealth:            make(map[string]map[string]any),
		poolHealthFail:        make(map[string]int),
	}
	mux := http.NewServeMux()

	// Account validation.
	mux.HandleFunc("GET /accounts/{accountID}", m.handleAccountGet)

	// Monitors (account-scoped). Update is PATCH (partial edit), matching the
	// SDK's Monitors.Edit.
	mux.HandleFunc("GET /accounts/{accountID}/load_balancers/monitors", m.handleMonitorList)
	mux.HandleFunc("POST /accounts/{accountID}/load_balancers/monitors", m.handleMonitorCreate)
	mux.HandleFunc("PATCH /accounts/{accountID}/load_balancers/monitors/{id}", m.handleMonitorUpdate)
	mux.HandleFunc("DELETE /accounts/{accountID}/load_balancers/monitors/{id}", m.handleMonitorDelete)

	// Pools (account-scoped). Update is PATCH (partial edit).
	mux.HandleFunc("GET /accounts/{accountID}/load_balancers/pools", m.handlePoolList)
	mux.HandleFunc("POST /accounts/{accountID}/load_balancers/pools", m.handlePoolCreate)
	mux.HandleFunc("PATCH /accounts/{accountID}/load_balancers/pools/{id}", m.handlePoolUpdate)
	mux.HandleFunc("DELETE /accounts/{accountID}/load_balancers/pools/{id}", m.handlePoolDelete)
	mux.HandleFunc("GET /accounts/{accountID}/load_balancers/pools/{id}/health", m.handlePoolHealth)

	// Load balancers (zone-scoped). Update is PATCH (partial edit).
	mux.HandleFunc("GET /zones/{zoneID}/load_balancers", m.handleLBList)
	mux.HandleFunc("POST /zones/{zoneID}/load_balancers", m.handleLBCreate)
	mux.HandleFunc("PATCH /zones/{zoneID}/load_balancers/{id}", m.handleLBUpdate)
	mux.HandleFunc("DELETE /zones/{zoneID}/load_balancers/{id}", m.handleLBDelete)

	m.server = httptest.NewServer(mux)
	return m
}

func (m *lbMockServer) URL() string { return m.server.URL }
func (m *lbMockServer) Close()      { m.server.Close() }

func (m *lbMockServer) nextID(prefix string) string {
	m.idCounter++
	return fmt.Sprintf("mock-%s-id-%04d", prefix, m.idCounter)
}

// --- response envelope helpers ---

func writeResult(w http.ResponseWriter, code int, result any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": result})
}

func writeList(w http.ResponseWriter, results any, n int) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success":     true,
		"result":      results,
		"result_info": map[string]any{"page": 1, "per_page": 100, "count": n, "total_count": n},
	})
}

func writeCFError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": false,
		"errors":  []map[string]any{{"code": code, "message": msg}},
	})
}

// applyPatch models Cloudflare's PATCH (partial edit) faithfully: JSON objects
// (top-level and nested) are deep-merged key-by-key, a key whose patch value is
// null is REMOVED, and arrays/scalars replace wholesale. This mirrors real
// Cloudflare -- map properties (region_pools / country_pools / pop_pools, monitor
// header, random_steering.pool_weights) and nested objects (load_shedding, etc.)
// are deep-merged, so a dropped key lingers unless the operator sends an explicit
// null. Modeling this (rather than a wholesale top-level replace) is what lets the
// mock catch removal-by-omission drift-loop bugs instead of masking them. A field
// the patch body omits survives untouched (PATCH != PUT). existing and out are the
// same mock record type.
func applyPatch(existing any, body []byte, out any) error {
	base, err := json.Marshal(existing)
	if err != nil {
		return err
	}
	var merged map[string]json.RawMessage
	if err := json.Unmarshal(base, &merged); err != nil {
		return err
	}
	var patch map[string]json.RawMessage
	if err := json.Unmarshal(body, &patch); err != nil {
		return err
	}
	deepMergePatch(merged, patch)
	mergedBytes, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	return json.Unmarshal(mergedBytes, out)
}

// deepMergePatch merges patch into base with Cloudflare's PATCH semantics: a null
// value removes the key; two JSON objects are merged recursively; arrays and
// scalars replace wholesale; a key absent from patch is left untouched.
func deepMergePatch(base, patch map[string]json.RawMessage) {
	for k, pv := range patch {
		if isJSONNull(pv) {
			delete(base, k)
			continue
		}
		if bv, ok := base[k]; ok {
			bObj := map[string]json.RawMessage{}
			pObj := map[string]json.RawMessage{}
			// Recurse only when BOTH sides are JSON objects (unmarshal into a map
			// succeeds and yields non-nil). Arrays/scalars/null fail or nil-out and
			// fall through to wholesale replace.
			if json.Unmarshal(bv, &bObj) == nil && json.Unmarshal(pv, &pObj) == nil && bObj != nil && pObj != nil {
				deepMergePatch(bObj, pObj)
				if remerged, err := json.Marshal(bObj); err == nil {
					base[k] = remerged
					continue
				}
			}
		}
		base[k] = pv
	}
}

// isJSONNull reports whether a raw JSON value is the literal null.
func isJSONNull(raw json.RawMessage) bool {
	return strings.TrimSpace(string(raw)) == "null"
}

// canonicalizeHeaderKeys returns hdr with every key canonicalized via
// http.CanonicalHeaderKey, mirroring how Cloudflare stores monitor probe header
// names (HTTP header keys are case-insensitive; Cloudflare stores "Host", not
// "host"). Modeling that keeps the header-removal round-trip honest: editMonitor's
// WithJSONSet override sends canonical keys (plus explicit nulls), so the mock's
// deep-merge must have a canonical base for a dropped key's null to match and
// remove it. A nil map is returned unchanged (an unmanaged header).
func canonicalizeHeaderKeys(hdr map[string][]string) map[string][]string {
	if hdr == nil {
		return nil
	}
	out := make(map[string][]string, len(hdr))
	for k, v := range hdr {
		out[http.CanonicalHeaderKey(k)] = v
	}
	return out
}

// --- account ---

func (m *lbMockServer) handleAccountGet(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("accountID")
	if code, ok := m.accountStatusOverride[id]; ok {
		if code >= 400 {
			writeCFError(w, code, "forced account status")
			return
		}
		writeResult(w, http.StatusOK, map[string]any{"id": id, "name": "Test Account", "type": "standard"})
		return
	}
	if id != m.accountID {
		writeCFError(w, http.StatusNotFound, "account not found")
		return
	}
	writeResult(w, http.StatusOK, map[string]any{"id": id, "name": "Test Account", "type": "standard"})
}

// setAccountStatus forces handleAccountGet to respond for the given account ID
// with a specific HTTP status: >= 400 returns that error, < 400 marks the account
// valid. Used by the account_initialized sticky tests.
func (m *lbMockServer) setAccountStatus(id string, code int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accountStatusOverride[id] = code
}

// --- monitors ---

func (m *lbMockServer) handleMonitorList(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	results := make([]mockMonitor, 0, len(m.monitors))
	for _, mon := range m.monitors {
		results = append(results, mon)
	}
	writeList(w, results, len(results))
}

func (m *lbMockServer) handleMonitorCreate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeCFError(w, http.StatusBadRequest, "read error")
		return
	}
	var rec mockMonitor
	if err := json.Unmarshal(body, &rec); err != nil {
		writeCFError(w, http.StatusBadRequest, "invalid json")
		return
	}
	// Cloudflare canonicalizes probe header names on store (see canonicalizeHeaderKeys).
	rec.Header = canonicalizeHeaderKeys(rec.Header)
	// Model Cloudflare's server-side default: an absent "retries" becomes 2.
	// This keeps the retries=0 regression honest -- if the operator's build guard
	// were ever restored (omitting retries on create), CF would default it to 2
	// and the test would catch it, not silently see the zero-value 0.
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(body, &raw)
	if _, ok := raw["retries"]; !ok {
		rec.Retries = 2
	}
	rec.ID = m.nextID("mon")
	m.monitors[rec.ID] = rec
	if m.failMonitorCreateOnce {
		// Record persisted server-side, but the client sees a failure -- the
		// classic timed-out-but-succeeded create. The reconciler's guarded
		// retry must re-list and adopt this record rather than create a second.
		m.failMonitorCreateOnce = false
		writeCFError(w, http.StatusInternalServerError, "simulated create timeout")
		return
	}
	writeResult(w, http.StatusCreated, rec)
}

func (m *lbMockServer) handleMonitorUpdate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("id")
	existing, ok := m.monitors[id]
	if !ok {
		writeCFError(w, http.StatusNotFound, "monitor not found")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeCFError(w, http.StatusBadRequest, "read error")
		return
	}
	// PATCH partial-merge: only the top-level keys present in the body overwrite
	// the stored record; absent keys survive. The bool-drift regression relies on
	// an explicitly-sent false overwriting CF's true.
	var rec mockMonitor
	if err := applyPatch(existing, body, &rec); err != nil {
		writeCFError(w, http.StatusBadRequest, "invalid json")
		return
	}
	rec.ID = id
	m.monitors[id] = rec
	writeResult(w, http.StatusOK, rec)
}

func (m *lbMockServer) handleMonitorDelete(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("id")
	if _, ok := m.monitors[id]; !ok {
		writeCFError(w, http.StatusNotFound, "monitor not found")
		return
	}
	delete(m.monitors, id)
	writeResult(w, http.StatusOK, map[string]any{"id": id})
}

// --- pools ---

func (m *lbMockServer) handlePoolList(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	results := make([]mockPool, 0, len(m.pools))
	for _, p := range m.pools {
		results = append(results, p)
	}
	writeList(w, results, len(results))
}

func (m *lbMockServer) handlePoolCreate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rec mockPool
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		writeCFError(w, http.StatusBadRequest, "invalid json")
		return
	}
	rec.ID = m.nextID("pool")
	m.pools[rec.ID] = rec
	writeResult(w, http.StatusCreated, rec)
}

func (m *lbMockServer) handlePoolUpdate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("id")
	existing, ok := m.pools[id]
	if !ok {
		writeCFError(w, http.StatusNotFound, "pool not found")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeCFError(w, http.StatusBadRequest, "read error")
		return
	}
	// Model Cloudflare's rejection of monitor="" (412 code 1004): a detach must be an
	// explicit JSON null, never an empty string. Without this the mock would silently
	// accept "" and hide the monitorless-pool / detach bug (only live CF 412s).
	var probe map[string]json.RawMessage
	if json.Unmarshal(body, &probe) == nil {
		if mv, ok := probe["monitor"]; ok && string(mv) == `""` {
			writeCFError(w, http.StatusPreconditionFailed, "monitor id is invalid (code 1004)")
			return
		}
	}
	var rec mockPool
	if err := applyPatch(existing, body, &rec); err != nil {
		writeCFError(w, http.StatusBadRequest, "invalid json")
		return
	}
	rec.ID = id
	m.pools[id] = rec
	writeResult(w, http.StatusOK, rec)
}

func (m *lbMockServer) handlePoolDelete(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("id")
	if _, ok := m.pools[id]; !ok {
		writeCFError(w, http.StatusNotFound, "pool not found")
		return
	}
	delete(m.pools, id)
	writeResult(w, http.StatusOK, map[string]any{"id": id})
}

// handlePoolHealth serves GET .../pools/{id}/health. The result is the CF pool
// health shape (pool_id + pop_health map), which the operator decodes from the
// raw JSON because the SDK mis-flattens pop_health. A forced-fail entry responds
// with an error; an unseeded pool responds with an empty pop_health.
func (m *lbMockServer) handlePoolHealth(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.poolHealthGetCount++
	id := r.PathValue("id")
	if code, ok := m.poolHealthFail[id]; ok && code >= 400 {
		writeCFError(w, code, "forced pool health status")
		return
	}
	if result, ok := m.poolHealth[id]; ok {
		writeResult(w, http.StatusOK, result)
		return
	}
	writeResult(w, http.StatusOK, map[string]any{"pool_id": id, "pop_health": map[string]any{}})
}

// seedPoolHealth stores the pool-health result object served for a pool id.
func (m *lbMockServer) seedPoolHealth(id string, result map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.poolHealth[id] = result
}

// setPoolHealthFail forces handlePoolHealth to respond with an HTTP error for a
// pool id (>= 400), used to exercise poll-failure isolation.
func (m *lbMockServer) setPoolHealthFail(id string, code int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.poolHealthFail[id] = code
}

// poolHealthGets returns the number of pool-health GET calls served.
func (m *lbMockServer) poolHealthGets() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.poolHealthGetCount
}

// --- load balancers ---

func (m *lbMockServer) handleLBList(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	results := make([]mockLB, 0, len(m.lbs))
	for _, l := range m.lbs {
		results = append(results, l)
	}
	writeList(w, results, len(results))
}

// cfIgnoreTTLWhenProxied models real Cloudflare: it ignores the ttl on proxied
// (orange-clouded) load balancers and echoes its own value rather than the
// requested one. Forcing ttl to 0 for proxied LBs is what makes the TTL fix
// necessary -- without the fix (send ttl + compare it while proxied) this would
// perpetually drift-loop, which the regression test asserts against.
func cfIgnoreTTLWhenProxied(rec *mockLB) {
	if rec.Proxied {
		rec.TTL = 0
	}
}

func (m *lbMockServer) handleLBCreate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var rec mockLB
	if err := json.NewDecoder(r.Body).Decode(&rec); err != nil {
		writeCFError(w, http.StatusBadRequest, "invalid json")
		return
	}
	cfIgnoreTTLWhenProxied(&rec)
	// Model Cloudflare's server-side default: enabled is not accepted by the create
	// API (absent from LoadBalancerNewParams), so a newly created LB is enabled.
	// The operator applies any explicit disable via a follow-up edit (create-then-
	// edit), which the drift path exercises.
	rec.Enabled = true
	rec.ID = m.nextID("lb")
	m.lbs[rec.ID] = rec
	writeResult(w, http.StatusCreated, rec)
}

func (m *lbMockServer) handleLBUpdate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("id")
	existing, ok := m.lbs[id]
	if !ok {
		writeCFError(w, http.StatusNotFound, "load balancer not found")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeCFError(w, http.StatusBadRequest, "read error")
		return
	}
	var rec mockLB
	if err := applyPatch(existing, body, &rec); err != nil {
		writeCFError(w, http.StatusBadRequest, "invalid json")
		return
	}
	cfIgnoreTTLWhenProxied(&rec)
	rec.ID = id
	m.lbs[id] = rec
	m.lbUpdateCount++
	writeResult(w, http.StatusOK, rec)
}

func (m *lbMockServer) handleLBDelete(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("id")
	if _, ok := m.lbs[id]; !ok {
		writeCFError(w, http.StatusNotFound, "load balancer not found")
		return
	}
	delete(m.lbs, id)
	writeResult(w, http.StatusOK, map[string]any{"id": id})
}

// --- state getters / seeders (mutex-guarded, mirror hostnameCount) ---

func (m *lbMockServer) seedMonitor(rec mockMonitor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.monitors[rec.ID] = rec
}

func (m *lbMockServer) seedPool(rec mockPool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pools[rec.ID] = rec
}

func (m *lbMockServer) seedLB(rec mockLB) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lbs[rec.ID] = rec
}

func (m *lbMockServer) setFailMonitorCreateOnce() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failMonitorCreateOnce = true
}

func (m *lbMockServer) monitorsWithMarker(marker string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, mon := range m.monitors {
		if strings.Contains(mon.Description, marker) {
			n++
		}
	}
	return n
}

func (m *lbMockServer) monitorByMarker(marker string) (mockMonitor, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mon := range m.monitors {
		if strings.Contains(mon.Description, marker) {
			return mon, true
		}
	}
	return mockMonitor{}, false
}

// replaceMonitorID reassigns the CF id of the monitor carrying marker, keeping
// the record otherwise intact. Simulates an external recreate: the CR's
// status.ID becomes stale while the marker-matched record survives with a new
// id, so deletePolicy=own-only sees a mismatch and refuses to delete.
func (m *lbMockServer) replaceMonitorID(marker, newID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, mon := range m.monitors {
		if strings.Contains(mon.Description, marker) {
			delete(m.monitors, id)
			mon.ID = newID
			m.monitors[newID] = mon
			return
		}
	}
}

// setMonitorFollowRedirects flips the CF-side follow_redirects on the
// marker-matched monitor, simulating external drift for the bool-drift
// regression test.
func (m *lbMockServer) setMonitorFollowRedirects(marker string, v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, mon := range m.monitors {
		if strings.Contains(mon.Description, marker) {
			mon.FollowRedirects = v
			m.monitors[id] = mon
			return
		}
	}
}

func (m *lbMockServer) poolByName(name string) (mockPool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range m.pools {
		if p.Name == name {
			return p, true
		}
	}
	return mockPool{}, false
}

func (m *lbMockServer) poolsWithName(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, p := range m.pools {
		if p.Name == name {
			n++
		}
	}
	return n
}

// setPoolLoadShedding sets the un-modeled LoadShedding field on the CF pool with
// the given name, simulating Cloudflare-side state the operator does not manage.
func (m *lbMockServer) setPoolLoadShedding(name, v string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range m.pools {
		if p.Name == name {
			p.LoadShedding = v
			m.pools[id] = p
			return
		}
	}
}

// poolReplaceID reassigns the CF id of the pool with the given name, keeping the
// record otherwise intact. Simulates an external recreate so deletePolicy=own-only
// sees a status.ID mismatch and refuses to delete.
func (m *lbMockServer) poolReplaceID(name, newID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, p := range m.pools {
		if p.Name == name {
			delete(m.pools, id)
			p.ID = newID
			m.pools[newID] = p
			return
		}
	}
}

func (m *lbMockServer) lbByName(name string) (mockLB, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, l := range m.lbs {
		if l.Name == name {
			return l, true
		}
	}
	return mockLB{}, false
}

func (m *lbMockServer) lbsWithName(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, l := range m.lbs {
		if l.Name == name {
			n++
		}
	}
	return n
}

// setLBRules sets the un-modeled Rules field on the CF LB with the given name,
// simulating Cloudflare-side state the operator does not manage.
func (m *lbMockServer) setLBRules(name string, rules json.RawMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, l := range m.lbs {
		if l.Name == name {
			l.Rules = rules
			m.lbs[id] = l
			return
		}
	}
}

// setLBNetworks sets the CF-side networks on the LB with the given name,
// simulating an out-of-band change to a create-only-write field so the operator
// surfaces (but does not correct) the divergence.
func (m *lbMockServer) setLBNetworks(name string, networks []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, l := range m.lbs {
		if l.Name == name {
			l.Networks = networks
			m.lbs[id] = l
			return
		}
	}
}

// setLBEnabled flips the CF-side enabled flag on the LB with the given name,
// simulating an out-of-band enable/disable that the operator must re-enforce.
func (m *lbMockServer) setLBEnabled(name string, enabled bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, l := range m.lbs {
		if l.Name == name {
			l.Enabled = enabled
			m.lbs[id] = l
			return
		}
	}
}

// mutateLB applies fn to the CF LB with the given name (hostname), simulating an
// out-of-band change to CF-side state for drift tests.
func (m *lbMockServer) mutateLB(name string, fn func(*mockLB)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, l := range m.lbs {
		if l.Name == name {
			fn(&l)
			m.lbs[id] = l
			return
		}
	}
}

// lbReplaceID reassigns the CF id of the LB with the given name (hostname),
// keeping the record otherwise intact. Simulates an external recreate so
// deletePolicy=own-only sees a status.ID mismatch and refuses to delete.
func (m *lbMockServer) lbReplaceID(name, newID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, l := range m.lbs {
		if l.Name == name {
			delete(m.lbs, id)
			l.ID = newID
			m.lbs[newID] = l
			return
		}
	}
}

func (m *lbMockServer) lbUpdates() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lbUpdateCount
}

// --- test constants ---

const (
	lbTestNS   = "lb-test"
	lbPolicyNS = "lb-policy-test"
	lbDryRunNS = "lb-dryrun-test"

	lbAccountID       = "a1111111111111111111111111111111"
	lbBadAccountID    = "b2222222222222222222222222222222"
	lbStickyAccountID = "d4444444444444444444444444444444"
	lbZoneID          = "c3333333333333333333333333333333"
	lbZoneName        = "lb.example.com"

	lbSecretName  = "cf-secret"
	lbAccountName = "lb-account"
	lbZoneCRName  = "lb-zone"
)

// --- test helpers ---

func monitorMarkerFor(ns, name string) string {
	return fmt.Sprintf("[cf-edge-operator:%s/%s]", ns, name)
}

// floatNear compares two float64s within a small epsilon, tolerating JSON
// round-trip imprecision on pool latitude/longitude.
func floatNear(a, b float64) bool {
	const eps = 1e-6
	d := a - b
	return d < eps && d > -eps
}

func readyCondition(conds []metav1.Condition) *metav1.Condition {
	return apimeta.FindStatusCondition(conds, conditionReady)
}

// startLBManager wires the four load-balancing reconcilers into a fresh manager
// whose cache is scoped to a single namespace, then starts it on the suite ctx.
// Namespace scoping isolates managers so multiple LB managers (main / dry-run /
// delete-policy) can coexist in one envtest process without reconciling each
// other's objects.
func startLBManager(ns string, dryRun bool, requeue time.Duration, baseURL string, enablePoolHealth bool) {
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{ns: {}},
		},
		// Multiple LB managers coexist in this single test process (main /
		// delete-policy / dry-run), each registering controllers with the same
		// hardcoded names. Skip the process-global uniqueness check.
		Controller: crconfig.Controller{SkipNameValidation: new(true)},
	})
	Expect(err).NotTo(HaveOccurred())

	Expect((&AccountReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		OperatorNamespace: ns,
		CFAPITimeout:      5 * time.Second,
		CFAPIMaxRetries:   1,
		CFBaseURL:         baseURL,
		RequeueInterval:   requeue,
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&LoadBalancerMonitorReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		OperatorNamespace: ns,
		ManagementPolicy:  ManagementPolicyManage,
		DeletePolicy:      DeletePolicyAlways,
		DryRun:            dryRun,
		CFAPITimeout:      5 * time.Second,
		CFAPIWriteTimeout: 5 * time.Second,
		CFAPIMaxRetries:   1,
		CFBaseURL:         baseURL,
		RequeueInterval:   requeue,
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&LoadBalancerPoolReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		OperatorNamespace: ns,
		ManagementPolicy:  ManagementPolicyManage,
		DeletePolicy:      DeletePolicyAlways,
		DryRun:            dryRun,
		CFAPITimeout:      5 * time.Second,
		CFAPIWriteTimeout: 5 * time.Second,
		CFAPIMaxRetries:   1,
		CFBaseURL:         baseURL,
		RequeueInterval:   requeue,
		EnablePoolHealth:  enablePoolHealth,
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&LoadBalancerReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		Recorder:          mgr.GetEventRecorder("loadbalancer"),
		OperatorNamespace: ns,
		ManagementPolicy:  ManagementPolicyManage,
		DeletePolicy:      DeletePolicyAlways,
		DryRun:            dryRun,
		CFAPITimeout:      5 * time.Second,
		CFAPIWriteTimeout: 5 * time.Second,
		CFAPIMaxRetries:   1,
		CFBaseURL:         baseURL,
		RequeueInterval:   requeue,
	}).SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()
}

// createLBFixtures creates the namespace, credentials secret, and Account CR
// shared by an LB manager's tests. zone controls whether a Zone CR (needed for
// LoadBalancer tests) is also created.
func createLBFixtures(ns string, zone bool) {
	Expect(k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})).To(Succeed())

	Expect(k8sClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: lbSecretName, Namespace: ns},
		Data:       map[string][]byte{"apiToken": []byte("test-token")},
	})).To(Succeed())

	Expect(k8sClient.Create(ctx, &accountsv1beta1.Account{
		ObjectMeta: metav1.ObjectMeta{Name: lbAccountName, Namespace: ns},
		Spec: accountsv1beta1.AccountSpec{
			ID:             lbAccountID,
			CredentialsRef: accountsv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
		},
	})).To(Succeed())

	if zone {
		Expect(k8sClient.Create(ctx, &domainsv1beta1.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: lbZoneCRName, Namespace: ns},
			Spec: domainsv1beta1.ZoneSpec{
				ID:             lbZoneID,
				CredentialsRef: domainsv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
			},
		})).To(Succeed())
	}
}

// accountInitializedSeriesExists reports whether a cf_edge_operator_account_initialized
// series currently exists for the given Account CR. Gathering from the registry
// (rather than testutil.ToFloat64, which would recreate a missing series at 0)
// lets a test distinguish a deleted series from a zero-valued one.
func accountInitializedSeriesExists(name string) bool {
	families, err := crtlmetrics.Registry.Gather()
	if err != nil {
		return false
	}
	for _, mf := range families {
		if mf.GetName() != "cf_edge_operator_account_initialized" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "account_cr" && l.GetValue() == name {
					return true
				}
			}
		}
	}
	return false
}

const (
	lbEventuallyTimeout = 10 * time.Second
	lbPollInterval      = 150 * time.Millisecond

	// steeringOff is a non-default LB steering policy, used to exercise drift
	// correction away from the CRD default (dynamic_latency).
	steeringOff = "off"
)

// --- Main suite: account / monitor / pool / load balancer / create-guard / delete ---

var _ = Describe("LoadBalancing", Ordered, func() {
	var lbMock *lbMockServer

	BeforeAll(func() {
		lbMock = newLBMockServer()
		createLBFixtures(lbTestNS, true)

		// A second, deliberately-invalid Account for the validation-failure case.
		Expect(k8sClient.Create(ctx, &accountsv1beta1.Account{
			ObjectMeta: metav1.ObjectMeta{Name: "lb-account-bad", Namespace: lbTestNS},
			Spec: accountsv1beta1.AccountSpec{
				ID:             lbBadAccountID,
				CredentialsRef: accountsv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
			},
		})).To(Succeed())

		startLBManager(lbTestNS, false, 2*time.Second, lbMock.URL(), false)
	})

	AfterAll(func() {
		lbMock.Close()
	})

	// -- Scenario 1: Account validation --
	Context("Account validation", func() {
		It("marks a valid Account Initialized=True", func() {
			Eventually(func() bool {
				var a accountsv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: lbAccountName, Namespace: lbTestNS}, &a); err != nil {
					return false
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				return c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			var a accountsv1beta1.Account
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lbAccountName, Namespace: lbTestNS}, &a)).To(Succeed())
			Expect(a.Status.Name).To(Equal("Test Account"))

			// cf_edge_operator_account_initialized reflects the validated state.
			Expect(testutil.ToFloat64(accountInitialized.WithLabelValues(lbAccountName))).To(Equal(float64(1)))
		})

		It("marks an unknown Account Initialized=False with ValidationFailed", func() {
			Eventually(func() string {
				var a accountsv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-account-bad", Namespace: lbTestNS}, &a); err != nil {
					return ""
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				if c == nil || c.Status != metav1.ConditionFalse {
					return ""
				}
				return c.Reason
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("ValidationFailed"))

			// A failed validation reports account_initialized=0 for the alert.
			Expect(testutil.ToFloat64(accountInitialized.WithLabelValues("lb-account-bad"))).To(Equal(float64(0)))
		})

		It("clears cf_edge_operator_account_initialized when the Account is deleted", func() {
			const name = "lb-account-ephemeral"
			Expect(k8sClient.Create(ctx, &accountsv1beta1.Account{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: lbTestNS},
				Spec: accountsv1beta1.AccountSpec{
					ID:             lbAccountID,
					CredentialsRef: accountsv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
				},
			})).To(Succeed())
			// The controller publishes this Account's series once it reconciles.
			Eventually(func() bool {
				return accountInitializedSeriesExists(name)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			Expect(k8sClient.Delete(ctx, &accountsv1beta1.Account{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: lbTestNS},
			})).To(Succeed())
			// Deletion removes the series (no stale account_initialized).
			Eventually(func() bool {
				return accountInitializedSeriesExists(name)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeFalse())
		})

		It("keeps account_initialized on a transient CF failure and flips only on a definitive auth failure", func() {
			const stickyName = "lb-account-sticky"
			// A dedicated, isolated account ID so toggling its CF response never
			// affects the shared account used by the pool/monitor/LB tests.
			lbMock.setAccountStatus(lbStickyAccountID, 200)
			Expect(k8sClient.Create(ctx, &accountsv1beta1.Account{
				ObjectMeta: metav1.ObjectMeta{Name: stickyName, Namespace: lbTestNS},
				Spec: accountsv1beta1.AccountSpec{
					ID:             lbStickyAccountID,
					CredentialsRef: accountsv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
				},
			})).To(Succeed())

			// Validated once: Initialized=True + metric=1.
			Eventually(func() bool {
				var a accountsv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: stickyName, Namespace: lbTestNS}, &a); err != nil {
					return false
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				return c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			Eventually(func() float64 {
				return testutil.ToFloat64(accountInitialized.WithLabelValues(stickyName))
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(float64(1)))

			// Transient CF failure (5xx): the controller re-validates every requeue,
			// but a transient failure must NOT flip the condition or metric (sticky).
			lbMock.setAccountStatus(lbStickyAccountID, 500)
			Consistently(func() bool {
				var a accountsv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: stickyName, Namespace: lbTestNS}, &a); err != nil {
					return false
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				metricOK := testutil.ToFloat64(accountInitialized.WithLabelValues(stickyName)) == 1
				return c != nil && c.Status == metav1.ConditionTrue && metricOK
			}, 3*time.Second, lbPollInterval).Should(BeTrue())

			// Definitive auth failure (403): now flip Initialized=False + metric=0.
			lbMock.setAccountStatus(lbStickyAccountID, 403)
			Eventually(func() string {
				var a accountsv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: stickyName, Namespace: lbTestNS}, &a); err != nil {
					return ""
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				if c == nil || c.Status != metav1.ConditionFalse {
					return ""
				}
				return c.Reason
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("ValidationFailed"))
			Eventually(func() float64 {
				return testutil.ToFloat64(accountInitialized.WithLabelValues(stickyName))
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(float64(0)))
		})
	})

	// -- Scenario 2: Monitor create / drift / adopt --
	Context("Monitor", func() {
		It("creates a monitor in CF and then corrects drift", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-basic")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-basic", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
					Path:       "/health",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			// Create path: exactly one monitor, id recorded, Ready=True, createCount=1.
			Eventually(func() int {
				return lbMock.monitorsWithMarker(marker)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			Eventually(func() bool {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-basic", Namespace: lbTestNS}, &m); err != nil {
					return false
				}
				c := readyCondition(m.Status.Conditions)
				return m.Status.ID != "" && c != nil && c.Status == metav1.ConditionTrue && m.Status.CreateCount == 1
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			rec, ok := lbMock.monitorByMarker(marker)
			Expect(ok).To(BeTrue())
			Expect(rec.Path).To(Equal("/health"))

			// Drift path: mutate spec.path -> PUT -> mock reflects, Ready stays True.
			var current lbv1beta1.LoadBalancerMonitor
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "mon-basic", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Path = "/healthz"
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() string {
				r, ok := lbMock.monitorByMarker(marker)
				if !ok {
					return ""
				}
				return r.Path
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("/healthz"))

			var after lbv1beta1.LoadBalancerMonitor
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "mon-basic", Namespace: lbTestNS}, &after)).To(Succeed())
			c := readyCondition(after.Status.Conditions)
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
			Expect(lbMock.monitorsWithMarker(marker)).To(Equal(1))

			// The deferred state-gauge recompute publishes the ready monitor under
			// its account owner (Eventually absorbs the one-cycle cache lag).
			Eventually(func() float64 {
				return testutil.ToFloat64(loadBalancerMonitors.WithLabelValues(lbAccountName, lbStateReady))
			}, lbEventuallyTimeout, lbPollInterval).Should(BeNumerically(">=", 1))
		})

		It("adopts a pre-existing CF monitor by its marker without creating a duplicate", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-adopt")
			// Seed a monitor carrying this CR's marker so the reconcile adopts it.
			// The assertion is on adoption (status.ID reused, no duplicate create),
			// not on drift -- a corrective PATCH may still run harmlessly.
			lbMock.seedMonitor(mockMonitor{
				ID:          "mock-mon-seeded-adopt",
				Type:        "https",
				Interval:    60,
				Retries:     2,
				Timeout:     5,
				Description: marker,
			})

			before := testutil.ToFloat64(operationsTotal.WithLabelValues(cfResourceLoadBalancerMon, cfOpAdopt))

			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-adopt", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			Eventually(func() string {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-adopt", Namespace: lbTestNS}, &m); err != nil {
					return ""
				}
				return m.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("mock-mon-seeded-adopt"))

			// No duplicate, no fresh create (adoption, not creation).
			Expect(lbMock.monitorsWithMarker(marker)).To(Equal(1))
			var m lbv1beta1.LoadBalancerMonitor
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "mon-adopt", Namespace: lbTestNS}, &m)).To(Succeed())
			Expect(m.Status.CreateCount).To(Equal(int32(0)))

			Eventually(func() float64 {
				return testutil.ToFloat64(operationsTotal.WithLabelValues(cfResourceLoadBalancerMon, cfOpAdopt))
			}, lbEventuallyTimeout, lbPollInterval).Should(BeNumerically(">=", before+1))
		})
	})

	// -- Scenario 3: Pool create / drift / adopt --
	Context("Pool", func() {
		It("creates a pool in CF and then corrects drift", func() {
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-basic", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.0.1", Enabled: new(true)},
					},
					Enabled: new(true),
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			Eventually(func() bool {
				var p lbv1beta1.LoadBalancerPool
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-basic", Namespace: lbTestNS}, &p); err != nil {
					return false
				}
				c := readyCondition(p.Status.Conditions)
				return p.Status.ID != "" && c != nil && c.Status == metav1.ConditionTrue && p.Status.Enabled && p.Status.CreateCount == 1
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			rec, ok := lbMock.poolByName("pool-basic")
			Expect(ok).To(BeTrue())
			Expect(rec.Enabled).To(BeTrue())
			Expect(rec.Origins).To(HaveLen(1))
			Expect(rec.Origins[0].Address).To(Equal("10.0.0.1"))

			// Drift: disable the pool -> PUT -> mock reflects.
			var current lbv1beta1.LoadBalancerPool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-basic", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Enabled = new(false)
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.poolByName("pool-basic")
				return ok && !r.Enabled
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			Expect(lbMock.poolsWithName("pool-basic")).To(Equal(1))
		})

		It("adopts a pre-existing CF pool by name without creating a duplicate", func() {
			lbMock.seedPool(mockPool{
				ID:      "mock-pool-seeded-adopt",
				Name:    "pool-adopt",
				Enabled: true,
				Origins: []mockOrigin{{Name: "o1", Address: "10.0.0.9", Enabled: true}},
			})

			before := testutil.ToFloat64(operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, cfOpAdopt))

			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-adopt", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.0.9", Enabled: new(true)},
					},
					Enabled: new(true),
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			Eventually(func() string {
				var p lbv1beta1.LoadBalancerPool
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-adopt", Namespace: lbTestNS}, &p); err != nil {
					return ""
				}
				return p.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("mock-pool-seeded-adopt"))

			Expect(lbMock.poolsWithName("pool-adopt")).To(Equal(1))
			var p lbv1beta1.LoadBalancerPool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-adopt", Namespace: lbTestNS}, &p)).To(Succeed())
			Expect(p.Status.CreateCount).To(Equal(int32(0)))

			Eventually(func() float64 {
				return testutil.ToFloat64(operationsTotal.WithLabelValues(cfResourceLoadBalancerPool, cfOpAdopt))
			}, lbEventuallyTimeout, lbPollInterval).Should(BeNumerically(">=", before+1))
		})
	})

	// -- Scenario 4: Pool gated on a not-yet-ready monitor --
	Context("Pool gated on monitor", func() {
		It("waits for the monitor, then resolves it via the monitor->pool watch", func() {
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-gated", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.1.1", Enabled: new(true)},
					},
					Enabled:    new(true),
					MonitorRef: &lbv1beta1.LoadBalancerMonitorRef{Name: "mon-gated"},
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			// Blocked on the missing monitor; no CF pool created yet.
			Eventually(func() string {
				var p lbv1beta1.LoadBalancerPool
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-gated", Namespace: lbTestNS}, &p); err != nil {
					return ""
				}
				c := readyCondition(p.Status.Conditions)
				if c == nil || c.Status != metav1.ConditionFalse {
					return ""
				}
				return c.Reason
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("WaitingForMonitor"))
			Expect(lbMock.poolsWithName("pool-gated")).To(Equal(0))

			// Create the monitor; the watch wakes the pool once status.id lands.
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-gated", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			var monID string
			Eventually(func() string {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-gated", Namespace: lbTestNS}, &m); err != nil {
					return ""
				}
				monID = m.Status.ID
				return m.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

			Eventually(func() bool {
				var p lbv1beta1.LoadBalancerPool
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-gated", Namespace: lbTestNS}, &p); err != nil {
					return false
				}
				c := readyCondition(p.Status.Conditions)
				return c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			Eventually(func() string {
				r, ok := lbMock.poolByName("pool-gated")
				if !ok {
					return ""
				}
				return r.Monitor
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(monID))
		})
	})

	// -- Scenario 5 + 6: LoadBalancer best-effort resolution, watch, degrade, drift --
	Context("LoadBalancer", func() {
		It("waits for the fallback pool, then creates the LB via the pool->LB watch", func() {
			// Fallback pool referenced before it exists.
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-a", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        "lb-a." + lbZoneName,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() string {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-a", Namespace: lbTestNS}, &l); err != nil {
					return ""
				}
				c := readyCondition(l.Status.Conditions)
				if c == nil || c.Status != metav1.ConditionFalse {
					return ""
				}
				return c.Reason
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("WaitingForFallbackPool"))
			Expect(lbMock.lbsWithName("lb-a." + lbZoneName)).To(Equal(0))

			// Create the fallback pool; the watch drives the LB to creation.
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-fb", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.2.1", Enabled: new(true)},
					},
					Enabled: new(true),
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			var poolID string
			Eventually(func() string {
				var p lbv1beta1.LoadBalancerPool
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-fb", Namespace: lbTestNS}, &p); err != nil {
					return ""
				}
				poolID = p.Status.ID
				return p.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-a", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				return c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			Eventually(func() int {
				return lbMock.lbsWithName("lb-a." + lbZoneName)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			rec, ok := lbMock.lbByName("lb-a." + lbZoneName)
			Expect(ok).To(BeTrue())
			Expect(rec.FallbackPool).To(Equal(poolID))
			Expect(rec.DefaultPools).To(Equal([]string{poolID}))
		})

		It("comes up degraded when an extra default pool ref is unresolved", func() {
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-deg", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:  lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname: "lb-deg." + lbZoneName,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{
						{Name: "pool-fb"},
						{Name: "pool-missing"},
					},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-deg", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				return c != nil && c.Status == metav1.ConditionTrue &&
					strings.Contains(c.Message, "degraded") && strings.Contains(c.Message, "pool-missing")
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// The CF LB is written with only the resolved pool; the missing ref
			// is dropped, not blocking.
			pool, ok := lbMock.poolByName("pool-fb")
			Expect(ok).To(BeTrue())
			Eventually(func() []string {
				r, ok := lbMock.lbByName("lb-deg." + lbZoneName)
				if !ok {
					return nil
				}
				return r.DefaultPools
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal([]string{pool.ID}))
		})

		It("corrects drift on a managed field (steeringPolicy)", func() {
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-drift", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        "lb-drift." + lbZoneName,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "dynamic_latency",
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName("lb-drift." + lbZoneName)
				return ok && r.SteeringPolicy == "dynamic_latency"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-drift", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.SteeringPolicy = steeringOff
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() string {
				r, ok := lbMock.lbByName("lb-drift." + lbZoneName)
				if !ok {
					return ""
				}
				return r.SteeringPolicy
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(steeringOff))

			var after lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-drift", Namespace: lbTestNS}, &after)).To(Succeed())
			c := readyCondition(after.Status.Conditions)
			Expect(c).NotTo(BeNil())
			Expect(c.Status).To(Equal(metav1.ConditionTrue))
		})
	})

	// -- Partial (degraded) sync state --
	Context("LoadBalancer partial state", func() {
		It("marks Ready=True + reason Partial with unresolvedPoolRefs, a partial gauge series, and an Event", func() {
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-partial", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:  lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname: "lb-partial." + lbZoneName,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{
						{Name: "pool-fb"},
						{Name: "pool-partial-missing"},
					},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Ready stays True (it IS serving) but the reason is Partial and the
			// unresolved ref is recorded on status.
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-partial", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonPartial {
					return false
				}
				return len(l.Status.UnresolvedPoolRefs) == 1 && l.Status.UnresolvedPoolRefs[0] == "pool-partial-missing"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// The loadbalancers gauge shows a partial series for this zone.
			Eventually(func() float64 {
				return testutil.ToFloat64(loadBalancers.WithLabelValues(lbZoneCRName, lbStatePartial))
			}, lbEventuallyTimeout, lbPollInterval).Should(BeNumerically(">=", 1))

			// A Kubernetes Event announces the transition into partial.
			Eventually(func() bool {
				var events corev1.EventList
				if err := k8sClient.List(ctx, &events, client.InNamespace(lbTestNS)); err != nil {
					return false
				}
				for _, e := range events.Items {
					if e.InvolvedObject.Name == "lb-partial" && e.Reason == "Partial" {
						return true
					}
				}
				return false
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("feeds partial from an unresolved random-steering weighted pool while still serving", func() {
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-rs", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        "lb-rs." + lbZoneName,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					RandomSteering: &lbv1beta1.LoadBalancerRandomSteering{
						PoolWeights: []lbv1beta1.LoadBalancerPoolWeight{
							{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "rs-missing"}, Weight: "0.5"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Default + fallback resolve, so the LB serves; the unresolved weighted
			// pool degrades it to partial.
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-rs", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				if c == nil || c.Status != metav1.ConditionTrue || c.Reason != reasonPartial {
					return false
				}
				return len(l.Status.UnresolvedPoolRefs) == 1 && l.Status.UnresolvedPoolRefs[0] == "rs-missing"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			Eventually(func() int {
				return lbMock.lbsWithName("lb-rs." + lbZoneName)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))
		})
	})

	// -- enabled: always-managed (enforce + surface-on-adopt) --
	Context("LoadBalancer enabled enforcement", func() {
		It("re-enables an out-of-band disabled LB (enabled always-managed)", func() {
			hostname := "lb-enabled." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-enabled", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.Enabled
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Disable out-of-band; the operator must correct it back to true.
			lbMock.setLBEnabled(hostname, false)
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.Enabled
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("adopts a disabled CF LB, warns via an Event, and re-enforces enabled", func() {
			hostname := "lb-adopt-enabled." + lbZoneName
			pool, ok := lbMock.poolByName("pool-fb")
			Expect(ok).To(BeTrue())
			// Seed a pre-existing, disabled CF LB for this hostname so the reconcile
			// adopts it (rather than creating) and re-enforces enabled=true.
			lbMock.seedLB(mockLB{
				ID:           "mock-lb-seeded-adopt",
				Name:         hostname,
				DefaultPools: []string{pool.ID},
				FallbackPool: pool.ID,
				Proxied:      true,
				Enabled:      false,
			})

			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-adopt-enabled", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Adopted (status.ID reuses the seeded id, no duplicate create).
			Eventually(func() string {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-adopt-enabled", Namespace: lbTestNS}, &l); err != nil {
					return ""
				}
				return l.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("mock-lb-seeded-adopt"))
			Expect(lbMock.lbsWithName(hostname)).To(Equal(1))

			// enabled re-enforced to true.
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.Enabled
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// A warning Event surfaces the on-adopt enforcement.
			Eventually(func() bool {
				var events corev1.EventList
				if err := k8sClient.List(ctx, &events, client.InNamespace(lbTestNS)); err != nil {
					return false
				}
				for _, e := range events.Items {
					if e.InvolvedObject.Name == "lb-adopt-enabled" && e.Reason == "EnabledEnforced" {
						return true
					}
				}
				return false
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})
	})

	// -- networks: create-only-write + drift surfacing (surface-only, Ready unaffected) --
	Context("LoadBalancer networks drift", func() {
		It("writes networks on create, surfaces later drift without flipping Ready, and clears on resync", func() {
			hostname := "lb-net." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-net", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					Networks:        []string{"net-a", "net-b"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Networks written on create; NetworksSynced=True; Ready=True.
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && len(r.Networks) == 2
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-net", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				ns := apimeta.FindStatusCondition(l.Status.Conditions, conditionNetworksSynced)
				rc := readyCondition(l.Status.Conditions)
				return ns != nil && ns.Status == metav1.ConditionTrue &&
					rc != nil && rc.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Out-of-band drift (drop net-b). Surface it: NetworksSynced=False,
			// metric > 0, Ready STILL True.
			lbMock.setLBNetworks(hostname, []string{"net-a"})
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-net", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				ns := apimeta.FindStatusCondition(l.Status.Conditions, conditionNetworksSynced)
				rc := readyCondition(l.Status.Conditions)
				return ns != nil && ns.Status == metav1.ConditionFalse &&
					rc != nil && rc.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			Eventually(func() float64 {
				return testutil.ToFloat64(loadBalancerNetworksDrift.WithLabelValues(lbZoneCRName))
			}, lbEventuallyTimeout, lbPollInterval).Should(BeNumerically(">=", 1))

			// Networks are create-only-write: the operator must NOT push them back.
			Consistently(func() []string {
				r, _ := lbMock.lbByName(hostname)
				return r.Networks
			}, 3*time.Second, lbPollInterval).Should(Equal([]string{"net-a"}))

			// Resync out-of-band; NetworksSynced clears back to True and the drift
			// metric for this zone returns to 0.
			lbMock.setLBNetworks(hostname, []string{"net-a", "net-b"})
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-net", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				ns := apimeta.FindStatusCondition(l.Status.Conditions, conditionNetworksSynced)
				return ns != nil && ns.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			Eventually(func() float64 {
				return testutil.ToFloat64(loadBalancerNetworksDrift.WithLabelValues(lbZoneCRName))
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(float64(0)))
		})
	})

	// -- Scenario 7: cfCreateGuarded adopts a timed-out-but-succeeded create --
	Context("cfCreateGuarded", func() {
		It("adopts rather than duplicating when the first create times out server-side", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-guard")
			lbMock.setFailMonitorCreateOnce()

			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-guard", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			Eventually(func() bool {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-guard", Namespace: lbTestNS}, &m); err != nil {
					return false
				}
				c := readyCondition(m.Status.Conditions)
				return m.Status.ID != "" && c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Exactly one monitor exists despite the failed-then-retried create.
			Consistently(func() int {
				return lbMock.monitorsWithMarker(marker)
			}, 2*time.Second, lbPollInterval).Should(Equal(1))
		})
	})

	// -- Scenario 8 (delete): always / never --
	Context("DeletePolicy always/never", func() {
		It("deletes the CF monitor when deletePolicy=always (operator default)", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-del-always")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-del-always", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())
			Eventually(func() int {
				return lbMock.monitorsWithMarker(marker)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			Expect(k8sClient.Delete(ctx, mon)).To(Succeed())

			Eventually(func() int {
				return lbMock.monitorsWithMarker(marker)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
			Eventually(func() bool {
				var m lbv1beta1.LoadBalancerMonitor
				return k8sClient.Get(ctx, types.NamespacedName{Name: "mon-del-always", Namespace: lbTestNS}, &m) != nil
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("keeps the CF monitor when spec.deletePolicy=never", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-del-never")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-del-never", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef:   lbv1beta1.AccountRef{Name: lbAccountName},
					Type:         "https",
					DeletePolicy: DeletePolicyNever,
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())
			Eventually(func() int {
				return lbMock.monitorsWithMarker(marker)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			Expect(k8sClient.Delete(ctx, mon)).To(Succeed())

			// CR is released (gone) but the CF record remains.
			Eventually(func() bool {
				var m lbv1beta1.LoadBalancerMonitor
				return k8sClient.Get(ctx, types.NamespacedName{Name: "mon-del-never", Namespace: lbTestNS}, &m) != nil
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			Expect(lbMock.monitorsWithMarker(marker)).To(Equal(1))
		})
	})

	// -- LoadBalancer deletion (always) --
	Context("LoadBalancer deletion", func() {
		It("deletes the CF load balancer when deletePolicy=always", func() {
			hostname := "lb-del-always." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-del-always", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())
			Eventually(func() int {
				return lbMock.lbsWithName(hostname)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			Expect(k8sClient.Delete(ctx, lb)).To(Succeed())

			Eventually(func() int {
				return lbMock.lbsWithName(hostname)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				return k8sClient.Get(ctx, types.NamespacedName{Name: "lb-del-always", Namespace: lbTestNS}, &l) != nil
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})
	})

	// -- LoadBalancerPool deletion (always) --
	Context("LoadBalancerPool deletion", func() {
		It("deletes the CF pool when deletePolicy=always", func() {
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-del-always", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.7.1", Enabled: new(true)},
					},
					Enabled: new(true),
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			Eventually(func() int {
				return lbMock.poolsWithName("pool-del-always")
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			Expect(k8sClient.Delete(ctx, pool)).To(Succeed())

			Eventually(func() int {
				return lbMock.poolsWithName("pool-del-always")
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
			Eventually(func() bool {
				var p lbv1beta1.LoadBalancerPool
				return k8sClient.Get(ctx, types.NamespacedName{Name: "pool-del-always", Namespace: lbTestNS}, &p) != nil
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})
	})

	// -- Keyed pools (region / country / pop) end-to-end + drift --
	Context("Keyed pools", func() {
		It("resolves regionPools/countryPools/popPools to CF pool IDs and converges on change", func() {
			// Two ready pools to steer between.
			for _, spec := range []struct{ name, addr string }{
				{"pool-k-a", "10.0.8.1"},
				{"pool-k-b", "10.0.8.2"},
			} {
				p := &lbv1beta1.LoadBalancerPool{
					ObjectMeta: metav1.ObjectMeta{Name: spec.name, Namespace: lbTestNS},
					Spec: lbv1beta1.LoadBalancerPoolSpec{
						AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
						Origins: []lbv1beta1.LoadBalancerPoolOrigin{
							{Name: "o1", Address: spec.addr, Enabled: new(true)},
						},
						Enabled: new(true),
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
			}

			var poolAID, poolBID string
			Eventually(func() bool {
				a, okA := lbMock.poolByName("pool-k-a")
				b, okB := lbMock.poolByName("pool-k-b")
				poolAID, poolBID = a.ID, b.ID
				return okA && okB && poolAID != "" && poolBID != ""
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			hostname := "lb-keyed." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-keyed", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-k-a"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-k-a"},
					SteeringPolicy:  "geo",
					RegionPools:     &map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-k-a"}}},
					CountryPools:    &map[string][]lbv1beta1.LoadBalancerPoolRef{"US": {{Name: "pool-k-a"}}},
					PopPools:        &map[string][]lbv1beta1.LoadBalancerPoolRef{"LAX": {{Name: "pool-k-a"}}},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// All keyed maps resolve to pool-k-a's CF id.
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				if !ok {
					return false
				}
				return stringSlicesEqual(r.RegionPools["WNAM"], []string{poolAID}) &&
					stringSlicesEqual(r.CountryPools["US"], []string{poolAID}) &&
					stringSlicesEqual(r.PopPools["LAX"], []string{poolAID})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Change WNAM's pool -> drift correction through resolveKeyedPools.
			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-keyed", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.RegionPools = &map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-k-b"}}}
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() []string {
				r, ok := lbMock.lbByName(hostname)
				if !ok {
					return nil
				}
				return r.RegionPools["WNAM"]
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal([]string{poolBID}))

			// Country / pop mappings are untouched by the WNAM change.
			r, ok := lbMock.lbByName(hostname)
			Expect(ok).To(BeTrue())
			Expect(r.CountryPools["US"]).To(Equal([]string{poolAID}))
			Expect(r.PopPools["LAX"]).To(Equal([]string{poolAID}))
		})
	})

	// -- Readopt after external recreate (self-requeue driven) --
	Context("Readopt", func() {
		It("readopts a monitor whose CF id changed out from under it", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-readopt")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-readopt", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			var firstID string
			Eventually(func() string {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-readopt", Namespace: lbTestNS}, &m); err != nil {
					return ""
				}
				firstID = m.Status.ID
				return m.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

			// External delete+recreate: same marker, new CF id.
			lbMock.replaceMonitorID(marker, "mock-mon-recreated")

			// The 2s self-requeue drives the readopt branch; status.ID follows.
			Eventually(func() string {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-readopt", Namespace: lbTestNS}, &m); err != nil {
					return ""
				}
				return m.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("mock-mon-recreated"))
			Expect(firstID).NotTo(Equal("mock-mon-recreated"))
		})
	})

	// -- Monitor bool drift regression (always-send-bools fix) --
	Context("Monitor bool drift", func() {
		It("re-asserts followRedirects=false against CF-side true without drift-looping", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-bool")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-bool", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
					// FollowRedirects defaults to false.
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				return ok && !r.FollowRedirects
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Force CF-side drift to true, then let the reconciler correct it.
			lbMock.setMonitorFollowRedirects(marker, true)

			Eventually(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				return ok && !r.FollowRedirects
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// And it stays false -- no perpetual drift-loop.
			Consistently(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				return ok && !r.FollowRedirects
			}, 3*time.Second, lbPollInterval).Should(BeTrue())
		})
	})

	// -- TTL regression (managed only when NOT proxied) --
	Context("LoadBalancer TTL", func() {
		It("does not manage ttl on a proxied LB and does not drift-loop", func() {
			hostname := "lb-ttl-proxied." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-ttl-proxied", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					Proxied:         new(true),
					TTL:             300,
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-ttl-proxied", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				r, ok := lbMock.lbByName(hostname)
				return ok && r.TTL == 0 && c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Once settled, the proxied LB must not keep issuing PUTs (ttl was the
			// pre-fix drift-loop trigger).
			before := lbMock.lbUpdates()
			Consistently(func() int {
				return lbMock.lbUpdates()
			}, 3*time.Second, lbPollInterval).Should(Equal(before))
		})

		It("manages ttl on a non-proxied LB", func() {
			hostname := "lb-ttl-unproxied." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-ttl-unproxied", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					Proxied:         new(false),
					TTL:             120,
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-ttl-unproxied", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				r, ok := lbMock.lbByName(hostname)
				return ok && r.TTL == 120 && c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})
	})

	// -- Optional nested LB features: adaptive_routing / location_strategy /
	//    session_affinity_attributes+ttl (send-iff-expressed + drift + no-loop) --
	Context("LoadBalancer optional nested features", func() {
		It("sends adaptive_routing on create+edit and corrects out-of-band drift", func() {
			hostname := "lb-ar." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-ar", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					AdaptiveRouting: &lbv1beta1.LoadBalancerAdaptiveRouting{FailoverAcrossPools: new(true)},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Sent on create: CF stores failover_across_pools=true.
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.AdaptiveRouting != nil && r.AdaptiveRouting.FailoverAcrossPools
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Out-of-band flip to false -> drift -> corrected back to true (edit).
			lbMock.mutateLB(hostname, func(rec *mockLB) {
				rec.AdaptiveRouting = &mockAdaptiveRouting{FailoverAcrossPools: false}
			})
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.AdaptiveRouting != nil && r.AdaptiveRouting.FailoverAcrossPools
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("sends location_strategy on create+edit and corrects out-of-band drift", func() {
			hostname := "lb-ls." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-ls", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:          lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:         hostname,
					DefaultPoolRefs:  []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef:  lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					LocationStrategy: &lbv1beta1.LoadBalancerLocationStrategy{Mode: "pop", PreferECS: "always"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.LocationStrategy != nil &&
					r.LocationStrategy.Mode == "pop" && r.LocationStrategy.PreferECS == "always"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			lbMock.mutateLB(hostname, func(rec *mockLB) {
				rec.LocationStrategy = &mockLocationStrategy{Mode: "resolver_ip", PreferECS: "never"}
			})
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.LocationStrategy != nil &&
					r.LocationStrategy.Mode == "pop" && r.LocationStrategy.PreferECS == "always"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("sends session_affinity_attributes and ttl on create+edit and corrects out-of-band drift", func() {
			hostname := "lb-sa." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-sa", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:            lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:           hostname,
					DefaultPoolRefs:    []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef:    lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SessionAffinity:    "cookie",
					SessionAffinityTtl: 1800,
					SessionAffinityAttributes: &lbv1beta1.LoadBalancerSessionAffinityAttributes{
						Samesite:             "Lax",
						Secure:               "Always",
						ZeroDowntimeFailover: "temporary",
					},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.SessionAffinity == "cookie" && r.SessionAffinityTTL == 1800 &&
					r.SessionAffinityAttributes != nil &&
					r.SessionAffinityAttributes.Samesite == "Lax" &&
					r.SessionAffinityAttributes.Secure == "Always" &&
					r.SessionAffinityAttributes.ZeroDowntimeFailover == "temporary"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Out-of-band drift on both the nested attrs object and the ttl scalar.
			lbMock.mutateLB(hostname, func(rec *mockLB) {
				rec.SessionAffinityAttributes = &mockSessionAffinityAttributes{Samesite: "Strict", Secure: "Never", ZeroDowntimeFailover: "none"}
				rec.SessionAffinityTTL = 60
			})
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.SessionAffinityTTL == 1800 &&
					r.SessionAffinityAttributes != nil &&
					r.SessionAffinityAttributes.Samesite == "Lax" &&
					r.SessionAffinityAttributes.Secure == "Always" &&
					r.SessionAffinityAttributes.ZeroDowntimeFailover == "temporary"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("does not send adaptive_routing / location_strategy / random_steering when unset and does not drift-loop", func() {
			hostname := "lb-noopt." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-noopt", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				_, ok := lbMock.lbByName(hostname)
				return ok
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Not sent on create: the optional nested features are absent CF-side.
			r, ok := lbMock.lbByName(hostname)
			Expect(ok).To(BeTrue())
			Expect(r.AdaptiveRouting).To(BeNil())
			Expect(r.LocationStrategy).To(BeNil())
			Expect(r.RandomSteering).To(BeNil())

			// Seed out-of-band values for the un-expressed fields. Leave-alone: the
			// operator must never compare or correct them, so they persist and no
			// corrective edit loop is issued.
			lbMock.mutateLB(hostname, func(rec *mockLB) {
				rec.AdaptiveRouting = &mockAdaptiveRouting{FailoverAcrossPools: true}
				rec.LocationStrategy = &mockLocationStrategy{Mode: "resolver_ip"}
				rec.RandomSteering = &mockRandomSteering{DefaultWeight: 0.4, PoolWeights: map[string]float64{"pool-x": 0.9}}
			})
			before := lbMock.lbUpdates()
			Consistently(func() int {
				return lbMock.lbUpdates()
			}, 3*time.Second, lbPollInterval).Should(Equal(before))
			rr, _ := lbMock.lbByName(hostname)
			Expect(rr.AdaptiveRouting).NotTo(BeNil())
			Expect(rr.LocationStrategy).NotTo(BeNil())
			Expect(rr.RandomSteering).NotTo(BeNil())
		})

		It("does not send session-affinity attributes/ttl when affinity is none and does not drift-loop", func() {
			hostname := "lb-sanone." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-sanone", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SessionAffinity: "none",
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.SessionAffinity == "none"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// affinity=none: attributes/ttl are neither sent nor drift-checked.
			r, ok := lbMock.lbByName(hostname)
			Expect(ok).To(BeTrue())
			Expect(r.SessionAffinityAttributes).To(BeNil())
			Expect(r.SessionAffinityTTL).To(BeZero())

			// Seed out-of-band attributes + ttl; the operator must leave them and
			// not issue a corrective edit loop (gated on sessionAffinityActive).
			lbMock.mutateLB(hostname, func(rec *mockLB) {
				rec.SessionAffinityAttributes = &mockSessionAffinityAttributes{Samesite: "Strict"}
				rec.SessionAffinityTTL = 120
			})
			before := lbMock.lbUpdates()
			Consistently(func() int {
				return lbMock.lbUpdates()
			}, 3*time.Second, lbPollInterval).Should(Equal(before))
			rr, _ := lbMock.lbByName(hostname)
			Expect(rr.SessionAffinityAttributes).NotTo(BeNil())
			Expect(rr.SessionAffinityTTL).To(Equal(float64(120)))
		})
	})

	// -- random_steering: resolved weights sent (create+edit+drift); an unresolved
	//    weighted pool is omitted from pool_weights and feeds partial --
	Context("LoadBalancer random steering", func() {
		It("sends resolved pool weights on create+edit and corrects out-of-band drift", func() {
			hostname := "lb-rsw." + lbZoneName
			pool, ok := lbMock.poolByName("pool-fb")
			Expect(ok).To(BeTrue())
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-rsw", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "random",
					RandomSteering: &lbv1beta1.LoadBalancerRandomSteering{
						DefaultWeight: "0.2",
						PoolWeights: []lbv1beta1.LoadBalancerPoolWeight{
							{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"}, Weight: "0.7"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Sent on create: default_weight + the resolved pool weight (keyed by CF
			// pool ID) round-trip.
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.RandomSteering != nil &&
					floatNear(r.RandomSteering.DefaultWeight, 0.2) &&
					len(r.RandomSteering.PoolWeights) == 1 &&
					floatNear(r.RandomSteering.PoolWeights[pool.ID], 0.7)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Out-of-band drift on both default_weight and the per-pool weight ->
			// corrected back (edit).
			lbMock.mutateLB(hostname, func(rec *mockLB) {
				rec.RandomSteering = &mockRandomSteering{DefaultWeight: 0.9, PoolWeights: map[string]float64{pool.ID: 0.1}}
			})
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.RandomSteering != nil &&
					floatNear(r.RandomSteering.DefaultWeight, 0.2) &&
					len(r.RandomSteering.PoolWeights) == 1 &&
					floatNear(r.RandomSteering.PoolWeights[pool.ID], 0.7)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})

		It("omits an unresolved weighted pool from pool_weights and records it in unresolvedPoolRefs", func() {
			hostname := "lb-rspartial." + lbZoneName
			pool, ok := lbMock.poolByName("pool-fb")
			Expect(ok).To(BeTrue())
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-rspartial", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "random",
					RandomSteering: &lbv1beta1.LoadBalancerRandomSteering{
						PoolWeights: []lbv1beta1.LoadBalancerPoolWeight{
							{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"}, Weight: "0.6"},
							{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "rspartial-missing"}, Weight: "0.4"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Only the resolved pool appears in pool_weights; the unresolved ref is
			// dropped (not re-implemented here -- resolution happens in chunk 2).
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.RandomSteering != nil &&
					len(r.RandomSteering.PoolWeights) == 1 &&
					floatNear(r.RandomSteering.PoolWeights[pool.ID], 0.6)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// The unresolved weighted pool degrades the LB to partial and is surfaced
			// via status.unresolvedPoolRefs (Ready stays True -- it still serves).
			Eventually(func() bool {
				var l lbv1beta1.LoadBalancer
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-rspartial", Namespace: lbTestNS}, &l); err != nil {
					return false
				}
				c := readyCondition(l.Status.Conditions)
				return c != nil && c.Status == metav1.ConditionTrue && c.Reason == reasonPartial &&
					len(l.Status.UnresolvedPoolRefs) == 1 && l.Status.UnresolvedPoolRefs[0] == "rspartial-missing"
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})
	})

	// -- Pool latitude/longitude regression (now drift-checked) --
	Context("LoadBalancerPool lat/long", func() {
		It("creates a pool with latitude/longitude and converges on change", func() {
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-geo", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.9.1", Enabled: new(true)},
					},
					Enabled:   new(true),
					Latitude:  new("37.7749"),
					Longitude: new("-122.4194"),
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.poolByName("pool-geo")
				if !ok {
					return false
				}
				return floatNear(r.Latitude, 37.7749) && floatNear(r.Longitude, -122.4194)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Patch to new coordinates -> poolDrifted must now catch it.
			var current lbv1beta1.LoadBalancerPool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-geo", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Latitude = new("40.7128")
			current.Spec.Longitude = new("-74.0060")
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.poolByName("pool-geo")
				if !ok {
					return false
				}
				return floatNear(r.Latitude, 40.7128) && floatNear(r.Longitude, -74.0060)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		})
	})

	// -- PATCH semantics: un-modeled Cloudflare fields survive an operator edit --
	Context("Partial edit preserves un-modeled fields", func() {
		It("pool: LoadShedding survives an edit that only touches modeled fields", func() {
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-unmodeled", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.10.1", Enabled: new(true)},
					},
					Enabled: new(true),
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			Eventually(func() bool {
				r, ok := lbMock.poolByName("pool-unmodeled")
				return ok && r.ID != ""
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// CF-side field the operator does not model.
			lbMock.setPoolLoadShedding("pool-unmodeled", "custom-shedding")

			// Trigger an operator edit by mutating a modeled field.
			var current lbv1beta1.LoadBalancerPool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-unmodeled", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Enabled = new(false)
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			// The edit lands (enabled cleared) AND the un-modeled field survives.
			Eventually(func() bool {
				r, ok := lbMock.poolByName("pool-unmodeled")
				return ok && !r.Enabled
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			r, ok := lbMock.poolByName("pool-unmodeled")
			Expect(ok).To(BeTrue())
			Expect(r.LoadShedding).To(Equal("custom-shedding"))
			// And it keeps surviving subsequent reconciles (no full-replace wipe).
			Consistently(func() string {
				rr, _ := lbMock.poolByName("pool-unmodeled")
				return rr.LoadShedding
			}, 3*time.Second, lbPollInterval).Should(Equal("custom-shedding"))
		})

		It("loadbalancer: Rules survive an edit that only touches modeled fields", func() {
			hostname := "lb-unmodeled." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-unmodeled", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "dynamic_latency",
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())
			Eventually(func() int {
				return lbMock.lbsWithName(hostname)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

			lbMock.setLBRules(hostname, json.RawMessage(`[{"name":"rule-1","disabled":false}]`))

			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-unmodeled", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.SteeringPolicy = steeringOff
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.SteeringPolicy == steeringOff
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			r, ok := lbMock.lbByName(hostname)
			Expect(ok).To(BeTrue())
			Expect(string(r.Rules)).To(ContainSubstring("rule-1"))
			Consistently(func() string {
				rr, _ := lbMock.lbByName(hostname)
				return string(rr.Rules)
			}, 3*time.Second, lbPollInterval).Should(ContainSubstring("rule-1"))
		})
	})

	// -- Keyed pools: a present map is fully managed (dropping a key removes it via
	//    an explicit null since Cloudflare deep-merges; emptying clears all); an
	//    unset (nil) map is left untouched -- presence, not emptiness, manages. --
	Context("Keyed pools removal", func() {
		It("removes a dropped key and clears the map when emptied (present, not nil)", func() {
			hostname := "lb-keyclear." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-keyclear", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "geo",
					RegionPools: &map[string][]lbv1beta1.LoadBalancerPoolRef{
						"WNAM": {{Name: "pool-fb"}},
						"EEU":  {{Name: "pool-fb"}},
					},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			var poolID string
			Eventually(func() bool {
				p, okP := lbMock.poolByName("pool-fb")
				poolID = p.ID
				r, ok := lbMock.lbByName(hostname)
				return okP && ok &&
					stringSlicesEqual(r.RegionPools["WNAM"], []string{poolID}) &&
					stringSlicesEqual(r.RegionPools["EEU"], []string{poolID})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Drop ONE key (EEU) but keep the map non-empty. The operator must send an
			// explicit null for EEU -- Cloudflare deep-merges the map, so an omitted
			// key would linger and drift-loop (the bug this fix addresses).
			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-keyclear", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.RegionPools = &map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-fb"}}}
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				if !ok {
					return false
				}
				_, eeuPresent := r.RegionPools["EEU"]
				return !eeuPresent && stringSlicesEqual(r.RegionPools["WNAM"], []string{poolID})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			// No drift-loop re-adding EEU.
			Consistently(func() int {
				r, _ := lbMock.lbByName(hostname)
				return len(r.RegionPools)
			}, 3*time.Second, lbPollInterval).Should(Equal(1))

			// Empty the map (present, not nil) -> clear every remaining key.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-keyclear", Namespace: lbTestNS}, &current)).To(Succeed())
			patch = client.MergeFrom(current.DeepCopy())
			current.Spec.RegionPools = &map[string][]lbv1beta1.LoadBalancerPoolRef{}
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() int {
				r, ok := lbMock.lbByName(hostname)
				if !ok {
					return -1
				}
				return len(r.RegionPools)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
			Consistently(func() int {
				r, _ := lbMock.lbByName(hostname)
				return len(r.RegionPools)
			}, 3*time.Second, lbPollInterval).Should(Equal(0))
		})

		It("leaves region_pools untouched when the field is unset (nil)", func() {
			hostname := "lb-keyleave." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-keyleave", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "geo",
					RegionPools:     &map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-fb"}}},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			var poolID string
			Eventually(func() bool {
				p, okP := lbMock.poolByName("pool-fb")
				poolID = p.ID
				r, ok := lbMock.lbByName(hostname)
				return okP && ok && stringSlicesEqual(r.RegionPools["WNAM"], []string{poolID})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Unset the field (nil) -> the operator stops managing region_pools and
			// leaves Cloudflare's value in place (presence, not emptiness, manages).
			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-keyleave", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.RegionPools = nil
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Consistently(func() []string {
				r, ok := lbMock.lbByName(hostname)
				if !ok {
					return nil
				}
				return r.RegionPools["WNAM"]
			}, 3*time.Second, lbPollInterval).Should(Equal([]string{poolID}))
		})
	})

	// -- Monitor header removal: the FIX 1 null-removal path for the probe header
	//    map (editMonitor's WithJSONSet("header", ...) -> deep-merge), the sibling of
	//    the region_pools case above. A regression to typed-header handling (which
	//    cannot emit a JSON null) would leave a dropped key lingering and fail here. --
	Context("Monitor header removal", func() {
		It("removes a dropped header key and clears the map when emptied (present, not nil)", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-hdrclear")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-hdrclear", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
					// Lowercase "host" deliberately: Cloudflare stores the canonical
					// "Host", and headerEditOverride canonicalizes before diffing, so a
					// regression that dropped canonicalization would emit a spurious null
					// for the kept "Host" (removing it) or fail to remove "Accept".
					Header: &map[string][]string{
						"host":   {"api.internal"},
						"accept": {"application/json"},
					},
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			// Create path: both header keys land on Cloudflare, canonicalized.
			Eventually(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				if !ok {
					return false
				}
				return stringSlicesEqual(r.Header["Host"], []string{"api.internal"}) &&
					stringSlicesEqual(r.Header["Accept"], []string{"application/json"})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Drop ONE key (accept) but keep the map non-empty. The operator must send
			// an explicit null for Accept via WithJSONSet -- Cloudflare deep-merges the
			// header map, so an omitted key would linger and drift-loop (the bug FIX 1
			// addresses).
			var current lbv1beta1.LoadBalancerMonitor
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "mon-hdrclear", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Header = &map[string][]string{"host": {"api.internal"}}
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				if !ok {
					return false
				}
				_, acceptPresent := r.Header["Accept"]
				return !acceptPresent && stringSlicesEqual(r.Header["Host"], []string{"api.internal"})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			// No drift-loop re-adding Accept.
			Consistently(func() int {
				r, _ := lbMock.monitorByMarker(marker)
				return len(r.Header)
			}, 3*time.Second, lbPollInterval).Should(Equal(1))

			// Empty the map (present, not nil) -> clear every remaining header key.
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "mon-hdrclear", Namespace: lbTestNS}, &current)).To(Succeed())
			patch = client.MergeFrom(current.DeepCopy())
			current.Spec.Header = &map[string][]string{}
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() int {
				r, ok := lbMock.monitorByMarker(marker)
				if !ok {
					return -1
				}
				return len(r.Header)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
			Consistently(func() int {
				r, _ := lbMock.monitorByMarker(marker)
				return len(r.Header)
			}, 3*time.Second, lbPollInterval).Should(Equal(0))
		})

		It("leaves the monitor header untouched when the field is unset (nil)", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-hdrleave")
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-hdrleave", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
					Header:     &map[string][]string{"host": {"api.internal"}},
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			// Seed a header on Cloudflare (canonicalized) so there is something to leave.
			Eventually(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				return ok && stringSlicesEqual(r.Header["Host"], []string{"api.internal"})
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Unset the field (nil) -> the operator stops managing headers and leaves
			// Cloudflare's value in place (presence, not emptiness, manages).
			var current lbv1beta1.LoadBalancerMonitor
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "mon-hdrleave", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Header = nil
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Consistently(func() []string {
				r, ok := lbMock.monitorByMarker(marker)
				if !ok {
					return nil
				}
				return r.Header["Host"]
			}, 3*time.Second, lbPollInterval).Should(Equal([]string{"api.internal"}))
		})
	})

	// -- random_steering.pool_weights removal: the FIX 1 null-removal path for the
	//    weighted-pool map (editLoadBalancer's WithJSONSet("random_steering.pool_weights",
	//    ...) -> deep-merge). A dropped weighted pool must be nulled out, not omitted,
	//    or it lingers on Cloudflare and drift-loops. --
	Context("LoadBalancer random steering removal", func() {
		It("removes a dropped weighted pool from pool_weights without a drift-loop", func() {
			// Two ready pools to weight between.
			for _, spec := range []struct{ name, addr string }{
				{"pool-rs-a", "10.0.12.1"},
				{"pool-rs-b", "10.0.12.2"},
			} {
				p := &lbv1beta1.LoadBalancerPool{
					ObjectMeta: metav1.ObjectMeta{Name: spec.name, Namespace: lbTestNS},
					Spec: lbv1beta1.LoadBalancerPoolSpec{
						AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
						Origins: []lbv1beta1.LoadBalancerPoolOrigin{
							{Name: "o1", Address: spec.addr, Enabled: new(true)},
						},
						Enabled: new(true),
					},
				}
				Expect(k8sClient.Create(ctx, p)).To(Succeed())
			}

			var poolAID, poolBID string
			Eventually(func() bool {
				a, okA := lbMock.poolByName("pool-rs-a")
				b, okB := lbMock.poolByName("pool-rs-b")
				poolAID, poolBID = a.ID, b.ID
				return okA && okB && poolAID != "" && poolBID != ""
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			hostname := "lb-rsdrop." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-rsdrop", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-rs-a"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-rs-a"},
					SteeringPolicy:  "random",
					RandomSteering: &lbv1beta1.LoadBalancerRandomSteering{
						DefaultWeight: "0.2",
						PoolWeights: []lbv1beta1.LoadBalancerPoolWeight{
							{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-rs-a"}, Weight: "0.6"},
							{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-rs-b"}, Weight: "0.4"},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, lb)).To(Succeed())

			// Both resolved pool weights land on Cloudflare (keyed by CF pool ID).
			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				return ok && r.RandomSteering != nil &&
					len(r.RandomSteering.PoolWeights) == 2 &&
					floatNear(r.RandomSteering.PoolWeights[poolAID], 0.6) &&
					floatNear(r.RandomSteering.PoolWeights[poolBID], 0.4)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Drop pool-rs-b from the weights. The operator must send an explicit null
			// for its CF id via WithJSONSet -- Cloudflare deep-merges pool_weights, so an
			// omitted key would linger and drift-loop.
			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-rsdrop", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.RandomSteering = &lbv1beta1.LoadBalancerRandomSteering{
				DefaultWeight: "0.2",
				PoolWeights: []lbv1beta1.LoadBalancerPoolWeight{
					{PoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-rs-a"}, Weight: "0.6"},
				},
			}
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() bool {
				r, ok := lbMock.lbByName(hostname)
				if !ok || r.RandomSteering == nil {
					return false
				}
				_, bPresent := r.RandomSteering.PoolWeights[poolBID]
				return !bPresent &&
					len(r.RandomSteering.PoolWeights) == 1 &&
					floatNear(r.RandomSteering.PoolWeights[poolAID], 0.6)
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
			// No drift-loop re-adding the dropped weighted pool.
			Consistently(func() int {
				r, ok := lbMock.lbByName(hostname)
				if !ok || r.RandomSteering == nil {
					return -1
				}
				return len(r.RandomSteering.PoolWeights)
			}, 3*time.Second, lbPollInterval).Should(Equal(1))
		})
	})

	// -- Monitor detach when the pool drops its monitorRef (always-send "") --
	Context("Monitor detach", func() {
		It("clears the pool's monitor when monitorRef is removed", func() {
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-detach", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())
			var monID string
			Eventually(func() string {
				var m lbv1beta1.LoadBalancerMonitor
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-detach", Namespace: lbTestNS}, &m); err != nil {
					return ""
				}
				monID = m.Status.ID
				return m.Status.ID
			}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-detach", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.11.1", Enabled: new(true)},
					},
					Enabled:    new(true),
					MonitorRef: &lbv1beta1.LoadBalancerMonitorRef{Name: "mon-detach"},
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())

			Eventually(func() string {
				r, ok := lbMock.poolByName("pool-detach")
				if !ok {
					return ""
				}
				return r.Monitor
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(monID))

			// Remove the ref -> operator sends monitor:null on edit -> CF detaches
			// ("" would 412; the mock rejects it).
			var current lbv1beta1.LoadBalancerPool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-detach", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.MonitorRef = nil
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() string {
				r, ok := lbMock.poolByName("pool-detach")
				if !ok {
					return "<missing>"
				}
				return r.Monitor
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(""))
		})

		It("edits a monitorless pool without a 412 (monitor sent as null, never \"\")", func() {
			pool := &lbv1beta1.LoadBalancerPool{
				ObjectMeta: metav1.ObjectMeta{Name: "pool-monitorless", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerPoolSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Origins: []lbv1beta1.LoadBalancerPoolOrigin{
						{Name: "o1", Address: "10.0.12.1", Enabled: new(true)},
					},
					Enabled: new(true),
					// no MonitorRef -> monitorless
				},
			}
			Expect(k8sClient.Create(ctx, pool)).To(Succeed())
			Eventually(func() bool {
				_, ok := lbMock.poolByName("pool-monitorless")
				return ok
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			// Force an edit on the monitorless pool by adding an origin. Before the fix
			// the edit sent monitor="" and Cloudflare 412'd (the mock now models that);
			// the fix sends monitor:null, a safe no-op when the pool has none.
			var current lbv1beta1.LoadBalancerPool
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "pool-monitorless", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.Origins = append(current.Spec.Origins,
				lbv1beta1.LoadBalancerPoolOrigin{Name: "o2", Address: "10.0.12.2", Enabled: new(true)})
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() int {
				r, ok := lbMock.poolByName("pool-monitorless")
				if !ok {
					return -1
				}
				return len(r.Origins)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(2))
			r, _ := lbMock.poolByName("pool-monitorless")
			Expect(r.Monitor).To(Equal(""))
		})
	})

	// -- retries=0 is expressible end-to-end via the plain typed path --
	Context("Monitor retries=0", func() {
		It("preserves an explicit retries=0 through the normal reconcile to CF", func() {
			// Two fixes make this work on the ordinary path a user would take:
			// the build guard was dropped (0 is sent to CF), and "omitempty" was
			// removed from the Retries type so the operator's finalizer-add spec
			// Update no longer drops the 0 and let the API server re-apply the
			// default (2). No unstructured payload and no pre-set finalizer needed.
			mon := &lbv1beta1.LoadBalancerMonitor{
				ObjectMeta: metav1.ObjectMeta{Name: "mon-retries0", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerMonitorSpec{
					AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
					Type:       "https",
					Retries:    0,
				},
			}
			Expect(k8sClient.Create(ctx, mon)).To(Succeed())

			marker := monitorMarkerFor(lbTestNS, "mon-retries0")
			Eventually(func() bool {
				r, ok := lbMock.monitorByMarker(marker)
				return ok && r.ID != ""
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			r, ok := lbMock.monitorByMarker(marker)
			Expect(ok).To(BeTrue())
			Expect(r.Retries).To(Equal(int64(0)))
			// retries=0 must not drift back to the default 2.
			Consistently(func() int64 {
				rr, _ := lbMock.monitorByMarker(marker)
				return rr.Retries
			}, 3*time.Second, lbPollInterval).Should(Equal(int64(0)))
		})
	})
})

// --- Opt-in pool-health axis: reconcile-level glue (--enable-pool-health) ---
//
// pool_health_test.go drives poll/tally/publish/prune in isolation. This suite is
// the only one that runs a real manager with EnablePoolHealth ON, so it proves the
// two reconcile glue points that wire those pieces in:
//
//	(1) reconcileCloudflareState calls maybePollPoolHealth on the existing-pool
//	    path, so a ready pool's Cloudflare health is polled and published; and
//	(2) recomputeStateGauge prunes a deleted pool's health series (built + pruned
//	    only under the flag).
//
// The main / dry-run / own-only suites all pass enablePoolHealth=false, so without
// this suite either glue point could be deleted and every test would still pass.

var _ = Describe("LoadBalancerPool health axis", Ordered, func() {
	const (
		poolHealthNS      = "lb-poolhealth-test"
		poolHealthPool    = "pool-health"
		poolHealthMonitor = "mon-health"
	)
	var lbMock *lbMockServer

	BeforeAll(func() {
		lbMock = newLBMockServer()
		createLBFixtures(poolHealthNS, false)
		// Flag ON + a short requeue so the existing-pool path (and thus the health
		// poll) runs promptly after the pool settles Ready.
		startLBManager(poolHealthNS, false, 2*time.Second, lbMock.URL(), true)
	})

	AfterAll(func() {
		lbMock.Close()
		// The health gauges live in the process-global registry; drop any residue so
		// later specs start clean (mirrors pool_health_test.go's end-of-test prune).
		poolHealthGaugeSet.prune(map[string]bool{})
	})

	It("polls and publishes pool health from the reconcile path, then prunes on delete", func() {
		// (a) account + secret (BeforeAll) + monitor + pool -> Ready with a CF ID.
		mon := &lbv1beta1.LoadBalancerMonitor{
			ObjectMeta: metav1.ObjectMeta{Name: poolHealthMonitor, Namespace: poolHealthNS},
			Spec: lbv1beta1.LoadBalancerMonitorSpec{
				AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
				Type:       "https",
				Path:       "/health",
			},
		}
		Expect(k8sClient.Create(ctx, mon)).To(Succeed())

		pool := &lbv1beta1.LoadBalancerPool{
			ObjectMeta: metav1.ObjectMeta{Name: poolHealthPool, Namespace: poolHealthNS},
			Spec: lbv1beta1.LoadBalancerPoolSpec{
				AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
				Origins: []lbv1beta1.LoadBalancerPoolOrigin{
					{Name: "o1", Address: "10.0.9.1", Enabled: new(true)},
				},
				Enabled:    new(true),
				MonitorRef: &lbv1beta1.LoadBalancerMonitorRef{Name: poolHealthMonitor},
				// checkRegions exercises the per-region gauges; the mock round-trips
				// check_regions so the pool converges instead of drift-looping.
				CheckRegions: []string{"WNAM", "ENAM"},
			},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())

		var poolID string
		Eventually(func() bool {
			var p lbv1beta1.LoadBalancerPool
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: poolHealthPool, Namespace: poolHealthNS}, &p); err != nil {
				return false
			}
			c := readyCondition(p.Status.Conditions)
			poolID = p.Status.ID
			return poolID != "" && c != nil && c.Status == metav1.ConditionTrue
		}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

		// (b) Seed the mock health for the pool's CF-assigned ID: one healthy region
		// (WNAM) + one unhealthy (ENAM), matching spec.checkRegions (mirrors
		// TestPollPoolHealthThroughSDK).
		lbMock.seedPoolHealth(poolID, map[string]any{
			"pool_id": poolID,
			"pop_health": map[string]any{
				"WNAM": map[string]any{
					"healthy": true,
					"origins": []any{
						map[string]any{"10.0.9.1": map[string]any{"healthy": true}},
					},
				},
				"ENAM": map[string]any{
					"healthy": false,
					"origins": []any{
						map[string]any{"10.0.9.1": map[string]any{"healthy": false}},
					},
				},
			},
		})

		base := map[string]string{"account_cr": lbAccountName, "pool_cr": poolHealthPool}

		// (c) Glue point 1: the controller self-requeues at the harness interval, so
		// the existing-pool path runs maybePollPoolHealth. Prove the poll fired FROM
		// the reconcile (only this suite calls it) and that the seeded health was
		// decoded, tallied and published -- including the per-region breakdown.
		Eventually(func() int {
			return lbMock.poolHealthGets()
		}, lbEventuallyTimeout, lbPollInterval).Should(BeNumerically(">", 0))

		Eventually(func() float64 {
			return gaugeValue("cf_edge_operator_loadbalancerpool_health", merge(base, "status", "healthy"))
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(float64(1)))
		Expect(gaugeValue("cf_edge_operator_loadbalancerpool_health", merge(base, "status", "unhealthy"))).To(Equal(float64(1)))
		Expect(gaugeValue("cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "WNAM", "status", "healthy"))).To(Equal(float64(1)))
		Expect(gaugeValue("cf_edge_operator_loadbalancerpool_health_region", merge(base, "region", "ENAM", "status", "unhealthy"))).To(Equal(float64(1)))

		// (d) Glue point 2: deleting the pool CR makes recomputeStateGauge (deferred
		// on the delete reconcile, under the flag) prune the health series to zero.
		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
		Eventually(func() bool {
			var p lbv1beta1.LoadBalancerPool
			return k8sClient.Get(ctx, types.NamespacedName{Name: poolHealthPool, Namespace: poolHealthNS}, &p) != nil
		}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

		Eventually(func() int {
			return countSeries("cf_edge_operator_loadbalancerpool_health", base)
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
	})
})

// --- DeletePolicy own-only: needs a manager with no periodic self-requeue so a
// status.ID vs CF-id mismatch stays stable long enough to delete against. ---

var _ = Describe("LoadBalancing DeletePolicy own-only", Ordered, func() {
	var lbMock *lbMockServer

	BeforeAll(func() {
		lbMock = newLBMockServer()
		createLBFixtures(lbPolicyNS, true)
		// Long requeue: reconciles are driven by create/update/delete events
		// only, so a settled resource's status.ID does not get re-adopted between
		// mutating the mock and deleting the CR.
		startLBManager(lbPolicyNS, false, 10*time.Minute, lbMock.URL(), false)
	})

	AfterAll(func() {
		lbMock.Close()
	})

	It("deletes when the CF id matches status.ID", func() {
		marker := monitorMarkerFor(lbPolicyNS, "mon-own-match")
		mon := &lbv1beta1.LoadBalancerMonitor{
			ObjectMeta: metav1.ObjectMeta{Name: "mon-own-match", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerMonitorSpec{
				AccountRef:   lbv1beta1.AccountRef{Name: lbAccountName},
				Type:         "https",
				DeletePolicy: DeletePolicyOwnOnly,
			},
		}
		Expect(k8sClient.Create(ctx, mon)).To(Succeed())
		Eventually(func() string {
			var m lbv1beta1.LoadBalancerMonitor
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-own-match", Namespace: lbPolicyNS}, &m); err != nil {
				return ""
			}
			return m.Status.ID
		}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

		Expect(k8sClient.Delete(ctx, mon)).To(Succeed())
		Eventually(func() int {
			return lbMock.monitorsWithMarker(marker)
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
	})

	It("refuses to delete when the CF id differs from status.ID", func() {
		marker := monitorMarkerFor(lbPolicyNS, "mon-own-mismatch")
		mon := &lbv1beta1.LoadBalancerMonitor{
			ObjectMeta: metav1.ObjectMeta{Name: "mon-own-mismatch", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerMonitorSpec{
				AccountRef:   lbv1beta1.AccountRef{Name: lbAccountName},
				Type:         "https",
				DeletePolicy: DeletePolicyOwnOnly,
			},
		}
		Expect(k8sClient.Create(ctx, mon)).To(Succeed())
		Eventually(func() string {
			var m lbv1beta1.LoadBalancerMonitor
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-own-mismatch", Namespace: lbPolicyNS}, &m); err != nil {
				return ""
			}
			return m.Status.ID
		}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

		// Simulate an external recreate: the marker-matched record now has a
		// different CF id than the CR's recorded status.ID.
		lbMock.replaceMonitorID(marker, "mock-mon-external-recreated")

		Expect(k8sClient.Delete(ctx, mon)).To(Succeed())

		// CR finalizer is released (CR gone) but the CF record is left intact.
		Eventually(func() bool {
			var m lbv1beta1.LoadBalancerMonitor
			return k8sClient.Get(ctx, types.NamespacedName{Name: "mon-own-mismatch", Namespace: lbPolicyNS}, &m) != nil
		}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		Expect(lbMock.monitorsWithMarker(marker)).To(Equal(1))
	})

	It("pool: deletes when the CF id matches status.ID", func() {
		pool := &lbv1beta1.LoadBalancerPool{
			ObjectMeta: metav1.ObjectMeta{Name: "pool-own-match", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerPoolSpec{
				AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
				Origins: []lbv1beta1.LoadBalancerPoolOrigin{
					{Name: "o1", Address: "10.1.0.1", Enabled: new(true)},
				},
				Enabled:      new(true),
				DeletePolicy: DeletePolicyOwnOnly,
			},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Eventually(func() string {
			var p lbv1beta1.LoadBalancerPool
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-own-match", Namespace: lbPolicyNS}, &p); err != nil {
				return ""
			}
			return p.Status.ID
		}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
		Eventually(func() int {
			return lbMock.poolsWithName("pool-own-match")
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
	})

	It("pool: refuses to delete when the CF id differs from status.ID", func() {
		pool := &lbv1beta1.LoadBalancerPool{
			ObjectMeta: metav1.ObjectMeta{Name: "pool-own-mismatch", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerPoolSpec{
				AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
				Origins: []lbv1beta1.LoadBalancerPoolOrigin{
					{Name: "o1", Address: "10.1.0.2", Enabled: new(true)},
				},
				Enabled:      new(true),
				DeletePolicy: DeletePolicyOwnOnly,
			},
		}
		Expect(k8sClient.Create(ctx, pool)).To(Succeed())
		Eventually(func() string {
			var p lbv1beta1.LoadBalancerPool
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-own-mismatch", Namespace: lbPolicyNS}, &p); err != nil {
				return ""
			}
			return p.Status.ID
		}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

		lbMock.poolReplaceID("pool-own-mismatch", "mock-pool-external-recreated")

		Expect(k8sClient.Delete(ctx, pool)).To(Succeed())
		Eventually(func() bool {
			var p lbv1beta1.LoadBalancerPool
			return k8sClient.Get(ctx, types.NamespacedName{Name: "pool-own-mismatch", Namespace: lbPolicyNS}, &p) != nil
		}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		Expect(lbMock.poolsWithName("pool-own-mismatch")).To(Equal(1))
	})

	It("loadbalancer: deletes when the CF id matches status.ID", func() {
		// A ready fallback pool for the LB tests (reused by the mismatch case).
		fbPool := &lbv1beta1.LoadBalancerPool{
			ObjectMeta: metav1.ObjectMeta{Name: "pool-policy-fb", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerPoolSpec{
				AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
				Origins: []lbv1beta1.LoadBalancerPoolOrigin{
					{Name: "o1", Address: "10.1.1.1", Enabled: new(true)},
				},
				Enabled: new(true),
			},
		}
		Expect(k8sClient.Create(ctx, fbPool)).To(Succeed())
		Eventually(func() string {
			var p lbv1beta1.LoadBalancerPool
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "pool-policy-fb", Namespace: lbPolicyNS}, &p); err != nil {
				return ""
			}
			return p.Status.ID
		}, lbEventuallyTimeout, lbPollInterval).ShouldNot(BeEmpty())

		hostname := "lb-own-match." + lbZoneName
		lb := &lbv1beta1.LoadBalancer{
			ObjectMeta: metav1.ObjectMeta{Name: "lb-own-match", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerSpec{
				ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
				Hostname:        hostname,
				DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-policy-fb"}},
				FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-policy-fb"},
				DeletePolicy:    DeletePolicyOwnOnly,
			},
		}
		Expect(k8sClient.Create(ctx, lb)).To(Succeed())
		Eventually(func() int {
			return lbMock.lbsWithName(hostname)
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

		Expect(k8sClient.Delete(ctx, lb)).To(Succeed())
		Eventually(func() int {
			return lbMock.lbsWithName(hostname)
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
	})

	It("loadbalancer: refuses to delete when the CF id differs from status.ID", func() {
		hostname := "lb-own-mismatch." + lbZoneName
		lb := &lbv1beta1.LoadBalancer{
			ObjectMeta: metav1.ObjectMeta{Name: "lb-own-mismatch", Namespace: lbPolicyNS},
			Spec: lbv1beta1.LoadBalancerSpec{
				ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
				Hostname:        hostname,
				DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-policy-fb"}},
				FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-policy-fb"},
				DeletePolicy:    DeletePolicyOwnOnly,
			},
		}
		Expect(k8sClient.Create(ctx, lb)).To(Succeed())
		Eventually(func() int {
			return lbMock.lbsWithName(hostname)
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal(1))

		lbMock.lbReplaceID(hostname, "mock-lb-external-recreated")

		Expect(k8sClient.Delete(ctx, lb)).To(Succeed())
		Eventually(func() bool {
			var l lbv1beta1.LoadBalancer
			return k8sClient.Get(ctx, types.NamespacedName{Name: "lb-own-mismatch", Namespace: lbPolicyNS}, &l) != nil
		}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())
		Expect(lbMock.lbsWithName(hostname)).To(Equal(1))
	})
})

// --- DryRun: a manager with DryRun=true must not write to CF. ---

var _ = Describe("LoadBalancing DryRun", Ordered, func() {
	var lbMock *lbMockServer

	BeforeAll(func() {
		lbMock = newLBMockServer()
		createLBFixtures(lbDryRunNS, false)
		startLBManager(lbDryRunNS, true, 2*time.Second, lbMock.URL(), false)
	})

	AfterAll(func() {
		lbMock.Close()
	})

	It("does not create the monitor in CF and reports reason DryRun", func() {
		marker := monitorMarkerFor(lbDryRunNS, "mon-dryrun")
		mon := &lbv1beta1.LoadBalancerMonitor{
			ObjectMeta: metav1.ObjectMeta{Name: "mon-dryrun", Namespace: lbDryRunNS},
			Spec: lbv1beta1.LoadBalancerMonitorSpec{
				AccountRef: lbv1beta1.AccountRef{Name: lbAccountName},
				Type:       "https",
			},
		}
		Expect(k8sClient.Create(ctx, mon)).To(Succeed())

		Eventually(func() string {
			var m lbv1beta1.LoadBalancerMonitor
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: "mon-dryrun", Namespace: lbDryRunNS}, &m); err != nil {
				return ""
			}
			c := readyCondition(m.Status.Conditions)
			if c == nil {
				return ""
			}
			return c.Reason
		}, lbEventuallyTimeout, lbPollInterval).Should(Equal("DryRun"))

		// Nothing was written to CF.
		Consistently(func() int {
			return lbMock.monitorsWithMarker(marker)
		}, 2*time.Second, lbPollInterval).Should(Equal(0))
	})
})
