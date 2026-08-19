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
	"maps"
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
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

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

	// lbUpdateCount counts LoadBalancer PUT (update) calls. Used by the TTL
	// regression test to assert an LB does not drift-loop (repeated PUTs) after
	// it has settled.
	lbUpdateCount int
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
	SteeringPolicy  string              `json:"steering_policy"`
	SessionAffinity string              `json:"session_affinity"`
	TTL             float64             `json:"ttl"`
	Description     string              `json:"description"`
	RegionPools     map[string][]string `json:"region_pools"`
	CountryPools    map[string][]string `json:"country_pools"`
	PopPools        map[string][]string `json:"pop_pools"`
	// Rules is an example of a Cloudflare-side field the operator does NOT model.
	// Under PATCH (partial edit) it must survive an operator update that touches
	// only the modeled fields. The operator never sends it.
	Rules json.RawMessage `json:"rules,omitempty"`
}

func newLBMockServer(accountID, zoneID, zoneName string) *lbMockServer {
	m := &lbMockServer{
		accountID: accountID,
		zoneID:    zoneID,
		zoneName:  zoneName,
		monitors:  make(map[string]mockMonitor),
		pools:     make(map[string]mockPool),
		lbs:       make(map[string]mockLB),
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

// applyPatch models Cloudflare's PATCH (partial edit): only the top-level keys
// PRESENT in the request body are applied onto the existing record; every other
// field (including fields the operator does not model) survives untouched. It
// merges at top-level field granularity -- a present object/array key replaces
// that field wholesale, matching how the operator always sends whole
// origins/pool-map values. existing and out are the same mock record type.
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
	maps.Copy(merged, patch)
	mergedBytes, err := json.Marshal(merged)
	if err != nil {
		return err
	}
	return json.Unmarshal(mergedBytes, out)
}

// --- account ---

func (m *lbMockServer) handleAccountGet(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := r.PathValue("accountID")
	if id != m.accountID {
		writeCFError(w, http.StatusNotFound, "account not found")
		return
	}
	writeResult(w, http.StatusOK, map[string]any{"id": id, "name": "Test Account", "type": "standard"})
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

	lbAccountID    = "a1111111111111111111111111111111"
	lbBadAccountID = "b2222222222222222222222222222222"
	lbZoneID       = "c3333333333333333333333333333333"
	lbZoneName     = "lb.example.com"

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
func startLBManager(ns string, dryRun bool, requeue time.Duration, baseURL string) {
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
	}).SetupWithManager(mgr)).To(Succeed())

	Expect((&LoadBalancerReconciler{
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

	Expect(k8sClient.Create(ctx, &lbv1beta1.Account{
		ObjectMeta: metav1.ObjectMeta{Name: lbAccountName, Namespace: ns},
		Spec: lbv1beta1.AccountSpec{
			ID:             lbAccountID,
			CredentialsRef: lbv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
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
		lbMock = newLBMockServer(lbAccountID, lbZoneID, lbZoneName)
		createLBFixtures(lbTestNS, true)

		// A second, deliberately-invalid Account for the validation-failure case.
		Expect(k8sClient.Create(ctx, &lbv1beta1.Account{
			ObjectMeta: metav1.ObjectMeta{Name: "lb-account-bad", Namespace: lbTestNS},
			Spec: lbv1beta1.AccountSpec{
				ID:             lbBadAccountID,
				CredentialsRef: lbv1beta1.SecretRef{Name: lbSecretName, Key: "apiToken"},
			},
		})).To(Succeed())

		startLBManager(lbTestNS, false, 2*time.Second, lbMock.URL())
	})

	AfterAll(func() {
		lbMock.Close()
	})

	// -- Scenario 1: Account validation --
	Context("Account validation", func() {
		It("marks a valid Account Initialized=True", func() {
			Eventually(func() bool {
				var a lbv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: lbAccountName, Namespace: lbTestNS}, &a); err != nil {
					return false
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				return c != nil && c.Status == metav1.ConditionTrue
			}, lbEventuallyTimeout, lbPollInterval).Should(BeTrue())

			var a lbv1beta1.Account
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: lbAccountName, Namespace: lbTestNS}, &a)).To(Succeed())
			Expect(a.Status.Name).To(Equal("Test Account"))
		})

		It("marks an unknown Account Initialized=False with ValidationFailed", func() {
			Eventually(func() string {
				var a lbv1beta1.Account
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lb-account-bad", Namespace: lbTestNS}, &a); err != nil {
					return ""
				}
				c := apimeta.FindStatusCondition(a.Status.Conditions, conditionInitialized)
				if c == nil || c.Status != metav1.ConditionFalse {
					return ""
				}
				return c.Reason
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal("ValidationFailed"))
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
		})

		It("adopts a pre-existing CF monitor by its marker without creating a duplicate", func() {
			marker := monitorMarkerFor(lbTestNS, "mon-adopt")
			// Seed a monitor that matches the CR defaults so no drift correction
			// fires -- this keeps the assertion focused on adoption.
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
					RegionPools:     map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-k-a"}}},
					CountryPools:    map[string][]lbv1beta1.LoadBalancerPoolRef{"US": {{Name: "pool-k-a"}}},
					PopPools:        map[string][]lbv1beta1.LoadBalancerPoolRef{"LAX": {{Name: "pool-k-a"}}},
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
			current.Spec.RegionPools = map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-k-b"}}}
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

	// -- Keyed pools cleared declaratively on ref removal (always-send {}) --
	Context("Keyed pools removal", func() {
		It("clears region_pools when the CR drops the mapping", func() {
			hostname := "lb-keyclear." + lbZoneName
			lb := &lbv1beta1.LoadBalancer{
				ObjectMeta: metav1.ObjectMeta{Name: "lb-keyclear", Namespace: lbTestNS},
				Spec: lbv1beta1.LoadBalancerSpec{
					ZoneRef:         lbv1beta1.ZoneRef{Name: lbZoneCRName},
					Hostname:        hostname,
					DefaultPoolRefs: []lbv1beta1.LoadBalancerPoolRef{{Name: "pool-fb"}},
					FallbackPoolRef: lbv1beta1.LoadBalancerPoolRef{Name: "pool-fb"},
					SteeringPolicy:  "geo",
					RegionPools:     map[string][]lbv1beta1.LoadBalancerPoolRef{"WNAM": {{Name: "pool-fb"}}},
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

			// Drop the mapping entirely -> operator sends region_pools:{} on edit.
			var current lbv1beta1.LoadBalancer
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lb-keyclear", Namespace: lbTestNS}, &current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.RegionPools = nil
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())

			Eventually(func() int {
				r, ok := lbMock.lbByName(hostname)
				if !ok {
					return -1
				}
				return len(r.RegionPools)
			}, lbEventuallyTimeout, lbPollInterval).Should(Equal(0))
			// Stays cleared -- no drift-loop re-adding it.
			Consistently(func() int {
				r, _ := lbMock.lbByName(hostname)
				return len(r.RegionPools)
			}, 3*time.Second, lbPollInterval).Should(Equal(0))
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

			// Remove the ref -> operator sends monitor:"" on edit -> CF detaches.
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

// --- DeletePolicy own-only: needs a manager with no periodic self-requeue so a
// status.ID vs CF-id mismatch stays stable long enough to delete against. ---

var _ = Describe("LoadBalancing DeletePolicy own-only", Ordered, func() {
	var lbMock *lbMockServer

	BeforeAll(func() {
		lbMock = newLBMockServer(lbAccountID, lbZoneID, lbZoneName)
		createLBFixtures(lbPolicyNS, true)
		// Long requeue: reconciles are driven by create/update/delete events
		// only, so a settled resource's status.ID does not get re-adopted between
		// mutating the mock and deleting the CR.
		startLBManager(lbPolicyNS, false, 10*time.Minute, lbMock.URL())
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
		lbMock = newLBMockServer(lbAccountID, lbZoneID, lbZoneName)
		createLBFixtures(lbDryRunNS, false)
		startLBManager(lbDryRunNS, true, 2*time.Second, lbMock.URL())
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
