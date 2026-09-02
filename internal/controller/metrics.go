package controller

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/prometheus/client_golang/prometheus"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Cloudflare API label values for cfAPICallDuration and cfAPIErrorsByCode.
// Shared strings referenced in >1 place; single-use strings stay as literals.
const (
	cfResourceCustomHostname   = "customhostname"
	cfResourceZone             = "zone"
	cfResourceLoadBalancerMon  = "loadbalancermonitor"
	cfResourceLoadBalancerPool = "loadbalancerpool"
	cfResourceLoadBalancer     = "loadbalancer"
	cfResourceAccount          = "account"

	cfOpGet      = "get"
	cfOpList     = "list"
	cfOpAdopt    = "adopt"
	cfOpCreate   = "create"
	cfOpRecreate = "recreate"
	cfOpUpdate   = "update"
	cfOpDelete   = "delete"
	// cfOpHealth labels the opt-in pool-health poll (PoolHealth.Get) on the shared
	// api_errors_by_code / api_duration / api_retries families, keeping the health
	// axis distinguishable from the sync operations above. Emitted only when
	// --enable-pool-health is set.
	cfOpHealth = "health"

	driftSourceCFList  = "cf_list"
	driftSourceK8sList = "k8s_list"
)

// Prometheus metric label KEYS. These names are shared across the metric vector
// definitions (the []string{...} label sets) and the call sites that reference
// them (prometheus.Labels{...} and lbStateGauge.ownerLabel), so each label name
// has a single source of truth.
const (
	labelResource  = "resource"
	labelAccountCR = "account_cr"
	labelZoneCR    = "zone_cr"
	labelPoolCR    = "pool_cr"
	labelState     = "state"
	labelStatus    = "status"
	labelRegion    = "region"
	labelOrigin    = "origin"
	labelOperation = "operation"
)

var (
	// operationsTotal counts successful CF operations by resource and type.
	// The same vocabulary applies to CustomHostname and the load-balancing resources:
	// "adopt" = existing resource found and tracked; "create" = first-time provisioning;
	// "recreate" = recovery after external deletion; "update" = drift correction;
	// "delete" = removal.
	// Pre-initialized for all known values so all series appear at startup.
	operationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_operations_total",
		Help: "Total number of successful Cloudflare operations by resource and type.",
	}, []string{labelResource, labelOperation})

	// sslProvisioningDuration records the time from CF hostname creation to ssl.status == active.
	// Labels: zone_cr (Zone CR name), hostname (the custom hostname), method (DCV method).
	// Set once per provisioning cycle; reset on recreation so it reflects the latest cycle.
	// Gauge instead of histogram: SSL provisioning is a rare event (zero to few per week),
	// making histogram rate/increase queries ineffective with GMP's cumulative counter semantics.
	// The hostname label gives per-hostname visibility (1 series each vs 14 with histogram).
	sslProvisioningDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_ssl_provisioning_duration_seconds",
		Help: "Time from Cloudflare hostname creation to ssl.status becoming active, by zone CR, hostname, and DCV method.",
	}, []string{labelZoneCR, "hostname", "method"})

	// customHostnames counts CustomHostname CRs by zone and state.
	// States are mutually exclusive: conflict > ready > unhealthy > pending.
	// Sum across states equals total CRs in that zone.
	customHostnames = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_customhostnames",
		Help: "Number of CustomHostname CRs by zone CR and state (ready, pending, unhealthy, conflict).",
	}, []string{labelZoneCR, labelState})

	// hostnameStatus counts managed Cloudflare custom hostnames by zone CR and CF
	// activation status (active, pending, active_redeploying, blocked, moved, deleted, etc.).
	// Only includes hostnames with an associated CR (not orphans).
	hostnameStatusGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_customhostname_status",
		Help: "Number of managed Cloudflare custom hostnames by zone CR and CF activation status.",
	}, []string{labelZoneCR, labelStatus})

	// zoneCustomHostnames counts Cloudflare custom hostnames by zone CR and type.
	// managed: hostname has an associated CR; orphan: hostname has no associated CR.
	// type=total is the CF quota usage for that zone.
	zoneCustomHostnames = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_customhostnames",
		Help: "Number of Cloudflare custom hostnames by zone CR and type (managed, orphan, drifted, total).",
	}, []string{labelZoneCR, "type"})

	// zoneInitialized is 1 after the Zone CR has been initialized (zone name resolved
	// from Cloudflare API), 0 otherwise. Set once on first successful zone GET; not
	// toggled on transient failures. Labeled by zone_cr (the Zone CR name).
	zoneInitialized = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_initialized",
		Help: "1 if Zone CR has been initialized (zone name resolved from Cloudflare API), 0 otherwise.",
	}, []string{labelZoneCR})

	// accountInitialized is 1 after the Account CR has been validated (credentials
	// confirmed against the Cloudflare account), 0 otherwise. The load-balancing
	// analog of zoneInitialized; set from the Account controller and labeled by
	// account_cr (the Account CR name).
	accountInitialized = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_account_initialized",
		Help: "1 if Account CR has been initialized (credentials validated against the Cloudflare account), 0 otherwise.",
	}, []string{labelAccountCR})

	// loadBalancers counts LoadBalancer CRs by owning zone CR and state. States are
	// mutually exclusive (see lbReadyState); the sum across states equals the total
	// LoadBalancer CRs referencing that zone. The load-balancing analog of
	// customHostnames. LoadBalancers are zone-scoped, so the owner label is zone_cr.
	// The "partial" state is LB-only (an LB serving with some pool refs unresolved);
	// the pool and monitor gauges never emit it.
	loadBalancers = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancers",
		Help: "Number of LoadBalancer CRs by zone CR and state (ready, partial, waiting, dryrun, error).",
	}, []string{labelZoneCR, labelState})

	// loadBalancerNetworksDrift counts LoadBalancer CRs whose spec.networks diverges
	// from the Cloudflare-observed networks, by owning zone CR. Networks are enforced
	// only at create (Cloudflare's Edit API does not accept networks), so post-create
	// divergence is surfaced -- not auto-corrected -- here, on the NetworksSynced
	// status condition, and via a log line on the transition into drift. A value of 0
	// (or an absent series) means every LB for that zone is in sync. Recomputed each
	// reconcile from the NetworksSynced conditions, mirroring the loadBalancers state
	// gauge; stale owners are removed when their last LB is deleted.
	loadBalancerNetworksDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancer_networks_drift",
		Help: "Number of LoadBalancer CRs whose spec.networks diverges from Cloudflare, by zone CR (create-enforced; drift is surfaced, not auto-corrected).",
	}, []string{labelZoneCR})

	// loadBalancerPools counts LoadBalancerPool CRs by owning account CR and state.
	// Pools are account-scoped, so the owner label is account_cr. Sum across states
	// equals the total pools referencing that account.
	loadBalancerPools = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancerpools",
		Help: "Number of LoadBalancerPool CRs by account CR and state (ready, waiting, dryrun, error).",
	}, []string{labelAccountCR, labelState})

	// loadBalancerMonitors counts LoadBalancerMonitor CRs by owning account CR and
	// state. Monitors are account-scoped leaf resources: they do not wait on another
	// CR, but they can still enter the "waiting" state under managementPolicy=observe
	// (WaitingForExternal), where the operator waits for the monitor to appear in
	// Cloudflare instead of creating it. The state label set is kept uniform across
	// all three load-balancing gauges for consistency.
	loadBalancerMonitors = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancermonitors",
		Help: "Number of LoadBalancerMonitor CRs by account CR and state (ready, waiting, dryrun, error).",
	}, []string{labelAccountCR, labelState})

	// ---- Pool health (opt-in runtime axis) --------------------------------
	//
	// The four gauges below are the CF-side health view of a pool, populated only
	// when --enable-pool-health is set. They are an INDEPENDENT axis from the
	// loadbalancerpools sync-state gauge above: sync tracks whether the operator
	// has reconciled the CR to Cloudflare; these track what Cloudflare's health
	// checks observe. Value semantics are threshold-free -- raw CF health booleans
	// tallied by region so a consumer derives fully/partial/down in PromQL. Series
	// appear only when Set (which only happens under the flag), so an operator
	// without the flag surfaces no health series. See pool_health.go for the decode,
	// tally, and stale-series cleanup.

	// loadBalancerPoolHealth counts, per pool, how many of the regions Cloudflare
	// health-checked are in each status (healthy, unhealthy, unknown). The sum
	// across statuses equals the number of regions checked. Emitted for every
	// polled pool -- including pools with spec.checkRegions unset (all-DC probing),
	// whose region set is unbounded and therefore only summarized here, not labeled
	// per region. This is the bounded baseline (3 series per pool) and encodes the
	// unknown status (a region Cloudflare reports as indeterminate).
	loadBalancerPoolHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancerpool_health",
		Help: "Number of Cloudflare-checked regions for a pool in each health status (healthy, unhealthy, unknown), by account CR and pool CR. Opt-in (--enable-pool-health).",
	}, []string{labelAccountCR, labelPoolCR, labelStatus})

	// loadBalancerPoolHealthRegion reports a pool's per-region health status,
	// emitted ONLY for pools with spec.checkRegions set (a CR-declared, bounded
	// dimension of at most 13 regions). For each declared region exactly one status
	// series holds 1 and the others 0, so the sum per region is always 1. A pool
	// with checkRegions unset probes from every data center (an unbounded region
	// set), so it gets no per-region series -- only the summarized
	// loadBalancerPoolHealth above.
	loadBalancerPoolHealthRegion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancerpool_health_region",
		Help: "Per-region health status of a pool (1 for the current status, 0 otherwise), by account CR, pool CR, region, and status. Emitted only for pools with spec.checkRegions set. Opt-in (--enable-pool-health).",
	}, []string{labelAccountCR, labelPoolCR, labelRegion, labelStatus})

	// loadBalancerPoolOriginHealth counts, per origin, how many of the regions
	// Cloudflare health-checked report that origin in each status. The origin label
	// is the origin ADDRESS as observed in the health response; two origins sharing
	// an address collapse to one series (documented caveat). Emitted for every
	// polled pool, the origin-level analog of loadBalancerPoolHealth.
	loadBalancerPoolOriginHealth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancerpool_origin_health",
		Help: "Number of Cloudflare-checked regions reporting an origin in each health status (healthy, unhealthy, unknown), by account CR, pool CR, origin address, and status. Opt-in (--enable-pool-health).",
	}, []string{labelAccountCR, labelPoolCR, labelOrigin, labelStatus})

	// loadBalancerPoolOriginHealthRegion reports an origin's per-region health
	// status, emitted ONLY for pools with spec.checkRegions set. For each
	// (origin, declared region) exactly one status series holds 1 and the others 0.
	// The origin label is the origin address (same same-address caveat as
	// loadBalancerPoolOriginHealth).
	loadBalancerPoolOriginHealthRegion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_loadbalancerpool_origin_health_region",
		Help: "Per-region health status of an origin (1 for the current status, 0 otherwise), by account CR, pool CR, origin address, region, and status. Emitted only for pools with spec.checkRegions set. Opt-in (--enable-pool-health).",
	}, []string{labelAccountCR, labelPoolCR, labelOrigin, labelRegion, labelStatus})

	// cfAPICallDuration observes Cloudflare API call latency by resource and operation.
	// resource: "customhostname" or "zone"; also "account", "loadbalancer", "loadbalancerpool",
	// "loadbalancermonitor" when the control-plane role is enabled. operation: get, list, create, update, delete.
	// For "list", the duration spans all paginated HTTP calls (N data pages + 1 end marker),
	// so observations can exceed the per-request WithRequestTimeout. Buckets cover both
	// single-call operations (<5s) and multi-page list totals (up to 20s).
	cfAPICallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_api_duration_seconds",
		Help:    "Cloudflare API call duration in seconds, by resource and operation.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 7.5, 10, 15, 20},
	}, []string{labelResource, labelOperation})

	// cfAPIErrorsByCode counts Cloudflare API errors by resource, operation, and HTTP status code.
	// Only appears in /metrics when errors have occurred.
	cfAPIErrorsByCode = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_errors_by_code_total",
		Help: "Cloudflare API errors by resource, operation, and HTTP status code.",
	}, []string{labelResource, labelOperation, "status_code"})

	// driftBufferDepth reports the current number of items in a drift event channel
	// at the end of each zone reconcile cycle. Labeled by resource type. Approaching
	// --drift-buffer capacity indicates the worker controller is not draining fast enough.
	driftBufferDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_drift_buffer_depth",
		Help: "Current number of items in the drift event channel buffer, by resource type.",
	}, []string{labelResource})

	// driftBufferOverflowTotal counts how many times a drift event send blocked
	// because the channel buffer was full. Labeled by resource type. Non-zero means
	// the zone controller stalled waiting for the worker controller to drain events.
	driftBufferOverflowTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_drift_buffer_overflow_total",
		Help: "Number of times the drift event buffer was full, by resource type.",
	}, []string{labelResource})

	// driftDetectionErrorsTotal counts drift detection failures by resource type and
	// error source. source=cf_list: CF API list call failed. source=k8s_list: k8s CR
	// list call failed.
	driftDetectionErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_drift_detection_errors_total",
		Help: "Number of drift detection failures, by resource type and error source.",
	}, []string{labelResource, "source"})

	// cfAPIRetriesTotal counts retry attempts for single (non-paginated) CF API calls.
	// Incremented per retry attempt (attempt > 0). Non-zero means first attempts are
	// failing and retries are absorbing transient CF API slowdowns.
	cfAPIRetriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_retries_total",
		Help: "Number of retry attempts for Cloudflare API calls, by resource and operation.",
	}, []string{labelResource, labelOperation})

	// buildInfoGauge exposes the operator's build identity as a constant 1-valued
	// series, labeled by version and commit -- the standard *_build_info pattern, so
	// dashboards and alerts can display or group by the running build. Set once at
	// startup via SetBuildInfo; the values come from cmd/main's -ldflags-injected
	// build identity. Cardinality is 1 (version and commit are fixed per build).
	buildInfoGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_build_info",
		Help: "Operator build identity; constant 1, labeled by version and commit.",
	}, []string{"version", "commit"})
)

func init() {
	crtlmetrics.Registry.MustRegister(
		operationsTotal,
		sslProvisioningDuration,
		customHostnames,
		hostnameStatusGauge,
		zoneCustomHostnames,
		zoneInitialized,
		buildInfoGauge,
		accountInitialized,
		loadBalancers,
		loadBalancerNetworksDrift,
		loadBalancerPools,
		loadBalancerMonitors,
		loadBalancerPoolHealth,
		loadBalancerPoolHealthRegion,
		loadBalancerPoolOriginHealth,
		loadBalancerPoolOriginHealthRegion,
		cfAPICallDuration,
		cfAPIErrorsByCode,
		cfAPIRetriesTotal,
		driftBufferDepth,
		driftBufferOverflowTotal,
		driftDetectionErrorsTotal,
	)
	// NOTE: CustomHostname/Zone and LoadBalancer-family series are NOT pre-initialized
	// here -- each family's label pre-seeding is gated behind its feature flag via
	// PreInitCustomHostnameMetrics (--enable-customhostname) and
	// PreInitLoadBalancerMetrics (--enable-loadbalancing), so an operator surfaces
	// only the metric series for the features it enables. The owner-keyed gauges
	// (customHostnames, zoneInitialized, accountInitialized, loadBalancers,
	// loadBalancerNetworksDrift, loadBalancerPools, loadBalancerMonitors) are not
	// pre-seeded at all -- their zone_cr / account_cr label values are dynamic, so
	// they are populated at reconcile time by the controllers that own them. The
	// pool-health gauges (loadBalancerPoolHealth and friends) and the operation
	// "health" series on the shared api_* families are likewise never pre-seeded:
	// they are opt-in (--enable-pool-health) with dynamic labels, so they surface
	// only once a pool is actually polled, keeping the off path free of any series.
}

// PreInitCustomHostnameMetrics pre-initializes the CustomHostname and Zone metric
// series so dashboards can plot from t=0 without a "no data" gap. Called from main
// only when --enable-customhostname is set; the metric vectors themselves are
// already registered in init(). Kept out of init() so an operator with custom
// hostname management disabled (e.g. a pure load-balancing control cluster) never
// surfaces these series. Zone series are pre-seeded here rather than under the
// load-balancing gate: the Zone controller's only bulk Cloudflare interaction is
// the custom hostname drift list, which is custom-hostname-specific.
func PreInitCustomHostnameMetrics() {
	for _, op := range []string{cfOpList, cfOpGet, cfOpCreate, cfOpUpdate, cfOpDelete} {
		cfAPICallDuration.WithLabelValues(cfResourceCustomHostname, op)
	}
	cfAPICallDuration.WithLabelValues(cfResourceZone, cfOpGet)
	for _, op := range []string{cfOpAdopt, cfOpCreate, cfOpRecreate, cfOpUpdate, cfOpDelete} {
		operationsTotal.WithLabelValues(cfResourceCustomHostname, op)
	}
	// Pre-initialize retry counters for single-call operations.
	for _, op := range []string{cfOpGet, cfOpCreate, cfOpUpdate, cfOpDelete} {
		cfAPIRetriesTotal.WithLabelValues(cfResourceCustomHostname, op)
	}
	cfAPIRetriesTotal.WithLabelValues(cfResourceZone, cfOpGet)
}

// PreInitLoadBalancerMetrics pre-initializes the load-balancing metric series
// (LoadBalancer / LoadBalancerPool / LoadBalancerMonitor / Account) so
// dashboards can plot from t=0 without a "no data" gap. Called from main only
// when --enable-loadbalancing is set; the metric vectors themselves are already
// registered in init(). Kept out of init() so a per-cluster operator (load
// balancing disabled) never surfaces these series.
func PreInitLoadBalancerMetrics() {
	// LoadBalancer / Pool / Monitor are located by a paginated list (find-by-name /
	// find-by-hostname) wrapped in cfRetry and recorded as op=list, not a point get,
	// so op=get is not pre-seeded for them (only Account does a point get, below). The
	// find (list) and the single-call write ops (create/update/delete) retry through
	// cfRetry, so their retry series are pre-seeded.
	for _, res := range []string{cfResourceLoadBalancerMon, cfResourceLoadBalancerPool, cfResourceLoadBalancer} {
		for _, op := range []string{cfOpList, cfOpCreate, cfOpUpdate, cfOpDelete} {
			cfAPICallDuration.WithLabelValues(res, op)
		}
		for _, op := range []string{cfOpAdopt, cfOpCreate, cfOpRecreate, cfOpUpdate, cfOpDelete} {
			operationsTotal.WithLabelValues(res, op)
		}
		for _, op := range []string{cfOpList, cfOpCreate, cfOpUpdate, cfOpDelete} {
			cfAPIRetriesTotal.WithLabelValues(res, op)
		}
	}
	// Account is validated with a single get.
	cfAPICallDuration.WithLabelValues(cfResourceAccount, cfOpGet)
	cfAPIRetriesTotal.WithLabelValues(cfResourceAccount, cfOpGet)
}

// SetBuildInfo publishes the operator's build identity as the cf_edge_operator_build_info
// gauge (constant value 1), labeled by version and commit. Called once at startup from
// main with the -ldflags-injected build values.
func SetBuildInfo(version, commit string) {
	buildInfoGauge.WithLabelValues(version, commit).Set(1)
}

// recordCFCall records duration and any error for a Cloudflare API call.
// Use cfResource* and cfOp* constants for resource and operation.
// Call at the call site: defer recordCFCall(cfResourceCustomHostname, cfOpCreate, time.Now(), &err)
func recordCFCall(resource, operation string, start time.Time, err *error) {
	cfAPICallDuration.WithLabelValues(resource, operation).Observe(time.Since(start).Seconds())
	if err != nil && *err != nil {
		statusCode := "unknown"
		if errors.Is(*err, context.DeadlineExceeded) {
			statusCode = "timeout"
		} else if errors.Is(*err, context.Canceled) {
			statusCode = "canceled"
		} else {
			if cfErr, ok := errors.AsType[*cloudflare.Error](*err); ok {
				statusCode = strconv.Itoa(cfErr.StatusCode)
			}
		}
		cfAPIErrorsByCode.WithLabelValues(resource, operation, statusCode).Inc()
	}
}

// cfRetry retries a single (non-paginated) CF API call with immediate retry (no backoff).
// The callback fn should call recordCFCall internally so each attempt records metrics.
// Returns the final error and the number of attempts made (1 = no retry, 2 = one retry, etc.).
// Skips retry on 429 (rate limit) and context cancellation.
func cfRetry(ctx context.Context, resource, operation string, maxRetries int, fn func() error) (int, error) {
	var err error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Check context before retrying -- don't waste an attempt on a cancelled context.
			if ctx.Err() != nil {
				return attempt, err
			}
			cfAPIRetriesTotal.WithLabelValues(resource, operation).Inc()
		}
		err = fn()
		if err == nil {
			return attempt + 1, nil
		}
		// Don't retry 429 (rate limit) -- immediate retry will likely get another 429.
		// Let controller-runtime backoff handle it.
		var cfErr *cloudflare.Error
		if errors.As(err, &cfErr) && cfErr.StatusCode == 429 {
			return attempt + 1, err
		}
	}
	return maxRetries + 1, err
}

// cfCreateGuarded creates a Cloudflare resource with a duplicate-safe retry.
// Cloudflare load-balancer pools and monitors have no server-side name
// uniqueness, so a create that succeeds on Cloudflare but whose response times
// out on the client would be duplicated by a blind retry (cfRetry). Before each
// retry this re-lists via find and adopts any resource the prior attempt may
// have created, instead of creating a second one.
//
//   - find returns (resource, nil) if a matching resource exists, (nil, nil) if
//     none exists, or (nil, err) if existence can't be determined.
//   - create performs the Cloudflare create call (and should record metrics via
//     recordCFCall internally, mirroring cfRetry's contract).
//
// Returns the resource, whether it was adopted (recovered via find rather than
// freshly created), the number of create attempts made (1 = no retry), and any
// error. If find errors during a retry, the retry is abandoned -- returning the
// original create error rather than risking a duplicate; the next reconcile
// re-lists and adopts. Skips retry on 429, matching cfRetry.
func cfCreateGuarded[T any](
	ctx context.Context, resource string, maxRetries int,
	find func() (*T, error), create func() (*T, error),
) (result *T, adopted bool, attempts int, err error) {
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			if ctx.Err() != nil {
				return nil, false, attempt, err
			}
			existing, findErr := find()
			if findErr != nil {
				// Can't confirm whether the prior attempt landed on Cloudflare;
				// abandon the retry to avoid a duplicate.
				return nil, false, attempt, err
			}
			if existing != nil {
				return existing, true, attempt, nil
			}
			cfAPIRetriesTotal.WithLabelValues(resource, cfOpCreate).Inc()
		}
		result, err = create()
		if err == nil {
			return result, false, attempt + 1, nil
		}
		var cfErr *cloudflare.Error
		if errors.As(err, &cfErr) && cfErr.StatusCode == 429 {
			return nil, false, attempt + 1, err
		}
	}
	return nil, false, maxRetries + 1, err
}

// sslProvisioningTTL is how long the SSL provisioning gauge stays in /metrics after
// being set. Long enough for GMP to scrape it (60s interval), short enough to avoid
// unbounded cardinality growth from the per-hostname label.
const sslProvisioningTTL = 3 * time.Minute

// sslProvisioningEntry tracks an SSL provisioning gauge value with an expiration time.
type sslProvisioningEntry struct {
	zoneCR    string
	hostname  string
	method    string
	expiresAt time.Time
}

// sslProvisioningCache tracks active SSL provisioning gauge entries for TTL-based cleanup.
// The CH controller sets entries via setSSLProvisioningDuration; the zone controller
// cleans expired entries via cleanExpiredSSLProvisioning on each drift cycle.
//
// Protected by mutex rather than sync.Map: type safety, consistent iteration,
// and simpler reasoning. Contention is negligible (written on SSL provisioning,
// read every 30s during drift cleanup).
var (
	sslProvisioningMu    sync.Mutex
	sslProvisioningCache = map[string]sslProvisioningEntry{}
)

// setSSLProvisioningDuration sets the SSL provisioning gauge and registers it for
// TTL-based cleanup. Called by the CH controller when ssl.status transitions to active.
func setSSLProvisioningDuration(zoneCR, hostname, method string, duration time.Duration) {
	sslProvisioningDuration.WithLabelValues(zoneCR, hostname, method).Set(duration.Seconds())
	key := zoneCR + "/" + hostname
	sslProvisioningMu.Lock()
	// If re-provisioning changed the DCV method, delete the old gauge series
	// to avoid a stale series with the previous method label.
	if prev, ok := sslProvisioningCache[key]; ok && prev.method != method {
		sslProvisioningDuration.DeleteLabelValues(prev.zoneCR, prev.hostname, prev.method)
	}
	sslProvisioningCache[key] = sslProvisioningEntry{
		zoneCR:    zoneCR,
		hostname:  hostname,
		method:    method,
		expiresAt: time.Now().Add(sslProvisioningTTL),
	}
	sslProvisioningMu.Unlock()
}

// cleanExpiredSSLProvisioning removes expired SSL provisioning gauge entries.
// Called by the zone controller at the end of each drift cycle.
func cleanExpiredSSLProvisioning() {
	now := time.Now()
	sslProvisioningMu.Lock()
	defer sslProvisioningMu.Unlock()
	for key, entry := range sslProvisioningCache {
		if now.After(entry.expiresAt) {
			sslProvisioningDuration.DeleteLabelValues(entry.zoneCR, entry.hostname, entry.method)
			delete(sslProvisioningCache, key)
		}
	}
}

// ---- Load-balancing aggregate state gauges -----------------------------

// Load-balancing CR state label values for the loadbalancers /
// loadbalancerpools / loadbalancermonitors gauges. Mutually exclusive; the sum
// across the states equals the total CRs for that owner. Derived from the Ready
// condition by lbReadyState.
const (
	lbStateReady   = "ready"   // Ready=True (reason Reconciled): synchronized with Cloudflare
	lbStatePartial = "partial" // Ready=True (reason Partial): LB-only, serving with >=1 pool ref unresolved
	lbStateWaiting = "waiting" // Ready=False, soft wait on a dependency (no error counted)
	lbStateDryRun  = "dryrun"  // Ready=False, writes suppressed by --dry-run
	lbStateError   = "error"   // Ready=False, a reconcile failure (a setError reason)
)

// lbStateLabels is the state set published for the pool and monitor gauges. Every
// state is always written (0 or positive) so a state that empties reads 0 rather
// than leaving a stale series, and alerts always have a series to match. It omits
// "partial", which only a LoadBalancer can reach: pools and monitors never set
// reason Partial, so their gauges must never emit a partial series (always-0),
// mirroring how each gauge seeds only the states its resource can actually enter.
var lbStateLabels = []string{lbStateReady, lbStateWaiting, lbStateDryRun, lbStateError}

// lbStateLabelsLB is the state set published for the loadbalancers gauge -- the
// pool/monitor set plus the LB-only "partial" state.
var lbStateLabelsLB = []string{lbStateReady, lbStatePartial, lbStateWaiting, lbStateDryRun, lbStateError}

// lbStateGauge wraps a load-balancing aggregate state gauge with the bookkeeping
// to rebuild it in place from the full CR set on every reconcile, without
// leaving stale series.
//
// Load balancing has no Zone-style coordinator: each LoadBalancer / Pool /
// Monitor controller reconciles a single CR and self-requeues (RequeueInterval).
// So each controller recomputes its own owner -> state counts from its full
// (cache-backed) CR list on every reconcile and calls set(), mirroring how the
// Zone controller recomputes customHostnames each drift cycle.
//
// set() publishes in place -- it Sets each state for every present owner rather
// than Reset()ing the vector, so a concurrent /metrics scrape never observes a
// half-rebuilt gauge (matching customHostnames, which also Sets in place). An
// owner whose last CR was deleted is dropped via DeletePartialMatch; this is the
// load-balancing analog of the Zone controller's customHostnames.DeletePartialMatch
// on Zone deletion. There is no owner-deletion event to hook here (the zone_cr /
// account_cr owner is not a CR this controller watches), so vanished owners are
// found by diffing against the previously published owner set.
//
// Each gauge has exactly one writer controller whose reconciles are serialized
// (MaxConcurrentReconciles defaults to 1), so the only concurrent access is the
// scrape goroutine, which never touches prevOwners. The mutex guards prevOwners
// for defense in depth and documents the shared state.
type lbStateGauge struct {
	gauge       *prometheus.GaugeVec
	ownerLabel  string
	stateLabels []string
	mu          sync.Mutex
	prevOwners  map[string]bool
}

var (
	lbGaugeLoadBalancer = &lbStateGauge{gauge: loadBalancers, ownerLabel: labelZoneCR, stateLabels: lbStateLabelsLB, prevOwners: map[string]bool{}}
	lbGaugePool         = &lbStateGauge{gauge: loadBalancerPools, ownerLabel: labelAccountCR, stateLabels: lbStateLabels, prevOwners: map[string]bool{}}
	lbGaugeMonitor      = &lbStateGauge{gauge: loadBalancerMonitors, ownerLabel: labelAccountCR, stateLabels: lbStateLabels, prevOwners: map[string]bool{}}
)

// set publishes freshly computed owner -> state -> count values. Every present
// owner gets each of the gauge's state series seeded (0 or positive); owners
// published on a prior call but absent now have their series removed. counts holds
// only owners that currently have at least one CR.
func (g *lbStateGauge) set(counts map[string]map[string]int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for owner, states := range counts {
		for _, s := range g.stateLabels {
			g.gauge.WithLabelValues(owner, s).Set(float64(states[s]))
		}
	}
	for owner := range g.prevOwners {
		if _, ok := counts[owner]; !ok {
			g.gauge.DeletePartialMatch(prometheus.Labels{g.ownerLabel: owner})
		}
	}
	next := make(map[string]bool, len(counts))
	for owner := range counts {
		next[owner] = true
	}
	g.prevOwners = next
}

// lbNetworksDriftGauge tracks the owner set previously published to
// loadBalancerNetworksDrift so a zone whose last LoadBalancer was deleted has its
// series removed, mirroring lbStateGauge's stale-owner cleanup. This gauge is
// written only by the LoadBalancer controller's (serialized) recompute; the mutex
// guards prevOwners against the concurrent scrape goroutine.
var lbNetworksDriftGauge = struct {
	mu         sync.Mutex
	prevOwners map[string]bool
}{prevOwners: map[string]bool{}}

// setNetworksDriftGauge publishes per-zone counts of networks-drifted LoadBalancer
// CRs and removes series for zones no longer present. counts must include every
// zone that currently has at least one LB (value 0 when none are drifted) so a
// synced zone reads 0 rather than leaving a stale series. It is the networks-drift
// analog of lbStateGauge.set.
func setNetworksDriftGauge(counts map[string]int) {
	lbNetworksDriftGauge.mu.Lock()
	defer lbNetworksDriftGauge.mu.Unlock()
	for owner, n := range counts {
		loadBalancerNetworksDrift.WithLabelValues(owner).Set(float64(n))
	}
	for owner := range lbNetworksDriftGauge.prevOwners {
		if _, ok := counts[owner]; !ok {
			loadBalancerNetworksDrift.DeleteLabelValues(owner)
		}
	}
	next := make(map[string]bool, len(counts))
	for owner := range counts {
		next[owner] = true
	}
	lbNetworksDriftGauge.prevOwners = next
}
