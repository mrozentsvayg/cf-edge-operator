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
	cfResourceCustomHostname = "customhostname"
	cfResourceZone           = "zone"

	cfOpGet      = "get"
	cfOpList     = "list"
	cfOpAdopt    = "adopt"
	cfOpCreate   = "create"
	cfOpRecreate = "recreate"
	cfOpUpdate   = "update"
	cfOpDelete   = "delete"

	driftSourceCFList  = "cf_list"
	driftSourceK8sList = "k8s_list"
)

var (
	// operationsTotal counts successful CF operations by resource and type.
	// CustomHostname operations: "adopt" = existing hostname found and tracked;
	// "create" = first-time provisioning; "recreate" = recovery after external deletion;
	// "update" = drift correction; "delete" = removal.
	// Pre-initialized for all known values so all series appear at startup.
	operationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_operations_total",
		Help: "Total number of successful Cloudflare operations by resource and type.",
	}, []string{"resource", "operation"})

	// sslProvisioningDuration records the time from CF hostname creation to ssl.status == active.
	// Labels: zone_cr (Zone CR name), hostname (the custom hostname), method (DCV method).
	// Set once per provisioning cycle; reset on recreation so it reflects the latest cycle.
	// Gauge instead of histogram: SSL provisioning is a rare event (zero to few per week),
	// making histogram rate/increase queries ineffective with GMP's cumulative counter semantics.
	// The hostname label gives per-hostname visibility (1 series each vs 14 with histogram).
	sslProvisioningDuration = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_ssl_provisioning_duration_seconds",
		Help: "Time from Cloudflare hostname creation to ssl.status becoming active, by zone CR, hostname, and DCV method.",
	}, []string{"zone_cr", "hostname", "method"})

	// customHostnames counts CustomHostname CRs by zone and state.
	// States are mutually exclusive: conflict > ready > unhealthy > pending.
	// Sum across states equals total CRs in that zone.
	customHostnames = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_customhostnames",
		Help: "Number of CustomHostname CRs by zone CR and state (ready, pending, unhealthy, conflict).",
	}, []string{"zone_cr", "state"})

	// zoneCustomHostnames counts Cloudflare custom hostnames by zone CR and type.
	// managed: hostname has an associated CR; orphan: hostname has no associated CR.
	// type=total is the CF quota usage for that zone.
	zoneCustomHostnames = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_customhostnames",
		Help: "Number of Cloudflare custom hostnames by zone CR and type (managed, orphan, drifted, total).",
	}, []string{"zone_cr", "type"})

	// zoneInitialized is 1 after the Zone CR has been initialized (zone name resolved
	// from Cloudflare API), 0 otherwise. Set once on first successful zone GET; not
	// toggled on transient failures. Labeled by zone_cr (the Zone CR name).
	zoneInitialized = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_initialized",
		Help: "1 if Zone CR has been initialized (zone name resolved from Cloudflare API), 0 otherwise.",
	}, []string{"zone_cr"})

	// cfAPICallDuration observes Cloudflare API call latency by resource and operation.
	// resource: "customhostname" or "zone". operation: get, list, create, update, delete.
	// For "list", the duration spans all paginated HTTP calls (N data pages + 1 end marker),
	// so observations can exceed the per-request WithRequestTimeout. Buckets cover both
	// single-call operations (<5s) and multi-page list totals (up to 20s).
	cfAPICallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_api_duration_seconds",
		Help:    "Cloudflare API call duration in seconds, by resource and operation.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 7.5, 10, 15, 20},
	}, []string{"resource", "operation"})

	// cfAPIErrorsByCode counts Cloudflare API errors by resource, operation, and HTTP status code.
	// Only appears in /metrics when errors have occurred.
	cfAPIErrorsByCode = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_errors_by_code_total",
		Help: "Cloudflare API errors by resource, operation, and HTTP status code.",
	}, []string{"resource", "operation", "status_code"})

	// driftBufferDepth reports the current number of items in a drift event channel
	// at the end of each zone reconcile cycle. Labeled by resource type. Approaching
	// --drift-buffer capacity indicates the worker controller is not draining fast enough.
	driftBufferDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_drift_buffer_depth",
		Help: "Current number of items in the drift event channel buffer, by resource type.",
	}, []string{"resource"})

	// driftBufferOverflowTotal counts how many times a drift event send blocked
	// because the channel buffer was full. Labeled by resource type. Non-zero means
	// the zone controller stalled waiting for the worker controller to drain events.
	driftBufferOverflowTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_drift_buffer_overflow_total",
		Help: "Number of times the drift event buffer was full, by resource type.",
	}, []string{"resource"})

	// driftDetectionErrorsTotal counts drift detection failures by resource type and
	// error source. source=cf_list: CF API list call failed. source=k8s_list: k8s CR
	// list call failed.
	driftDetectionErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_drift_detection_errors_total",
		Help: "Number of drift detection failures, by resource type and error source.",
	}, []string{"resource", "source"})

	// cfAPIRetriesTotal counts retry attempts for single (non-paginated) CF API calls.
	// Incremented per retry attempt (attempt > 0). Non-zero means first attempts are
	// failing and retries are absorbing transient CF API slowdowns.
	cfAPIRetriesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_retries_total",
		Help: "Number of retry attempts for Cloudflare API calls, by resource and operation.",
	}, []string{"resource", "operation"})
)

func init() {
	crtlmetrics.Registry.MustRegister(
		operationsTotal,
		sslProvisioningDuration,
		customHostnames,
		zoneCustomHostnames,
		zoneInitialized,
		cfAPICallDuration,
		cfAPIErrorsByCode,
		cfAPIRetriesTotal,
		driftBufferDepth,
		driftBufferOverflowTotal,
		driftDetectionErrorsTotal,
	)
	// Pre-initialize counters and histograms so they appear in /metrics from startup.
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
			var cfErr *cloudflare.Error
			if errors.As(*err, &cfErr) {
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
