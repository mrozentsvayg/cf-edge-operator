package controller

import (
	"errors"
	"strconv"
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

	// sslProvisioningDuration observes the time from CF hostname creation to ssl.status == active.
	// Labels: zone_cr (Zone CR name), hostname (the custom hostname), method (DCV method).
	// Observed once per provisioning cycle; reset on recreation so it reflects the latest cycle.
	// Buckets cover the bimodal distribution: quick completers (minutes) and slow ones (days/weeks).
	sslProvisioningDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_ssl_provisioning_duration_seconds",
		Help:    "Time from Cloudflare hostname creation to ssl.status becoming active, by zone CR, hostname, and DCV method.",
		Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 21600, 43200, 86400, 259200, 604800},
		// 1m, 5m, 10m, 30m, 1h, 2h, 6h, 12h, 1d, 3d, 1w
	}, []string{"zone_cr", "hostname", "method"})
	// NOTE: The "hostname" label creates one histogram per custom hostname (14 series each
	// with 11 buckets). This is intentional — per-hostname provisioning visibility is needed
	// for debugging slow SSL issuance. The cardinality is bounded by the number of managed
	// hostnames and each series is static after SSL becomes active (fires once per lifecycle).

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

	// zoneReady is 1 when the Zone CR credentials are valid and the Cloudflare API
	// is reachable, 0 otherwise. Labeled by zone_cr (the Zone CR name, always
	// available) rather than the CF domain name, which may not be known on failure.
	zoneReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_ready",
		Help: "1 if the Zone CR is healthy (credentials valid, Cloudflare API reachable), 0 otherwise.",
	}, []string{"zone_cr"})

	// cfAPICallDuration observes Cloudflare API call latency by resource and operation.
	// resource: "customhostname" or "zone". operation: get, list, create, update, delete.
	cfAPICallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_api_duration_seconds",
		Help:    "Cloudflare API call duration in seconds, by resource and operation.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
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

	// driftDetectionErrorsTotal counts drift detection failures by resource type.
	// Non-zero means drift detection is failing for one or more zones.
	driftDetectionErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_drift_detection_errors_total",
		Help: "Number of drift detection failures, by resource type.",
	}, []string{"resource"})
)

func init() {
	crtlmetrics.Registry.MustRegister(
		operationsTotal,
		sslProvisioningDuration,
		customHostnames,
		zoneCustomHostnames,
		zoneReady,
		cfAPICallDuration,
		cfAPIErrorsByCode,
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
}

// recordCFCall records duration and any error for a Cloudflare API call.
// Use cfResource* and cfOp* constants for resource and operation.
// Call at the call site: defer recordCFCall(cfResourceCustomHostname, cfOpCreate, time.Now(), &err)
func recordCFCall(resource, operation string, start time.Time, err *error) {
	cfAPICallDuration.WithLabelValues(resource, operation).Observe(time.Since(start).Seconds())
	if err != nil && *err != nil {
		statusCode := "unknown"
		var cfErr *cloudflare.Error
		if errors.As(*err, &cfErr) {
			statusCode = strconv.Itoa(cfErr.StatusCode)
		}
		cfAPIErrorsByCode.WithLabelValues(resource, operation, statusCode).Inc()
	}
}
