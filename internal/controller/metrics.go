package controller

import (
	"strconv"
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/prometheus/client_golang/prometheus"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// customHostnameOperationsTotal counts successful CF write operations by type.
	// "create" = first-time provisioning; "recreate" = recovery after external deletion;
	// "update" = drift correction; "delete" = removal.
	// Pre-initialized for all four values so all series appear at startup.
	customHostnameOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_customhostname_operations_total",
		Help: "Total number of successful Cloudflare write operations by type (create, recreate, update, delete).",
	}, []string{"operation"})

	// sslProvisioningDuration observes the time from CF hostname creation to ssl.status == active.
	// Labels: zone (zone domain name), hostname (the custom hostname), method (DCV method).
	// Observed once per provisioning cycle; reset on recreation so it reflects the latest cycle.
	// Buckets cover the bimodal distribution: quick completers (minutes) and slow ones (days/weeks).
	sslProvisioningDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_ssl_provisioning_duration_seconds",
		Help:    "Time from Cloudflare hostname creation to ssl.status becoming active, by zone, hostname, and DCV method.",
		Buckets: []float64{60, 300, 600, 1800, 3600, 7200, 21600, 43200, 86400, 259200, 604800},
		// 1m, 5m, 10m, 30m, 1h, 2h, 6h, 12h, 1d, 3d, 1w
	}, []string{"zone", "hostname", "method"})

	// customHostnames counts CustomHostname CRs by zone and state.
	// States are mutually exclusive: conflict > ready > unhealthy > pending.
	// Sum across states equals total CRs in that zone.
	customHostnames = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_customhostnames",
		Help: "Number of CustomHostname CRs by zone and state (ready, pending, unhealthy, conflict).",
	}, []string{"zone", "state"})

	// zoneCustomHostnames counts Cloudflare custom hostnames by zone and type.
	// managed: hostname has a corresponding CR; orphan: hostname has no CR.
	// Sum across types equals total CF quota usage for that zone.
	zoneCustomHostnames = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_customhostnames",
		Help: "Number of Cloudflare custom hostnames by zone and type (managed, orphan). Sum = CF quota usage.",
	}, []string{"zone", "type"})

	// cfAPICallDuration observes Cloudflare API call latency by operation.
	cfAPICallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_api_duration_seconds",
		Help:    "Cloudflare API call duration in seconds, by operation.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"operation"})

	// cfAPIErrorsTotal is a simple total count of all Cloudflare API errors.
	cfAPIErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_errors_total",
		Help: "Total number of Cloudflare API errors.",
	})
	// cfAPIErrorsByCode counts Cloudflare API errors by operation and HTTP status code.
	// Only appears in /metrics when errors have occurred.
	cfAPIErrorsByCode = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_errors_by_code_total",
		Help: "Cloudflare API errors by operation and HTTP status code.",
	}, []string{"operation", "status_code"})
)

func init() {
	crtlmetrics.Registry.MustRegister(
		customHostnameOperationsTotal,
		sslProvisioningDuration,
		customHostnames,
		zoneCustomHostnames,
		cfAPICallDuration,
		cfAPIErrorsTotal,
		cfAPIErrorsByCode,
	)
	// Pre-initialize counters and histograms so they appear in /metrics from startup.
	for _, op := range []string{"create", "update", "delete", "list"} {
		cfAPICallDuration.WithLabelValues(op)
	}
	for _, op := range []string{"create", "recreate", "update", "delete"} {
		customHostnameOperationsTotal.WithLabelValues(op)
	}
}

// recordCFCall records duration and any error for a Cloudflare API call.
// Call at the call site: defer recordCFCall("create", time.Now(), &err)
func recordCFCall(operation string, start time.Time, err *error) {
	cfAPICallDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	if err != nil && *err != nil {
		cfAPIErrorsTotal.Inc()
		statusCode := "unknown"
		if cfErr, ok := (*err).(*cloudflare.Error); ok {
			statusCode = strconv.Itoa(cfErr.StatusCode)
		}
		cfAPIErrorsByCode.WithLabelValues(operation, statusCode).Inc()
	}
}
