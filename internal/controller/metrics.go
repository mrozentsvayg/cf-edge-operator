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
	// Pre-initialized for create, update, delete so all series appear at startup.
	customHostnameOperationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_customhostname_operations_total",
		Help: "Total number of successful Cloudflare write operations by type (create, update, delete).",
	}, []string{"operation"})

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
	for _, op := range []string{"create", "update", "delete"} {
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
