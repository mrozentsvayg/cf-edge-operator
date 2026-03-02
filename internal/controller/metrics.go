package controller

import (
	"strconv"
	"time"

	"github.com/cloudflare/cloudflare-go/v6"
	"github.com/prometheus/client_golang/prometheus"
	crtlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	customHostnameCreatesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cf_edge_operator_customhostname_creates_total",
		Help: "Total number of custom hostnames created in Cloudflare.",
	})
	customHostnameUpdatesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cf_edge_operator_customhostname_updates_total",
		Help: "Total number of custom hostnames updated (drift corrections) in Cloudflare.",
	})
	customHostnameDeletesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cf_edge_operator_customhostname_deletes_total",
		Help: "Total number of custom hostnames deleted from Cloudflare.",
	})
	// zoneOrphans tracks custom hostnames in Cloudflare that have no corresponding CR.
	// Label: zone name (from zone.status.name).
	zoneOrphans = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "cf_edge_operator_zone_orphans",
		Help: "Number of custom hostnames in Cloudflare with no corresponding CR, per zone.",
	}, []string{"zone"})

	// cfAPICallDuration observes Cloudflare API call latency by operation.
	cfAPICallDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "cf_edge_operator_api_duration_seconds",
		Help:    "Cloudflare API call duration in seconds, by operation.",
		Buckets: []float64{.05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"operation"})

	// cfAPIErrorsTotal counts Cloudflare API errors by operation and HTTP status code.
	cfAPIErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cf_edge_operator_api_errors_total",
		Help: "Total number of Cloudflare API errors, by operation and status code.",
	}, []string{"operation", "status_code"})
)

func init() {
	crtlmetrics.Registry.MustRegister(
		customHostnameCreatesTotal,
		customHostnameUpdatesTotal,
		customHostnameDeletesTotal,
		zoneOrphans,
		cfAPICallDuration,
		cfAPIErrorsTotal,
	)
}

// recordCFCall records duration and any error for a Cloudflare API call.
// Call at the call site: defer recordCFCall("create", time.Now(), &err)
func recordCFCall(operation string, start time.Time, err *error) {
	cfAPICallDuration.WithLabelValues(operation).Observe(time.Since(start).Seconds())
	if err != nil && *err != nil {
		statusCode := "unknown"
		if cfErr, ok := (*err).(*cloudflare.Error); ok {
			statusCode = strconv.Itoa(cfErr.StatusCode)
		}
		cfAPIErrorsTotal.WithLabelValues(operation, statusCode).Inc()
	}
}
