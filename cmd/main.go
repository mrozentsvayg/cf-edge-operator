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

package main

import (
	"crypto/tls"
	"flag"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	loadbalancingv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/loadbalancing/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
	"github.com/mrozentsvayg/cf-edge-operator/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(domainsv1beta1.AddToScheme(scheme))
	utilruntime.Must(saasv1beta1.AddToScheme(scheme))
	utilruntime.Must(loadbalancingv1beta1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// levelEncoder names V(2) as "trace" instead of zap's default "Level(-2)".
// Standard levels (info, debug, warn, error) fall through to the default encoder.
func levelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	if l == zapcore.Level(-2) {
		enc.AppendString("trace")
		return
	}
	zapcore.LowercaseLevelEncoder(l, enc)
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var operatorNamespace string
	var managementPolicy string
	var deletePolicy string
	var dryRun bool
	var enableLoadBalancer bool
	var driftInterval time.Duration
	var driftBuffer int
	var cfAPITimeout time.Duration
	var cfAPIWriteTimeout time.Duration
	var cfAPIMaxRetries int
	var cfAPIBulkTimeout time.Duration
	var cfAPIBulkMaxRetries int
	var cfAPIWriteDelay time.Duration
	var sslCertificateAuthority, sslMinTLSVersion, sslMethod, sslType string
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.StringVar(&operatorNamespace, "operator-namespace", "cf-edge-operator-system",
		"Namespace where Zone resources are managed. Used to resolve ZoneRef when namespace is omitted.")
	flag.StringVar(&managementPolicy, "management-policy", "manage",
		"Default management policy for CustomHostname CRs. "+
			"'manage': full lifecycle (create, update, delete). "+
			"'create': provision if missing, never update (safe coexistence). "+
			"'observe': read-only tracking, no CF writes. "+
			"Per-CR spec.managementPolicy overrides this default.")
	flag.StringVar(&deletePolicy, "delete-policy", "always",
		"Default delete policy for CustomHostname CRs. "+
			"'always': delete from Cloudflare by ID regardless. "+
			"'own-only': only delete if the current Cloudflare hostname ID matches status.id. "+
			"'never': release the finalizer without deleting from Cloudflare. "+
			"Per-CR spec.deletePolicy overrides this default.")
	flag.BoolVar(&dryRun, "dry-run", false,
		"If set, skip all Cloudflare write operations and log what would happen instead.")
	flag.BoolVar(&enableLoadBalancer, "enable-loadbalancer", false,
		"Enable the load-balancing controllers (Account, LoadBalancer, LoadBalancerPool, "+
			"LoadBalancerMonitor). Off by default: load balancing is a single-owner control-plane "+
			"role, so only the control cluster runs these controllers. When off, none of the "+
			"controllers start and their metric series are not pre-initialized (per-cluster "+
			"deployments stay unchanged). Requires the load-balancing CRDs and RBAC to be present "+
			"(chart: controlPlane.enabled=true).")
	flag.DurationVar(&driftInterval, "drift-interval", time.Minute,
		"How often the zone controller bulk-lists Cloudflare custom hostnames to detect external drift, "+
			"and how often the load-balancing controllers (Account, LoadBalancer, LoadBalancerPool, "+
			"LoadBalancerMonitor) self-requeue to re-check external drift and retry transient errors. "+
			"Lower values reduce the window in which external changes go undetected.")
	flag.IntVar(&driftBuffer, "drift-buffer", 1024,
		"Buffer size of the internal channel used to enqueue drifted CustomHostname CRs. "+
			"Increase if operating with many zones and very frequent drift cycles.")
	flag.DurationVar(&cfAPITimeout, "cf-api-timeout", 5*time.Second,
		"Per-request timeout for CF API read calls (zone lookup, CH get/findByHostname).")
	flag.DurationVar(&cfAPIWriteTimeout, "cf-api-write-timeout", 15*time.Second,
		"Per-request timeout for CF API write calls (CH create/update/delete). "+
			"Longer than read timeout to accommodate CF processing during degradation.")
	flag.IntVar(&cfAPIMaxRetries, "cf-api-max-retries", 1,
		"Maximum number of retries for single CF API calls (immediate, no backoff). "+
			"Does not apply to paginated bulk list (see --cf-api-bulk-max-retries). "+
			"Skips retry on 429 (rate limit).")
	flag.DurationVar(&cfAPIBulkTimeout, "cf-api-bulk-timeout", 5*time.Second,
		"Per-page timeout for paginated CF API calls (bulk drift list). "+
			"Longer than --cf-api-timeout because each page returns up to 50 hostnames.")
	flag.IntVar(&cfAPIBulkMaxRetries, "cf-api-bulk-max-retries", 0,
		"Maximum number of per-page retries for paginated CF API calls (SDK-level, ~2s backoff). "+
			"Only the failed page is retried, not the whole list.")
	flag.DurationVar(&cfAPIWriteDelay, "cf-api-write-delay", 250*time.Millisecond,
		"Pause after each successful CF write operation (create/edit/delete). "+
			"Paces sequential writes to avoid triggering CF API throttling during bulk changes.")
	flag.StringVar(&sslCertificateAuthority, "ssl-certificate-authority", "",
		"Default certificate authority for new custom hostnames (lets_encrypt, google, ssl_com). "+
			"Applied on create when the CR's spec.ssl.certificateAuthority is empty. If empty, Cloudflare uses its own default.")
	flag.StringVar(&sslMinTLSVersion, "ssl-min-tls-version", "",
		"Default minimum TLS version for new custom hostnames (1.0, 1.1, 1.2, 1.3). "+
			"Applied on create when the CR's spec.ssl.minTLSVersion is empty. If empty, Cloudflare uses its own default.")
	flag.StringVar(&sslMethod, "ssl-method", "",
		"Default DCV method for new custom hostnames (http, txt, email). "+
			"Applied on create when the CR's spec.ssl.method is empty. If empty, defaults to http.")
	flag.StringVar(&sslType, "ssl-type", "",
		"Default validation type for new custom hostnames (dv). "+
			"Applied on create when the CR's spec.ssl.type is empty. If empty, defaults to dv.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics server")
	// NOTE: Development: true = human-readable console logs, DPanic panics.
	// Flip to false for production JSON output. Overridable via --zap-devel.
	opts := zap.Options{
		Development: true,
		EncoderConfigOptions: []zap.EncoderConfigOption{
			func(cfg *zapcore.EncoderConfig) {
				cfg.EncodeLevel = levelEncoder
			},
		},
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	setupLog.Info("Configuration",
		"operatorNamespace", operatorNamespace,
		"managementPolicy", managementPolicy,
		"deletePolicy", deletePolicy,
		"dryRun", dryRun,
		"enableLoadBalancer", enableLoadBalancer,
		"driftInterval", driftInterval,
		"driftBuffer", driftBuffer,
		"cfAPITimeout", cfAPITimeout,
		"cfAPIWriteTimeout", cfAPIWriteTimeout,
		"cfAPIMaxRetries", cfAPIMaxRetries,
		"cfAPIBulkTimeout", cfAPIBulkTimeout,
		"cfAPIBulkMaxRetries", cfAPIBulkMaxRetries,
		"cfAPIWriteDelay", cfAPIWriteDelay,
		"leaderElect", enableLeaderElection,
		"sslCertificateAuthority", sslCertificateAuthority,
		"sslMinTLSVersion", sslMinTLSVersion,
		"sslMethod", sslMethod,
		"sslType", sslType,
		"zapDevel", opts.Development,
		"zapLogLevel", flag.Lookup("zap-log-level").Value.String(),
	)
	switch managementPolicy {
	case controller.ManagementPolicyManage, controller.ManagementPolicyCreate, controller.ManagementPolicyObserve:
	default:
		setupLog.Error(nil, "invalid --management-policy", "value", managementPolicy)
		os.Exit(1)
	}
	switch deletePolicy {
	case controller.DeletePolicyAlways, controller.DeletePolicyOwnOnly, controller.DeletePolicyNever:
	default:
		setupLog.Error(nil, "invalid --delete-policy", "value", deletePolicy)
		os.Exit(1)
	}
	if driftInterval <= 0 {
		setupLog.Error(nil, "invalid --drift-interval: must be positive", "value", driftInterval)
		os.Exit(1)
	}
	if driftBuffer <= 0 {
		setupLog.Error(nil, "invalid --drift-buffer: must be positive", "value", driftBuffer)
		os.Exit(1)
	}
	if cfAPITimeout <= 0 {
		setupLog.Error(nil, "invalid --cf-api-timeout: must be positive", "value", cfAPITimeout)
		os.Exit(1)
	}
	if cfAPIWriteTimeout <= 0 {
		setupLog.Error(nil, "invalid --cf-api-write-timeout: must be positive", "value", cfAPIWriteTimeout)
		os.Exit(1)
	}
	if cfAPIMaxRetries < 0 {
		setupLog.Error(nil, "invalid --cf-api-max-retries: must be non-negative", "value", cfAPIMaxRetries)
		os.Exit(1)
	}
	if cfAPIBulkTimeout <= 0 {
		setupLog.Error(nil, "invalid --cf-api-bulk-timeout: must be positive", "value", cfAPIBulkTimeout)
		os.Exit(1)
	}
	if cfAPIBulkMaxRetries < 0 {
		setupLog.Error(nil, "invalid --cf-api-bulk-max-retries: must be non-negative", "value", cfAPIBulkMaxRetries)
		os.Exit(1)
	}
	if cfAPIWriteDelay < 0 {
		setupLog.Error(nil, "invalid --cf-api-write-delay: must be non-negative", "value", cfAPIWriteDelay)
		os.Exit(1)
	}
	if sslCertificateAuthority != "" {
		switch sslCertificateAuthority {
		case saasv1beta1.SSLCALetsEncrypt, saasv1beta1.SSLCAGoogle, saasv1beta1.SSLCASSLCom:
		default:
			setupLog.Error(nil, "invalid --ssl-certificate-authority", "value", sslCertificateAuthority)
			os.Exit(1)
		}
	}
	if sslMinTLSVersion != "" {
		switch sslMinTLSVersion {
		case saasv1beta1.SSLMinTLS10, saasv1beta1.SSLMinTLS11, saasv1beta1.SSLMinTLS12, saasv1beta1.SSLMinTLS13:
		default:
			setupLog.Error(nil, "invalid --ssl-min-tls-version", "value", sslMinTLSVersion)
			os.Exit(1)
		}
	}
	if sslMethod != "" {
		switch sslMethod {
		case saasv1beta1.SSLMethodHTTP, saasv1beta1.SSLMethodTXT, saasv1beta1.SSLMethodEmail:
		default:
			setupLog.Error(nil, "invalid --ssl-method", "value", sslMethod)
			os.Exit(1)
		}
	}
	if sslType != "" {
		switch sslType {
		case saasv1beta1.SSLTypeDV:
		default:
			setupLog.Error(nil, "invalid --ssl-type", "value", sslType)
			os.Exit(1)
		}
	}
	if dryRun {
		setupLog.Info("DRY-RUN mode enabled -- no Cloudflare write operations will be performed")
	}

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.1/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "888e4210.cf-edge.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	// Shared channel: Zone coordinator sends drifted CustomHostname CRs to the worker
	driftEvents := make(chan event.GenericEvent, driftBuffer)

	if err := (&controller.CustomHostnameReconciler{
		Client:            mgr.GetClient(),
		Scheme:            mgr.GetScheme(),
		OperatorNamespace: operatorNamespace,
		ManagementPolicy:  managementPolicy,
		DeletePolicy:      deletePolicy,
		DryRun:            dryRun,
		SSLDefaults: controller.SSLDefaults{
			CertificateAuthority: sslCertificateAuthority,
			MinTLSVersion:        sslMinTLSVersion,
			Method:               sslMethod,
			Type:                 sslType,
		},
		CFAPITimeout:      cfAPITimeout,
		CFAPIWriteTimeout: cfAPIWriteTimeout,
		CFAPIMaxRetries:   cfAPIMaxRetries,
		CFAPIWriteDelay:   cfAPIWriteDelay,
	}).SetupWithManager(mgr, driftEvents); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "CustomHostname")
		os.Exit(1)
	}
	if err := (&controller.ZoneReconciler{
		Client:               mgr.GetClient(),
		Scheme:               mgr.GetScheme(),
		CustomHostnameEvents: driftEvents,
		DryRun:               dryRun,
		DriftInterval:        driftInterval,
		CFAPITimeout:         cfAPITimeout,
		CFAPIMaxRetries:      cfAPIMaxRetries,
		CFAPIBulkTimeout:     cfAPIBulkTimeout,
		CFAPIBulkMaxRetries:  cfAPIBulkMaxRetries,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "Zone")
		os.Exit(1)
	}
	// Load-balancing controllers are opt-in (single-owner control-plane role).
	// When disabled, none of them start and their metric series are not
	// pre-initialized, so per-cluster deployments are unaffected. driftInterval
	// is validated > 0 above, so RequeueInterval (the LB self-requeue cadence)
	// is always positive here.
	if enableLoadBalancer {
		setupLog.Info("Load-balancing controllers enabled")
		controller.PreInitLoadBalancerMetrics()
		if err := (&controller.AccountReconciler{
			Client:            mgr.GetClient(),
			Scheme:            mgr.GetScheme(),
			OperatorNamespace: operatorNamespace,
			CFAPITimeout:      cfAPITimeout,
			CFAPIMaxRetries:   cfAPIMaxRetries,
			RequeueInterval:   driftInterval,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create controller", "controller", "Account")
			os.Exit(1)
		}
		if err := (&controller.LoadBalancerMonitorReconciler{
			Client:            mgr.GetClient(),
			Scheme:            mgr.GetScheme(),
			OperatorNamespace: operatorNamespace,
			ManagementPolicy:  managementPolicy,
			DeletePolicy:      deletePolicy,
			DryRun:            dryRun,
			CFAPITimeout:      cfAPITimeout,
			CFAPIWriteTimeout: cfAPIWriteTimeout,
			CFAPIMaxRetries:   cfAPIMaxRetries,
			CFAPIWriteDelay:   cfAPIWriteDelay,
			RequeueInterval:   driftInterval,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create controller", "controller", "LoadBalancerMonitor")
			os.Exit(1)
		}
		if err := (&controller.LoadBalancerPoolReconciler{
			Client:            mgr.GetClient(),
			Scheme:            mgr.GetScheme(),
			OperatorNamespace: operatorNamespace,
			ManagementPolicy:  managementPolicy,
			DeletePolicy:      deletePolicy,
			DryRun:            dryRun,
			CFAPITimeout:      cfAPITimeout,
			CFAPIWriteTimeout: cfAPIWriteTimeout,
			CFAPIMaxRetries:   cfAPIMaxRetries,
			CFAPIWriteDelay:   cfAPIWriteDelay,
			RequeueInterval:   driftInterval,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create controller", "controller", "LoadBalancerPool")
			os.Exit(1)
		}
		if err := (&controller.LoadBalancerReconciler{
			Client:            mgr.GetClient(),
			Scheme:            mgr.GetScheme(),
			OperatorNamespace: operatorNamespace,
			ManagementPolicy:  managementPolicy,
			DeletePolicy:      deletePolicy,
			DryRun:            dryRun,
			CFAPITimeout:      cfAPITimeout,
			CFAPIWriteTimeout: cfAPIWriteTimeout,
			CFAPIMaxRetries:   cfAPIMaxRetries,
			CFAPIWriteDelay:   cfAPIWriteDelay,
			RequeueInterval:   driftInterval,
		}).SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create controller", "controller", "LoadBalancer")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	// NOTE: Probes are trivial pings -- they don't check CF API reachability or
	// controller health. Per-zone health is surfaced via the zoneInitialized metric
	// and Zone CR conditions. Failing the readiness probe would remove the pod from
	// service, which is worse than a single zone's token expiring.
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}
