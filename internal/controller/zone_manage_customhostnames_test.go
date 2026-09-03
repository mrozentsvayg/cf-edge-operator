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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
)

// This suite exercises the per-zone spec.manageCustomHostnames gate with
// EnableCustomHostname=true -- the configuration a custom-hostname coordinator
// cluster runs. It confirms that a Zone with manageCustomHostnames=false still
// initializes (load balancing needs status.name / the zone ID) but never lists
// custom hostnames from Cloudflare, while a Zone that leaves the field unset gets
// the CRD default (true) and does run the drift bulk-list. The mock's
// custom_hostnames list endpoint 403s, modelling an LB-scoped token, so a broken
// gate would surface as a non-zero list-call count.
var _ = Describe("Zone controller (per-zone manageCustomHostnames gate, EnableCustomHostname=true)", Ordered, func() {
	const chGateNS = "zone-ch-gate"

	var (
		cfMock  *cfMockServer
		gateMgr ctrl.Manager
	)

	BeforeAll(func() {
		cfMock = newCFMockServer(testZoneID, testZoneName)
		// Model an LB-scoped token: reading custom_hostnames 403s. The gate must ensure
		// this endpoint is never reached for a manageCustomHostnames=false zone.
		cfMock.failCHList()

		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: chGateNS},
		})).To(Succeed())

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: chGateNS},
			Data:       map[string][]byte{"apiToken": []byte("test-token")},
		})).To(Succeed())

		// Buffered channel, mirroring the integration suite. Drift never actually
		// enqueues here (the managed zone's list 403s before any send), but supplying a
		// real channel keeps the reconciler wired exactly as in production.
		chEvents := make(chan event.GenericEvent, 1024)

		var err error
		gateMgr, err = ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme.Scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
			// Scope the cache to this namespace so this manager does not reconcile
			// (or fight over) objects owned by the other suites' managers.
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{chGateNS: {}},
			},
			// Multiple suites register a controller named "zone"; skip the process-global
			// uniqueness check so they can coexist in one test process.
			Controller: crconfig.Controller{SkipNameValidation: new(true)},
		})
		Expect(err).NotTo(HaveOccurred())

		// EnableCustomHostname=true: operator-wide custom hostname management is ON (this
		// is the coordinator role). The per-zone toggle must still suppress the drift
		// bulk-list for a zone that opts out.
		Expect((&ZoneReconciler{
			Client:               gateMgr.GetClient(),
			Scheme:               gateMgr.GetScheme(),
			EnableCustomHostname: true,
			CustomHostnameEvents: chEvents,
			DriftInterval:        1 * time.Second,
			CFAPITimeout:         5 * time.Second,
			CFAPIMaxRetries:      1,
			CFAPIBulkTimeout:     5 * time.Second,
			CFAPIBulkMaxRetries:  0,
			CFBaseURL:            cfMock.URL(),
		}).SetupWithManager(gateMgr)).To(Succeed())

		go func() {
			defer GinkgoRecover()
			Expect(gateMgr.Start(ctx)).To(Succeed())
		}()
	})

	AfterAll(func() {
		cfMock.Close()
	})

	It("initializes a manageCustomHostnames=false zone without listing custom hostnames", func() {
		Expect(k8sClient.Create(ctx, &domainsv1beta1.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: "ch-off-zone", Namespace: chGateNS},
			Spec: domainsv1beta1.ZoneSpec{
				ID: testZoneID,
				CredentialsRef: domainsv1beta1.SecretRef{
					Name: "cf-secret",
					Key:  "apiToken",
				},
				ManageCustomHostnames: new(false),
			},
		})).To(Succeed())

		// The zone initializes even though custom hostname management is off:
		// load balancing needs status.name / the resolved zone ID.
		Eventually(func(g Gomega) {
			var z domainsv1beta1.Zone
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ch-off-zone", Namespace: chGateNS}, &z)).To(Succeed())
			g.Expect(z.Status.Name).To(Equal(testZoneName))
			cond := apimeta.FindStatusCondition(z.Status.Conditions, conditionInitialized)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		// The custom_hostnames list must never be called for this zone -- across several
		// drift intervals (DriftInterval=1s). A broken gate would reach the 403 endpoint
		// (bumping the counter) and increment cf_edge_operator_drift_detection_errors_total.
		Consistently(func() int {
			return cfMock.chListCallCount()
		}, 3*time.Second, 200*time.Millisecond).Should(BeZero())
	})

	It("clears stale CH metrics once on the unmanaged transition, not every cycle", func() {
		const zoneName = "ch-transition-zone"
		// Seed CH-family drift gauges as if this zone had previously been managed.
		customHostnames.WithLabelValues(zoneName, chStateReady).Set(4)
		zoneCustomHostnames.WithLabelValues(zoneName, "total").Set(4)

		// Created unmanaged: the first reconcile runs zone init (status.Name==""), which is
		// the transition cycle -- the seeded series are cleared exactly once.
		Expect(k8sClient.Create(ctx, &domainsv1beta1.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: zoneName, Namespace: chGateNS},
			Spec: domainsv1beta1.ZoneSpec{
				ID: testZoneID,
				CredentialsRef: domainsv1beta1.SecretRef{
					Name: "cf-secret",
					Key:  "apiToken",
				},
				ManageCustomHostnames: new(false),
			},
		})).To(Succeed())

		Eventually(func() int {
			return countSeries("cf_edge_operator_customhostnames", map[string]string{labelZoneCR: zoneName})
		}, 10*time.Second, 100*time.Millisecond).Should(BeZero())

		// Steady-state cycles must NOT re-clear (the clear is gated on the init/transition
		// cycle): re-seed a series and confirm it survives across several drift intervals.
		customHostnames.WithLabelValues(zoneName, chStateReady).Set(2)
		Consistently(func() float64 {
			return gaugeValue("cf_edge_operator_customhostnames", map[string]string{labelZoneCR: zoneName, labelState: chStateReady})
		}, 3*time.Second, 300*time.Millisecond).Should(Equal(float64(2)))
	})

	It("applies the CRD default (true) when manageCustomHostnames is unset and runs drift", func() {
		Expect(k8sClient.Create(ctx, &domainsv1beta1.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: "ch-default-zone", Namespace: chGateNS},
			Spec: domainsv1beta1.ZoneSpec{
				ID: testZoneID,
				CredentialsRef: domainsv1beta1.SecretRef{
					Name: "cf-secret",
					Key:  "apiToken",
				},
			},
		})).To(Succeed())

		// The API server applies the CRD default, so the round-tripped object is managed.
		Eventually(func(g Gomega) {
			var z domainsv1beta1.Zone
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ch-default-zone", Namespace: chGateNS}, &z)).To(Succeed())
			g.Expect(z.Spec.ManageCustomHostnames).NotTo(BeNil())
			g.Expect(*z.Spec.ManageCustomHostnames).To(BeTrue())
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())

		// Because it is managed, the drift pass runs and reaches the (403) list endpoint,
		// so the counter climbs -- proving the gate lets managed zones through.
		Eventually(func() int {
			return cfMock.chListCallCount()
		}, 10*time.Second, 200*time.Millisecond).Should(BeNumerically(">", 0))
	})
})
