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
	"sigs.k8s.io/controller-runtime/pkg/client"
	crconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

// This suite exercises the Zone controller in its pure load-balancing role
// (EnableCustomHostname=false), the configuration a control cluster runs when
// only features.loadBalancing.enabled is set. It confirms that the controller
// reconciles a Zone cleanly and that SetupWithManager skips the custom-hostname
// substrate -- it does not register the CustomHostname zoneRef field index (and
// therefore does not run the custom hostname drift bulk-list).
var _ = Describe("Zone controller (pure load-balancing, EnableCustomHostname=false)", Ordered, func() {
	const lbOnlyNS = "zone-lbonly"

	var (
		cfMock    *cfMockServer
		lbOnlyMgr ctrl.Manager
	)

	BeforeAll(func() {
		cfMock = newCFMockServer(testZoneID, testZoneName)

		Expect(k8sClient.Create(ctx, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: lbOnlyNS},
		})).To(Succeed())

		Expect(k8sClient.Create(ctx, &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: lbOnlyNS},
			Data:       map[string][]byte{"apiToken": []byte("test-token")},
		})).To(Succeed())

		var err error
		lbOnlyMgr, err = ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme.Scheme,
			Metrics: metricsserver.Options{BindAddress: "0"},
			// Scope the cache to this namespace so this manager does not reconcile
			// (or fight over) objects owned by the other suites' managers.
			Cache: cache.Options{
				DefaultNamespaces: map[string]cache.Config{lbOnlyNS: {}},
			},
			// The integration suite also registers a controller named "zone"; skip the
			// process-global uniqueness check so both can coexist in one test process.
			Controller: crconfig.Controller{SkipNameValidation: new(true)},
		})
		Expect(err).NotTo(HaveOccurred())

		// EnableCustomHostname=false and CustomHostnameEvents=nil: the pure-LB
		// substrate config. SetupWithManager must not register the CustomHostname
		// zoneRef index in this mode.
		Expect((&ZoneReconciler{
			Client:               lbOnlyMgr.GetClient(),
			Scheme:               lbOnlyMgr.GetScheme(),
			EnableCustomHostname: false,
			CustomHostnameEvents: nil,
			DriftInterval:        5 * time.Second,
			CFAPITimeout:         5 * time.Second,
			CFAPIMaxRetries:      1,
			CFAPIBulkTimeout:     5 * time.Second,
			CFAPIBulkMaxRetries:  0,
			CFBaseURL:            cfMock.URL(),
		}).SetupWithManager(lbOnlyMgr)).To(Succeed())

		go func() {
			defer GinkgoRecover()
			Expect(lbOnlyMgr.Start(ctx)).To(Succeed())
		}()
	})

	AfterAll(func() {
		cfMock.Close()
	})

	It("reconciles a Zone cleanly without custom hostname setup", func() {
		Expect(k8sClient.Create(ctx, &domainsv1beta1.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: "lbonly-zone", Namespace: lbOnlyNS},
			Spec: domainsv1beta1.ZoneSpec{
				ID: testZoneID,
				CredentialsRef: domainsv1beta1.SecretRef{
					Name: "cf-secret",
					Key:  "apiToken",
				},
			},
		})).To(Succeed())

		Eventually(func(g Gomega) {
			var z domainsv1beta1.Zone
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lbonly-zone", Namespace: lbOnlyNS}, &z)).To(Succeed())
			g.Expect(z.Status.Name).To(Equal(testZoneName))
			cond := apimeta.FindStatusCondition(z.Status.Conditions, conditionInitialized)
			g.Expect(cond).NotTo(BeNil())
			g.Expect(cond.Status).To(Equal(metav1.ConditionTrue))
		}, 10*time.Second, 100*time.Millisecond).Should(Succeed())
	})

	It("does not register the custom hostname zoneRef field index", func() {
		// With EnableCustomHostname=false the zoneRef index is never registered, so
		// a field-selector list on it is rejected by the cache. (When the index IS
		// registered -- see the integration suite -- this same list succeeds and
		// returns an empty result.)
		Eventually(func() error {
			var chs saasv1beta1.CustomHostnameList
			return lbOnlyMgr.GetClient().List(ctx, &chs, client.MatchingFields{zoneRefField: "lbonly-zone"})
		}, 10*time.Second, 200*time.Millisecond).Should(MatchError(ContainSubstring("does not exist")))
	})
})
