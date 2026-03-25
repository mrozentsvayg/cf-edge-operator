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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	domainsv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/domains/v1beta1"
	saasv1beta1 "github.com/mrozentsvayg/cf-edge-operator/api/saas/v1beta1"
)

// cfMockServer is an in-memory Cloudflare API mock for integration tests.
type cfMockServer struct {
	mu        sync.Mutex
	hostnames map[string]mockHostname // keyed by ID
	zoneID    string
	zoneName  string
	server    *httptest.Server
	idCounter int
}

type mockHostname struct {
	ID                 string  `json:"id"`
	Hostname           string  `json:"hostname"`
	CustomOriginServer string  `json:"custom_origin_server"`
	CustomOriginSNI    string  `json:"custom_origin_sni,omitempty"`
	SSL                mockSSL `json:"ssl"`
}

type mockSSL struct {
	ID                   string       `json:"id"`
	Status               string       `json:"status"`
	Method               string       `json:"method"`
	Type                 string       `json:"type"`
	CertificateAuthority string       `json:"certificate_authority,omitempty"`
	BundleMethod         string       `json:"bundle_method"`
	Settings             mockSettings `json:"settings"`
}

type mockSettings struct {
	MinTLSVersion string `json:"min_tls_version,omitempty"`
}

func newCFMockServer(zoneID, zoneName string) *cfMockServer {
	m := &cfMockServer{
		hostnames: make(map[string]mockHostname),
		zoneID:    zoneID,
		zoneName:  zoneName,
	}
	mux := http.NewServeMux()

	// GET zones/{zoneID}
	mux.HandleFunc(fmt.Sprintf("GET /zones/%s", zoneID), m.handleZoneGet)

	// POST zones/{zoneID}/custom_hostnames
	mux.HandleFunc(fmt.Sprintf("POST /zones/%s/custom_hostnames", zoneID), m.handleCHCreate)

	// GET zones/{zoneID}/custom_hostnames (list)
	mux.HandleFunc(fmt.Sprintf("GET /zones/%s/custom_hostnames", zoneID), m.handleCHList)

	// PATCH zones/{zoneID}/custom_hostnames/{id}
	mux.HandleFunc(fmt.Sprintf("PATCH /zones/%s/custom_hostnames/", zoneID), m.handleCHEdit)

	// DELETE zones/{zoneID}/custom_hostnames/{id}
	mux.HandleFunc(fmt.Sprintf("DELETE /zones/%s/custom_hostnames/", zoneID), m.handleCHDelete)

	m.server = httptest.NewServer(mux)
	return m
}

func (m *cfMockServer) URL() string {
	return m.server.URL
}

func (m *cfMockServer) Close() {
	m.server.Close()
}

func (m *cfMockServer) nextID() string {
	m.idCounter++
	return fmt.Sprintf("mock-ch-id-%04d", m.idCounter)
}

func (m *cfMockServer) nextSSLID() string {
	m.idCounter++
	return fmt.Sprintf("mock-ssl-id-%04d", m.idCounter)
}

func (m *cfMockServer) handleZoneGet(w http.ResponseWriter, r *http.Request) {
	resp := map[string]any{
		"success": true,
		"result": map[string]any{
			"id":   m.zoneID,
			"name": m.zoneName,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *cfMockServer) handleCHCreate(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var body struct {
		Hostname           string `json:"hostname"`
		CustomOriginServer string `json:"custom_origin_server"`
		CustomOriginSNI    string `json:"custom_origin_sni"`
		SSL                struct {
			Method               string `json:"method"`
			Type                 string `json:"type"`
			CertificateAuthority string `json:"certificate_authority"`
			Settings             struct {
				MinTLSVersion string `json:"min_tls_version"`
			} `json:"settings"`
		} `json:"ssl"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, `{"success":false,"errors":[{"message":"invalid json"}]}`, 400)
		return
	}

	id := m.nextID()
	sslID := m.nextSSLID()
	ch := mockHostname{
		ID:                 id,
		Hostname:           body.Hostname,
		CustomOriginServer: body.CustomOriginServer,
		CustomOriginSNI:    body.CustomOriginSNI,
		SSL: mockSSL{
			ID:                   sslID,
			Status:               "pending_validation",
			Method:               body.SSL.Method,
			Type:                 body.SSL.Type,
			CertificateAuthority: body.SSL.CertificateAuthority,
			BundleMethod:         "ubiquitous",
			Settings: mockSettings{
				MinTLSVersion: body.SSL.Settings.MinTLSVersion,
			},
		},
	}
	m.hostnames[id] = ch

	resp := map[string]any{
		"success": true,
		"result":  ch,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(201)
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *cfMockServer) handleCHList(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var results []mockHostname
	hostname := r.URL.Query().Get("hostname")
	for _, ch := range m.hostnames {
		if hostname == "" || ch.Hostname == hostname {
			results = append(results, ch)
		}
	}
	if results == nil {
		results = []mockHostname{}
	}

	resp := map[string]any{
		"success":     true,
		"result":      results,
		"result_info": map[string]any{"page": 1, "per_page": 50, "total_pages": 1, "count": len(results), "total_count": len(results)},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *cfMockServer) handleCHEdit(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Extract ID from path: /zones/{zoneID}/custom_hostnames/{chID}
	parts := strings.Split(r.URL.Path, "/")
	chID := parts[len(parts)-1]

	ch, ok := m.hostnames[chID]
	if !ok {
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "errors": []map[string]any{{"message": "not found"}}})
		return
	}

	// Decode the full body as a generic map to handle WithJSONSet fields
	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, `{"success":false,"errors":[{"message":"invalid json"}]}`, 400)
		return
	}

	if v, ok := raw["custom_origin_server"].(string); ok && v != "" {
		ch.CustomOriginServer = v
	}
	if v, ok := raw["custom_origin_sni"].(string); ok && v != "" {
		ch.CustomOriginSNI = v
	}

	// Parse SSL from nested structure
	if sslRaw, ok := raw["ssl"].(map[string]any); ok {
		if v, ok := sslRaw["method"].(string); ok && v != "" {
			ch.SSL.Method = v
		}
		if v, ok := sslRaw["type"].(string); ok && v != "" {
			ch.SSL.Type = v
		}
		if v, ok := sslRaw["certificate_authority"].(string); ok && v != "" {
			ch.SSL.CertificateAuthority = v
		}
		if settings, ok := sslRaw["settings"].(map[string]any); ok {
			if v, ok := settings["min_tls_version"].(string); ok && v != "" {
				ch.SSL.Settings.MinTLSVersion = v
			}
		}
	}
	m.hostnames[chID] = ch

	resp := map[string]any{
		"success": true,
		"result":  ch,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (m *cfMockServer) handleCHDelete(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	parts := strings.Split(r.URL.Path, "/")
	chID := parts[len(parts)-1]

	if _, ok := m.hostnames[chID]; !ok {
		w.WriteHeader(404)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "errors": []map[string]any{{"code": 1, "message": "not found"}}})
		return
	}

	delete(m.hostnames, chID)
	resp := map[string]any{
		"success": true,
		"result":  map[string]any{"id": chID},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// hostnameCount returns the number of hostnames in the mock.
func (m *cfMockServer) hostnameCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.hostnames)
}

// --- Integration Tests ---

const (
	testZoneID   = "00000000000000000000000000000001"
	testZoneName = "test.example.com"
	testNS       = "test-integration"
)

var _ = Describe("Integration", Ordered, func() {
	var (
		cfMock   *cfMockServer
		chEvents chan event.GenericEvent
	)

	BeforeAll(func() {
		// Create mock CF server
		cfMock = newCFMockServer(testZoneID, testZoneName)

		// Create test namespace
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testNS}}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		// Create Zone CR
		zone := &domainsv1beta1.Zone{
			ObjectMeta: metav1.ObjectMeta{Name: "test-zone", Namespace: testNS},
			Spec: domainsv1beta1.ZoneSpec{
				ID: testZoneID,
				CredentialsRef: domainsv1beta1.SecretRef{
					Name: "cf-secret",
					Key:  "apiToken",
				},
			},
		}
		Expect(k8sClient.Create(ctx, zone)).To(Succeed())

		// Create Secret
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: testNS},
			Data:       map[string][]byte{"apiToken": []byte("test-token")},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())

		// Set up event channel
		chEvents = make(chan event.GenericEvent, 1024)

		// Start manager with controllers
		mgr, err := ctrl.NewManager(cfg, ctrl.Options{
			Scheme:  scheme.Scheme,
			Metrics: metricsserver.Options{BindAddress: "0"}, // disable metrics server in tests
		})
		Expect(err).NotTo(HaveOccurred())

		// Set up CH controller
		// NOTE: field indexer for hostnameField is registered inside SetupWithManager
		err = (&CustomHostnameReconciler{
			Client:            mgr.GetClient(),
			Scheme:            mgr.GetScheme(),
			OperatorNamespace: testNS,
			ManagementPolicy:  ManagementPolicyManage,
			DeletePolicy:      DeletePolicyAlways,
			SSLDefaults: SSLDefaults{
				CertificateAuthority: "lets_encrypt",
				MinTLSVersion:        "1.2",
			},
			CFBaseURL: cfMock.URL(),
		}).SetupWithManager(mgr, chEvents)
		Expect(err).NotTo(HaveOccurred())

		// Set up Zone controller
		err = (&ZoneReconciler{
			Client:               mgr.GetClient(),
			Scheme:               mgr.GetScheme(),
			CustomHostnameEvents: chEvents,
			DriftInterval:        5 * time.Second,
			CFBaseURL:            cfMock.URL(),
		}).SetupWithManager(mgr)
		Expect(err).NotTo(HaveOccurred())

		// Start manager in background (uses suite ctx, cancelled in AfterSuite)
		go func() {
			defer GinkgoRecover()
			err := mgr.Start(ctx)
			Expect(err).NotTo(HaveOccurred())
		}()
	})

	AfterAll(func() {
		cfMock.Close()
	})

	Describe("Full lifecycle", func() {
		It("should create a custom hostname in CF when CR is created", func() {
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lifecycle-test",
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "lifecycle.test.example.com",
					OriginServer: "origin.test.example.com",
					ZoneRef:      saasv1beta1.ZoneRef{Name: "test-zone"},
				},
			}
			Expect(k8sClient.Create(ctx, ch)).To(Succeed())

			// Wait for CF hostname to be created
			Eventually(func() int {
				return cfMock.hostnameCount()
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(1))

			// Verify CR status
			Eventually(func() string {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle-test", Namespace: testNS}, &updated); err != nil {
					return ""
				}
				return updated.Status.ID
			}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

			// Verify SSL defaults applied
			var updated saasv1beta1.CustomHostname
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle-test", Namespace: testNS}, &updated)).To(Succeed())
			Expect(updated.Status.SSL).NotTo(BeNil())
			Expect(updated.Status.SSL.CertificateAuthority).To(Equal("lets_encrypt"))
			Expect(updated.Status.SSL.MinTLSVersion).To(Equal("1.2"))
			Expect(updated.Status.SSL.Method).To(Equal("http"))
			Expect(updated.Status.SSL.Type).To(Equal("dv"))
			Expect(updated.Status.CreateCount).To(Equal(int32(1)))
		})

		It("should delete the CF hostname when CR is deleted", func() {
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "lifecycle-test",
					Namespace: testNS,
				},
			}
			Expect(k8sClient.Delete(ctx, ch)).To(Succeed())

			// Wait for CF hostname to be deleted
			Eventually(func() int {
				return cfMock.hostnameCount()
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(0))

			// Verify CR is gone
			Eventually(func() bool {
				var updated saasv1beta1.CustomHostname
				err := k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle-test", Namespace: testNS}, &updated)
				return err != nil
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		})
	})

	Describe("Zone Secret deletion and recovery", func() {
		var chName string

		It("should create a CH successfully", func() {
			chName = "secret-recovery-test"
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      chName,
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "secret-recovery.test.example.com",
					OriginServer: "origin.test.example.com",
					ZoneRef:      saasv1beta1.ZoneRef{Name: "test-zone"},
				},
			}
			Expect(k8sClient.Create(ctx, ch)).To(Succeed())

			Eventually(func() string {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: chName, Namespace: testNS}, &updated); err != nil {
					return ""
				}
				return updated.Status.ID
			}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())
		})

		It("should recover Zone readiness after Secret is deleted and restored", func() {
			// Verify Zone is Ready
			Eventually(func() bool {
				var zone domainsv1beta1.Zone
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "test-zone", Namespace: testNS}, &zone); err != nil {
					return false
				}
				for _, c := range zone.Status.Conditions {
					if c.Type == conditionReady && c.Status == metav1.ConditionTrue {
						return true
					}
				}
				return false
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

			// Delete Secret
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: testNS}}
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())

			// Zone should become NotReady
			Eventually(func() bool {
				var zone domainsv1beta1.Zone
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "test-zone", Namespace: testNS}, &zone); err != nil {
					return false
				}
				for _, c := range zone.Status.Conditions {
					if c.Type == conditionReady && c.Status == metav1.ConditionFalse {
						return true
					}
				}
				return false
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

			// Restore Secret
			newSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: testNS},
				Data:       map[string][]byte{"apiToken": []byte("test-token")},
			}
			Expect(k8sClient.Create(ctx, newSecret)).To(Succeed())

			// Zone should recover to Ready within 30s (backoff cap)
			Eventually(func() bool {
				var zone domainsv1beta1.Zone
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "test-zone", Namespace: testNS}, &zone); err != nil {
					return false
				}
				for _, c := range zone.Status.Conditions {
					if c.Type == conditionReady && c.Status == metav1.ConditionTrue {
						return true
					}
				}
				return false
			}, 35*time.Second, 500*time.Millisecond).Should(BeTrue())
		})

		It("should clean up the test CH", func() {
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{Name: chName, Namespace: testNS},
			}
			Expect(k8sClient.Delete(ctx, ch)).To(Succeed())
			Eventually(func() int {
				return cfMock.hostnameCount()
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(0))
		})
	})

	Describe("Terminating CR with missing Secret", func() {
		It("should release finalizer without CF client for deletePolicy=never", func() {
			// Create CH with deletePolicy=never
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "terminating-test",
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "terminating.test.example.com",
					OriginServer: "origin.test.example.com",
					DeletePolicy: DeletePolicyNever,
					ZoneRef:      saasv1beta1.ZoneRef{Name: "test-zone"},
				},
			}
			Expect(k8sClient.Create(ctx, ch)).To(Succeed())

			// Wait for it to be created in CF
			Eventually(func() string {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "terminating-test", Namespace: testNS}, &updated); err != nil {
					return ""
				}
				return updated.Status.ID
			}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

			// Delete Secret (CF client can't be built)
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: testNS}}
			Expect(k8sClient.Delete(ctx, secret)).To(Succeed())

			// Wait for Zone to go NotReady (confirms Secret is gone)
			Eventually(func() bool {
				var zone domainsv1beta1.Zone
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "test-zone", Namespace: testNS}, &zone); err != nil {
					return false
				}
				for _, c := range zone.Status.Conditions {
					if c.Type == conditionReady && c.Status == metav1.ConditionFalse {
						return true
					}
				}
				return false
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

			// Delete CH -- should release finalizer without needing CF client
			Expect(k8sClient.Delete(ctx, ch)).To(Succeed())

			// CR should be gone (finalizer released)
			Eventually(func() bool {
				var updated saasv1beta1.CustomHostname
				err := k8sClient.Get(ctx, types.NamespacedName{Name: "terminating-test", Namespace: testNS}, &updated)
				return err != nil
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

			// CF hostname should still exist (deletePolicy=never)
			Expect(cfMock.hostnameCount()).To(Equal(1))

			// Restore Secret for next tests
			newSecret := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{Name: "cf-secret", Namespace: testNS},
				Data:       map[string][]byte{"apiToken": []byte("test-token")},
			}
			Expect(k8sClient.Create(ctx, newSecret)).To(Succeed())

			// Wait for Zone to recover
			Eventually(func() bool {
				var zone domainsv1beta1.Zone
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "test-zone", Namespace: testNS}, &zone); err != nil {
					return false
				}
				for _, c := range zone.Status.Conditions {
					if c.Type == conditionReady && c.Status == metav1.ConditionTrue {
						return true
					}
				}
				return false
			}, 35*time.Second, 500*time.Millisecond).Should(BeTrue())
		})
	})

	Describe("Drift correction", func() {
		It("should correct origin drift when spec changes", func() {
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "drift-test",
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "drift.test.example.com",
					OriginServer: "origin.test.example.com",
					SSL: &saasv1beta1.CustomHostnameSSL{
						CertificateAuthority: "google",
						Method:               "http",
						Type:                 "dv",
					},
					ZoneRef: saasv1beta1.ZoneRef{Name: "test-zone"},
				},
			}
			Expect(k8sClient.Create(ctx, ch)).To(Succeed())

			// Wait for creation and status.id
			Eventually(func() string {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "drift-test", Namespace: testNS}, &updated); err != nil {
					return ""
				}
				return updated.Status.ID
			}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

			// Let controller finish all status updates
			time.Sleep(2 * time.Second)

			// Read fresh state, log it, patch spec
			var current saasv1beta1.CustomHostname
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "drift-test", Namespace: testNS}, &current)).To(Succeed())
			GinkgoWriter.Printf("BEFORE PATCH: gen=%d rv=%s del=%v fins=%v id=%s\n",
				current.Generation, current.ResourceVersion, current.DeletionTimestamp, current.Finalizers, current.Status.ID)

			patch := client.MergeFrom(current.DeepCopy())
			current.Spec.OriginServer = "new-origin.test.example.com"
			Expect(k8sClient.Patch(ctx, &current, patch)).To(Succeed())
			GinkgoWriter.Printf("AFTER PATCH: gen=%d rv=%s\n", current.Generation, current.ResourceVersion)

			// Wait and check
			time.Sleep(5 * time.Second)

			var after saasv1beta1.CustomHostname
			err := k8sClient.Get(ctx, types.NamespacedName{Name: "drift-test", Namespace: testNS}, &after)
			if err != nil {
				GinkgoWriter.Printf("CR GONE after patch: %v\n", err)
			} else {
				GinkgoWriter.Printf("CR EXISTS: gen=%d rv=%s del=%v fins=%v id=%s\n",
					after.Generation, after.ResourceVersion, after.DeletionTimestamp, after.Finalizers, after.Status.ID)
			}

			// Verify the mock has the corrected origin
			Expect(err).NotTo(HaveOccurred())
			cfMock.mu.Lock()
			var correctedOrigin string
			for _, h := range cfMock.hostnames {
				if h.Hostname == "drift.test.example.com" {
					correctedOrigin = h.CustomOriginServer
				}
			}
			cfMock.mu.Unlock()
			Expect(correctedOrigin).To(Equal("new-origin.test.example.com"))

			// Verify SSL fields were preserved during edit (v0.3.6 regression)
			cfMock.mu.Lock()
			for _, h := range cfMock.hostnames {
				if h.Hostname == "drift.test.example.com" {
					Expect(h.SSL.CertificateAuthority).To(Equal("google"))
					Expect(h.SSL.Method).To(Equal("http"))
					Expect(h.SSL.Type).To(Equal("dv"))
				}
			}
			cfMock.mu.Unlock()

			// Clean up
			countBefore := cfMock.hostnameCount()
			Expect(k8sClient.Delete(ctx, &after)).To(Succeed())
			Eventually(func() int {
				return cfMock.hostnameCount()
			}, 10*time.Second, 100*time.Millisecond).Should(Equal(countBefore - 1))
		})
	})

	Describe("Conflict detection", func() {
		It("should detect duplicate CRs claiming the same hostname", func() {
			// Create first CR
			ch1 := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "conflict-owner",
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "conflict.test.example.com",
					OriginServer: "origin.test.example.com",
					ZoneRef:      saasv1beta1.ZoneRef{Name: "test-zone"},
				},
			}
			Expect(k8sClient.Create(ctx, ch1)).To(Succeed())

			// Wait for first CR to be provisioned
			Eventually(func() string {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "conflict-owner", Namespace: testNS}, &updated); err != nil {
					return ""
				}
				return updated.Status.ID
			}, 10*time.Second, 100*time.Millisecond).ShouldNot(BeEmpty())

			// Create second CR with same hostname
			ch2 := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "conflict-duplicate",
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "conflict.test.example.com",
					OriginServer: "origin.test.example.com",
					ZoneRef:      saasv1beta1.ZoneRef{Name: "test-zone"},
				},
			}
			Expect(k8sClient.Create(ctx, ch2)).To(Succeed())

			// Second CR should get Ready=False with reason HostnameConflict
			Eventually(func() bool {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "conflict-duplicate", Namespace: testNS}, &updated); err != nil {
					return false
				}
				return isHostnameConflict(&updated)
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())

			// At least one hostname in CF (race may create two before conflict is detected)
			Expect(cfMock.hostnameCount()).To(BeNumerically(">=", 1))

			// Clean up both
			Expect(k8sClient.Delete(ctx, ch2)).To(Succeed())
			Expect(k8sClient.Delete(ctx, ch1)).To(Succeed())
			// Wait for owner CR's hostname to be deleted (duplicate may leave an orphan)
			Eventually(func() bool {
				return cfMock.hostnameCount() <= 1
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		})
	})

	Describe("SSL defaults cascade", func() {
		It("should apply operator defaults when spec.ssl is nil", func() {
			ch := &saasv1beta1.CustomHostname{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ssl-defaults-test",
					Namespace: testNS,
				},
				Spec: saasv1beta1.CustomHostnameSpec{
					Hostname:     "ssl-defaults.test.example.com",
					OriginServer: "origin.test.example.com",
					ZoneRef:      saasv1beta1.ZoneRef{Name: "test-zone"},
					// No SSL spec -- operator defaults should apply
				},
			}
			Expect(k8sClient.Create(ctx, ch)).To(Succeed())

			// Verify SSL defaults from operator flags
			Eventually(func() string {
				var updated saasv1beta1.CustomHostname
				if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ssl-defaults-test", Namespace: testNS}, &updated); err != nil {
					return ""
				}
				if updated.Status.SSL == nil {
					return ""
				}
				return updated.Status.SSL.CertificateAuthority
			}, 10*time.Second, 100*time.Millisecond).Should(Equal("lets_encrypt"))

			var updated saasv1beta1.CustomHostname
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "ssl-defaults-test", Namespace: testNS}, &updated)).To(Succeed())
			Expect(updated.Status.SSL.MinTLSVersion).To(Equal("1.2"))
			Expect(updated.Status.SSL.Method).To(Equal("http"))
			Expect(updated.Status.SSL.Type).To(Equal("dv"))

			// Clean up
			Expect(k8sClient.Delete(ctx, ch)).To(Succeed())
			Eventually(func() bool {
				var u saasv1beta1.CustomHostname
				return k8sClient.Get(ctx, types.NamespacedName{Name: "ssl-defaults-test", Namespace: testNS}, &u) != nil
			}, 10*time.Second, 100*time.Millisecond).Should(BeTrue())
		})
	})
})
