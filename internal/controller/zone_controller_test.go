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
	"context"
	"testing"

	"github.com/cloudflare/cloudflare-go/v6/custom_hostnames"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cfv1alpha1 "github.com/mrozentsvayg/cf-edge-operator/api/v1alpha1"
)

func TestHasDrift(t *testing.T) {
	sni := "sni.example.com"
	tests := []struct {
		name string
		ch   cfv1alpha1.CustomHostname
		cfCH custom_hostnames.CustomHostnameListResponse
		want bool
	}{
		{
			name: "no drift",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com"},
			want: false,
		},
		{
			name: "origin server drift",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "new.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "old.example.com"},
			want: true,
		},
		{
			name: "sni set and matches",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com", OriginSNI: &sni}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: sni},
			want: false,
		},
		{
			name: "sni set and differs",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com", OriginSNI: &sni}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: "other.example.com"},
			want: true,
		},
		{
			name: "sni not spec'd, cf has sni set",
			ch:   cfv1alpha1.CustomHostname{Spec: cfv1alpha1.CustomHostnameSpec{OriginServer: "origin.example.com"}},
			cfCH: custom_hostnames.CustomHostnameListResponse{CustomOriginServer: "origin.example.com", CustomOriginSNI: sni},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasDrift(&tt.ch, tt.cfCH); got != tt.want {
				t.Errorf("hasDrift() = %v, want %v", got, tt.want)
			}
		})
	}
}

var _ = Describe("Zone Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		zone := &cfv1alpha1.Zone{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Zone")
			err := k8sClient.Get(ctx, typeNamespacedName, zone)
			if err != nil && errors.IsNotFound(err) {
				resource := &cfv1alpha1.Zone{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					// TODO(user): Specify other spec details if needed.
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &cfv1alpha1.Zone{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Zone")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &ZoneReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
