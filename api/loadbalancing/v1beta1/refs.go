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

package v1beta1

// AccountRef references an Account resource (loadbalancing.cf-edge.io/Account),
// which supplies the Cloudflare account ID and API credentials for
// account-scoped resources (LoadBalancerPool, LoadBalancerMonitor).
type AccountRef struct {
	// Name of the Account resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the Account resource. Defaults to the operator namespace
	// if omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// ZoneRef references a Zone resource (domains.cf-edge.io/Zone), which supplies
// the Cloudflare zone ID and API credentials for zone-scoped resources
// (LoadBalancer).
type ZoneRef struct {
	// Name of the Zone resource.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the Zone resource. Defaults to the operator namespace
	// if omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SecretRef references a Kubernetes secret holding a Cloudflare API token.
type SecretRef struct {
	// Name of the secret.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key within the secret containing the API token.
	// +kubebuilder:default=apiToken
	// +kubebuilder:validation:MinLength=1
	// +optional
	Key string `json:"key,omitempty"`
}
