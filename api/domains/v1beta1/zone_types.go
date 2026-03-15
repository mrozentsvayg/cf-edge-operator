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

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ZoneSpec defines the desired state of Zone
type ZoneSpec struct {
	// ID is the Cloudflare zone ID (32-character hex string).
	// Immutable — changing it would redirect all associated CustomHostname CRs to a different zone.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern="^[0-9a-f]{32}$"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="zone ID is immutable"
	ID string `json:"id"`

	// CredentialsRef references the secret containing the Cloudflare API token.
	// The secret must be in the same namespace as the Zone resource.
	// +kubebuilder:validation:Required
	CredentialsRef SecretRef `json:"credentialsRef"`
}

// SecretRef references a Kubernetes secret
type SecretRef struct {
	// Name of the secret
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Key within the secret containing the API token
	// +kubebuilder:default=apiToken
	// +kubebuilder:validation:MinLength=1
	// +optional
	Key string `json:"key,omitempty"`
}

// ZoneStatus defines the observed state of Zone.
// NOTE: No top-level ObservedGeneration — it's set per-condition in setReady().
type ZoneStatus struct {
	// Name is the Cloudflare zone name (e.g. example.com), populated from the API
	// +optional
	Name string `json:"name,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Zone",type=string,JSONPath=`.status.name`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// Zone is the Schema for the zones API
type Zone struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ZoneSpec `json:"spec"`

	// +optional
	Status ZoneStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ZoneList contains a list of Zone
type ZoneList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Zone `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Zone{}, &ZoneList{})
}
