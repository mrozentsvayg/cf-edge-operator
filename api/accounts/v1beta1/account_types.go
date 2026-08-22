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

// AccountSpec defines the desired state of an Account.
//
// An Account carries the Cloudflare account ID and API credentials used by
// account-scoped resources (LoadBalancerPool, LoadBalancerMonitor). It is the
// account-scope analog of a Zone: account-scoped resources reference an Account
// via AccountRef, the same way zone-scoped resources reference a Zone. Account is
// a foundational credential/identity group of its own (accounts.cf-edge.io),
// parallel to Zone in domains.cf-edge.io -- it carries no load-balancing-specific
// fields so other account-scoped features can reuse it.
type AccountSpec struct {
	// ID is the Cloudflare account ID (32-character hex string).
	// Immutable -- changing it would repoint all referencing resources at a
	// different Cloudflare account.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern="^[0-9a-f]{32}$"
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="account ID is immutable"
	ID string `json:"id"`

	// CredentialsRef references the secret containing the Cloudflare API token.
	// The token needs account-scoped Load Balancing permissions. The secret
	// must be in the same namespace as the Account resource.
	// +kubebuilder:validation:Required
	CredentialsRef SecretRef `json:"credentialsRef"`
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

// AccountStatus defines the observed state of an Account.
// NOTE: No top-level ObservedGeneration -- it's set per-condition.
type AccountStatus struct {
	// Name is the Cloudflare account name, populated from the API on init.
	// +optional
	Name string `json:"name,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Account ID",type=string,JSONPath=`.spec.id`
// +kubebuilder:printcolumn:name="Account",type=string,JSONPath=`.status.name`
// +kubebuilder:printcolumn:name="Initialized",type=string,JSONPath=`.status.conditions[?(@.type=="Initialized")].status`

// Account is the Schema for the accounts API. It supplies the Cloudflare
// account ID and credentials for account-scoped Load Balancing resources.
type Account struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec AccountSpec `json:"spec"`

	// +optional
	Status AccountStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// AccountList contains a list of Account.
type AccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []Account `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Account{}, &AccountList{})
}
