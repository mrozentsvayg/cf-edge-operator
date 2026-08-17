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

// LoadBalancerSpec defines the desired state of a Cloudflare LoadBalancer.
//
// Load Balancers are Cloudflare zone-scoped: the Hostname becomes a DNS
// record in the ZoneRef zone that CF geo-steers to healthy origin pools.
// The CR name is a Kubernetes identifier; the CF-side LB is named after the
// Hostname, so charts must ensure hostnames are unique per zone.
//
// DefaultPoolRefs / FallbackPoolRef / RegionPools etc. reference
// LoadBalancerPool CRs by name; the controller resolves each to its CF pool
// ID from the referenced Pool CR's Status.ID. For the multi-region pattern,
// the regional Pool CRs and this LoadBalancer are managed together by one
// (control-cluster) operator, so every ref is a local CR.
type LoadBalancerSpec struct {
	// ZoneRef references the Zone hosting the LB's DNS name. The Zone
	// provides both the CF zone ID (for the LB API call) and the CF API
	// credentials.
	// +kubebuilder:validation:Required
	ZoneRef ZoneRef `json:"zoneRef"`

	// Hostname is the DNS name that clients resolve. Must be within the
	// referenced zone. Immutable once set -- changing it would orphan the
	// existing CF load balancer.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Format=hostname
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="hostname is immutable"
	Hostname string `json:"hostname"`

	// DefaultPoolRefs is the ordered list of pool references CF uses when no
	// region_pools / country_pools / pop_pools rule matches. Each ref is
	// resolved to a CF pool ID from the referenced Pool CR's Status.ID.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	DefaultPoolRefs []LoadBalancerPoolRef `json:"defaultPoolRefs"`

	// FallbackPoolRef is the pool CF sends traffic to when EVERY other pool
	// is unhealthy. Required by CF, so we require it here.
	// +kubebuilder:validation:Required
	FallbackPoolRef LoadBalancerPoolRef `json:"fallbackPoolRef"`

	// SteeringPolicy is how CF picks a pool from the pool list.
	// - "off": always use defaultPools order; no geo/latency logic.
	// - "geo": use region_pools / country_pools / pop_pools.
	// - "random": pick a random pool weighted by random_steering weights.
	// - "dynamic_latency": pick the pool with lowest observed latency from
	//   the client's PoP (uses CF's real-time analytics).
	// - "proximity": pick the pool closest by geographic distance.
	// - "least_outstanding_requests" / "least_connections": Enterprise only.
	// Defaults to dynamic_latency for latency-sensitive multi-region workloads.
	// +kubebuilder:validation:Enum=off;geo;random;dynamic_latency;proximity;least_outstanding_requests;least_connections
	// +kubebuilder:default=dynamic_latency
	// +optional
	SteeringPolicy string `json:"steeringPolicy,omitempty"`

	// Proxied controls whether the LB hostname is orange-clouded (CF proxy
	// on, HTTP/S routing) or grey-clouded (DNS-only, direct IP steering).
	// Load-balancer features (health checks, most steering policies)
	// require proxied=true.
	// +kubebuilder:default=true
	// +optional
	Proxied *bool `json:"proxied,omitempty"`

	// TTL is the DNS TTL in seconds. Only meaningful when Proxied=false;
	// CF ignores it for proxied LBs (uses its own internal TTL).
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=86400
	// +optional
	TTL int32 `json:"ttl,omitempty"`

	// RegionPools maps CF region codes to ordered pool reference lists that
	// override DefaultPoolRefs for clients from that region. Only used when
	// SteeringPolicy=geo. Region codes are CF's macro-regions:
	//   WNAM, ENAM, WEU, EEU, NSAM, SSAM, OC, ME, NAF, SAF, SAS, SEAS, NEAS.
	// See https://developers.cloudflare.com/load-balancing/reference/region-mapping-api/.
	// +optional
	RegionPools map[string][]LoadBalancerPoolRef `json:"regionPools,omitempty"`

	// CountryPools maps ISO-3166-1-alpha-2 country codes to ordered pool
	// reference lists that override DefaultPoolRefs (and RegionPools) for
	// clients from that country. Only used when SteeringPolicy=geo.
	// +optional
	CountryPools map[string][]LoadBalancerPoolRef `json:"countryPools,omitempty"`

	// PopPools maps CF PoP identifiers to ordered pool reference lists.
	// Enterprise-only feature. Only used when SteeringPolicy=geo.
	// +optional
	PopPools map[string][]LoadBalancerPoolRef `json:"popPools,omitempty"`

	// SessionAffinity sets client-to-pool stickiness across requests.
	// - "none" (default): each request may hit any healthy pool.
	// - "cookie": CF sets a cookie so a client sticks to one origin.
	// - "ip_cookie": cookie tied to client IP.
	// - "header": stickiness by a request header value.
	// +kubebuilder:validation:Enum=none;cookie;ip_cookie;header
	// +optional
	SessionAffinity string `json:"sessionAffinity,omitempty"`

	// Description is a human-readable description shown in the CF dashboard.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	Description string `json:"description,omitempty"`

	// ManagementPolicy overrides the operator-wide --management-policy for
	// this CR. See CustomHostname docs for semantics.
	// +kubebuilder:validation:Enum=manage;create;observe
	// +optional
	ManagementPolicy string `json:"managementPolicy,omitempty"`

	// DeletePolicy overrides the operator-wide --delete-policy for this CR.
	// See CustomHostname docs for semantics.
	// +kubebuilder:validation:Enum=always;own-only;never
	// +optional
	DeletePolicy string `json:"deletePolicy,omitempty"`
}

// LoadBalancerPoolRef references a LoadBalancerPool CR by name. The controller
// resolves it to a CF pool ID by reading that Pool CR's Status.ID.
type LoadBalancerPoolRef struct {
	// Name of the LoadBalancerPool CR (which is also the CF pool name).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the LoadBalancerPool CR. Defaults to this LoadBalancer's
	// namespace if omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// LoadBalancerStatus is the observed state of a LoadBalancer.
type LoadBalancerStatus struct {
	// ID is the Cloudflare-assigned LB ID, used for updates and deletes.
	// +optional
	ID string `json:"id,omitempty"`

	// ResolvedDefaultPoolIDs is the CF pool IDs the controller resolved
	// from spec.defaultPoolRefs on the last successful reconcile.
	// Order matches the spec.
	// +optional
	ResolvedDefaultPoolIDs []string `json:"resolvedDefaultPoolIDs,omitempty"`

	// ResolvedFallbackPoolID is the CF pool ID for spec.fallbackPoolRef.
	// +optional
	ResolvedFallbackPoolID string `json:"resolvedFallbackPoolID,omitempty"`

	// CreateCount tracks how many times this LB has been (re)created in
	// Cloudflare. Values greater than 1 indicate external deletions occurred.
	// +optional
	CreateCount int32 `json:"createCount,omitempty"`

	// ConsecutiveErrors is the number of consecutive reconcile failures.
	// Resets to 0 on the next successful reconcile.
	// +optional
	ConsecutiveErrors int32 `json:"consecutiveErrors,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Hostname",type=string,JSONPath=`.spec.hostname`
// +kubebuilder:printcolumn:name="Steering",type=string,JSONPath=`.spec.steeringPolicy`
// +kubebuilder:printcolumn:name="Pools",type=integer,JSONPath=`.spec.defaultPoolRefs.length()`
// +kubebuilder:printcolumn:name="CF ID",type=string,JSONPath=`.status.id`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Creates",type=integer,JSONPath=`.status.createCount`
// +kubebuilder:printcolumn:name="Errors",type=integer,JSONPath=`.status.consecutiveErrors`

// LoadBalancer is the Schema for the loadbalancers API.
type LoadBalancer struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec LoadBalancerSpec `json:"spec"`

	// +optional
	Status LoadBalancerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// LoadBalancerList contains a list of LoadBalancer.
type LoadBalancerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []LoadBalancer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LoadBalancer{}, &LoadBalancerList{})
}
