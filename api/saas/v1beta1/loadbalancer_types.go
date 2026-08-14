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
// CR names are used verbatim as the CF-side LB name (which is the hostname
// itself for public LBs), so charts must ensure names are unique per zone.
//
// Pool references are by-name across the whole CF account, so this CR can
// reference LoadBalancerPool CRs that live in a different namespace or even
// a different cluster (as long as both clusters point at the same CF
// account). The controller resolves peer pool names by:
//  1. Look up the local LoadBalancerPool CR (same-cluster) if it exists.
//  2. Fall back to a CF pool-list-by-name lookup if the CR isn't found
//     locally (i.e. it's owned by a peer cluster).
//
// This is what enables the multi-region pattern: each region's cluster owns
// its own LoadBalancerPool CR; the parent region owns the LoadBalancer CR
// that stitches them together.
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
	// region_pools / country_pools / pop_pools rule matches. Pool references
	// are resolved to CF pool IDs at reconcile time (see the CRD-level doc
	// for cross-cluster resolution semantics).
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

	// MinimumPools controls behavior when some referenced pools can't be
	// resolved (e.g., peer cluster hasn't yet reconciled). Interpretation:
	//   - unset (nil): fail-hard. LB reconcile errors if any pool ref is
	//     unresolvable; requeues until all pools exist.
	//   - set to N: partial. LB is created with whatever pools resolve, as
	//     long as at least N of the total (default + fallback) resolve.
	//     Missing pools are omitted from the CF LB config; they get added
	//     on the next reconcile after they appear in CF.
	// The partial mode trades slightly less-precise routing for faster
	// bootstrap: a new multi-region service can bring its global endpoint
	// up as soon as one region's pool exists, and other regions light up
	// as they come online.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MinimumPools *int32 `json:"minimumPools,omitempty"`

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

// LoadBalancerPoolRef references a LoadBalancerPool by name.
//
// If Namespace is set, the controller looks up a local LoadBalancerPool CR
// at that (Name, Namespace) and reads its Status.ID for the CF pool ID.
//
// If Namespace is unset, the controller first tries a local same-namespace
// LoadBalancerPool CR lookup. On miss (which is expected for peer-cluster
// pools), it falls back to a CF-side pool-list-by-name resolution.
//
// The cross-cluster fallback is what enables the multi-region pattern
// (parent-region LB references peer-region pools created by peer-region
// clusters). See the LoadBalancer CRD-level doc.
type LoadBalancerPoolRef struct {
	// Name of the LoadBalancerPool (which is also the CF pool name).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the local LoadBalancerPool CR, when Name is
	// same-cluster. Omit for cross-cluster peer pools; the controller
	// resolves via CF API by name.
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

	// UnresolvedPoolRefs lists pool ref names the controller couldn't
	// resolve on the last reconcile. Under fail-hard mode this triggers a
	// requeue; under partial mode (spec.minimumPools set) these are
	// dropped from the CF LB config until they appear.
	// +optional
	UnresolvedPoolRefs []string `json:"unresolvedPoolRefs,omitempty"`

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
// +kubebuilder:printcolumn:name="Unresolved",type=integer,JSONPath=`.status.unresolvedPoolRefs.length()`,priority=1
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
