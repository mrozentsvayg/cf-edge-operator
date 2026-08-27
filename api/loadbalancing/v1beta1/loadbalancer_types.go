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
//
// +kubebuilder:validation:XValidation:rule="!has(self.sessionAffinity) || self.sessionAffinity != 'header' || (has(self.sessionAffinityAttributes) && has(self.sessionAffinityAttributes.headers) && size(self.sessionAffinityAttributes.headers) > 0)",message="sessionAffinity=header requires sessionAffinityAttributes.headers to be non-empty"
// +kubebuilder:validation:XValidation:rule="!has(self.sessionAffinityTtl) || (has(self.sessionAffinity) && self.sessionAffinity != 'none')",message="sessionAffinityTtl is only valid when sessionAffinity is set and not 'none'"
// +kubebuilder:validation:XValidation:rule="!has(self.sessionAffinityAttributes) || (has(self.sessionAffinity) && self.sessionAffinity != 'none')",message="sessionAffinityAttributes is only valid when sessionAffinity is set and not 'none'"
//
// Pool weights (defaultPoolRefs[].weight and defaultWeight) only take effect under a weighted
// steering policy, so reject them otherwise -- catching silently-inert config at admission. The
// policy set here MUST stay in sync with weightedSteeringActive in
// internal/controller/loadbalancer_controller.go (CEL cannot call Go).
// +kubebuilder:validation:XValidation:rule="!(self.defaultPoolRefs.exists(p, has(p.weight)) || has(self.defaultWeight)) || self.steeringPolicy in ['random','least_outstanding_requests','least_connections']",message="pool weights (defaultPoolRefs[].weight, defaultWeight) require steeringPolicy to be random, least_outstanding_requests, or least_connections"
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
	// resolved to a CF pool ID from the referenced Pool CR's Status.ID, and may
	// carry an optional per-pool weight used by weighted steering policies (see
	// LoadBalancerDefaultPoolRef.Weight and SteeringPolicy). Names must be unique
	// within the list (the CR name is the CF pool name, which is account-unique).
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	// +listType=map
	// +listMapKey=name
	DefaultPoolRefs []LoadBalancerDefaultPoolRef `json:"defaultPoolRefs"`

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
	// - "least_outstanding_requests" / "least_connections": weighted, traffic-
	//   steering-add-on gated (like geo/dynamic_latency/proximity).
	// Defaults to "off" -- Cloudflare's own API default, and the only policy
	// available without the traffic-steering add-on (dynamic_latency and the
	// other advanced policies fail LB creation on the base tier). Set an advanced
	// policy explicitly when the account has the add-on.
	// Pool weights (defaultPoolRefs[].weight, defaultWeight) apply only under the
	// weighted policies random / least_outstanding_requests / least_connections.
	// +kubebuilder:validation:Enum=off;geo;random;dynamic_latency;proximity;least_outstanding_requests;least_connections
	// +kubebuilder:default=off
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

	// Enabled controls whether the load balancer serves traffic. A disabled LB
	// keeps its configuration but stops resolving to any pool. Defaults to true;
	// the operator always manages this flag (an out-of-band disable is corrected
	// back to the CR's value), unifying the enabled representation across
	// LoadBalancer, LoadBalancerPool, and pool origins.
	// NOTE: Cloudflare's create API does not accept enabled, so the controller
	// applies it via a follow-up edit (create-then-edit).
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// RegionPools maps CF region codes to ordered pool reference lists that
	// override DefaultPoolRefs for clients from that region. Only used when
	// SteeringPolicy=geo. Region codes are CF's macro-regions:
	//   WNAM, ENAM, WEU, EEU, NSAM, SSAM, OC, ME, NAF, SAF, SAS, SEAS, NEAS.
	// See https://developers.cloudflare.com/load-balancing/reference/region-mapping-api/.
	//
	// Presence, not emptiness, decides management: when set (even to an empty
	// map) the operator owns the full region map on Cloudflare -- codes the CR
	// drops are removed. Cloudflare deep-merges the geo maps on PATCH and rejects
	// a per-key null, so a dropped code is removed by clearing the whole map (a
	// top-level null) and re-adding the remaining codes. When omitted (nil) the
	// operator leaves Cloudflare's region_pools untouched. A pointer distinguishes
	// an explicit empty map ("clear all region steering") from "unset" and survives
	// the operator's own spec writes (see LoadBalancerMonitor.Header for why).
	// +optional
	RegionPools *map[string][]LoadBalancerPoolRef `json:"regionPools,omitempty"`

	// CountryPools maps ISO-3166-1-alpha-2 country codes to ordered pool
	// reference lists that override DefaultPoolRefs (and RegionPools) for
	// clients from that country. Only used when SteeringPolicy=geo.
	// Presence-not-emptiness management, as for RegionPools.
	// +optional
	CountryPools *map[string][]LoadBalancerPoolRef `json:"countryPools,omitempty"`

	// PopPools maps CF PoP identifiers to ordered pool reference lists.
	// Enterprise-only feature. Only used when SteeringPolicy=geo.
	// Presence-not-emptiness management, as for RegionPools.
	// +optional
	PopPools *map[string][]LoadBalancerPoolRef `json:"popPools,omitempty"`

	// SessionAffinity sets client-to-pool stickiness across requests.
	// - "none" (default): each request may hit any healthy pool.
	// - "cookie": CF sets a cookie so a client sticks to one origin.
	// - "ip_cookie": cookie tied to client IP.
	// - "header": stickiness by a request header value.
	// +kubebuilder:validation:Enum=none;cookie;ip_cookie;header
	// +optional
	SessionAffinity string `json:"sessionAffinity,omitempty"`

	// SessionAffinityAttributes tunes session affinity behavior. Only meaningful
	// when SessionAffinity is not "none" (enforced by a CEL rule). When
	// SessionAffinity="header", Headers must be non-empty.
	// +optional
	SessionAffinityAttributes *LoadBalancerSessionAffinityAttributes `json:"sessionAffinityAttributes,omitempty"`

	// SessionAffinityTtl is the time, in seconds, until a client's session
	// expires after creation. Only meaningful when SessionAffinity is not "none"
	// (enforced by a CEL rule). The accepted range is policy-dependent: Cloudflare
	// requires 1800-604800 for cookie / ip_cookie affinity and 30-3600 for header
	// affinity, so the schema envelope is intentionally broad -- pick a value valid
	// for the chosen SessionAffinity.
	// +kubebuilder:validation:Minimum=30
	// +kubebuilder:validation:Maximum=604800
	// +optional
	SessionAffinityTtl int32 `json:"sessionAffinityTtl,omitempty"`

	// AdaptiveRouting configures zero-downtime failover across pools. Optional:
	// unset leaves Cloudflare's setting intact; set enforces it.
	// +optional
	AdaptiveRouting *LoadBalancerAdaptiveRouting `json:"adaptiveRouting,omitempty"`

	// LocationStrategy controls location-based steering for non-proxied requests.
	// Optional: unset leaves Cloudflare's setting intact; set enforces it.
	// +optional
	LocationStrategy *LoadBalancerLocationStrategy `json:"locationStrategy,omitempty"`

	// DefaultWeight is the weight applied to default pools that do not carry an
	// explicit defaultPoolRefs[].weight, relative to the other pools. It only
	// takes effect under a weighted steering policy (random /
	// least_outstanding_requests / least_connections); Cloudflare defaults it to
	// 1. Expressed as a string in the range 0.0-1.0 to preserve fractional
	// precision (Cloudflare accepts a float). Maps to Cloudflare's wire field
	// random_steering.default_weight.
	// +kubebuilder:validation:Pattern="^(0(\\.[0-9]+)?|1(\\.0+)?)$"
	// +optional
	DefaultWeight string `json:"defaultWeight,omitempty"`

	// Networks is the list of network identifiers (e.g. private-network scopes)
	// the load balancer is available on. Optional: unset leaves Cloudflare's
	// setting intact.
	// +optional
	Networks []string `json:"networks,omitempty"`

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

// LoadBalancerDefaultPoolRef is a default-pool reference plus this LoadBalancer's
// per-pool weight for weighted steering. The weight lives on the reference (a
// per-LB policy property), NOT on the account-wide LoadBalancerPool CR, so the
// same pool can carry different weights in different LoadBalancers -- matching
// Cloudflare's per-LB random_steering.pool_weights map.
type LoadBalancerDefaultPoolRef struct {
	LoadBalancerPoolRef `json:",inline"`

	// Weight is this pool's weight relative to the other default pools, used only
	// under a weighted steering policy (random / least_outstanding_requests /
	// least_connections). Expressed as a string in the range 0.0-1.0 to preserve
	// fractional precision. Omit to use the LoadBalancer's defaultWeight (or
	// Cloudflare's default of 1). Rejected by admission under a non-weighting
	// steering policy (see the LoadBalancerSpec validation rules).
	// +kubebuilder:validation:Pattern="^(0(\\.[0-9]+)?|1(\\.0+)?)$"
	// +optional
	Weight string `json:"weight,omitempty"`
}

// LoadBalancerAdaptiveRouting configures how Cloudflare routes requests in
// response to dynamic conditions (e.g. zero-downtime failover between pools).
type LoadBalancerAdaptiveRouting struct {
	// FailoverAcrossPools extends zero-downtime failover to healthy origins in
	// alternate pools when no healthy origin exists in the same pool, following the
	// traffic/origin steering order. Cloudflare defaults this to false (failover
	// stays within a pool). Leave unset to keep Cloudflare's value.
	// +optional
	FailoverAcrossPools *bool `json:"failoverAcrossPools,omitempty"`
}

// LoadBalancerLocationStrategy controls location-based steering for non-proxied
// (DNS-only) requests. See SteeringPolicy for how steering is affected.
type LoadBalancerLocationStrategy struct {
	// Mode is the authoritative location used when ECS is not preferred, absent,
	// or its GeoIP lookup fails.
	//   - "pop": use the Cloudflare PoP location.
	//   - "resolver_ip": use the DNS resolver GeoIP location (falling back to the PoP).
	// +kubebuilder:validation:Enum=pop;resolver_ip
	// +optional
	Mode string `json:"mode,omitempty"`

	// PreferECS controls whether the EDNS Client Subnet (ECS) GeoIP should be
	// preferred as the authoritative location.
	//   - "always" / "never": always or never prefer ECS.
	//   - "proximity": prefer ECS only when SteeringPolicy=proximity.
	//   - "geo": prefer ECS only when SteeringPolicy=geo.
	// +kubebuilder:validation:Enum=always;never;proximity;geo
	// +optional
	PreferECS string `json:"preferECS,omitempty"`
}

// LoadBalancerSessionAffinityAttributes tunes session affinity behavior. It is
// only meaningful when the LoadBalancer's SessionAffinity is not "none".
type LoadBalancerSessionAffinityAttributes struct {
	// DrainDuration is the drain duration in seconds, applied when session
	// affinity is enabled. 0 leaves Cloudflare's default.
	// +kubebuilder:validation:Minimum=0
	// +optional
	DrainDuration int32 `json:"drainDuration,omitempty"`

	// Headers is the list of HTTP header names session affinity is based on when
	// SessionAffinity="header". At least one header name is required in that mode
	// (enforced by a CEL rule on the spec). To pin specific cookies, use an item
	// of the form "cookie:<name-1>,<name-2>".
	// +optional
	Headers []string `json:"headers,omitempty"`

	// RequireAllHeaders, when SessionAffinity="header", controls how the Headers
	// list is matched: true requires all listed headers to be present to create a
	// session; false requires at least one. Leave unset to keep Cloudflare's value.
	// +optional
	RequireAllHeaders *bool `json:"requireAllHeaders,omitempty"`

	// Samesite configures the SameSite attribute on the affinity cookie. "Auto"
	// resolves to "Lax" or "None" depending on whether Always Use HTTPS is enabled;
	// when using "None", Secure must not be "Never".
	// +kubebuilder:validation:Enum=Auto;Lax;None;Strict
	// +optional
	Samesite string `json:"samesite,omitempty"`

	// Secure configures the Secure attribute on the affinity cookie. "Always" sets
	// it, "Never" omits it, and "Auto" sets it based on whether Always Use HTTPS is
	// enabled.
	// +kubebuilder:validation:Enum=Auto;Always;Never
	// +optional
	Secure string `json:"secure,omitempty"`

	// ZeroDowntimeFailover configures failover between origins within a pool while
	// session affinity is enabled.
	//   - "none": no failover for pinned sessions (default).
	//   - "temporary": send traffic elsewhere until the pinned origin is healthy.
	//   - "sticky": update the affinity cookie and pin to the new origin.
	// +kubebuilder:validation:Enum=none;temporary;sticky
	// +optional
	ZeroDowntimeFailover string `json:"zeroDowntimeFailover,omitempty"`
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

	// UnresolvedPoolRefs lists the referenced pool names (from any ref slot --
	// default (including a weighted entry), fallback, or geo) that had no ready
	// Pool CR on the last reconcile. A non-empty list means the LB is serving in a
	// degraded/partial state (some pools dropped); it converges as those pools
	// become ready. Surfaced for observability -- see the Unresolved printcolumn.
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
// +kubebuilder:printcolumn:name="Unresolved",type=integer,JSONPath=`.status.unresolvedPoolRefs.length()`
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
