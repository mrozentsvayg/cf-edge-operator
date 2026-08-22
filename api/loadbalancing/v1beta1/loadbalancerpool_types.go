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

// LoadBalancerPoolSpec defines the desired state of a Cloudflare
// LoadBalancerPool.
//
// Pools are Cloudflare account-scoped resources; the CR's AccountRef points at
// an Account (Cloudflare account ID + credentials). CR names are used verbatim
// as the CF-side pool name, so charts must ensure names are unique across the
// fleet (typically by prefixing with the service / namespace, e.g. "web-us-pool").
//
// +kubebuilder:validation:XValidation:rule="!has(self.latitude) == !has(self.longitude)",message="latitude and longitude must be set together (both or neither)"
// +kubebuilder:validation:XValidation:rule="!has(self.checkRegions) || size(self.checkRegions) <= 1 || !('ALL_REGIONS' in self.checkRegions)",message="checkRegions: ALL_REGIONS must be the only entry when used"
type LoadBalancerPoolSpec struct {
	// AccountRef references the Account supplying the Cloudflare account ID
	// and API credentials. Pools are account-scoped in Cloudflare.
	// +kubebuilder:validation:Required
	AccountRef AccountRef `json:"accountRef"`

	// Origins is the set of backend endpoints this pool balances traffic
	// across. Each origin has its own health status tracked by CF using the
	// pool's monitor.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinItems=1
	Origins []LoadBalancerPoolOrigin `json:"origins"`

	// Enabled controls whether the pool receives traffic. Disabled pools are
	// excluded from steering and health checks; any LB referencing a disabled
	// pool will fail over to the next pool in its list.
	// Defaults to true.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// MonitorRef references the LoadBalancerMonitor whose CF ID CF should use
	// to health-check origins in this pool. The controller resolves the ref
	// to a monitor CR, reads its Status.ID, and passes the CF monitor ID to
	// CF. If the monitor CR isn't ready yet (Status.ID empty), the pool
	// reconcile requeues.
	//
	// TODO: Cloudflare monitor groups (pool.monitor_group) are deferred to a
	// future LoadBalancerMonitorGroup CRD. When that lands, a pool will reference
	// either a monitor or a monitor group via a monitorGroupRef field, constrained
	// to at-most-one of {monitorRef, monitorGroupRef}. A monitorless pool stays
	// valid today (no monitor is attached).
	// +optional
	MonitorRef *LoadBalancerMonitorRef `json:"monitorRef,omitempty"`

	// MinimumOrigins is the number of origins that must be healthy for the
	// pool itself to be considered healthy. If fewer origins are healthy the
	// pool is marked unhealthy and any LB referencing it fails over.
	// CF default is 1.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1024
	// +optional
	MinimumOrigins int32 `json:"minimumOrigins,omitempty"`

	// OriginSteering controls how origins are selected for new sessions and
	// traffic without session affinity. Optional: unset leaves Cloudflare's
	// setting intact; set enforces it.
	// +optional
	OriginSteering *LoadBalancerPoolOriginSteering `json:"originSteering,omitempty"`

	// CheckRegions restricts which Cloudflare regions health-check this pool's
	// origins. Empty means Cloudflare probes from all regions (the default). Region
	// codes are CF's macro-regions; ALL_REGIONS is Enterprise-only and, if used,
	// must be the sole entry (enforced by a CEL rule on the spec). Applied via a
	// follow-up edit (Cloudflare rejects check_regions on create).
	// +kubebuilder:validation:items:Enum=WNAM;ENAM;WEU;EEU;NSAM;SSAM;OC;ME;NAF;SAF;SAS;SEAS;NEAS;ALL_REGIONS
	// +optional
	CheckRegions []string `json:"checkRegions,omitempty"`

	// LoadShedding configures how the pool sheds traffic. Optional pointer: unset
	// means the operator does not manage load shedding (an incident-time manual
	// shed set out-of-band survives). When set, each subfield is managed only when
	// specified (leave-alone per subfield): a subfield left empty keeps Cloudflare's
	// current value rather than being reset.
	// +optional
	LoadShedding *LoadBalancerPoolLoadShedding `json:"loadShedding,omitempty"`

	// NotificationEmail is a comma-separated list of email addresses CF
	// notifies when the pool's health changes. Deprecated by CF in favor of
	// their centralized notifications, but still functional.
	// +kubebuilder:validation:MaxLength=1024
	// +optional
	NotificationEmail string `json:"notificationEmail,omitempty"`

	// Latitude sets the pool's geographic latitude for latitude/longitude-based
	// steering. Valid range -90 to 90 (validated via pattern; use decimal
	// degrees, e.g. "37.7749"). If set, Longitude must also be set.
	// Represented as a string to preserve fractional precision; CF accepts
	// float in its API.
	// +kubebuilder:validation:Pattern="^-?([0-8]?[0-9](\\.[0-9]+)?|90(\\.0+)?)$"
	// +optional
	Latitude *string `json:"latitude,omitempty"`

	// Longitude sets the pool's geographic longitude for latitude/longitude-based
	// steering. Valid range -180 to 180 (validated via pattern; use decimal
	// degrees, e.g. "-122.4194"). If set, Latitude must also be set.
	// +kubebuilder:validation:Pattern="^-?(1[0-7][0-9](\\.[0-9]+)?|[0-9]{1,2}(\\.[0-9]+)?|180(\\.0+)?)$"
	// +optional
	Longitude *string `json:"longitude,omitempty"`

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

// LoadBalancerPoolOrigin is a single backend endpoint in a pool.
type LoadBalancerPoolOrigin struct {
	// Name is a human-readable identifier for the origin, shown in CF status
	// and dashboards. Not the routing target; use Address for that.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=255
	Name string `json:"name"`

	// Address is the IP address or publicly-resolvable hostname of the
	// origin. Hostnames MUST resolve directly to the origin -- a
	// Cloudflare-proxied CNAME won't work. To use an internal/reserved address,
	// VirtualNetworkID must also be set.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Address string `json:"address"`

	// Port is the upstream port for connections to this origin. 0 (the default)
	// means the protocol's default port is used. Modeled so the operator no longer
	// clobbers an out-of-band port on every update.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// VirtualNetworkID is the virtual network subnet ID the origin belongs to.
	// Required when Address is an internal/reserved address; the virtual network
	// must belong to the same Cloudflare account. Modeled so the operator no longer
	// clobbers an out-of-band value on every update.
	// +optional
	VirtualNetworkID string `json:"virtualNetworkID,omitempty"`

	// Enabled controls whether this specific origin receives traffic and is
	// health-checked. Disabled origins are hidden from steering.
	// Defaults to true.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// Weight is the origin's weight for the pool's origin_steering policy.
	// A weight of 0 excludes the origin from origin steering; higher weights
	// send more traffic. Only meaningful when multiple origins are present.
	// Represented as a string to preserve fractional precision (e.g. "0.5"),
	// but CF only accepts values from 0.0 to 1.0.
	// +kubebuilder:validation:Pattern="^(0(\\.\\d+)?|1(\\.0+)?)$"
	// +optional
	Weight string `json:"weight,omitempty"`

	// Header overrides request headers Cloudflare sends to this origin when
	// routing traffic or health-checking. Cloudflare currently supports
	// overriding only the Host header, so this is a typed field rather than a
	// free-form map (an arbitrary map would silently drop unsupported keys).
	// +optional
	Header *LoadBalancerOriginHeader `json:"header,omitempty"`
}

// LoadBalancerOriginHeader configures the per-origin request headers Cloudflare
// sends to an origin. Only the Host header is supported by Cloudflare.
type LoadBalancerOriginHeader struct {
	// Host overrides the Host header Cloudflare sends to this origin. Commonly
	// set when multiple origins share an address and are distinguished by
	// hostname. Cloudflare supports one Host override per origin, expressed as a
	// list. An empty or omitted list clears the override.
	// +optional
	Host []string `json:"host,omitempty"`
}

// LoadBalancerMonitorRef references a LoadBalancerMonitor CR whose Status.ID
// will be resolved to a CF monitor ID.
type LoadBalancerMonitorRef struct {
	// Name of the LoadBalancerMonitor CR.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace of the LoadBalancerMonitor CR. Defaults to the pool's
	// namespace if omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// LoadBalancerPoolOriginSteering configures how origins are selected within the
// pool for new sessions and traffic without session affinity.
type LoadBalancerPoolOriginSteering struct {
	// Policy is the origin-steering policy.
	//   - "random": select an origin randomly.
	//   - "hash": select an origin by hashing the CF-Connecting-IP address.
	//   - "least_outstanding_requests": weight by outstanding requests.
	//   - "least_connections": weight by open connections.
	// +kubebuilder:validation:Enum=random;hash;least_outstanding_requests;least_connections
	// +optional
	Policy string `json:"policy,omitempty"`
}

// LoadBalancerPoolLoadShedding configures how the pool sheds traffic. Each subfield
// is managed independently: the operator sends and drift-checks a subfield only when
// the CR specifies it, so an unset subfield keeps Cloudflare's current value.
type LoadBalancerPoolLoadShedding struct {
	// DefaultPercent is the percent of new-session and non-affinity traffic to
	// shed, per DefaultPolicy. Expressed as a string in the range 0.0-100.0 to
	// preserve fractional precision (Cloudflare accepts a float).
	// +kubebuilder:validation:Pattern="^(100(\\.0+)?|[0-9]{1,2}(\\.[0-9]+)?)$"
	// +optional
	DefaultPercent string `json:"defaultPercent,omitempty"`

	// DefaultPolicy is the policy used when shedding new-session traffic.
	//   - "random": shed a random percent of requests.
	//   - "hash": hash CF-Connecting-IP and shed all requests from a percent of IPs.
	// +kubebuilder:validation:Enum=random;hash
	// +optional
	DefaultPolicy string `json:"defaultPolicy,omitempty"`

	// SessionPercent is the percent of existing sessions to shed, per
	// SessionPolicy. Expressed as a string in the range 0.0-100.0.
	// +kubebuilder:validation:Pattern="^(100(\\.0+)?|[0-9]{1,2}(\\.[0-9]+)?)$"
	// +optional
	SessionPercent string `json:"sessionPercent,omitempty"`

	// SessionPolicy is the policy used when shedding existing sessions. Only "hash"
	// is supported (to avoid exponential decay).
	// +kubebuilder:validation:Enum=hash
	// +optional
	SessionPolicy string `json:"sessionPolicy,omitempty"`
}

// LoadBalancerPoolStatus is the observed state of a LoadBalancerPool.
type LoadBalancerPoolStatus struct {
	// ID is the Cloudflare-assigned pool ID, used for updates and deletes.
	// Also read by the LoadBalancer controller to resolve this pool's refs
	// (spec.defaultPoolRefs etc.) to CF pool IDs.
	// +optional
	ID string `json:"id,omitempty"`

	// Enabled is the pool's administrative enabled state as last observed from
	// Cloudflare. This reflects whether the pool is eligible to receive traffic,
	// NOT per-origin health -- Cloudflare exposes origin health via a separate
	// endpoint the operator does not currently poll.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// MonitorID is the resolved CF monitor ID from spec.monitorRef. Empty
	// when monitorRef is unset or the referenced monitor CR isn't ready.
	// +optional
	MonitorID string `json:"monitorID,omitempty"`

	// CreateCount tracks how many times this pool has been (re)created in
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
// +kubebuilder:printcolumn:name="Origins",type=integer,JSONPath=`.spec.origins.length()`
// +kubebuilder:printcolumn:name="Monitor",type=string,JSONPath=`.spec.monitorRef.name`
// +kubebuilder:printcolumn:name="CF ID",type=string,JSONPath=`.status.id`
// +kubebuilder:printcolumn:name="Enabled",type=boolean,JSONPath=`.status.enabled`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Creates",type=integer,JSONPath=`.status.createCount`
// +kubebuilder:printcolumn:name="Errors",type=integer,JSONPath=`.status.consecutiveErrors`

// LoadBalancerPool is the Schema for the loadbalancerpools API.
type LoadBalancerPool struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec LoadBalancerPoolSpec `json:"spec"`

	// +optional
	Status LoadBalancerPoolStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// LoadBalancerPoolList contains a list of LoadBalancerPool.
type LoadBalancerPoolList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []LoadBalancerPool `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LoadBalancerPool{}, &LoadBalancerPoolList{})
}
