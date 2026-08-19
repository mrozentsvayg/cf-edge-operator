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

// LoadBalancerMonitorSpec defines the desired state of LoadBalancerMonitor.
//
// A LoadBalancerMonitor is a Cloudflare health-check definition that pools
// reference to probe their origins. Monitors are Cloudflare account-scoped
// (not zone-scoped): a monitor created in one namespace/cluster is visible
// across the whole CF account. The controller keys off the CR name for the
// CF-side identity, so charts must ensure names are unique across the fleet
// (e.g. by prefixing with the service / namespace).
type LoadBalancerMonitorSpec struct {
	// AccountRef references the Account supplying the Cloudflare account ID
	// and API credentials. Monitors are account-scoped in Cloudflare.
	// +kubebuilder:validation:Required
	AccountRef AccountRef `json:"accountRef"`

	// Type is the protocol to use for the health check.
	// +kubebuilder:validation:Enum=http;https;tcp;udp_icmp;icmp_ping;smtp
	// +kubebuilder:default=https
	// +optional
	Type string `json:"type,omitempty"`

	// Method is the HTTP method (for http/https types) or "connection_established"
	// for tcp. Defaults to "GET" for http/https on the CF side if omitted.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	Method string `json:"method,omitempty"`

	// Path is the endpoint path to probe. Only valid for http/https types.
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	Path string `json:"path,omitempty"`

	// Port is the port to connect to. Only required for tcp/udp/smtp.
	// For http/https, only set to override the protocol default (80/443).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=65535
	// +optional
	Port int32 `json:"port,omitempty"`

	// Header is the set of request headers to send with the probe. The Host
	// header is commonly needed when probing shared origins.
	// +optional
	Header map[string][]string `json:"header,omitempty"`

	// ExpectedCodes is the HTTP response code or range considered healthy
	// (e.g. "200", "2xx", "200-299"). Only valid for http/https.
	// +kubebuilder:validation:MaxLength=32
	// +optional
	ExpectedCodes string `json:"expectedCodes,omitempty"`

	// ExpectedBody is a case-insensitive substring that must appear in the
	// response body for the origin to be considered healthy. Only valid for
	// http/https.
	// +kubebuilder:validation:MaxLength=8192
	// +optional
	ExpectedBody string `json:"expectedBody,omitempty"`

	// FollowRedirects controls whether the probe follows HTTP redirects.
	// Only valid for http/https.
	// +optional
	FollowRedirects bool `json:"followRedirects,omitempty"`

	// AllowInsecure skips TLS certificate validation. Only valid for https.
	// +optional
	AllowInsecure bool `json:"allowInsecure,omitempty"`

	// Interval is the seconds between health-check probes. Shorter intervals
	// detect failures faster but add load to origins. CF minimum is 15s;
	// account-plan-dependent.
	// +kubebuilder:validation:Minimum=15
	// +kubebuilder:validation:Maximum=3600
	// +kubebuilder:default=60
	// +optional
	Interval int32 `json:"interval,omitempty"`

	// Retries is the number of immediate retries before marking the origin
	// unhealthy on a single interval. Total retries per interval = Retries + 1.
	// 0 is valid (fail on the first probe). NOTE: no "omitempty" -- 0 is a
	// meaningful value, and with a default of 2 an omitempty tag would drop an
	// explicit 0 from the operator's own spec writes (e.g. adding the finalizer),
	// causing the API server to re-apply the default and silently turn 0 into 2.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=5
	// +kubebuilder:default=2
	// +optional
	Retries int32 `json:"retries"`

	// Timeout is seconds to wait for a probe response before considering the
	// origin unhealthy for that attempt.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10
	// +kubebuilder:default=5
	// +optional
	Timeout int32 `json:"timeout,omitempty"`

	// ConsecutiveUp is the number of consecutive healthy probes before an
	// unhealthy origin is marked healthy again. 0 means "use CF default".
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	// +optional
	ConsecutiveUp int32 `json:"consecutiveUp,omitempty"`

	// ConsecutiveDown is the number of consecutive unhealthy probes before
	// a healthy origin is marked unhealthy. 0 means "use CF default".
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	// +optional
	ConsecutiveDown int32 `json:"consecutiveDown,omitempty"`

	// ProbeZone sets the zone to emulate when probing. If set, the probe
	// sends a Host header for this zone; useful when origins serve multiple
	// hostnames. Only valid for http/https.
	// +kubebuilder:validation:MaxLength=253
	// +optional
	ProbeZone string `json:"probeZone,omitempty"`

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

// LoadBalancerMonitorStatus is the observed state of a LoadBalancerMonitor.
type LoadBalancerMonitorStatus struct {
	// ID is the Cloudflare-assigned monitor ID, used for updates and deletes.
	// Read by the LoadBalancerPool controller so a pool's spec.monitorRef can
	// resolve to a CF monitor ID without an extra CF API round-trip.
	// +optional
	ID string `json:"id,omitempty"`

	// CreateCount tracks how many times this monitor has been (re)created in
	// Cloudflare. Values greater than 1 indicate external deletions occurred
	// (e.g. via CF dashboard).
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
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.type`
// +kubebuilder:printcolumn:name="Path",type=string,JSONPath=`.spec.path`
// +kubebuilder:printcolumn:name="Interval",type=integer,JSONPath=`.spec.interval`
// +kubebuilder:printcolumn:name="CF ID",type=string,JSONPath=`.status.id`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Creates",type=integer,JSONPath=`.status.createCount`
// +kubebuilder:printcolumn:name="Errors",type=integer,JSONPath=`.status.consecutiveErrors`

// LoadBalancerMonitor is the Schema for the loadbalancermonitors API.
type LoadBalancerMonitor struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec LoadBalancerMonitorSpec `json:"spec"`

	// +optional
	Status LoadBalancerMonitorStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// LoadBalancerMonitorList contains a list of LoadBalancerMonitor.
type LoadBalancerMonitorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []LoadBalancerMonitor `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LoadBalancerMonitor{}, &LoadBalancerMonitorList{})
}
