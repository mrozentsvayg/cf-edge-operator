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

// CustomHostnameSpec defines the desired state of CustomHostname
type CustomHostnameSpec struct {
	// Hostname is the custom hostname to register with Cloudflare (e.g. customer.example.com).
	// Immutable after creation -- changing it would orphan the existing CF custom hostname.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Format=hostname
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="hostname is immutable"
	Hostname string `json:"hostname"`

	// OriginServer is the origin the custom hostname points to (e.g. origin.internal.example.com)
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Format=hostname
	OriginServer string `json:"originServer"`

	// OriginSNI overrides the SNI sent to the origin during TLS handshake.
	// If omitted, the operator does not manage Origin SNI -- Cloudflare uses its
	// default (the origin server hostname) and external changes are not corrected.
	// When set, the operator enforces the value on every reconcile.
	// Set to ":request_host_header:" to forward the incoming Host header as SNI.
	// Requires a Cloudflare account entitlement -- contact your Cloudflare account team to enable.
	// +kubebuilder:validation:MinLength=1
	// +optional
	OriginSNI *string `json:"originSNI,omitempty"`

	// ZoneRef references the Zone resource that owns this custom hostname.
	// The Zone must exist in the operator namespace.
	// +kubebuilder:validation:Required
	ZoneRef ZoneRef `json:"zoneRef"`

	// SSL configures the SSL/TLS certificate settings for this custom hostname.
	// +optional
	SSL *CustomHostnameSSL `json:"ssl,omitempty"`

	// ManagementPolicy overrides the operator-wide --management-policy for this CR.
	// "manage": full lifecycle -- create, update (drift correction), and delete per deletePolicy.
	// "create": provisions the hostname if missing, but never updates it (safe coexistence with
	// other tools like external-dns that may also modify the hostname). Deletes per deletePolicy.
	// "observe": read-only -- tracks an externally-managed hostname without creating, updating,
	// or deleting it. deletePolicy is ignored; the finalizer is always released on CR deletion.
	// If not set, the operator-wide default applies.
	// +kubebuilder:validation:Enum=manage;create;observe
	// +optional
	ManagementPolicy string `json:"managementPolicy,omitempty"`

	// DeletePolicy overrides the operator-wide --delete-policy for this CR.
	// "always" deletes from Cloudflare unconditionally; "own-only" skips deletion if the
	// current CF hostname ID differs from status.id (safe during migrations from other tools);
	// "never" releases the finalizer without deleting from Cloudflare.
	// Ignored when managementPolicy is "observe".
	// If not set, the operator-wide default applies.
	// +kubebuilder:validation:Enum=always;own-only;never
	// +optional
	DeletePolicy string `json:"deletePolicy,omitempty"`
}

// ZoneRef references a Zone resource
type ZoneRef struct {
	// Name of the Zone resource
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Namespace of the Zone resource. Defaults to the operator namespace if omitted.
	// +optional
	Namespace string `json:"namespace,omitempty"`
}

// SSL constant values matching CF API and kubebuilder Enum markers.
// Canonical order: CA, minTLS, method, type.
const (
	SSLCALetsEncrypt = "lets_encrypt"
	SSLCAGoogle      = "google"
	SSLCASSLCom      = "ssl_com"
	SSLMinTLS10      = "1.0"
	SSLMinTLS11      = "1.1"
	SSLMinTLS12      = "1.2"
	SSLMinTLS13      = "1.3"
	SSLMethodHTTP    = "http"
	SSLMethodTXT     = "txt"
	SSLMethodEmail   = "email"
	SSLTypeDV        = "dv"
	// NOTE: SSLSNIHostHeader is an SNI value, not an SSL setting. Kept here
	// for colocation with related CF constants.
	SSLSNIHostHeader = ":request_host_header:"
)

// CustomHostnameSSL configures SSL for a custom hostname.
// Each field is independently managed: set = enforce on create and drift correction,
// empty = use operator default on create (--ssl-* flags), don't correct drift.
type CustomHostnameSSL struct {
	// CertificateAuthority sets the CA for the certificate.
	// Requires an enterprise plan. Omit if not applicable.
	// +kubebuilder:validation:Enum=lets_encrypt;google;ssl_com
	// +optional
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// MinTLSVersion sets the minimum TLS version for this hostname.
	// +kubebuilder:validation:Enum="1.0";"1.1";"1.2";"1.3"
	// +optional
	MinTLSVersion string `json:"minTLSVersion,omitempty"`

	// Method of DCV (Domain Control Validation).
	// Empty = use --ssl-method operator default, then http.
	// +kubebuilder:validation:Enum=http;txt;email
	// +optional
	Method string `json:"method,omitempty"`

	// Type of SSL validation.
	// Empty = use --ssl-type operator default, then dv.
	// +kubebuilder:validation:Enum=dv
	// +optional
	Type string `json:"type,omitempty"`
}

// CustomHostnameStatus defines the observed state of CustomHostname
type CustomHostnameStatus struct {
	// ID is the Cloudflare-assigned custom hostname ID, used for updates and deletes
	// +optional
	ID string `json:"id,omitempty"`

	// HostnameStatus is the CF custom hostname activation status
	// (active, active_redeploying, pending, blocked, moved, etc.)
	// +optional
	HostnameStatus string `json:"hostnameStatus,omitempty"`

	// SSL reflects the current SSL certificate state as reported by Cloudflare
	// +optional
	SSL *CustomHostnameSSLStatus `json:"ssl,omitempty"`

	// CreateCount tracks how many times this hostname has been (re)created in Cloudflare.
	// A value greater than 1 indicates external deletions occurred (e.g. via CF dashboard).
	// +optional
	CreateCount int32 `json:"createCount,omitempty"`

	// ConsecutiveErrors is the number of consecutive reconcile failures.
	// Resets to 0 on the next successful reconcile.
	// +optional
	ConsecutiveErrors int32 `json:"consecutiveErrors,omitempty"`

	// SSLProvisioningStartedAt records when the hostname was created (or recreated) in Cloudflare.
	// Used to measure SSL provisioning duration (time until ssl.status == active).
	// Reset on each recreation so it always reflects the most recent provisioning cycle.
	// +optional
	SSLProvisioningStartedAt *metav1.Time `json:"sslProvisioningStartedAt,omitempty"`

	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// CustomHostnameSSLStatus reflects the SSL state from Cloudflare
type CustomHostnameSSLStatus struct {
	// Status is the certificate verification state (e.g. pending_validation, active, expired)
	// +optional
	Status string `json:"status,omitempty"`

	// CertificateAuthority is the CA that issued this certificate (lets_encrypt, google, ssl_com)
	// +optional
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// MinTLSVersion is the minimum TLS version configured for this hostname
	// +optional
	MinTLSVersion string `json:"minTLSVersion,omitempty"`

	// Method is the DCV method used for this certificate (http, txt, email)
	// +optional
	Method string `json:"method,omitempty"`

	// Type is the validation type (dv)
	// +optional
	Type string `json:"type,omitempty"`

	// ID is the Cloudflare SSL certificate identifier. Changes on reissue.
	// +optional
	ID string `json:"id,omitempty"`

	// Issuer is the certificate issuer (e.g. "Google Trust Services LLC")
	// +optional
	Issuer string `json:"issuer,omitempty"`

	// SerialNumber is the certificate serial number. Changes on reissue.
	// +optional
	SerialNumber string `json:"serialNumber,omitempty"`

	// BundleMethod is the certificate chain bundling method (ubiquitous, optimal, force)
	// +optional
	BundleMethod string `json:"bundleMethod,omitempty"`

	// Wildcard indicates whether the certificate covers a wildcard hostname
	// +optional
	Wildcard bool `json:"wildcard,omitempty"`

	// Hosts lists the hostnames covered by this certificate
	// +optional
	Hosts []string `json:"hosts,omitempty"`

	// UploadedOn is the time the certificate was issued/uploaded
	// +optional
	UploadedOn *metav1.Time `json:"uploadedOn,omitempty"`

	// ExpiresOn is the certificate expiration time
	// +optional
	ExpiresOn *metav1.Time `json:"expiresOn,omitempty"`

	// ValidationRecords contains the DCV tokens Cloudflare requires to complete SSL issuance.
	// The customer must satisfy these before SSL becomes active.
	// +optional
	ValidationRecords []SSLValidationRecord `json:"validationRecords,omitempty"`

	// ValidationErrors contains any errors encountered during SSL validation
	// +optional
	ValidationErrors []string `json:"validationErrors,omitempty"`
}

// SSLValidationRecord mirrors the Cloudflare API ssl.validation_records entry
type SSLValidationRecord struct {
	// TXTName is the DNS TXT record name the customer must create (txt method)
	// +optional
	TXTName string `json:"txtName,omitempty"`

	// TXTValue is the value of the DNS TXT record (txt method)
	// +optional
	TXTValue string `json:"txtValue,omitempty"`

	// HTTPUrl is the URL where the token must be served (http method)
	// +optional
	HTTPUrl string `json:"httpUrl,omitempty"`

	// HTTPBody is the content that must be served at HTTPUrl (http method)
	// +optional
	HTTPBody string `json:"httpBody,omitempty"`

	// Emails lists contact addresses used for email-based validation (email method)
	// +optional
	Emails []string `json:"emails,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Hostname",type=string,JSONPath=`.spec.hostname`
// +kubebuilder:printcolumn:name="Origin",type=string,JSONPath=`.spec.originServer`
// +kubebuilder:printcolumn:name="CF Status",type=string,JSONPath=`.status.hostnameStatus`
// +kubebuilder:printcolumn:name="SSL",type=string,JSONPath=`.status.ssl.status`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Creates",type=integer,JSONPath=`.status.createCount`
// +kubebuilder:printcolumn:name="Errors",type=integer,JSONPath=`.status.consecutiveErrors`

// CustomHostname is the Schema for the customhostnames API
type CustomHostname struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec CustomHostnameSpec `json:"spec"`

	// +optional
	Status CustomHostnameStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// CustomHostnameList contains a list of CustomHostname
type CustomHostnameList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []CustomHostname `json:"items"`
}

func init() {
	SchemeBuilder.Register(&CustomHostname{}, &CustomHostnameList{})
}
