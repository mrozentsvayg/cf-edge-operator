{{- define "cf-edge-operator.name" -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "cf-edge-operator.serviceAccountName" -}}
{{- .Values.serviceAccount.name | default (include "cf-edge-operator.name" .) }}
{{- end }}

{{- define "cf-edge-operator.operatorNamespace" -}}
{{- .Values.operatorNamespace | default .Release.Namespace }}
{{- end }}

{{- define "cf-edge-operator.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default .Chart.AppVersion) }}
{{- end }}

{{- define "cf-edge-operator.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/name: cf-edge-operator
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- /*
Feature/CRD flag coercion. A value may arrive as a real bool (values.yaml) or as a
string "true"/"false" (ArgoCD forceString, --set-string, or an ApplicationSet values
cascade). Helm treats any non-empty string as truthy, so a bare `if .Values.x` would
read the string "false" as enabled. These helpers coerce each flag to a canonical
"true"/"false" string honoring its default, so every gate compares
`eq (include "..." .) "true"`. Keep the defaults here in sync with values.yaml.
*/ -}}
{{- define "cf-edge-operator.customhostnameEnabled" -}}
{{- if ne (lower (toString .Values.features.customhostname.enabled)) "false" -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "cf-edge-operator.loadBalancingEnabled" -}}
{{- if eq (lower (toString .Values.features.loadBalancing.enabled)) "true" -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "cf-edge-operator.poolHealthEnabled" -}}
{{- if and (eq (include "cf-edge-operator.loadBalancingEnabled" .) "true") (eq (lower (toString .Values.features.loadBalancing.poolHealth)) "true") -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "cf-edge-operator.crdsEnabled" -}}
{{- if ne (lower (toString .Values.crds.enabled)) "false" -}}true{{- else -}}false{{- end -}}
{{- end }}

{{- define "cf-edge-operator.crdsKeep" -}}
{{- if ne (lower (toString .Values.crds.keep)) "false" -}}true{{- else -}}false{{- end -}}
{{- end }}
