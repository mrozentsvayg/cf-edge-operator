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
