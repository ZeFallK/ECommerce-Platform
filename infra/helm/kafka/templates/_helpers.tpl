{{- define "kafka.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka.fullname" -}}
{{- default (include "kafka.name" .) .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "kafka.labels" -}}
app.kubernetes.io/name: {{ include "kafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: messaging
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{- end -}}

{{- define "kafka.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
