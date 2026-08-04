{{- define "common.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "common.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "common.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "common.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "common.labels" -}}
helm.sh/chart: {{ include "common.chart" . }}
{{ include "common.selectorLabels" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "common.selectorLabels" -}}
app.kubernetes.io/name: {{ include "common.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "common.image" -}}
{{- $global := default (dict) .Values.global -}}
{{- $registry := trimSuffix "/" (default "" $global.imageRegistry) -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry .Values.image.repository .Values.image.tag -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "common.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "common.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "common.runtimeEnv" -}}
{{- $global := default (dict) .Values.global -}}
{{- $kafka := default (dict) $global.kafka -}}
{{- $otel := default (dict) $global.otel -}}
- name: KAFKA_BOOTSTRAP_SERVERS
  value: {{ default "kafka:9092" $kafka.bootstrapServers | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ default "http://otel-collector:4317" $otel.endpoint | quote }}
{{- range $key, $val := .Values.env }}
- name: {{ $key }}
  value: {{ $val | quote }}
{{- end -}}
{{- end -}}
