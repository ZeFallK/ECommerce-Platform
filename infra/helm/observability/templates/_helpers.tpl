{{- define "observability.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{- define "observability.labels" -}}
helm.sh/chart: {{ include "observability.chart" .root }}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/part-of: ecommerce-platform
app.kubernetes.io/managed-by: {{ .root.Release.Service }}
{{- end -}}

{{- define "observability.selectorLabels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end -}}
