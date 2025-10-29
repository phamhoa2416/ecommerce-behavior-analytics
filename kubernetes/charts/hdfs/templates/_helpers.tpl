{{/*
Expand the name of the chart.
*/}}
{{ define "hdfs.name" -}}
{{ default .Chart.Name .Values.hdfs.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{ define "hdfs.chart" -}}
{{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{ define "hdfs.labels" -}}
helm.sh/chart: {{ include "hdfs.chart" . }}
{{ include "hdfs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{ define "hdfs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hdfs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}