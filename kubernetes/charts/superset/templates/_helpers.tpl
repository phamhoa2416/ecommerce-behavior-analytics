# templates/_helpers.tpl
{{/*
Expand the name of the chart.
*/}}
{{ define "superset.name" -}}
{{ default .Chart.Name .Values.superset.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{ define "superset.fullname" -}}
{{- if .Values.superset.fullnameOverride }}
{{ .Values.superset.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.superset.nameOverride }}
{{- if contains $name .Release.Name }}
{{ .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{ printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{ define "superset.chart" -}}
{{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{ define "superset.labels" -}}
helm.sh/chart: {{ include "superset.chart" . }}
{{ include "superset.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{ define "superset.selectorLabels" -}}
app.kubernetes.io/name: {{ include "superset.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}