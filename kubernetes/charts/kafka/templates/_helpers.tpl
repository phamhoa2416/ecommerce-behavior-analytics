{{/*
Expand the name of the chart.
*/}}
{{ define "kafka.name" -}}
{{ default .Chart.Name .Values.kafka.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{ define "kafka.fullname" -}}
{{- if .Values.kafka.fullnameOverride }}
{{ .Values.kafka.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.kafka.nameOverride }}
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
{{ define "kafka.chart" -}}
{{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{ define "kafka.labels" -}}
helm.sh/chart: {{ include "kafka.chart" . }}
{{ include "kafka.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{ define "kafka.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kafka.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Kafka service name
*/}}
{{ define "kafka.service.name" -}}
{{ include "kafka.fullname" . }}-kafka
{{- end }}

{{/*
Kafka UI service name
*/}}
{{ define "kafka-ui.service.name" -}}
{{ include "kafka.fullname" . }}-ui
{{- end }}