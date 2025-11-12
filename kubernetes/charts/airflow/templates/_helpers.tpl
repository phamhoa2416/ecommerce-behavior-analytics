{{/*
Expand the name of the charts.
*/}}
{{ define "airflow.name" -}}
{{ default .Chart.Name .Values.airflow.name | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create charts name and version as used by the charts label.
*/}}
{{ define "airflow.chart" -}}
{{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{ define "airflow.labels" -}}
helm.sh/chart: {{ include "airflow.chart" . }}
{{ include "airflow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{ define "airflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "airflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
PostgreSQL service name
*/}}
{{ define "airflow.postgres.service" -}}
{{- if .Values.postgres }}
{{ printf "%s" "postgres" }}
{{- else }}
{{ printf "%s-postgres" (include "airflow.name" .) }}
{{- end }}
{{- end }}

