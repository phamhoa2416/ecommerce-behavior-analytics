{{- define "postgres.name" -}}
{{ .Chart.Name }}
{{- end }}

{{ define "postgres.fullname" -}}
{{ include "postgres.name" . }}
{{- end }}

