{{- define "postgres.hive-metastore.name" -}}
{{ .Chart.Name }}
{{- end }}

{{ define "postgres.hive-metastore.fullname" -}}
{{ include "postgres.hive-metastore.name" . }}
{{- end }}
