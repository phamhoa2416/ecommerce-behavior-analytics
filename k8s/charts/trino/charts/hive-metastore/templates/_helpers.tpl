{{- define "hive-metastore.name" -}}
{{ .Chart.Name }}
{{- end }}

{{ define "hive-metastore.fullname" -}}
{{ include "hive-metastore.name" . }}
{{- end }}
