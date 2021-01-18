{{- define "generatorservice.image" -}}
  {{- if and .Values.images.generator.tagOverride  -}}
    {{- printf "%s:%s" .Values.images.generator.repository .Values.images.generator.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.images.generator.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}

{{- define "helm-toolkit.utils.joinListWithComma" -}}
{{- $local := dict "first" true -}}
{{- range $k, $v := . -}}{{- if not $local.first -}},{{- end -}}"{{- $v -}}"{{- $_ := set $local "first" false -}}{{- end -}}
{{- end -}}