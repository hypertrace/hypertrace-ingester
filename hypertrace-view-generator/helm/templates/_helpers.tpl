{{- define "creatorservice.image" -}}
  {{- if and .Values.images.creator.tagOverride  -}}
    {{- printf "%s:%s" .Values.images.creator.repository .Values.images.creator.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.images.creator.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}

{{- define "generatorservice.image" -}}
  {{- if and .Values.images.generator.tagOverride  -}}
    {{- printf "%s:%s" .Values.images.generator.repository .Values.images.generator.tagOverride }}
  {{- else -}}
    {{- printf "%s:%s" .Values.images.generator.repository .Chart.AppVersion }}
  {{- end -}}
{{- end -}}
