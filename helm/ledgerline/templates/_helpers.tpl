{{/*
Common labels applied to every resource this chart renders.
*/}}
{{- define "ledgerline.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}

{{/*
Fully-qualified Kafka broker-0 pod DNS name, used both as the advertised
listener the broker announces and as the bootstrap address every other
component in the chart connects to it with. Kept in one place so the two
never drift apart.
*/}}
{{- define "ledgerline.kafkaBootstrap" -}}
{{ .Release.Name }}-kafka-0.{{ .Release.Name }}-kafka.{{ .Release.Namespace }}.svc.cluster.local:9092
{{- end -}}

{{- define "ledgerline.kafkaInternalBootstrap" -}}
{{ .Release.Name }}-kafka-0.{{ .Release.Name }}-kafka.{{ .Release.Namespace }}.svc.cluster.local:29092
{{- end -}}

{{- define "ledgerline.postgresHost" -}}
{{ .Release.Name }}-postgres
{{- end -}}
