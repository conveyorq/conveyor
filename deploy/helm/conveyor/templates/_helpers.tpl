{{/*
Chart name, optionally overridden by nameOverride.
*/}}
{{- define "conveyor.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Fully qualified release name, capped at 63 chars for Kubernetes name limits.
*/}}
{{- define "conveyor.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Chart label value: chart name and version, sanitized.
*/}}
{{- define "conveyor.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels stamped on every resource.
*/}}
{{- define "conveyor.labels" -}}
helm.sh/chart: {{ include "conveyor.chart" . }}
{{ include "conveyor.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels: the stable identity used by the Deployment selector, the
Service, and pod discovery. Never include version here.
*/}}
{{- define "conveyor.selectorLabels" -}}
app.kubernetes.io/name: {{ include "conveyor.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
ServiceAccount name to use.
*/}}
{{- define "conveyor.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "conveyor.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
