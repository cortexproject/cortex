{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "cortex.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "cortex.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "cortex.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "cortex.labels" -}}
helm.sh/chart: {{ include "cortex.chart" . }}
{{ include "cortex.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Common selector labels
*/}}
{{- define "cortex.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cortex.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "cortex.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
    {{ default (include "cortex.fullname" .) .Values.serviceAccount.name }}
{{- else -}}
    {{ default "default" .Values.serviceAccount.name }}
{{- end -}}
{{- end -}}

{{/*
Cortex configuration secret name
*/}}
{{- define "cortex.configSecret.name" -}}
{{- printf "%s-%s" (include "cortex.fullname" .) "config" -}}
{{- end -}}

{{/* CORTEX COMPONENT NAMES */}}

{{/*
Cortex ingester name
*/}}
{{- define "cortex.ingester.name" -}}
{{- printf "%s-%s" (include "cortex.fullname" .) "ingester" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Cortex distributor name
*/}}
{{- define "cortex.distributor.name" -}}
{{- printf "%s-%s" (include "cortex.fullname" .) "distributor" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Cortex querier name
*/}}
{{- define "cortex.querier.name" -}}
{{- printf "%s-%s" (include "cortex.fullname" .) "querier" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Cortex query frontend name
*/}}
{{- define "cortex.queryFrontend.name" -}}
{{- printf "%s-%s" (include "cortex.fullname" .) "query-frontend" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Cortex table manager name
*/}}
{{- define "cortex.tableManager.name" -}}
{{- printf "%s-%s" (include "cortex.fullname" .) "table-manager" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* SELECTOR LABELS */}}

{{/*
Cortex ingester selector labels
*/}}
{{- define "cortex.ingester.selectorLabels" -}}
{{- include "cortex.selectorLabels" . }}
cortex: ingester
{{- end -}}

{{/*
Cortex distributor selector labels
*/}}
{{- define "cortex.distributor.selectorLabels" -}}
{{- include "cortex.selectorLabels" . }}
cortex: distributor
{{- end -}}

{{/*
Cortex querier selector labels
*/}}
{{- define "cortex.querier.selectorLabels" -}}
{{- include "cortex.selectorLabels" . }}
cortex: querier
{{- end -}}

{{/*
Cortex query frontend selector labels
*/}}
{{- define "cortex.queryFrontend.selectorLabels" -}}
{{- include "cortex.selectorLabels" . }}
cortex: query-frontend
{{- end -}}

{{/*
Cortex table manager selector labels
*/}}
{{- define "cortex.tableManager.selectorLabels" -}}
{{- include "cortex.selectorLabels" . }}
cortex: table-manager
{{- end -}}

{{/* CORTEX CONFIGURATION VALUES */}}

{{/*
Cortex HTTP listen port
*/}}
{{- define "cortex.config.httpListenPort" -}}
{{ .Values.config.server.http_listen_port }}
{{- end -}}

{{/*
Cortex gRPC listen port
*/}}
{{- define "cortex.config.grpcListenPort" -}}
{{ .Values.config.server.grpc_listen_port }}
{{- end -}}

{{/*
Cortex querier frontend address container argument
*/}}
{{- define "cortex.querier.containerArgs.query-frontend.address" -}}
- "-querier.frontend-address={{ include "cortex.queryFrontend.name" . }}:{{ include "cortex.config.grpcListenPort" . }}"
{{- end -}}

{{/*
Cortex gossip member list container arguments
*/}}
{{- define "cortex.ingester.containerArgs.memberlist.join" -}}
{{- $replicas := .Values.ingester.replicas | int }}
{{- $bindPort := .Values.config.memberlist.bind_port }}
{{- range $i, $e := until $replicas }}
- "-memberlist.join={{ include "cortex.ingester.name" $ }}-{{ $i }}.{{ include "cortex.ingester.name" $ }}:{{ $bindPort }}"
{{- end }}
{{- end -}}

{{/*
Cortex common container argument
*/}}
{{- define "cortex.common.containerArgs" -}}
- "-config.file=/etc/cortex/config.yaml"
{{ include "cortex.querier.containerArgs.query-frontend.address" . }}
{{ include "cortex.ingester.containerArgs.memberlist.join" . }}
{{- end -}}
