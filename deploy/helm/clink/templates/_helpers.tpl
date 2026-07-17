{{/* Expand the name of the chart. */}}
{{- define "clink.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name. */}}
{{- define "clink.fullname" -}}
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

{{- define "clink.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Common labels on every object. */}}
{{- define "clink.labels" -}}
helm.sh/chart: {{ include "clink.chart" . }}
{{ include "clink.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "clink.selectorLabels" -}}
app.kubernetes.io/name: {{ include "clink.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/* Per-component selector labels (coordinator / worker). */}}
{{- define "clink.componentSelectorLabels" -}}
{{ include "clink.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{/* Object names. */}}
{{- define "clink.coordinator.fullname" -}}
{{- printf "%s-coordinator" (include "clink.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "clink.worker.fullname" -}}
{{- printf "%s-worker" (include "clink.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "clink.worker.headless" -}}
{{- printf "%s-worker-headless" (include "clink.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Shared HA volume (file-coordinator --ha-dir): name, PVC name, volume + mount. */}}
{{- define "clink.haVolumeName" -}}ha-shared{{- end -}}
{{- define "clink.haPvcName" -}}
{{- printf "%s-ha" (include "clink.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "clink.haVolume" -}}
- name: {{ include "clink.haVolumeName" . }}
{{- if eq .Values.ha.storage.type "hostPath" }}
  hostPath:
    path: {{ .Values.ha.storage.hostPath.path }}
    type: DirectoryOrCreate
{{- else }}
  persistentVolumeClaim:
    claimName: {{ include "clink.haPvcName" . }}
{{- end }}
{{- end -}}

{{- define "clink.haVolumeMount" -}}
- name: {{ include "clink.haVolumeName" . }}
  mountPath: {{ .Values.ha.storage.mountPath }}
{{- end -}}

{{- define "clink.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "clink.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}
