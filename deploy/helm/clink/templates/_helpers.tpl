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

{{- define "clink.coordinator.headless" -}}
{{- printf "%s-coordinator-headless" (include "clink.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* CLINK_AUTH_TOKEN from the auth Secret, for every clink container. */}}
{{- define "clink.authEnv" -}}
{{- if .Values.auth.existingSecret -}}
- name: CLINK_AUTH_TOKEN
  valueFrom:
    secretKeyRef:
      name: {{ .Values.auth.existingSecret }}
      key: {{ .Values.auth.secretKey }}
{{- end }}
{{- end -}}

{{/* Probes on the HTTP port; both answer without the token. */}}
{{- define "clink.probes" -}}
readinessProbe:
  httpGet:
    path: /readyz
    port: http
  initialDelaySeconds: 2
  periodSeconds: 5
  timeoutSeconds: 2
  failureThreshold: 6
livenessProbe:
  httpGet:
    path: /livez
    port: http
  initialDelaySeconds: 10
  periodSeconds: 10
  timeoutSeconds: 2
  failureThreshold: 6
{{- end -}}

{{/* Control-plane TLS: where the certificates are mounted, the volume each
     role mounts, and the flags each role is given. */}}
{{- define "clink.tlsDir" -}}/etc/clink/tls{{- end -}}

{{- define "clink.tlsVolume" -}}
{{- if .root.Values.tls.enabled -}}
- name: tls
  secret:
    {{- if eq .role "coordinator" }}
    secretName: {{ required "tls.coordinatorSecret is required with tls.enabled" .root.Values.tls.coordinatorSecret }}
    {{- else if .root.Values.tls.workerSecret }}
    secretName: {{ .root.Values.tls.workerSecret }}
    {{- else }}
    secretName: {{ required "tls.coordinatorSecret is required with tls.enabled" .root.Values.tls.coordinatorSecret }}
    items:
      - key: ca.crt
        path: ca.crt
    {{- end }}
{{- end }}
{{- end -}}

{{- define "clink.tlsVolumeMount" -}}
{{- if .Values.tls.enabled -}}
- name: tls
  mountPath: {{ include "clink.tlsDir" . }}
  readOnly: true
{{- end }}
{{- end -}}

{{- define "clink.coordinatorTlsArgs" -}}
{{- if .Values.tls.enabled -}}
- --tls-cert={{ include "clink.tlsDir" . }}/tls.crt
- --tls-key={{ include "clink.tlsDir" . }}/tls.key
{{- if .Values.tls.workerSecret }}
- --tls-client-ca={{ include "clink.tlsDir" . }}/ca.crt
{{- end }}
{{- end }}
{{- end -}}

{{- define "clink.workerTlsArgs" -}}
{{- if .Values.tls.enabled -}}
- --tls-ca={{ include "clink.tlsDir" . }}/ca.crt
{{- if .Values.tls.workerSecret }}
- --tls-client-cert={{ include "clink.tlsDir" . }}/tls.crt
- --tls-client-key={{ include "clink.tlsDir" . }}/tls.key
{{- end }}
{{- if not .Values.tls.verifyHostname }}
- --tls-no-verify-hostname
{{- end }}
{{- end }}
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
