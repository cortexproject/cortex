
{{- define "cortex.service" -}}
{{ $root := .root }}
{{ $role := .role}}
{{ $settings := .settings }}
apiVersion: v1
kind: Service
metadata:
  name: {{ template "cortex.fullname" $root }}-{{ $role }}
  labels:
    app: {{ template "cortex.name" $root }}
    chart: {{ template "cortex.chart" $root }}
    release: {{ $root.Release.Name }}
    heritage: {{ $root.Release.Service }}
    {{- with $root.Values.service.labels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  annotations:
    {{- toYaml $root.Values.service.annotations | nindent 4 }}
spec:
  type: {{ $settings.serviceType }}
{{- if (and (eq $settings.serviceType "ClusterIP") (not (empty $settings.clusterIP))) }}
  clusterIP: {{ $settings.clusterIP }}
{{- end }}
  ports:
    {{- if $settings.http_listen_port }}
    - port: {{ $settings.http_listen_port }}
      protocol: TCP
      name: http-metrics
      targetPort: http-metrics
    {{- end }}
    {{- if $settings.grpc_listen_port }}
    - port: {{ $settings.grpc_listen_port }}
      protocol: TCP
      name: grpc
      targetPort: grpc
    {{- end }}  
{{- if (and (eq $settings.serviceType "NodePort") (not (empty $settings.nodePort))) }}
      nodePort: {{ $settings.nodePort }}
{{- end }}
  selector:
    app: {{ template "cortex.name" $root }}
    release: {{ $root.Release.Name }}
    role: {{ $role }} 
{{- end -}}        