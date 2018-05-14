FROM       quay.io/prometheus/busybox:latest
COPY       alertmanager /bin/alertmanager
EXPOSE     80
ENTRYPOINT [ "/bin/alertmanager" ]

ARG revision
LABEL org.opencontainers.image.title="alertmanager" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/alertmanager" \
      org.opencontainers.image.revision="${revision}"
