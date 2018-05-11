FROM       quay.io/prometheus/busybox:latest
COPY       ingester /bin/ingester
EXPOSE     80
ENTRYPOINT [ "/bin/ingester" ]

ARG revision
LABEL org.opencontainers.image.title="ingester" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/ingester" \
      org.opencontainers.image.revision="${revision}"
