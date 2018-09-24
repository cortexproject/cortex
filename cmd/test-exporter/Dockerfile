FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       test-exporter /
ENTRYPOINT ["/test-exporter"]

ARG revision
LABEL org.opencontainers.image.title="test-exporter" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/test-exporter" \
      org.opencontainers.image.revision="${revision}"
