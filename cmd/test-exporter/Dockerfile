ARG        BASEIMG=gcr.io/distroless/static-debian12:latest
FROM       ${BASEIMG}
ARG TARGETARCH
COPY       test-exporter-$TARGETARCH /test-exporter
ENTRYPOINT ["/test-exporter"]

ARG revision
LABEL org.opencontainers.image.title="test-exporter" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/test-exporter" \
      org.opencontainers.image.revision="${revision}"
