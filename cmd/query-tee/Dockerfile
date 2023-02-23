FROM       alpine:3.17
ARG TARGETARCH

RUN        apk add --no-cache ca-certificates
COPY       query-tee-$TARGETARCH /query-tee
ENTRYPOINT ["/query-tee"]

ARG revision
LABEL org.opencontainers.image.title="query-tee" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/tools/query-tee" \
      org.opencontainers.image.revision="${revision}"
