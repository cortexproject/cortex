FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       query-tee /
ENTRYPOINT ["/query-tee"]

ARG revision
LABEL org.opencontainers.image.title="query-tee" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/tools/query-tee" \
      org.opencontainers.image.revision="${revision}"
