FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates
COPY       blocksconvert /
ENTRYPOINT ["/blocksconvert"]

ARG revision
LABEL org.opencontainers.image.title="blocksconvert" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/tools/blocksconvert" \
      org.opencontainers.image.revision="${revision}"
