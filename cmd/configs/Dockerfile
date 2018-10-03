FROM       alpine:3.8
WORKDIR    /
RUN        apk add --no-cache ca-certificates
COPY       configs /bin/configs
COPY       migrations /migrations/
ENTRYPOINT [ "/bin/configs" ]

ARG revision
LABEL org.opencontainers.image.title="configs" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/configs" \
      org.opencontainers.image.revision="${revision}"
