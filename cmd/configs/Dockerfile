FROM       alpine:3.8
WORKDIR    /
COPY       configs /bin/configs
COPY       migrations /migrations/
RUN        apk add --no-cache ca-certificates
ENTRYPOINT [ "/bin/configs" ]

ARG revision
LABEL org.opencontainers.image.title="configs" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/configs" \
      org.opencontainers.image.revision="${revision}"
