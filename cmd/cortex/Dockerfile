FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       migrations /migrations/
COPY       cortex /bin/cortex
COPY       single-process-config.yaml /etc
EXPOSE     80
ENTRYPOINT [ "/bin/cortex" ]

ARG revision
LABEL org.opencontainers.image.title="cortex" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/cortex" \
      org.opencontainers.image.revision="${revision}"
