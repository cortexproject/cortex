FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       lite /bin/cortex
COPY       .migrations /migrations/
EXPOSE     80
ENTRYPOINT [ "/bin/cortex" ]

ARG revision
LABEL org.opencontainers.image.title="lite" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/lite" \
      org.opencontainers.image.revision="${revision}"
