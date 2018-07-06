FROM       alpine:3.8
COPY       lite /bin/cortex
RUN        apk add --no-cache ca-certificates
EXPOSE     80
ENTRYPOINT [ "/bin/cortex" ]

ARG revision
LABEL org.opencontainers.image.title="lite" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/lite" \
      org.opencontainers.image.revision="${revision}"
