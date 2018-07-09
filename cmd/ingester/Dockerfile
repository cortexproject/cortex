FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       ingester /bin/ingester
EXPOSE     80
ENTRYPOINT [ "/bin/ingester" ]

ARG revision
LABEL org.opencontainers.image.title="ingester" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/ingester" \
      org.opencontainers.image.revision="${revision}"
