FROM       alpine:3.8
COPY       ingester /bin/ingester
RUN        apk add --no-cache ca-certificates
EXPOSE     80
ENTRYPOINT [ "/bin/ingester" ]

ARG revision
LABEL org.opencontainers.image.title="ingester" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/ingester" \
      org.opencontainers.image.revision="${revision}"
