FROM       alpine:3.8
COPY       alertmanager /bin/alertmanager
RUN        apk add --no-cache ca-certificates
EXPOSE     80
ENTRYPOINT [ "/bin/alertmanager" ]

ARG revision
LABEL org.opencontainers.image.title="alertmanager" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/alertmanager" \
      org.opencontainers.image.revision="${revision}"
