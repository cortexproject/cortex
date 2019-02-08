FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       alertmanager /bin/alertmanager
COPY       .migrations /migrations/
EXPOSE     80
ENTRYPOINT [ "/bin/alertmanager" ]

ARG revision
LABEL org.opencontainers.image.title="alertmanager" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/alertmanager" \
      org.opencontainers.image.revision="${revision}"
