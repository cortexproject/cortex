FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       migrate-reader /bin/migrate-reader
EXPOSE     80
ENTRYPOINT [ "/bin/migrate-reader" ]

ARG revision
LABEL org.opencontainers.image.title="migrate-reader" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/migrate-reader" \
      org.opencontainers.image.revision="${revision}"
