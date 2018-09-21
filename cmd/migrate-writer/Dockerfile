FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       migrate-writer /bin/migrate-writer
EXPOSE     80
ENTRYPOINT [ "/bin/migrate-writer" ]

ARG revision
LABEL org.opencontainers.image.title="migrate-writer" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/migrate-writer" \
      org.opencontainers.image.revision="${revision}"
