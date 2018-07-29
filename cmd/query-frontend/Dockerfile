FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       query-frontend /bin/query-frontend
EXPOSE     80
ENTRYPOINT [ "/bin/query-frontend" ]

ARG revision
LABEL org.opencontainers.image.title="query-frontend" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/query-frontend" \
      org.opencontainers.image.revision="${revision}"
