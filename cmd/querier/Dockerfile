FROM       alpine:3.8
COPY       querier /bin/querier
RUN        apk add --no-cache ca-certificates
EXPOSE     80
ENTRYPOINT [ "/bin/querier" ]

ARG revision
LABEL org.opencontainers.image.title="querier" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/querier" \
      org.opencontainers.image.revision="${revision}"
