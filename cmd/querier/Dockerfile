FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       querier /bin/querier
EXPOSE     80
ENTRYPOINT [ "/bin/querier" ]

ARG revision
LABEL org.opencontainers.image.title="querier" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/querier" \
      org.opencontainers.image.revision="${revision}"
