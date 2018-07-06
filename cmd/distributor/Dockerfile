FROM       alpine:3.8
COPY       distributor /bin/distributor
RUN        apk add --no-cache ca-certificates
EXPOSE     80
ENTRYPOINT [ "/bin/distributor" ]

ARG revision
LABEL org.opencontainers.image.title="distributor" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/distributor" \
      org.opencontainers.image.revision="${revision}"
