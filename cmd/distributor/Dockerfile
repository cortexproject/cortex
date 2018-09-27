FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       distributor /bin/distributor
EXPOSE     80
ENTRYPOINT [ "/bin/distributor" ]

ARG revision
LABEL org.opencontainers.image.title="distributor" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/distributor" \
      org.opencontainers.image.revision="${revision}"
