FROM       quay.io/prometheus/busybox:latest
COPY       distributor /bin/distributor
EXPOSE     80
ENTRYPOINT [ "/bin/distributor" ]

ARG revision
LABEL org.opencontainers.image.title="distributor" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/distributor" \
      org.opencontainers.image.revision="${revision}"
