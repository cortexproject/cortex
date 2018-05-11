FROM       quay.io/prometheus/busybox:latest
COPY       table-manager /bin/table-manager
EXPOSE     9094
ENTRYPOINT [ "/bin/table-manager" ]

ARG revision
LABEL org.opencontainers.image.title="table-manager" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/table-manager" \
      org.opencontainers.image.revision="${revision}"
