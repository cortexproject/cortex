FROM       quay.io/prometheus/busybox:latest
COPY       querier /bin/querier
EXPOSE     80
ENTRYPOINT [ "/bin/querier" ]

ARG revision
LABEL org.opencontainers.image.title="querier" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/querier" \
      org.opencontainers.image.revision="${revision}"
