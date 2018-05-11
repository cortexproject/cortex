FROM       quay.io/prometheus/busybox:latest
COPY       ruler /bin/ruler
EXPOSE     80
ENTRYPOINT [ "/bin/ruler" ]

ARG revision
LABEL org.opencontainers.image.title="ruler" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/ruler" \
      org.opencontainers.image.revision="${revision}"
