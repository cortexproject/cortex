FROM       alpine:3.8
COPY       ruler /bin/ruler
RUN        apk add --no-cache ca-certificates
EXPOSE     80
ENTRYPOINT [ "/bin/ruler" ]

ARG revision
LABEL org.opencontainers.image.title="ruler" \
      org.opencontainers.image.source="https://github.com/weaveworks/cortex/tree/master/cmd/ruler" \
      org.opencontainers.image.revision="${revision}"
