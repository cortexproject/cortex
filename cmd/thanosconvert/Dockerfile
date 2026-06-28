ARG        BASEIMG=gcr.io/distroless/static-debian12:latest
FROM       ${BASEIMG}
ARG TARGETARCH
COPY       thanosconvert-$TARGETARCH /thanosconvert
ENTRYPOINT ["/thanosconvert"]

ARG revision
LABEL org.opencontainers.image.title="thanosconvert" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/tools/thanosconvert" \
      org.opencontainers.image.revision="${revision}"
