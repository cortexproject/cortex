FROM       alpine:3.17
ARG TARGETARCH
RUN        apk add --no-cache ca-certificates
COPY       thanosconvert-$TARGETARCH /thanosconvert
ENTRYPOINT ["/thanosconvert"]

ARG revision
LABEL org.opencontainers.image.title="thanosconvert" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/tools/thanosconvert" \
      org.opencontainers.image.revision="${revision}"
