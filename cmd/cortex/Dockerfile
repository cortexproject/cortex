FROM       alpine:3.19
ARG TARGETARCH

RUN        apk add --no-cache ca-certificates
COPY       migrations /migrations/
COPY       cortex-$TARGETARCH /bin/cortex
COPY       run.sh /bin/run.sh
RUN        chmod +x /bin/run.sh
EXPOSE     80
ENTRYPOINT [ "/bin/run.sh" ]

ARG revision
LABEL org.opencontainers.image.title="cortex" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/cortex" \
      org.opencontainers.image.revision="${revision}"
