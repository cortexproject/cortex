FROM       alpine:3.13
RUN        apk add --no-cache ca-certificates libcap

# create cortex user to run cortex as non-root
RUN addgroup -g 10000 -S cortex && \
    adduser  -u 10000 -S cortex -G cortex

RUN mkdir /data && \
    chown cortex:cortex /data

VOLUME /data
WORKDIR /data

COPY       migrations /migrations/
COPY       cortex /bin/cortex

# allow cortex to bind to port 80 as non-root
RUN setcap 'cap_net_bind_service=+ep' /bin/cortex

EXPOSE     80
ENTRYPOINT [ "/bin/cortex" ]

ARG revision
LABEL org.opencontainers.image.title="cortex" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/cortex" \
      org.opencontainers.image.revision="${revision}"
