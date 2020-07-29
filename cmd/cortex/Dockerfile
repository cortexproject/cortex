FROM       alpine:3.12
RUN        apk add --no-cache ca-certificates
COPY       migrations /migrations/
COPY       cortex /bin/cortex
EXPOSE     80
ENTRYPOINT [ "/bin/cortex" ]

# Install the CLI tool "gsutil" in order to help cluster operators to run manual operations
# when running the Cortex blocks storage with GCS.
# IMPORTANT: if you change python version, remember to also change the plugin path at /etc/boto.cfg.
RUN apk add --no-cache python3~=3.8 py3-pip && \
    pip install google-compute-engine && \
    wget https://storage.googleapis.com/pub/gsutil.tar.gz && \
    tar -zxf gsutil.tar.gz -C /usr/local/bin && \
    rm -f gsutil.tar.gz && \
    mv /usr/local/bin/gsutil /usr/local/bin/gsutil-sources && \
    ln -s /usr/local/bin/gsutil-sources/gsutil /usr/local/bin/gsutil && \
    ln -s /usr/bin/python3 /usr/bin/python

ARG revision
LABEL org.opencontainers.image.title="cortex" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/cortex" \
      org.opencontainers.image.revision="${revision}"
