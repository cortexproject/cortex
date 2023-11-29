FROM alpine:3.18

RUN apk add --no-cache \
        ruby \
        ruby-dev \
        ruby-etc \
        gcc \
        git \
        libc-dev \
        libffi-dev \
        make \
        rpm \
        tar \
        && gem install --no-document fpm

COPY package.sh /
ENTRYPOINT ["/package.sh"]

ARG revision
LABEL org.opencontainers.image.title="fpm" \
        # TODO: should this label point to the fpm source code?
        org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/packaging/fpm" \
        org.opencontainers.image.revision="${revision}"
