FROM alpine:3.4
MAINTAINER Weaveworks Inc <help@weave.works>
WORKDIR /
COPY cmd/configs/configs /
COPY db/migrations /migrations/
ENTRYPOINT ["/configs"]
