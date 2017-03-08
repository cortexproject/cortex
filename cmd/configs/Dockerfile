FROM       quay.io/prometheus/busybox:latest
MAINTAINER Weaveworks Inc <help@weave.works>
WORKDIR /
COPY configs /bin/configs
COPY migrations /migrations/
ENTRYPOINT [ "/bin/configs" ]
