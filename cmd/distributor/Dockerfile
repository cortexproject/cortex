FROM       quay.io/prometheus/busybox:latest
COPY       distributor /bin/distributor
EXPOSE     80
ENTRYPOINT [ "/bin/cortex-distributor" ]
