FROM       quay.io/prometheus/busybox:latest
COPY       ingester /bin/ingester
EXPOSE     80
ENTRYPOINT [ "/bin/cortex-ingester" ]
