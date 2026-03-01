FROM alpine:3.23

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
