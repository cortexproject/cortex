FROM alpine:3.10

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
