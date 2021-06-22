FROM alpine:3.13

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
