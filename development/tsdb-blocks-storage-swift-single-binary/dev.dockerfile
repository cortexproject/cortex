FROM alpine:3.14

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
