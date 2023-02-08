FROM alpine:3.17

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
