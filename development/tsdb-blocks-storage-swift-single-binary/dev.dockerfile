FROM alpine:3.18

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
