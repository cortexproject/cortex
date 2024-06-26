FROM alpine:3.19

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
