FROM alpine:3.12

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
