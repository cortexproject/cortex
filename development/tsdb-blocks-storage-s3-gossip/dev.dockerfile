FROM golang:1.23
ENV CGO_ENABLED=0
RUN go install github.com/go-delve/delve/cmd/dlv@latest

FROM alpine:3.23

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
COPY --from=0 /go/bin/dlv ./
