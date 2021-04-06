FROM golang:1.14
ENV CGO_ENABLED=0 GOOS=linux GOARCH=amd64
RUN go get github.com/go-delve/delve/cmd/dlv

FROM alpine:3.13

RUN     mkdir /cortex
WORKDIR /cortex
ADD     ./cortex ./
COPY --from=0 /go/bin/dlv ./
