FROM golang:1.7.3
RUN apt-get update && apt-get install -y python-requests python-yaml file jq && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN go clean -i net && \
	go install -tags netgo std && \
	go install -race -tags netgo std
RUN go get -tags netgo \
		github.com/fzipp/gocyclo \
		github.com/golang/lint/golint \
		github.com/kisielk/errcheck \
		github.com/mjibson/esc \
		github.com/client9/misspell/cmd/misspell \
		github.com/jteeuwen/go-bindata/go-bindata && \
	rm -rf /go/pkg /go/src
COPY build.sh /
ENTRYPOINT ["/build.sh"]
