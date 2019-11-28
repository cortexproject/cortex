FROM golang:1.13.3-stretch
RUN apt-get update && apt-get install -y curl python-requests python-yaml file jq unzip protobuf-compiler libprotobuf-dev && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN curl -sL https://deb.nodesource.com/setup_6.x | sh -
RUN apt-get install -y nodejs npm && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN npm install -g postcss-cli autoprefixer
RUN go clean -i net && \
	go install -tags netgo std && \
	go install -race -tags netgo std
ENV HUGO_VERSION=v0.59.1
RUN git clone https://github.com/gohugoio/hugo.git --branch ${HUGO_VERSION} --depth 1 && \
	cd hugo  && go install --tags extended && cd ../ && \
	rm -rf hugo/ && rm -rf /go/pkg /go/src
RUN curl -fsSLo shfmt https://github.com/mvdan/sh/releases/download/v1.3.0/shfmt_v1.3.0_linux_amd64 && \
	echo "b1925c2c405458811f0c227266402cf1868b4de529f114722c2e3a5af4ac7bb2  shfmt" | sha256sum -c && \
	chmod +x shfmt && \
	mv shfmt /usr/bin
RUN GO111MODULE=on go get -tags netgo \
		github.com/fzipp/gocyclo \
		golang.org/x/lint/golint \
		github.com/kisielk/errcheck@v1.2.0 \
		github.com/client9/misspell/cmd/misspell@v0.3.4 \
		github.com/golang/protobuf/protoc-gen-go@v1.3.1 \
		github.com/gogo/protobuf/protoc-gen-gogoslick@v1.2.1 \
		github.com/gogo/protobuf/gogoproto@v1.2.1 && \
	rm -rf /go/pkg /go/src
RUN curl -Ls https://github.com/golang/dep/releases/download/v0.5.0/dep-linux-amd64 -o $GOPATH/bin/dep && \
  chmod +x $GOPATH/bin/dep

ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="build-image" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/build-image" \
      org.opencontainers.image.revision="${revision}"
