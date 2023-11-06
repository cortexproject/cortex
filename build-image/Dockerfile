FROM golang:1.21.3-bullseye
ARG goproxyValue
ENV GOPROXY=${goproxyValue}
RUN apt-get update && apt-get install -y curl file jq unzip protobuf-compiler libprotobuf-dev && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install website builder dependencies. Whenever you change these version, please also change website/package.json
# and viceversa.
RUN npm install -g postcss-cli@7.1.2 autoprefixer@9.8.5

ENV SHFMT_VERSION=3.2.4
RUN GOARCH=$(go env GOARCH) && \
	if [ "$GOARCH" = "amd64" ]; then \
	DIGEST=3f5a47f8fec27fae3e06d611559a2063f5d27e4b9501171dde9959b8c60a3538; \
	elif [ "$GOARCH" = "arm64" ]; then \
	DIGEST=6474d9cc08a1c9fe2ef4be7a004951998e3067d46cf55a011ddd5ff7bfab3de6; \
	fi && \
	URL=https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/shfmt_v${SHFMT_VERSION}_linux_${GOARCH}; \
	curl -fsSLo shfmt "${URL}" && \
	echo "$DIGEST  shfmt" | sha256sum -c && \
	chmod +x shfmt && \
	mv shfmt /usr/bin

RUN curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b /usr/bin v1.51.2

ENV HUGO_VERSION=v0.101.0
RUN go install github.com/client9/misspell/cmd/misspell@v0.3.4 &&\
	go install github.com/golang/protobuf/protoc-gen-go@v1.3.1 &&\
	go install github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0 &&\
	go install github.com/weaveworks/tools/cover@bdd647e92546027e12cdde3ae0714bb495e43013 &&\
	go install github.com/fatih/faillint@v1.11.0 &&\
	go install github.com/campoy/embedmd@v1.0.0 &&\
	go install --tags extended github.com/gohugoio/hugo@${HUGO_VERSION} &&\
	rm -rf /go/pkg /go/src /root/.cache

ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="build-image" \
	org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/build-image" \
	org.opencontainers.image.revision="${revision}"
