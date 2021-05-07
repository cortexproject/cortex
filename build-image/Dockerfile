FROM golang:1.16.3-buster
ARG goproxyValue
ENV GOPROXY=${goproxyValue}
RUN apt-get update && apt-get install -y curl python-requests python-yaml file jq unzip protobuf-compiler libprotobuf-dev && \
	rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
RUN curl -sL https://deb.nodesource.com/setup_14.x | bash -
RUN apt-get install -y nodejs && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Install website builder dependencies. Whenever you change these version, please also change website/package.json
# and viceversa.
RUN npm install -g postcss-cli@5.0.1 autoprefixer@9.8.5

ENV HUGO_VERSION=0.72.0
RUN GOARCH=$(go env GOARCH) && echo $GOARCH && \
	if [ "$GOARCH"="amd64" ]; then \
		URL=https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_${HUGO_VERSION}_Linux-64bit.tar.gz; \
	elif [ "$GOARCH"="arm64" ]; then \
		URL=https://github.com/gohugoio/hugo/releases/download/v${HUGO_VERSION}/hugo_${HUGO_VERSION}_Linux-ARM64.tar.gz; \
	fi && \
	echo Downloading ${URL} && \
	curl -s -L -o hugo.tar.gz "${URL}" && \
	tar xzvf hugo.tar.gz -C /usr/bin && \
	rm hugo.tar.gz && \
	chmod +x /usr/bin/hugo

ENV SHFMT_VERSION=3.2.4
RUN curl -fsSLo shfmt https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/shfmt_v${SHFMT_VERSION}_linux_$(go env GOARCH) && \
	chmod +x shfmt && \
	mv shfmt /usr/bin

RUN curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b /usr/bin v1.27.0

RUN GO111MODULE=on go get \
		github.com/client9/misspell/cmd/misspell@v0.3.4 \
		github.com/golang/protobuf/protoc-gen-go@v1.3.1 \
		github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0 \
		github.com/gogo/protobuf/gogoproto@v1.3.0 \
		github.com/weaveworks/tools/cover@bdd647e92546027e12cdde3ae0714bb495e43013 \
		github.com/fatih/faillint@v1.5.0 \
		github.com/campoy/embedmd@v1.0.0 \
	&& true

# Cannot get it to run together in above go get.
RUN GO111MODULE=on go get  \
		github.com/instrumenta/kubeval@v0.16.1 \
	&& true

RUN rm -rf /go/pkg /go/src

ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

ARG revision
LABEL org.opencontainers.image.title="build-image" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/build-image" \
      org.opencontainers.image.revision="${revision}"
