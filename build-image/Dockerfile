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

ENV HUGO_VERSION=v0.72.0
RUN git clone https://github.com/gohugoio/hugo.git --branch ${HUGO_VERSION} --depth 1 && \
	cd hugo  && go install --tags extended && cd ../ && \
	rm -rf hugo/ && rm -rf /go/pkg /go/src
ENV SHFMT_VERSION=3.1.0
RUN curl -fsSLo shfmt https://github.com/mvdan/sh/releases/download/v${SHFMT_VERSION}/shfmt_v${SHFMT_VERSION}_linux_amd64 && \
	echo "cb91ea08a075a2f96b5230f09b4e211b7c108b1c97603caceb48d117d2ac5508  shfmt" | sha256sum -c && \
	chmod +x shfmt && \
	mv shfmt /usr/bin
RUN curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh| sh -s -- -b /usr/bin v1.27.0
RUN GO111MODULE=on go get -tags netgo \
		github.com/client9/misspell/cmd/misspell@v0.3.4 \
		github.com/golang/protobuf/protoc-gen-go@v1.3.1 \
		github.com/gogo/protobuf/protoc-gen-gogoslick@v1.3.0 \
		github.com/gogo/protobuf/gogoproto@v1.3.0 \
		github.com/weaveworks/tools/cover@bdd647e92546027e12cdde3ae0714bb495e43013 && \
	rm -rf /go/pkg /go/src

ENV KUBEVAL_VERSION=0.15.0
RUN curl -Ls https://github.com/instrumenta/kubeval/releases/download/${KUBEVAL_VERSION}/kubeval-linux-amd64.tar.gz -o /tmp/kubeval-linux-amd64.tar.gz && \
	tar -xf /tmp/kubeval-linux-amd64.tar.gz -C /tmp && \
	cp /tmp/kubeval /usr/local/bin && \
	rm -f /tmp/kubeval*

ENV NODE_PATH=/usr/lib/node_modules
COPY build.sh /
ENV GOCACHE=/go/cache
ENTRYPOINT ["/build.sh"]

# Install faillint used to lint go imports in CI.
ENV FAILLINT_VERSION=1.5.0
RUN GO111MODULE=on go get github.com/fatih/faillint@v${FAILLINT_VERSION} && \
    rm -rf /go/pkg /go/src

# Install embedmd used to embed content into markdown files.
ENV EMBEDMD_VERSION=1.0.0
RUN GO111MODULE=on go get github.com/campoy/embedmd@v${EMBEDMD_VERSION} && \
    rm -rf /go/pkg /go/src

ARG revision
LABEL org.opencontainers.image.title="build-image" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/build-image" \
      org.opencontainers.image.revision="${revision}"
