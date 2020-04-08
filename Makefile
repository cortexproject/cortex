.PHONY: all test clean images protos exes dist
.DEFAULT_GOAL := all

# Boiler plate for building Docker containers.
# All this must go at top of file I'm afraid.
IMAGE_PREFIX ?= quay.io/cortexproject/
# Use CIRCLE_TAG if present for releases.
IMAGE_TAG ?= $(if $(CIRCLE_TAG),$(CIRCLE_TAG),$(shell ./tools/image-tag))
GIT_REVISION := $(shell git rev-parse HEAD)
UPTODATE := .uptodate

# Building Docker images is now automated. The convention is every directory
# with a Dockerfile in it builds an image calls quay.io/cortexproject/<dirname>.
# Dependencies (i.e. things that go in the image) still need to be explicitly
# declared.
%/$(UPTODATE): %/Dockerfile
	@echo
	$(SUDO) docker build --build-arg=revision=$(GIT_REVISION) -t $(IMAGE_PREFIX)$(shell basename $(@D)) $(@D)/
	$(SUDO) docker tag $(IMAGE_PREFIX)$(shell basename $(@D)) $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG)
	touch $@

# We don't want find to scan inside a bunch of directories, to accelerate the
# 'make: Entering directory '/go/src/github.com/cortexproject/cortex' phase.
DONT_FIND := -name tools -prune -o -name vendor -prune -o -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o

# Get a list of directories containing Dockerfiles
DOCKERFILES := $(shell find . $(DONT_FIND) -type f -name 'Dockerfile' -print)
UPTODATE_FILES := $(patsubst %/Dockerfile,%/$(UPTODATE),$(DOCKERFILES))
DOCKER_IMAGE_DIRS := $(patsubst %/Dockerfile,%,$(DOCKERFILES))
IMAGE_NAMES := $(foreach dir,$(DOCKER_IMAGE_DIRS),$(patsubst %,$(IMAGE_PREFIX)%,$(shell basename $(dir))))
images:
	$(info $(IMAGE_NAMES))
	@echo > /dev/null

# Generating proto code is automated.
PROTO_DEFS := $(shell find . $(DONT_FIND) -type f -name '*.proto' -print)
PROTO_GOS := $(patsubst %.proto,%.pb.go,$(PROTO_DEFS))

# Building binaries is now automated.  The convention is to build a binary
# for every directory with main.go in it, in the ./cmd directory.
MAIN_GO := $(shell find . $(DONT_FIND) -type f -name 'main.go' -print)
EXES := $(foreach exe, $(patsubst ./cmd/%/main.go, %, $(MAIN_GO)), ./cmd/$(exe)/$(exe))
GO_FILES := $(shell find . $(DONT_FIND) -name cmd -prune -o -name '*.pb.go' -prune -o -type f -name '*.go' -print)
define dep_exe
$(1): $(dir $(1))/main.go $(GO_FILES) protos
$(dir $(1))$(UPTODATE): $(1)
endef
$(foreach exe, $(EXES), $(eval $(call dep_exe, $(exe))))

# Manually declared dependencies And what goes into each exe
pkg/ingester/client/cortex.pb.go: pkg/ingester/client/cortex.proto
pkg/ingester/wal.pb.go: pkg/ingester/wal.proto
pkg/ring/ring.pb.go: pkg/ring/ring.proto
pkg/querier/frontend/frontend.pb.go: pkg/querier/frontend/frontend.proto
pkg/querier/queryrange/queryrange.pb.go: pkg/querier/queryrange/queryrange.proto
pkg/chunk/storage/caching_index_client.pb.go: pkg/chunk/storage/caching_index_client.proto
pkg/distributor/ha_tracker.pb.go: pkg/distributor/ha_tracker.proto
pkg/ruler/rules/rules.pb.go: pkg/ruler/rules/rules.proto
pkg/ruler/ruler.pb.go: pkg/ruler/rules/rules.proto
pkg/ring/kv/memberlist/kv.pb.go: pkg/ring/kv/memberlist/kv.proto

all: $(UPTODATE_FILES)
test: protos
mod-check: protos
configs-integration-test: protos
lint: protos
build-image/$(UPTODATE): build-image/*

# All the boiler plate for building golang follows:
SUDO := $(shell docker info >/dev/null 2>&1 || echo "sudo -E")
BUILD_IN_CONTAINER := true
BUILD_IMAGE ?= $(IMAGE_PREFIX)build-image
# RM is parameterized to allow CircleCI to run builds, as it
# currently disallows `docker run --rm`. This value is overridden
# in circle.yml
RM := --rm
# TTY is parameterized to allow Google Cloud Builder to run builds,
# as it currently disallows TTY devices. This value needs to be overridden
# in any custom cloudbuild.yaml files
TTY := --tty
GO_FLAGS := -ldflags "-extldflags \"-static\" -s -w" -tags netgo

ifeq ($(BUILD_IN_CONTAINER),true)

GOVOLUMES=	-v $(shell pwd)/.cache:/go/cache:delegated \
			-v $(shell pwd)/.pkg:/go/pkg:delegated \
			-v $(shell pwd):/go/src/github.com/cortexproject/cortex:delegated

exes $(EXES) protos $(PROTO_GOS) lint test shell mod-check check-protos web-build web-pre web-deploy: build-image/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@echo
	@echo ">>>> Entering build container: $@"
	@$(SUDO) time docker run $(RM) $(TTY) -i $(GOVOLUMES) $(BUILD_IMAGE) $@;

configs-integration-test: build-image/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@DB_CONTAINER="$$(docker run -d -e 'POSTGRES_DB=configs_test' postgres:9.6.16)"; \
	echo ; \
	echo ">>>> Entering build container: $@"; \
	$(SUDO) docker run $(RM) $(TTY) -i $(GOVOLUMES) \
		-v $(shell pwd)/cmd/cortex/migrations:/migrations \
		--workdir /go/src/github.com/cortexproject/cortex \
		--link "$$DB_CONTAINER":configs-db.cortex.local \
		-e DB_ADDR=configs-db.cortex.local \
		$(BUILD_IMAGE) $@; \
	status=$$?; \
	test -n "$(CIRCLECI)" || docker rm -f "$$DB_CONTAINER"; \
	exit $$status

else

exes: $(EXES)

$(EXES):
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)

protos: $(PROTO_GOS)

%.pb.go:
	@# The store-gateway RPC is based on Thanos which uses relative references to other protos, so we need
	@# to configure all such relative paths.
	protoc -I $(GOPATH)/src:./vendor/github.com/thanos-io/thanos/pkg/store:./vendor/github.com/thanos-io/thanos/pkg/store/storepb:./vendor/github.com/gogo/protobuf:./vendor:./$(@D) --gogoslick_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

lint:
	misspell -error docs
	golangci-lint run --build-tags netgo,require_docker --timeout=5m --enable golint --enable misspell --enable gofmt

	# Ensure no blacklisted package is imported.
	faillint -paths "github.com/bmizerany/assert=github.com/stretchr/testify/assert" ./pkg/... ./cmd/... ./tools/... ./integration/...

	# Validate Kubernetes spec files. Requires:
	# https://kubeval.instrumenta.dev
	kubeval ./k8s/*

test:
	./tools/test -netgo

shell:
	bash

configs-integration-test:
	/bin/bash -c "go test -v -tags 'netgo integration' -timeout 30s ./pkg/configs/... ./pkg/ruler/..."

mod-check:
	GO111MODULE=on go mod download
	GO111MODULE=on go mod verify
	GO111MODULE=on go mod tidy
	GO111MODULE=on go mod vendor
	@git diff --exit-code -- go.sum go.mod vendor/

check-protos: clean-protos protos
	@git diff --exit-code -- $(PROTO_GOS)

web-pre:
	cd website && git submodule update --init --recursive
	./tools/website/web-pre.sh

web-build: web-pre
	cd website && HUGO_ENV=production hugo --config config.toml  --minify -v

web-deploy:
	./tools/website/web-deploy.sh

endif

clean:
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	rm -rf $(UPTODATE_FILES) $(EXES) .cache
	go clean ./...

clean-protos:
	rm -rf $(PROTO_GOS)

save-images:
	@mkdir -p docker-images
	for image_name in $(IMAGE_NAMES); do \
		if ! echo $$image_name | grep build; then \
			docker save $$image_name:$(IMAGE_TAG) -o docker-images/$$(echo $$image_name | tr "/" _):$(IMAGE_TAG); \
		fi \
	done

load-images:
	for image_name in $(IMAGE_NAMES); do \
		if ! echo $$image_name | grep build; then \
			docker load -i docker-images/$$(echo $$image_name | tr "/" _):$(IMAGE_TAG); \
		fi \
	done

# Generates the config file documentation.
doc: clean-doc
	go run ./tools/doc-generator ./docs/configuration/config-file-reference.template >> ./docs/configuration/config-file-reference.md
	go run ./tools/doc-generator ./docs/operations/blocks-storage.template           >> ./docs/operations/blocks-storage.md

clean-doc:
	rm -f ./docs/configuration/config-file-reference.md ./docs/operations/blocks-storage.md

check-doc: doc
	@git diff --exit-code -- ./docs/configuration/config-file-reference.md ./docs/operations/blocks-storage.md

web-serve:
	cd website && hugo --config config.toml -v server

# Generate binaries for a Cortex release
dist:
	rm -fr ./dist
	mkdir -p ./dist
	GOOS="linux"  GOARCH="amd64" CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/cortex-linux-amd64   ./cmd/cortex
	GOOS="darwin" GOARCH="amd64" CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/cortex-darwin-amd64  ./cmd/cortex
	shasum -a 256 ./dist/cortex-darwin-amd64 | cut -d ' ' -f 1 > ./dist/cortex-darwin-amd64-sha-256
	shasum -a 256 ./dist/cortex-linux-amd64  | cut -d ' ' -f 1 > ./dist/cortex-linux-amd64-sha-256
