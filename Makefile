# Local settings (optional). See Makefile.local.example for an example.
# WARNING: do not commit to a repository!
-include Makefile.local

.PHONY: all test cover clean images protos exes dist doc clean-doc check-doc push-multiarch-build-image
.DEFAULT_GOAL := all

# Version number
VERSION=$(shell cat "./VERSION" 2> /dev/null)
GOPROXY_VALUE=$(shell go env GOPROXY)

# Boiler plate for building Docker containers.
# All this must go at top of file I'm afraid.
IMAGE_PREFIX ?= quay.io/cortexproject/

# For a tag push GITHUB_REF will look like refs/tags/<tag_name>,
# If finding refs/tags/ does not equal emptystring then use
# the tag we are at as the image tag.
ifneq (,$(findstring refs/tags/, $(GITHUB_REF)))
	GIT_TAG := $(shell git tag --points-at HEAD)
endif
IMAGE_TAG ?= $(if $(GIT_TAG),$(GIT_TAG),$(shell ./tools/image-tag))
GIT_REVISION := $(shell git rev-parse --short HEAD)
GIT_BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
UPTODATE := .uptodate

.PHONY: image-tag
image-tag:
	@echo $(IMAGE_TAG)

# Support gsed on OSX (installed via brew), falling back to sed. On Linux
# systems gsed won't be installed, so will use sed as expected.
SED ?= $(shell which gsed 2>/dev/null || which sed)

# Building Docker images is now automated. The convention is every directory
# with a Dockerfile in it builds an image calls quay.io/cortexproject/<dirname>.
# Dependencies (i.e. things that go in the image) still need to be explicitly
# declared.
%/$(UPTODATE): %/Dockerfile
	@echo
	$(SUDO) docker build --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)) -t $(IMAGE_PREFIX)$(shell basename $(@D)):$(IMAGE_TAG) $(@D)/
	@echo
	@echo Please use push-multiarch-build-image to build and push build image for all supported architectures.
	touch $@

# This target fetches current build image, and tags it with "latest" tag. It can be used instead of building the image locally.
fetch-build-image:
	docker pull $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG)
	docker tag $(BUILD_IMAGE):$(LATEST_BUILD_IMAGE_TAG) $(BUILD_IMAGE):latest
	touch build-image/.uptodate

push-multiarch-build-image:
	@echo
	# Build image for each platform separately... it tends to generate fewer errors.
	$(SUDO) docker buildx build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) build-image/
	$(SUDO) docker buildx build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) build-image/
	# This command will run the same build as above, but it will reuse existing platform-specific images,
	# put them together and push to registry.
	$(SUDO) docker buildx build -o type=registry --platform linux/amd64,linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)build-image:$(IMAGE_TAG) build-image/

# We don't want find to scan inside a bunch of directories, to accelerate the
# 'make: Entering directory '/go/src/github.com/cortexproject/cortex' phase.
DONT_FIND := -name vendor -prune -o -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o -name packaging -prune -o

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
pkg/cortexpb/cortex.pb.go: pkg/cortexpb/cortex.proto
pkg/ingester/client/ingester.pb.go: pkg/ingester/client/ingester.proto
pkg/distributor/distributorpb/distributor.pb.go: pkg/distributor/distributorpb/distributor.proto
pkg/ingester/wal.pb.go: pkg/ingester/wal.proto
pkg/ring/ring.pb.go: pkg/ring/ring.proto
pkg/frontend/v1/frontendv1pb/frontend.pb.go: pkg/frontend/v1/frontendv1pb/frontend.proto
pkg/frontend/v2/frontendv2pb/frontend.pb.go: pkg/frontend/v2/frontendv2pb/frontend.proto
pkg/querier/queryrange/queryrange.pb.go: pkg/querier/queryrange/queryrange.proto
pkg/querier/stats/stats.pb.go: pkg/querier/stats/stats.proto
pkg/chunk/storage/caching_index_client.pb.go: pkg/chunk/storage/caching_index_client.proto
pkg/distributor/ha_tracker.pb.go: pkg/distributor/ha_tracker.proto
pkg/ruler/rulespb/rules.pb.go: pkg/ruler/rulespb/rules.proto
pkg/ruler/ruler.pb.go: pkg/ruler/ruler.proto
pkg/ring/kv/memberlist/kv.pb.go: pkg/ring/kv/memberlist/kv.proto
pkg/scheduler/schedulerpb/scheduler.pb.go: pkg/scheduler/schedulerpb/scheduler.proto
pkg/storegateway/storegatewaypb/gateway.pb.go: pkg/storegateway/storegatewaypb/gateway.proto
pkg/chunk/grpc/grpc.pb.go: pkg/chunk/grpc/grpc.proto
pkg/alertmanager/alertmanagerpb/alertmanager.pb.go: pkg/alertmanager/alertmanagerpb/alertmanager.proto
pkg/alertmanager/alertspb/alerts.pb.go: pkg/alertmanager/alertspb/alerts.proto

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
LATEST_BUILD_IMAGE_TAG ?= update-go-1-17-8-6aed4de76

# TTY is parameterized to allow Google Cloud Builder to run builds,
# as it currently disallows TTY devices. This value needs to be overridden
# in any custom cloudbuild.yaml files
TTY := --tty
GO_FLAGS := -ldflags "-X main.Branch=$(GIT_BRANCH) -X main.Revision=$(GIT_REVISION) -X main.Version=$(VERSION) -extldflags \"-static\" -s -w" -tags netgo

ifeq ($(BUILD_IN_CONTAINER),true)

GOVOLUMES=	-v $(shell pwd)/.cache:/go/cache:delegated,z \
			-v $(shell pwd)/.pkg:/go/pkg:delegated,z \
			-v $(shell pwd):/go/src/github.com/cortexproject/cortex:delegated,z

exes $(EXES) protos $(PROTO_GOS) lint test cover shell mod-check check-protos web-build web-pre web-deploy doc: build-image/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@echo
	@echo ">>>> Entering build container: $@"
	@$(SUDO) time docker run --rm $(TTY) -i $(GOVOLUMES) $(BUILD_IMAGE) $@;

configs-integration-test: build-image/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@DB_CONTAINER="$$(docker run -d -e 'POSTGRES_DB=configs_test' postgres:9.6.16)"; \
	echo ; \
	echo ">>>> Entering build container: $@"; \
	$(SUDO) docker run --rm $(TTY) -i $(GOVOLUMES) \
		-v $(shell pwd)/cmd/cortex/migrations:/migrations:z \
		--workdir /go/src/github.com/cortexproject/cortex \
		--link "$$DB_CONTAINER":configs-db.cortex.local \
		-e DB_ADDR=configs-db.cortex.local \
		$(BUILD_IMAGE) $@; \
	status=$$?; \
	docker rm -f "$$DB_CONTAINER"; \
	exit $$status

else

exes: $(EXES)

$(EXES):
	CGO_ENABLED=0 go build $(GO_FLAGS) -o $@ ./$(@D)

protos: $(PROTO_GOS)

%.pb.go:
	@# The store-gateway RPC is based on Thanos which uses relative references to other protos, so we need
	@# to configure all such relative paths.
	protoc -I $(GOPATH)/src:./vendor/github.com/thanos-io/thanos/pkg:./vendor/github.com/gogo/protobuf:./vendor:./$(@D) --gogoslick_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

lint:
	misspell -error docs

	# Configured via .golangci.yml.
	golangci-lint run

	# Ensure no blocklisted package is imported.
	GOFLAGS="-tags=requires_docker" faillint -paths "github.com/bmizerany/assert=github.com/stretchr/testify/assert,\
		golang.org/x/net/context=context,\
		sync/atomic=go.uber.org/atomic,\
		github.com/prometheus/client_golang/prometheus.{MultiError}=github.com/prometheus/prometheus/tsdb/errors.{NewMulti},\
		github.com/weaveworks/common/user.{ExtractOrgID}=github.com/cortexproject/cortex/pkg/tenant.{TenantID,TenantIDs},\
		github.com/weaveworks/common/user.{ExtractOrgIDFromHTTPRequest}=github.com/cortexproject/cortex/pkg/tenant.{ExtractTenantIDFromHTTPRequest}" ./pkg/... ./cmd/... ./tools/... ./integration/...

	# Ensure clean pkg structure.
	faillint -paths "\
		github.com/cortexproject/cortex/pkg/scheduler,\
		github.com/cortexproject/cortex/pkg/frontend,\
		github.com/cortexproject/cortex/pkg/frontend/transport,\
		github.com/cortexproject/cortex/pkg/frontend/v1,\
		github.com/cortexproject/cortex/pkg/frontend/v2" \
		./pkg/querier/...
	faillint -paths "github.com/cortexproject/cortex/pkg/querier/..." ./pkg/scheduler/...
	faillint -paths "github.com/cortexproject/cortex/pkg/storage/tsdb/..." ./pkg/storage/bucket/...
	faillint -paths "github.com/cortexproject/cortex/pkg/..." ./pkg/alertmanager/alertspb/...
	faillint -paths "github.com/cortexproject/cortex/pkg/..." ./pkg/ruler/rulespb/...

	# Ensure the query path is supporting multiple tenants
	faillint -paths "\
		github.com/cortexproject/cortex/pkg/tenant.{TenantID}=github.com/cortexproject/cortex/pkg/tenant.{TenantIDs}" \
		./pkg/scheduler/... \
		./pkg/frontend/... \
		./pkg/querier/tenantfederation/... \
		./pkg/querier/queryrange/...

	# Ensure packages that no longer use a global logger don't reintroduce it
	faillint -paths "github.com/cortexproject/cortex/pkg/util/log.{Logger}" \
		./pkg/alertmanager/alertstore/... \
		./pkg/ingester/... \
		./pkg/flusher/... \
		./pkg/querier/... \
		./pkg/ruler/...

test:
	go test -tags netgo -timeout 30m -race -count 1 ./...

cover:
	$(eval COVERDIR := $(shell mktemp -d coverage.XXXXXXXXXX))
	$(eval COVERFILE := $(shell mktemp $(COVERDIR)/unit.XXXXXXXXXX))
	go test -tags netgo -timeout 30m -race -count 1 -coverprofile=$(COVERFILE) ./...
	go tool cover -html=$(COVERFILE) -o cover.html
	go tool cover -func=cover.html | tail -n1

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

# Generates the config file documentation.
doc: clean-doc
	go run ./tools/doc-generator ./docs/configuration/config-file-reference.template > ./docs/configuration/config-file-reference.md
	go run ./tools/doc-generator ./docs/blocks-storage/compactor.template            > ./docs/blocks-storage/compactor.md
	go run ./tools/doc-generator ./docs/blocks-storage/store-gateway.template        > ./docs/blocks-storage/store-gateway.md
	go run ./tools/doc-generator ./docs/blocks-storage/querier.template              > ./docs/blocks-storage/querier.md
	go run ./tools/doc-generator ./docs/guides/encryption-at-rest.template           > ./docs/guides/encryption-at-rest.md
	embedmd -w docs/operations/requests-mirroring-to-secondary-cluster.md
	embedmd -w docs/guides/overrides-exporter.md

endif

clean:
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	rm -rf -- $(UPTODATE_FILES) $(EXES) .cache dist
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

clean-doc:
	rm -f \
		./docs/configuration/config-file-reference.md \
		./docs/blocks-storage/compactor.md \
		./docs/blocks-storage/store-gateway.md \
		./docs/blocks-storage/querier.md \
		./docs/guides/encryption-at-rest.md

check-doc: doc
	@git diff --exit-code -- ./docs/configuration/config-file-reference.md ./docs/blocks-storage/*.md ./docs/configuration/*.md

clean-white-noise:
	@find . -path ./.pkg -prune -o -path ./vendor -prune -o -path ./website -prune -or -type f -name "*.md" -print | \
	SED_BIN="$(SED)" xargs ./tools/cleanup-white-noise.sh

check-white-noise: clean-white-noise
	@git diff --exit-code --quiet -- '*.md' || (echo "Please remove trailing whitespaces running 'make clean-white-noise'" && false)

web-serve:
	cd website && hugo --config config.toml --minify -v server

# Generate binaries for a Cortex release
dist: dist/$(UPTODATE)

dist/$(UPTODATE):
	rm -fr ./dist
	mkdir -p ./dist
	for os in linux darwin; do \
      for arch in amd64 arm64; do \
        echo "Building Cortex for $$os/$$arch"; \
        GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/cortex-$$os-$$arch ./cmd/cortex; \
        sha256sum ./dist/cortex-$$os-$$arch | cut -d ' ' -f 1 > ./dist/cortex-$$os-$$arch-sha-256; \
        echo "Building query-tee for $$os/$$arch"; \
        GOOS=$$os GOARCH=$$arch CGO_ENABLED=0 go build $(GO_FLAGS) -o ./dist/query-tee-$$os-$$arch ./cmd/query-tee; \
        sha256sum ./dist/query-tee-$$os-$$arch | cut -d ' ' -f 1 > ./dist/query-tee-$$os-$$arch-sha-256; \
      done; \
    done; \
    touch $@

# Generate packages for a Cortex release.
FPM_OPTS := fpm -s dir -v $(VERSION) -n cortex -f \
	--license "Apache 2.0" \
	--url "https://github.com/cortexproject/cortex"

PACKAGE_IN_CONTAINER := true
PACKAGE_IMAGE ?= $(IMAGE_PREFIX)fpm
ifeq ($(PACKAGE_IN_CONTAINER), true)

.PHONY: packages
packages: dist packaging/fpm/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	@mkdir -p $(shell pwd)/.cache
	@echo ">>>> Entering build container: $@"
	@$(SUDO) time docker run --rm $(TTY) \
		-v  $(shell pwd):/src/github.com/cortexproject/cortex:delegated,z \
		-i $(PACKAGE_IMAGE) $@;

else

packages: dist/$(UPTODATE)-packages

dist/$(UPTODATE)-packages: dist $(wildcard packaging/deb/**) $(wildcard packaging/rpm/**)
	for arch in amd64 arm64; do \
  		rpm_arch=x86_64; \
  		deb_arch=x86_64; \
  		if [ "$$arch" = "arm64" ]; then \
		    rpm_arch=aarch64; \
		    deb_arch=arm64; \
		fi; \
		$(FPM_OPTS) -t deb \
			--architecture $$deb_arch \
			--after-install packaging/deb/control/postinst \
			--before-remove packaging/deb/control/prerm \
			--package dist/cortex-$(VERSION)_$$arch.deb \
			dist/cortex-linux-$$arch=/usr/local/bin/cortex \
			docs/chunks-storage/single-process-config.yaml=/etc/cortex/single-process-config.yaml \
			packaging/deb/default/cortex=/etc/default/cortex \
			packaging/deb/systemd/cortex.service=/etc/systemd/system/cortex.service; \
		$(FPM_OPTS) -t rpm  \
			--architecture $$rpm_arch \
			--after-install packaging/rpm/control/post \
			--before-remove packaging/rpm/control/preun \
			--package dist/cortex-$(VERSION)_$$arch.rpm \
			dist/cortex-linux-$$arch=/usr/local/bin/cortex \
			docs/chunks-storage/single-process-config.yaml=/etc/cortex/single-process-config.yaml \
			packaging/rpm/sysconfig/cortex=/etc/sysconfig/cortex \
			packaging/rpm/systemd/cortex.service=/etc/systemd/system/cortex.service; \
	done
	for pkg in dist/*.deb dist/*.rpm; do \
  		sha256sum $$pkg | cut -d ' ' -f 1 > $${pkg}-sha-256; \
  	done; \
  	touch $@

endif

# Build both arm64 and amd64 images, so that we can test deb/rpm packages for both architectures.
packaging/rpm/centos-systemd/$(UPTODATE): packaging/rpm/centos-systemd/Dockerfile
	$(SUDO) docker build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):amd64 $(@D)/
	$(SUDO) docker build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):arm64 $(@D)/
	touch $@

packaging/deb/debian-systemd/$(UPTODATE): packaging/deb/debian-systemd/Dockerfile
	$(SUDO) docker build --platform linux/amd64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):amd64 $(@D)/
	$(SUDO) docker build --platform linux/arm64 --build-arg=revision=$(GIT_REVISION) --build-arg=goproxyValue=$(GOPROXY_VALUE) -t $(IMAGE_PREFIX)$(shell basename $(@D)):arm64 $(@D)/
	touch $@

.PHONY: test-packages
test-packages: packages packaging/rpm/centos-systemd/$(UPTODATE) packaging/deb/debian-systemd/$(UPTODATE)
	./tools/packaging/test-packages $(IMAGE_PREFIX) $(VERSION)
