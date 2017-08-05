.PHONY: all test clean images
.DEFAULT_GOAL := all

# Boiler plate for bulding Docker containers.
# All this must go at top of file I'm afraid.
IMAGE_PREFIX := weaveworks
IMAGE_TAG := $(shell ./tools/image-tag)
UPTODATE := .uptodate

# Building Docker images is now automated. The convention is every directory
# with a Dockerfile in it builds an image calls quay.io/weaveworks/<dirname>.
# Dependencies (i.e. things that go in the image) still need to be explicitly
# declared.
%/$(UPTODATE): %/Dockerfile
	$(SUDO) docker build -t $(IMAGE_PREFIX)/$(shell basename $(@D)) $(@D)/
	$(SUDO) docker tag $(IMAGE_PREFIX)/$(shell basename $(@D)) $(IMAGE_PREFIX)/$(shell basename $(@D)):$(IMAGE_TAG)
	touch $@

# Get a list of directories containing Dockerfiles
DOCKERFILES := $(shell find . -type f -name Dockerfile ! -path "./tools/*" ! -path "./vendor/*")
UPTODATE_FILES := $(patsubst %/Dockerfile,%/$(UPTODATE),$(DOCKERFILES))
DOCKER_IMAGE_DIRS=$(patsubst %/Dockerfile,%,$(DOCKERFILES))
IMAGE_NAMES=$(foreach dir,$(DOCKER_IMAGE_DIRS),$(patsubst %,$(IMAGE_PREFIX)/%,$(shell basename $(dir))))
images:
	$(info $(IMAGE_NAMES))

# Generating proto code is automated.
PROTO_DEFS := $(shell find . -name tools -prune -o -name vendor -prune -o -type f -name '*.proto' -print)
PROTO_GOS := $(patsubst %.proto,%.pb.go,$(PROTO_DEFS))

# List of exes please
LOKI_EXE := cmd/loki/loki
EXES = $(LOKI_EXE)

all: $(UPTODATE_FILES)

# And what goes into each exe
GO_FILES := $(shell find . -name tools -prune -o -name vendor -prune -o -type f -name '*.go' -print)
$(LOKI_EXE): $(GO_FILES) pkg/zipkin-ui/bindata.go pkg/model/model.pb.go
pkg/zipkin-ui/bindata.go: $(shell find pkg/zipkin-ui/static)
pkg/model/model.pb.go: pkg/model/model.proto

# And now what goes into each image
loki-build/$(UPTODATE): loki-build/*
cmd/loki/$(UPTODATE): $(LOKI_EXE)

# All the boiler plate for building golang follows:
SUDO := $(shell docker info >/dev/null 2>&1 || echo "sudo -E")
BUILD_IN_CONTAINER := true
RM := --rm
GO_FLAGS := -ldflags "-extldflags \"-static\" -linkmode=external -s -w" -tags netgo -i
NETGO_CHECK = @strings $@ | grep cgo_stub\\\.go >/dev/null || { \
	rm $@; \
	echo "\nYour go standard library was built without the 'netgo' build tag."; \
	echo "To fix that, run"; \
	echo "    sudo go clean -i net"; \
	echo "    sudo go install -tags netgo std"; \
	false; \
}

ifeq ($(BUILD_IN_CONTAINER),true)

$(EXES) $(PROTO_GOS) pkg/zipkin-ui/bindata.go lint test shell: loki-build/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	$(SUDO) docker run $(RM) -ti \
		-v $(shell pwd)/.pkg:/go/pkg \
		-v $(shell pwd):/go/src/github.com/weaveworks-experiments/loki \
		$(IMAGE_PREFIX)/loki-build $@

else

$(EXES): loki-build/$(UPTODATE)
	go build $(GO_FLAGS) -o $@ ./$(@D)
	$(NETGO_CHECK)

pkg/zipkin-ui/bindata.go: loki-build/$(UPTODATE)
	go-bindata -pkg ui -o pkg/zipkin-ui/bindata.go pkg/zipkin-ui/static/

%.pb.go: loki-build/$(UPTODATE)
	protoc -I ./vendor:./$(@D) --gogoslick_out=plugins=grpc:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

lint: loki-build/$(UPTODATE)
	./tools/lint -notestpackage -nocomment .

test: loki-build/$(UPTODATE)
	./tools/test -no-go-get -netgo

shell: loki-build/$(UPTODATE)
	bash

endif

clean:
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	rm -rf $(UPTODATE_FILES) $(EXES) pkg/zipkin-ui/bindata.go
	go clean ./...
