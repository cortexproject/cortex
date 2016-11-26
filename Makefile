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

# List of exes please
CORTEX_EXE := ./cmd/cortex/cortex
EXES = $(CORTEX_EXE)

all: $(UPTODATE_FILES)

# And what goes into each exe
$(CORTEX_EXE): $(shell find . -name '*.go') ui/bindata.go cortex.pb.go
cortex.pb.go: cortex.proto
ui/bindata.go: $(shell find ui/static ui/templates)

# And now what goes into each image
cortex-build/$(UPTODATE): cortex-build/*
cmd/cortex/$(UPTODATE): $(CORTEX_EXE)

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

$(EXES) cortex.pb.go ui/bindata.go lint test shell: cortex-build/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	$(SUDO) docker run $(RM) -ti \
		-v $(shell pwd)/.pkg:/go/pkg \
		-v $(shell pwd):/go/src/github.com/weaveworks/cortex \
		$(IMAGE_PREFIX)/cortex-build $@

else

$(EXES): cortex-build/$(UPTODATE)
	go build $(GO_FLAGS) -o $@ ./$(@D)
	$(NETGO_CHECK)

cortex.pb.go: cortex-build/$(UPTODATE)
	protoc -I ./vendor:. --go_out=plugins=grpc:. ./cortex.proto

ui/bindata.go: cortex-build/$(UPTODATE)
	go-bindata -pkg ui -o ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  ui/templates/... ui/static/...

lint: cortex-build/$(UPTODATE)
	./tools/lint -notestpackage -ignorespelling queriers -ignorespelling Queriers .

test: cortex-build/$(UPTODATE) cortex.pb.go
	./tools/test -no-go-get

shell: cortex-build/$(UPTODATE)
	bash

endif

clean:
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	rm -rf $(UPTODATE_FILES) $(EXES) cortex.pb.go ui/bindata.go
	go clean ./...


