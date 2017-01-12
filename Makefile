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

PROTO_DEFS := $(shell find . -type f -name "*.proto" ! -path "./tools/*" ! -path "./vendor/*")
PROTO_GOS := $(patsubst %.proto,%.pb.go,$(PROTO_DEFS))

# List of exes please
CORTEX_EXE := ./cmd/cortex/cortex
CORTEX_TABLE_MANAGER_EXE := ./cmd/cortex_table_manager/cortex_table_manager
EXES = $(CORTEX_EXE) $(CORTEX_TABLE_MANAGER_EXE)

all: $(UPTODATE_FILES)
test: $(PROTO_GOS)

# And what goes into each exe
$(CORTEX_EXE): $(shell find . -name '*.go' ! -path "./tools/*" ! -path "./vendor/*") ui/bindata.go $(PROTO_GOS)
$(CORTEX_TABLE_MANAGER_EXE): $(shell find ./chunk/ -name '*.go') cmd/cortex_table_manager/main.go
%.pb.go: %.proto
ui/bindata.go: $(shell find ui/static ui/templates)
test: $(PROTO_GOS)

# And now what goes into each image
cortex-build/$(UPTODATE): cortex-build/*
cmd/cortex/$(UPTODATE): $(CORTEX_EXE)
cmd/cortex_table_manager/$(UPTODATE): $(CORTEX_TABLE_MANAGER_EXE)

# All the boiler plate for building golang follows:
SUDO := $(shell docker info >/dev/null 2>&1 || echo "sudo -E")
BUILD_IN_CONTAINER := true
RM := --rm
GO_FLAGS := -ldflags "-extldflags \"-static\" -linkmode=external -s -w" -i

ifeq ($(BUILD_IN_CONTAINER),true)

$(EXES) $(PROTO_GOS) ui/bindata.go lint test shell: cortex-build/$(UPTODATE)
	@mkdir -p $(shell pwd)/.pkg
	$(SUDO) docker run $(RM) -ti \
		-v $(shell pwd)/.pkg:/go/pkg \
		-v $(shell pwd):/go/src/github.com/weaveworks/cortex \
		$(IMAGE_PREFIX)/cortex-build $@

else

$(EXES): cortex-build/$(UPTODATE)
	go build $(GO_FLAGS) -o $@ ./$(@D)

%.pb.go: cortex-build/$(UPTODATE)
	protoc -I ./vendor:./$(@D) --go_out=plugins=grpc:./$(@D) ./$(patsubst %.pb.go,%.proto,$@)

ui/bindata.go: cortex-build/$(UPTODATE)
	go-bindata -pkg ui -o ui/bindata.go -ignore '(.*\.map|bootstrap\.js|bootstrap-theme\.css|bootstrap\.css)'  ui/templates/... ui/static/...

lint: cortex-build/$(UPTODATE)
	./tools/lint -notestpackage -ignorespelling queriers -ignorespelling Queriers .

test: cortex-build/$(UPTODATE)
	./tools/test -netgo

shell: cortex-build/$(UPTODATE)
	bash

endif

clean:
	$(SUDO) docker rmi $(IMAGE_NAMES) >/dev/null 2>&1 || true
	rm -rf $(UPTODATE_FILES) $(EXES) $(PROTO_GOS) ui/bindata.go
	go clean ./...


