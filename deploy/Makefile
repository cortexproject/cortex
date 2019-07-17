# Usage:
# make all		   # same as 'make demo'
# make cortex	   # cortex only
# make demo		   # cortex + nginx + prometheus
# make test		   # kubecfg validate on: cortex + nginx + prometheus

CURRENT_DIR := $(abspath .)

# Project Structure
MANIFESTS := $(CURRENT_DIR)/deployments
CORTEX_MANIFESTS := $(MANIFESTS)/cortex.yaml
DEMO_MANIFESTS := $(MANIFESTS)/demo.yaml
VENDOR := $(CURRENT_DIR)/vendor

.PHONY: godeps clean all test cortex demo

GOPATH ?= $(CURRENT_DIR)/gopath
GOPATH_DIR := $(GOPATH)
NEWPATH := $(PATH):$(CURRENT_DIR)/go/bin:$(GOPATH)/bin

JSONNET_LOCK_FILE = $(CURRENT_DIR)/jsonnetfile.lock.json
LIBRARY_SOURCES = $(wildcard $(CURRENT_DIR)/lib/*.libsonnet)
CORTEX_SOURCES = $(LIBRARY_SOURCES) $(CURRENT_DIR)/cortex.jsonnet
DEMO_SOURCES = $(LIBRARY_SOURCES) $(CURRENT_DIR)/demo.jsonnet

all: demo

# go get dependencies
godeps: $(GOPATH)

$(GOPATH): export PATH=$(NEWPATH)
$(GOPATH): export GOPATH=$(GOPATH_DIR)
$(GOPATH):
	bash $(CURRENT_DIR)/get-go-tools.sh
	go get github.com/jsonnet-bundler/jsonnet-bundler/cmd/jb
	go get github.com/bitnami/kubecfg

# jsonnet-bundler vendored libraries
$(VENDOR): export PATH=$(NEWPATH)
$(VENDOR): export GOPATH=$(GOPATH_DIR)
$(VENDOR): $(GOPATH) $(JSONNET_LOCK_FILE)
	jb install

$(MANIFESTS):
	mkdir -p $(MANIFESTS)

$(CORTEX_MANIFESTS): export PATH=$(NEWPATH)
$(CORTEX_MANIFESTS): export GOPATH=$(GOPATH_DIR)
$(CORTEX_MANIFESTS): $(GOPATH) $(VENDOR) $(CORTEX_SOURCES) $(MANIFESTS)
	kubecfg show cortex.jsonnet -J $(VENDOR) > $(CORTEX_MANIFESTS)

$(DEMO_MANIFESTS): export PATH=$(NEWPATH)
$(DEMO_MANIFESTS): export GOPATH=$(GOPATH_DIR)
$(DEMO_MANIFESTS): $(GOPATH) $(VENDOR) $(DEMO_SOURCES) $(MANIFESTS)
	kubecfg show demo.jsonnet -J $(VENDOR) > $(DEMO_MANIFESTS)

clean:
	rm -rf $(MANIFESTS)
	rm -rf $(VENDOR)
	rm -rf $(GOPATH)
	rm -rf tmp/
	rm -rf go

# Build cortex components only
cortex: $(CORTEX_MANIFESTS)

# Build cortex components and 
demo: $(DEMO_MANIFESTS)

test:
	kubecfg validate demo.jsonnet -J $(VENDOR)
