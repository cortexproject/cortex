BIN_DIR := ./bin
GOLANGCI_LINT_VERSION := 1.21.0
GOLANGCI_LINT := $(BIN_DIR)/golangci-lint
PROTOC_VERSION := 3.20.1
RELEASE_OS :=
PROTOC_DIR := .tmp/protoc-$(PROTOC_VERSION)
PROTOC_BIN := $(PROTOC_DIR)/bin/protoc

ifeq ($(OS),Windows_NT)
	echo "Windows not supported yet, sorry...."
	exit 1
else
	UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		RELEASE_OS = linux
	endif
	ifeq ($(UNAME_S),Darwin)
		RELEASE_OS = osx
	endif
endif


all: test lint

tidy:
	go mod tidy -v

build: protoc
	go build ./...

test: build
	go test -cover -race ./...

test-coverage:
	go test ./... -race -coverprofile=coverage.txt && go tool cover -html=coverage.txt

ci-test: build
	go test -race $$(go list ./...) -v -coverprofile coverage.txt -covermode=atomic

setup: setup-git-hooks

setup-git-hooks:
	git config core.hooksPath .githooks

lint: lint-install
	# -D typecheck until golangci-lint gets it together to propery work with go1.13
	$(GOLANGCI_LINT) run --fast --enable-all -D gochecknoglobals -D dupl -D typecheck -D wsl

lint-install:
	# Check if golanglint-ci exists and is of the correct version, if not install
	$(GOLANGCI_LINT) --version | grep $(GOLANGCI_LINT_VERSION) || \
		curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(BIN_DIR) v$(GOLANGCI_LINT_VERSION)

guard-%:
	@ if [ "${${*}}" = "" ]; then \
		echo "Environment variable $* not set"; \
		exit 1; \
	fi


$(PROTOC_BIN):
	@echo "Installing unzip (if required)"
	@which unzip || apt-get update || sudo apt-get update
	@which unzip || apt-get install unzip || sudo apt-get install unzip
	@echo Installing protoc
	rm -rf $(PROTOC_DIR)
	mkdir -p $(PROTOC_DIR)
	cd $(PROTOC_DIR) &&\
		curl -OL https://github.com/google/protobuf/releases/download/v$(PROTOC_VERSION)/protoc-$(PROTOC_VERSION)-$(RELEASE_OS)-x86_64.zip &&\
		unzip protoc-$(PROTOC_VERSION)-$(RELEASE_OS)-x86_64.zip
	chmod +x $(PROTOC_BIN)
	@echo "Installing protoc-gen-go (if required)"
	@which protoc-gen-go > /dev/null || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@echo "Installing protoc-gen-go-grpc (if required)"
	@which protoc-gen-go-grpc > /dev/null || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

run-demo-server:
	go run internal/demo/server/main/main.go

protoc: $(PROTOC_BIN)
	mkdir -p internal/generated/service
	$(PROTOC_BIN) --proto_path=internal/proto \
		--go_out=internal/generated/service --go_opt=paths=source_relative \
		--go-grpc_out=internal/generated/service --go-grpc_opt=paths=source_relative \
		internal/proto/*.proto
