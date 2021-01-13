# This Makefile encodes the "go generate" prerequisites ensuring that the proper tooling is installed and
# that the generate steps are executed when their prerequeisites files change.

SHELL := /bin/bash

GO_TAGS=
GO_ARGS=-tags '$(GO_TAGS)'

# This invokes a custom package config during Go builds, such that
# the Rust library libflux is built on the fly.
export PKG_CONFIG:=$(PWD)/pkg-config.sh

export GOOS=$(shell go env GOOS)
export GO_BUILD=env GO111MODULE=on go build $(GO_ARGS)
export GO_TEST=env GO111MODULE=on go test $(GO_ARGS)
export GO_TEST_FLAGS=
# Do not add GO111MODULE=on to the call to go generate so it doesn't pollute the environment.
export GO_GENERATE=go generate $(GO_ARGS)
export GO_VET=env GO111MODULE=on go vet $(GO_ARGS)
export CARGO=cargo
export CARGO_ARGS=

define go_deps
	$(shell env GO111MODULE=on go list -f "{{range .GoFiles}} {{$$.Dir}}/{{.}}{{end}}" $(1))
endef

default: build

STDLIB_SOURCES = $(shell find . -name '*.flux')

GENERATED_TARGETS = \
	ast/internal/fbast/ast_generated.go \
	ast/asttest/cmpopts.go \
	stdlib/packages.go \
	internal/fbsemantic/semantic_generated.go \
	internal/fbsemantic/semantic_generated.go \
	libflux/go/libflux/buildinfo.gen.go \
	$(LIBFLUX_GENERATED_TARGETS)

LIBFLUX_GENERATED_TARGETS = \
	libflux/core/src/ast/flatbuffers/ast_generated.rs \
	libflux/core/src/semantic/flatbuffers/semantic_generated.rs \
	libflux/scanner.c

generate: $(GENERATED_TARGETS)

ast/internal/fbast/ast_generated.go: ast/ast.fbs
	$(GO_GENERATE) ./ast
libflux/core/src/ast/flatbuffers/ast_generated.rs: ast/ast.fbs
	flatc --rust -o libflux/core/src/ast/flatbuffers ast/ast.fbs && rustfmt $@

internal/fbsemantic/semantic_generated.go: internal/fbsemantic/semantic.fbs
	$(GO_GENERATE) ./internal/fbsemantic
libflux/core/src/semantic/flatbuffers/semantic_generated.rs: internal/fbsemantic/semantic.fbs
	flatc --rust -o libflux/core/src/semantic/flatbuffers internal/fbsemantic/semantic.fbs && rustfmt $@
libflux/go/libflux/buildinfo.gen.go: $(LIBFLUX_GENERATED_TARGETS)
	$(GO_GENERATE) ./libflux/go/libflux

# Force a second expansion to happen so the call to go_deps works correctly.
.SECONDEXPANSION:
ast/asttest/cmpopts.go: ast/ast.go ast/asttest/gen.go $$(call go_deps,./internal/cmd/cmpgen)
	$(GO_GENERATE) ./ast/asttest

stdlib/packages.go: $(STDLIB_SOURCES) libflux-go internal/fbsemantic/semantic_generated.go
	$(GO_GENERATE) ./stdlib

libflux: $(LIBFLUX_GENERATED_TARGETS)
	cd libflux && $(CARGO) build $(CARGO_ARGS)

build: libflux-go
	$(GO_BUILD) ./...

clean:
	rm -rf bin
	cd libflux && $(CARGO) clean && rm -rf pkg
	cd libflux/c && $(MAKE) clean

cleangenerate:
	rm -rf $(GENERATED_TARGETS)

fmt: $(SOURCES_NO_VENDOR)
	go fmt ./...
	cd libflux; $(CARGO) fmt

checkfmt:
	./etc/checkfmt.sh

tidy:
	GO111MODULE=on go mod tidy

checktidy:
	./etc/checktidy.sh

checkgenerate:
	./etc/checkgenerate.sh

# Run this in two passes to to keep memory usage down. As of this commit,
# running on everything (./...) uses just over 4G of memory. Breaking stdlib
# out keeps memory under 3G.
staticcheck:
	GO111MODULE=on go mod vendor # staticcheck looks in vendor for dependencies.
	GO111MODULE=on ./gotool.sh honnef.co/go/tools/cmd/staticcheck \
		`go list ./... | grep -v '\/flux\/stdlib\>'`
	GO111MODULE=on ./gotool.sh honnef.co/go/tools/cmd/staticcheck ./stdlib/...

test: test-go test-rust

test-go: libflux-go
	$(GO_TEST) $(GO_TEST_FLAGS) ./...

test-rust:
	cd libflux && $(CARGO) test $(CARGO_ARGS) && $(CARGO) clippy $(CARGO_ARGS) -- -Dclippy::all

test-race: libflux-go
	$(GO_TEST) -race -count=1 ./...

test-bench: libflux-go
	$(GO_TEST) -run=NONE -bench=. -benchtime=1x ./...
	cd libflux && $(CARGO) bench

vet: libflux-go
	$(GO_VET) ./...

bench: libflux-go
	$(GO_TEST) -bench=. -run=^$$ ./...

# This requires ragel 7.0.1.
libflux/core/src/scanner/rust/scanner.rs: libflux/core/src/scanner/rust/scanner.rl
	ragel-rust -I libflux/core/src/scanner -o $@ $<
	rm libflux/core/src/scanner/rust/scanner.ri

# If you see the error:
#    ragel: -C is an invalid argument
# from this command, it means you have replaced ragel 6 with ragel 7. Instead,
# install ragel 7 to a unique location and put it on your path *after* ragel 6.
# This way the ragel 6 binary hides the ragel 7 binary, but ragel-rust from
# ragel 7 is still available. See Dockerfile_build for an example.
libflux/scanner.c: libflux/core/src/scanner/scanner.rl
	ragel -C -o libflux/scanner.c libflux/core/src/scanner/scanner.rl

# This target generates a file that forces the go libflux wrapper
# to recompile which forces pkg-config to run again.
libflux-go: $(LIBFLUX_GENERATED_TARGETS)
	$(GO_GENERATE) ./libflux/go/libflux

libflux-wasm:
	cd libflux/flux && CC=clang AR=llvm-ar wasm-pack build --scope influxdata --dev

clean-wasm:
	rm -rf libflux/flux/pkg

build-wasm:
	cd libflux/flux && CC=clang AR=llvm-ar wasm-pack build -t nodejs --scope influxdata

publish-wasm: clean-wasm build-wasm
	cd libflux/flux/pkg && npm publish --access public

test-valgrind: libflux
	cd libflux/c && $(MAKE) test-valgrind

# Build the set of supported cross-compiled binaries
test-release: Dockerfile_build
	docker build -t flux-release -f Dockerfile_build .
	docker run --rm -it -v "$(PWD):/home/builder/src" flux-release /bin/sh -c "\
		cd src/ &&\
	    go build -o /go/bin/pkg-config github.com/influxdata/pkg-config &&\
		./gotool.sh github.com/goreleaser/goreleaser release --rm-dist --snapshot"
  
.PHONY: generate \
	clean \
	cleangenerate \
	build \
	default \
	libflux \
	libflux-go \
	libflux-wasm \
	clean-wasm \
	build-wasm \
	publish-wasm \
	fmt \
	checkfmt \
	tidy \
	checktidy \
	checkgenerate \
	staticcheck \
	test \
	test-go \
	test-rust \
	test-race \
	test-bench \
	test-valgrind \
	vet \
	bench \
	checkfmt \
	release
