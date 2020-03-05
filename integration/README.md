# Cortex integration tests

## Supported environment variables

- `CORTEX_IMAGE`: Docker image used to run Cortex in integration tests (defaults to `quay.io/cortexproject/cortex:latest`)
- `CORTEX_CHECKOUT_DIR`: The absolute path of the Cortex repository local checkout (defaults to `$GOPATH/src/github.com/cortexproject/cortex`)

## Running

Integration tests have `integration` tag (`// +build integration` line followed by empty line on top of Go files), to avoid running them unintentionally, e.g. by `go test ./...` in main Cortex package.

To run integration tests, one needs to run `go test -tags=integration ./integration/...` (or just `go test -tags=integration ./...` to run unit tests as well).

## Owners

This package is used by other projects than Cortex. When modifying please ask @pracucci @bwplotka for review.
