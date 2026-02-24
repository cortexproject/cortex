# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Project Overview

Cortex is a horizontally scalable, highly available, multi-tenant, long-term storage solution for Prometheus metrics. It uses a microservices architecture with components that can run as separate processes or as a single binary.

## Build Commands

```bash
make                           # Build all (runs in Docker container by default)
make BUILD_IN_CONTAINER=false  # Build locally without Docker
make exes                      # Build binaries only
make protos                    # Generate protobuf files
make lint                      # Run all linters (golangci-lint, misspell, etc.)
make doc                       # Generate config documentation (run after changing flags/config)
make ./cmd/cortex/.uptodate    # Build Cortex Docker image for integration tests
```

## Vendored Dependencies

Go modules are vendored in the `vendor/` folder. When upgrading a dependency or component:

```bash
go get github.com/some/dependency@version  # Update go.mod
go mod vendor                               # Sync vendor folder
go mod tidy                                 # Clean up go.mod/go.sum
```

**Important**: Always check the `vendor/` folder for upstream library code (e.g., `vendor/github.com/prometheus/alertmanager/` for Alertmanager internals). Do not modify vendored code directly.

## Testing

### Unit Tests

```bash
go test -timeout 2400s -tags "netgo slicelabels" ./...          # Run tests with CI configuration
```

### Integration Tests

Integration tests require Docker and the Cortex image to be built first:

```bash
make ./cmd/cortex/.uptodate    # Build Cortex Docker image first

# Run all integration tests
go test -v -tags=integration,requires_docker,integration_alertmanager,integration_memberlist,integration_querier,integration_ruler,integration_query_fuzz ./integration/...

# Run a specific integration test
go test -v -tags=integration,integration_ruler -timeout 2400s -count=1 ./integration/... -run "^TestRulerAPISharding$"
```

Environment variables for integration tests:

- `CORTEX_IMAGE` - Docker image to test (default: `quay.io/cortexproject/cortex:latest`)
- `E2E_TEMP_DIR` - Directory for temporary test files

## Code Formatting

Use goimports with Cortex-specific import grouping:

```bash
goimports -local github.com/cortexproject/cortex -w ./path/to/file.go
```

Import order: stdlib, third-party packages, internal Cortex packages (separated by blank lines).

## Architecture

### Write Path

- **Distributor** (stateless) - Receives samples via remote write, validates, distributes to ingesters using consistent hashing
- **Ingester** (semi-stateful) - Stores samples in memory, periodically flushes to long-term storage (TSDB blocks)

### Read Path

- **Querier** (stateless) - Executes PromQL queries across ingesters and long-term storage
- **Query Frontend** (optional, stateless) - Query caching, splitting, and queueing
- **Query Scheduler** (optional, stateless) - Moves queue from frontend for independent scaling

### Storage

- **Compactor** (stateless) - Compacts TSDB blocks in object storage
- **Store Gateway** (semi-stateful) - Queries blocks from object storage

### Optional Services

- **Ruler** - Executes recording rules and alerts
- **Alertmanager** - Multi-tenant alert routing
- **Configs API** - Configuration management

### Key Patterns

- **Hash Ring** - Consistent hashing via Consul, Etcd, or memberlist gossip for data distribution
- **Multi-tenancy** - Tenant isolation via `X-Scope-OrgID` header
- **Blocks Storage** - TSDB-based storage with 2-hour block ranges, stored in S3/GCS/Azure/Swift

### Main Entry Points

- `cmd/cortex/main.go` - Main Cortex binary
- `pkg/cortex/cortex.go` - Service orchestration and configuration

## Code Conventions

- **No global variables** - Use dependency injection
- **Metrics**: Register with `promauto.With(reg)`, never use global prometheus registerer
- **Config naming**: YAML uses `snake_case`, CLI flags use `kebab-case`
- **Logging**: Use `github.com/go-kit/log` (not `github.com/go-kit/kit/log`)

## PR Requirements

- Sign commits with DCO: `git commit -s -m "message"`
- Run `make doc` if config/flags changed
- Include CHANGELOG entry for user-facing changes
