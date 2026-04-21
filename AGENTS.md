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

## Investigating CI Build Failures

When asked to investigate a CI build failure:

1. **Fetch job details** using `gh api repos/cortexproject/cortex/actions/runs/<run-id>/jobs` to identify failed jobs and steps. Note: `gh run view --log` only works after the entire run completes, not just individual jobs.
2. **Get annotations** using `gh api repos/cortexproject/cortex/check-runs/<job-id>/annotations` to surface error messages when full logs are unavailable.
3. **Fetch full logs** once the run completes using `gh run view <run-id> --job <job-id> --log`.
4. **Determine root cause** — distinguish between infrastructure failures (e.g., Docker Hub rate limits, runner issues) and code-related failures (e.g., test regressions). Use `git log` and `gh pr diff` to check if the failure relates to the PR's changes.
5. **For flaky/infrastructure issues**, use `git log` and `git log -p` to trace when the failing code was introduced and which PR added it.
6. **File GitHub issues** with the user's permission, including:
   - The full error output in a `<details>` block (since job links can expire)
   - Root cause analysis
   - Which PR introduced the issue (but do not assign or tag individuals without the user's approval)
   - Proposed solutions

## Related Policies

This file (`AGENTS.md`) provides technical guidance **to** AI coding agents working in this repository (build commands, architecture, conventions). For the policy governing **human use** of AI tools when preparing contributions, see [GENAI_POLICY.md](GENAI_POLICY.md).
