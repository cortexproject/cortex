# Flaky Test: TestVerticalShardingFuzz

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Non-deterministic fuzz test. Generates random PromQL queries and compares results across sharded and non-sharded execution. Some randomly generated queries produce different results due to floating point precision or edge cases. Also accompanied by Docker container disappearance (`No such container: e2e-cortex-test-prometheus`).

## Occurrences

### 2026-04-16T22:19:31Z
- **Job**: [ci / integration (ubuntu-24.04-arm, arm64, integration_query_fuzz)](https://github.com/cortexproject/cortex/actions/runs/24536561113)
- **Package**: `github.com/cortexproject/cortex/integration`
- **File**: `integration/query_fuzz_test.go:1816`
- **Notes**: Failed on arm64. Passed on amd64 in the same run.

<details><summary>Build logs</summary>

```
        Error Trace:	/home/runner/work/cortex/cortex/integration/query_fuzz_test.go:1816
        Error:      	finished query fuzzing tests
--- FAIL: TestVerticalShardingFuzz (14.28s)
22:18:53 Error response from daemon: No such container: e2e-cortex-test-prometheus
FAIL	github.com/cortexproject/cortex/integration	184.302s
```

</details>
