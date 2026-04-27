# Flaky Test: TestQuerierWithBlocksStorageRunningInSingleBinaryMode

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Transient Docker infrastructure failure. The Consul container (`e2e-cortex-test-consul`) disappeared mid-test, causing `Error response from daemon: No such container: e2e-cortex-test-consul`. This is a CI environment issue, not a code bug. The test passed on amd64 in the same run and passed on arm64 in run #6.

## Occurrences

### 2026-04-12T21:00:08Z
- **Job**: [ci / integration (ubuntu-24.04-arm, arm64, integration_querier)](https://github.com/cortexproject/cortex/actions/runs/24316060467)
- **Package**: `github.com/cortexproject/cortex/integration`
- **File**: `integration/querier_test.go:217`
- **Subtest**: `[TSDB]_blocks_sharding_enabled,_redis_index_cache,_bucket_index_enabled,thanosEngine=false`
- **Notes**: Passed on amd64 in the same run. Docker container `e2e-cortex-test-consul` vanished during test execution.

<details><summary>Build logs</summary>

```
20:57:39 Error response from daemon: No such container: e2e-cortex-test-consul
        Error Trace:	/home/runner/work/cortex/cortex/integration/querier_test.go:217
        Error:      	Received unexpected error:
--- FAIL: TestQuerierWithBlocksStorageRunningInSingleBinaryMode (69.50s)
    --- FAIL: TestQuerierWithBlocksStorageRunningInSingleBinaryMode/[TSDB]_blocks_sharding_enabled,_redis_index_cache,_bucket_index_enabled,thanosEngine=false (34.33s)
FAIL
FAIL	github.com/cortexproject/cortex/integration	279.595s
```

</details>
