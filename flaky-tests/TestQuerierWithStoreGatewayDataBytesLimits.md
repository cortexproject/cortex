# Flaky Test: TestQuerierWithStoreGatewayDataBytesLimits

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Transient Docker infrastructure failure. Multiple containers vanished mid-test (`e2e-cortex-test-consul`, `e2e-cortex-test-distributor`, `e2e-cortex-test-minio-9000`, `e2e-cortex-test-store-gateway`, `e2e-cortex-test-ingester`, `e2e-cortex-test-querier`). Same infrastructure instability pattern as TestQuerierWithBlocksStorageRunningInSingleBinaryMode. The test passed on arm64 in the same run and on both arches in runs #1-#4.

## Occurrences

### 2026-04-12T21:37:26Z
- **Job**: [ci / integration (ubuntu-24.04, amd64, integration_querier)](https://github.com/cortexproject/cortex/actions/runs/24316771541)
- **Package**: `github.com/cortexproject/cortex/integration`
- **File**: `integration/querier_test.go:565`
- **Notes**: Widespread Docker container disappearance. All `e2e-cortex-test-*` containers returned "No such container" errors.

<details><summary>Build logs</summary>

```
21:34:11 Error response from daemon: No such container: e2e-cortex-test-distributor
21:34:11 Error response from daemon: No such container: e2e-cortex-test-minio-9000
21:34:11 Error response from daemon: No such container: e2e-cortex-test-store-gateway
21:34:11 Error response from daemon: No such container: e2e-cortex-test-ingester
21:34:11 Error response from daemon: No such container: e2e-cortex-test-querier
21:34:19 Error response from daemon: No such container: e2e-cortex-test-consul
        Error Trace:	/home/runner/work/cortex/cortex/integration/querier_test.go:565
        Error:      	Not equal:
--- FAIL: TestQuerierWithStoreGatewayDataBytesLimits (10.69s)
FAIL	github.com/cortexproject/cortex/integration	202.827s
```

</details>
