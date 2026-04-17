# Flaky Test: TestIngesterMetadata

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: `WaitSumMetricsWithOptions` timed out waiting for ingester metrics at `ingester_metadata_test.go:48`. The ingester service didn't report expected metrics in time on arm64. Likely a slow startup or metric registration race in the integration test environment.

## Occurrences

### 2026-04-17T18:02:58Z
- **Job**: [ci / integration (ubuntu-24.04-arm, arm64, requires_docker)](https://github.com/cortexproject/cortex/actions/runs/24578865123)
- **Package**: `github.com/cortexproject/cortex/integration`
- **File**: `integration/ingester_metadata_test.go:48`
- **Notes**: Timeout in WaitSumMetricsWithOptions on arm64.

<details><summary>Build logs</summary>

```
        Error Trace:	/home/runner/work/cortex/cortex/integration/ingester_metadata_test.go:48
        Error:      	Received unexpected error:
            github.com/cortexproject/cortex/integration/e2e.(*HTTPService).WaitSumMetricsWithOptions
                /home/runner/work/cortex/cortex/integration/e2e/service.go:611
            github.com/cortexproject/cortex/integration.TestIngesterMetadata
                /home/runner/work/cortex/cortex/integration/ingester_metadata_test.go:48
        Test:       	TestIngesterMetadata
--- FAIL: TestIngesterMetadata (4.88s)
FAIL	github.com/cortexproject/cortex/integration	523.768s
```

</details>
