# Flaky Test: TestQuerierWithBlocksStorageLimits

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: The test expects HTTP 422 (Unprocessable Entity) for a query exceeding limits but received HTTP 500 (Internal Server Error) instead. This is likely a race where the store-gateway or querier hasn't fully initialized its limit-checking path, causing the error to surface as a generic 500 rather than the expected 422. Failed on arm64.

## Occurrences

### 2026-04-16T18:18:08Z
- **Job**: [ci / integration (ubuntu-24.04-arm, arm64, integration_querier)](https://github.com/cortexproject/cortex/actions/runs/24526116820)
- **Package**: `github.com/cortexproject/cortex/integration`
- **File**: `integration/querier_test.go:468`
- **Notes**: Expected 422, got 500. Passed on amd64 in the same run.

<details><summary>Build logs</summary>

```
        Error Trace:	/home/runner/work/cortex/cortex/integration/querier_test.go:468
        Error:      	Not equal:
                    	expected: 422
                    	actual  : 500
        Test:       	TestQuerierWithBlocksStorageLimits
--- FAIL: TestQuerierWithBlocksStorageLimits (11.00s)
FAIL	github.com/cortexproject/cortex/integration	193.371s
```

</details>
