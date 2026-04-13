# Flaky Test: TestParquetFuzz

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Fuzz test with non-deterministic query generation. The test generates random PromQL queries and compares Cortex results against Prometheus. 1 out of N randomly generated test cases failed, likely due to floating point precision differences or edge cases in query evaluation. This is inherently non-deterministic.

## Occurrences

### 2026-04-13T20:44:25Z
- **Job**: [ci / integration (ubuntu-24.04, amd64, integration_query_fuzz)](https://github.com/cortexproject/cortex/actions/runs/24365433548)
- **Package**: `github.com/cortexproject/cortex/integration`
- **File**: `integration/parquet_querier_test.go:176`
- **Notes**: "1 test cases failed" out of the fuzz suite. Non-deterministic by nature.

<details><summary>Build logs</summary>

```
        Error Trace:	/home/runner/work/cortex/cortex/integration/parquet_querier_test.go:176
        Error:      	finished query fuzzing tests
        Test:       	TestParquetFuzz
        Messages:   	1 test cases failed
--- FAIL: TestParquetFuzz (24.12s)
FAIL	github.com/cortexproject/cortex/integration	200.599s
```

</details>
