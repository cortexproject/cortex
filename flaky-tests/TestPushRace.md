# Flaky Test: TestPushRace

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Race condition in concurrent push operations. The test pushes samples concurrently and one goroutine receives an "out of bounds" error with `timestamp=1970-01-01T00:00:00Z` (zero value). This suggests a race where a sample's timestamp is read before it's fully initialized, or TSDB head boundaries shift between goroutines. Failed on arm64 without `-race`.

## Occurrences

### 2026-04-16T22:34:58Z
- **Job**: [ci / test-no-race (arm64)](https://github.com/cortexproject/cortex/actions/runs/24537394343)
- **Package**: `github.com/cortexproject/cortex/pkg/ingester`
- **File**: `pkg/ingester/ingester_test.go:719`
- **Notes**: Error: `rpc error: code = Code(400) desc = user=1: err: out of bounds. timestamp=1970-01-01T00:00:00Z`

<details><summary>Build logs</summary>

```
--- FAIL: TestPushRace (0.12s)
    ingester_test.go:719:
        Error Trace:	/__w/cortex/cortex/pkg/ingester/ingester_test.go:719
                    	/usr/local/go/src/runtime/asm_arm64.s:1268
        Error:      	Received unexpected error:
                    	rpc error: code = Code(400) desc = user=1: err: out of bounds. timestamp=1970-01-01T00:00:00Z, series={__name__="foo", k="10", userId="1"}
        Test:       	TestPushRace
FAIL
FAIL	github.com/cortexproject/cortex/pkg/ingester	22.814s
```

</details>
