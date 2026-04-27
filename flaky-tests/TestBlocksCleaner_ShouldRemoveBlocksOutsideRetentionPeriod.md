# Flaky Test: TestBlocksCleaner_ShouldRemoveBlocksOutsideRetentionPeriod

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Multiple subtests failed on arm64 without `-race`. Errors at `blocks_cleaner_test.go:699` ("Not equal") and `blocks_cleaner_test.go:829`/`:866` ("Received unexpected error"). Likely timing-dependent block cleanup assertions that assume synchronous completion.

## Occurrences

### 2026-04-13T21:00:49Z
- **Job**: [ci / test-no-race (arm64)](https://github.com/cortexproject/cortex/actions/runs/24366476213)
- **Package**: `github.com/cortexproject/cortex/pkg/compactor`
- **File**: `pkg/compactor/blocks_cleaner_test.go:699, :829, :866`
- **Notes**: Failed on arm64 without `-race`. Passed on amd64 (both race and no-race) and arm64 with `-race`.

<details><summary>Build logs</summary>

```
--- FAIL: TestBlocksCleaner_ShouldRemoveBlocksOutsideRetentionPeriod (0.47s)
        Error Trace:	/__w/cortex/cortex/pkg/compactor/blocks_cleaner_test.go:699
        Error:      	Not equal:
        Error Trace:	/__w/cortex/cortex/pkg/compactor/blocks_cleaner_test.go:829
        Error:      	Received unexpected error:
        Error Trace:	/__w/cortex/cortex/pkg/compactor/blocks_cleaner_test.go:699
        Error:      	Not equal:
        Error Trace:	/__w/cortex/cortex/pkg/compactor/blocks_cleaner_test.go:866
        Error:      	Received unexpected error:
FAIL	github.com/cortexproject/cortex/pkg/compactor	90.399s
```

</details>
