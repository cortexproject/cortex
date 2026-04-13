# Flaky Test: TestRuler_rules_limit

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Race condition in alert state. The test expects `"state":"unknown"` for an alerting rule but gets `"state":"inactive"`. The ruler evaluates rules asynchronously after startup, and under the race detector the alert state transitions from "unknown" to "inactive" before the HTTP response is captured. This is a timing-dependent assertion.

## Occurrences

### 2026-04-13T20:43:11Z
- **Job**: [ci / test (amd64)](https://github.com/cortexproject/cortex/actions/runs/24365433548)
- **Package**: `github.com/cortexproject/cortex/pkg/ruler`
- **File**: `pkg/ruler/api_test.go:422`
- **Notes**: Failed on amd64 with `-race`. The diff is `"state":"unknown"` (expected) vs `"state":"inactive"` (actual).

<details><summary>Build logs</summary>

```
--- FAIL: TestRuler_rules_limit (0.06s)
    api_test.go:422:
        Error Trace:	/__w/cortex/cortex/pkg/ruler/api_test.go:422
        Error:      	Not equal:
                    	expected: ..."state":"unknown"...
                    	actual  : ..."state":"inactive"...
        Test:       	TestRuler_rules_limit
FAIL	github.com/cortexproject/cortex/pkg/ruler	43.279s
```

</details>
