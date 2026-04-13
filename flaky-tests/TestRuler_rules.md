# Flaky Test: TestRuler_rules

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Same alert state race as TestRuler_rules_limit. The test expects `"state":"unknown"` for an alerting rule but gets `"state":"inactive"`. The ruler evaluates rules asynchronously after startup, and the alert state transitions before the HTTP response is captured. This manifested in the `integration-configs-db` job which runs the ruler tests with a database backend.

## Occurrences

### 2026-04-13T21:05:42Z
- **Job**: [ci / integration-configs-db (ubuntu-24.04, amd64)](https://github.com/cortexproject/cortex/actions/runs/24366476213)
- **Package**: `github.com/cortexproject/cortex/pkg/ruler`
- **File**: `pkg/ruler/api_test.go:306`
- **Notes**: Same `"state":"unknown"` vs `"state":"inactive"` pattern as TestRuler_rules_limit.

<details><summary>Build logs</summary>

```
        Error Trace:	/go/src/github.com/cortexproject/cortex/pkg/ruler/api_test.go:306
        Error:      	Not equal:
                    	expected: ..."state":"unknown"...
                    	actual  : ..."state":"inactive"...
--- FAIL: TestRuler_rules (0.05s)
```

</details>
