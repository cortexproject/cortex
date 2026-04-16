# Flaky Test: TestMinimizeSpreadTokenGenerator

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Floating point precision boundary. The test asserts that token spread distance error is <= 0.01, but the result was 0.01097 — barely over the threshold. The token generation algorithm involves randomness and the assertion threshold is too tight for non-deterministic outcomes. Failed on amd64 without `-race`.

## Occurrences

### 2026-04-16T21:27:42Z
- **Job**: [ci / test-no-race (amd64)](https://github.com/cortexproject/cortex/actions/runs/24534699120)
- **Package**: `github.com/cortexproject/cortex/pkg/ring`
- **File**: `pkg/ring/token_generator_test.go:202` (called from `:106`)
- **Notes**: Error was 0.01097, threshold is 0.01. Passed on arm64 and amd64 with `-race`.

<details><summary>Build logs</summary>

```
--- FAIL: TestMinimizeSpreadTokenGenerator (2.03s)
    token_generator_test.go:202:
        Error Trace:	/__w/cortex/cortex/pkg/ring/token_generator_test.go:202
                    	/__w/cortex/cortex/pkg/ring/token_generator_test.go:106
        Error:      	Condition failed!
        Test:       	TestMinimizeSpreadTokenGenerator
        Messages:   	[minimize-42-zone1] expected and real distance error is greater than 0.01 -> 0.010970902851567321[8.2595524e+07/8.3511723e+07]
FAIL
FAIL	github.com/cortexproject/cortex/pkg/ring	67.285s
```

</details>
