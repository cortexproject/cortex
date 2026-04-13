# Flaky Test: TestDistributorQuerier_QueryIngestersWithinBoundary

**Status**: Fixed
**Occurrences**: 2
**Root Cause**: Timing-sensitive test. `now` is captured at test setup (line 561) via `time.Now()`, but the code under test in `newDistributorQueryable` also calls `time.Now()` internally to compute the lookback boundary. Under the race detector (slower execution), enough wall-clock time elapses between setup and execution that the internal boundary shifts. The "maxT well after lookback boundary" subtest uses only a 10-second margin (`now.Add(-lookback + 10*time.Second)`), which is not enough buffer when the race detector slows execution. The result is that `queryMaxT` falls before the internally-computed boundary, the query is skipped, and `distributor.Calls` is empty instead of having 1 call.

## Occurrences

### 2026-04-12T19:09:52Z
- **Job**: [ci / test (amd64)](https://github.com/cortexproject/cortex/actions/runs/24314068155)
- **Package**: `github.com/cortexproject/cortex/pkg/querier`
- **File**: `pkg/querier/distributor_queryable_test.go:638`
- **Subtest**: `maxT_well_after_lookback_boundary`
- **Notes**: Passed on arm64, passed on test-no-race (amd64), failed only under `-race` on amd64.

<details><summary>Build logs</summary>

```
--- FAIL: TestDistributorQuerier_QueryIngestersWithinBoundary (0.00s)
    --- FAIL: TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary (0.01s)
        distributor_queryable_test.go:638:
            	Error Trace:	/__w/cortex/cortex/pkg/querier/distributor_queryable_test.go:638
            	Error:      	"[]" should have 1 item(s), but has 0
            	Test:       	TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary
            	Messages:   	should manipulate when maxT is well after boundary
FAIL
FAIL	github.com/cortexproject/cortex/pkg/querier	34.451s
```

</details>

### 2026-04-12T20:32:32Z
- **Job**: [ci / test (amd64)](https://github.com/cortexproject/cortex/actions/runs/24315645679)
- **Package**: `github.com/cortexproject/cortex/pkg/querier`
- **File**: `pkg/querier/distributor_queryable_test.go:638`
- **Subtest**: `maxT_well_after_lookback_boundary`
- **Notes**: Same failure, again on amd64 with `-race`. Passed on arm64 and test-no-race.

<details><summary>Build logs</summary>

```
--- FAIL: TestDistributorQuerier_QueryIngestersWithinBoundary (0.00s)
    --- FAIL: TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary (0.00s)
        distributor_queryable_test.go:638:
            	Error Trace:	/__w/cortex/cortex/pkg/querier/distributor_queryable_test.go:638
            	Error:      	"[]" should have 1 item(s), but has 0
            	Test:       	TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary
            	Messages:   	should manipulate when maxT is well after boundary
FAIL
FAIL	github.com/cortexproject/cortex/pkg/querier	33.505s
```

</details>