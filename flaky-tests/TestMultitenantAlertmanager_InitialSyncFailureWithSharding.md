# Flaky Test: TestMultitenantAlertmanager_InitialSyncFailureWithSharding

**Status**: Skipped
**Occurrences**: 1
**Root Cause**: Ring membership race condition. The test checks that initial sync failure is properly handled with sharding enabled, but the ring instance goes missing and gets re-added (`instance missing in the ring, adding it back`). The assertion at line 1778 (`Should be false`) fails because the ring state hasn't stabilized in time on arm64 with `-race`.

## Occurrences

### 2026-04-17T17:25:08Z
- **Job**: [ci / test (arm64)](https://github.com/cortexproject/cortex/actions/runs/24577873547)
- **Package**: `github.com/cortexproject/cortex/pkg/alertmanager`
- **File**: `pkg/alertmanager/multitenant_test.go:1778`
- **Notes**: Ring warnings about instance missing. Failed on arm64 with `-race`.

<details><summary>Build logs</summary>

```
level=warn component=MultiTenantAlertmanager msg="instance missing in the ring, adding it back" ring=alertmanager
level=warn component=MultiTenantAlertmanager msg="instance missing in the ring, adding it back" ring=alertmanager
--- FAIL: TestMultitenantAlertmanager_InitialSyncFailureWithSharding (1.76s)
    multitenant_test.go:1778:
        Error Trace:	/__w/cortex/cortex/pkg/alertmanager/multitenant_test.go:1778
        Error:      	Should be false
        Test:       	TestMultitenantAlertmanager_InitialSyncFailureWithSharding
FAIL
FAIL	github.com/cortexproject/cortex/pkg/alertmanager	50.012s
```

</details>
