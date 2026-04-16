# Flaky Test Audit Log

This file tracks every CI run on the `flaky-test-audit` branch. Any test failure on this branch is a flaky test since no test logic has been modified from `master`.

| Timestamp | Result | Details | CI Job |
|-----------|--------|---------|--------|
| 2026-04-12T19:09:52Z | FLAKY | TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary (pkg/querier) | [ci run 24314068155](https://github.com/cortexproject/cortex/actions/runs/24314068155) |
| 2026-04-12T19:47:00Z | PASS | No flaky tests detected | [ci run 24314518781](https://github.com/cortexproject/cortex/actions/runs/24314518781) |
| 2026-04-12T20:23:09Z | FLAKY | TestQueueConcurrency (pkg/scheduler/queue) — timed out after 30m on arm64 | [ci run 24314927948](https://github.com/cortexproject/cortex/actions/runs/24314927948) |
| 2026-04-12T20:32:32Z | FLAKY | TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary (pkg/querier) — occurrence #2 | [ci run 24315645679](https://github.com/cortexproject/cortex/actions/runs/24315645679) |
| 2026-04-12T20:49:56Z | FLAKY | TestQuerierWithBlocksStorageRunningInSingleBinaryMode (integration/querier, arm64) — Docker container vanished; TestQueueConcurrency timeout (arm64) — already skipped | [ci run 24316060467](https://github.com/cortexproject/cortex/actions/runs/24316060467) |
| 2026-04-12T21:22:00Z | PASS | No flaky tests detected (with skips applied) | [ci run 24316099145](https://github.com/cortexproject/cortex/actions/runs/24316099145) |
| 2026-04-12T21:37:26Z | FLAKY | TestQuerierWithStoreGatewayDataBytesLimits (integration/querier, amd64) — Docker containers vanished | [ci run 24316771541](https://github.com/cortexproject/cortex/actions/runs/24316771541) |
| 2026-04-13T20:43:11Z | FLAKY | TestRuler_rules_limit (pkg/ruler) — alert state race; TestParquetFuzz (integration/query_fuzz) — fuzz non-determinism; requires_docker — Docker install failure | [ci run 24365433548](https://github.com/cortexproject/cortex/actions/runs/24365433548) |
| 2026-04-13T21:00:49Z | FLAKY | TestBlocksCleaner_ShouldRemoveBlocksOutsideRetentionPeriod (pkg/compactor, arm64); TestRuler_rules (pkg/ruler, configs-db) — alert state race | [ci run 24366476213](https://github.com/cortexproject/cortex/actions/runs/24366476213) |
| 2026-04-16T17:27:44Z | INFRA | integration_overrides Docker install failed — Docker Hub rate limit (toomanyrequests). All tests passed. | [ci run 24524134384](https://github.com/cortexproject/cortex/actions/runs/24524134384) |
| 2026-04-16T17:50:32Z | INFRA | requires_docker Docker install failed — Docker Hub rate limit (toomanyrequests). All tests passed. | [ci run 24525138727](https://github.com/cortexproject/cortex/actions/runs/24525138727) |
| 2026-04-16T18:18:08Z | FLAKY | TestQuerierWithBlocksStorageLimits (integration/querier, arm64) — expected 422 got 500 | [ci run 24526116820](https://github.com/cortexproject/cortex/actions/runs/24526116820) |
| 2026-04-16T18:52:00Z | PASS | All jobs passed, no flaky tests detected | [ci run 24527178425](https://github.com/cortexproject/cortex/actions/runs/24527178425) |
| 2026-04-16T19:15:00Z | PASS | All jobs passed, no flaky tests detected (2nd consecutive clean run) | [ci run 24528187016](https://github.com/cortexproject/cortex/actions/runs/24528187016) |
| 2026-04-16T19:38:00Z | PASS | All jobs passed (3rd consecutive clean run) | [ci run 24529191249](https://github.com/cortexproject/cortex/actions/runs/24529191249) |
