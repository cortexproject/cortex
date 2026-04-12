# Flaky Test Audit Log

This file tracks every CI run on the `flaky-test-audit` branch. Any test failure on this branch is a flaky test since no test logic has been modified from `master`.

| Timestamp | Result | Details | CI Job |
|-----------|--------|---------|--------|
| 2026-04-12T19:09:52Z | FLAKY | TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary (pkg/querier) | [ci run 24314068155](https://github.com/cortexproject/cortex/actions/runs/24314068155) |
| 2026-04-12T19:47:00Z | PASS | No flaky tests detected | [ci run 24314518781](https://github.com/cortexproject/cortex/actions/runs/24314518781) |
| 2026-04-12T20:23:09Z | FLAKY | TestQueueConcurrency (pkg/scheduler/queue) — timed out after 30m on arm64 | [ci run 24314927948](https://github.com/cortexproject/cortex/actions/runs/24314927948) |
| 2026-04-12T20:32:32Z | FLAKY | TestDistributorQuerier_QueryIngestersWithinBoundary/maxT_well_after_lookback_boundary (pkg/querier) — occurrence #2 | [ci run 24315645679](https://github.com/cortexproject/cortex/actions/runs/24315645679) |
