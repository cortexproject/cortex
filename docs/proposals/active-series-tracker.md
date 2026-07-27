# Active Series Tracker

## Problem

AMP needs to monitor active series counts by configurable patterns (e.g., all series with `__name__=~"api_.*"`) for internal observability. The existing `LimitsPerLabelSet` feature is unsuitable because:

1. **No regex matching** — only supports exact `label=value` matching.
2. **Default partition side-effects** — adding labelset buckets reduces the default partition count.
3. **Coupled to limit enforcement** — designed for enforcing series limits, not pure monitoring.

## Requirements

- Track active series counts by configurable label matchers (including regex).
- Expose counts as Prometheus metrics on the ingester (internal only, not vended to customers).
- Configuration supports **per-tenant overrides** with a **default** fallback (same pattern as all other Limits fields).
- **Runtime hot-reloadable** via the existing runtime config file mechanism.
- **No limit enforcement** — purely observational.
- **No default partition** — unmatched series are simply not tracked.
- A series can match multiple tracker entries simultaneously.

## Design

### Configuration

Tracker config lives in the `Limits` struct, following the same per-tenant override pattern as `LimitsPerLabelSet`:

```yaml
# Default trackers (applied to all tenants without overrides)
limits:
  active_series_trackers:
    - name: api_metrics
      matchers: '{__name__=~"api_.*"}'

# Per-tenant overrides via runtime config
overrides:
  tenant-123:
    active_series_trackers:
      - name: api_metrics
        matchers: '{__name__=~"api_.*"}'
      - name: system_metrics
        matchers: '{__name__=~"node_.*|process_.*"}'
```

The `matchers` field uses standard PromQL matcher syntax parsed via `parser.ParseMetricSelector`.

### Runtime Reload

Tracker config is part of `Limits`, which is reloaded via the runtime config manager every `runtime-config.reload-period` (default 10s). Matchers are parsed and validated during YAML/JSON unmarshalling. Invalid configs are rejected (existing config stays active).

### Metrics

A new gauge metric emitted per ingester:

```
cortex_ingester_active_series_per_tracker{user="<tenant>", name="<tracker_name>"} <count>
```

### Matching Logic

On each active series metrics update tick (default 1min), for each tenant:
1. Read the tenant's tracker config via `i.limits.ActiveSeriesTrackers(userID)`
2. For each tracker, count active series whose labels satisfy all matchers
3. Emit the gauge metric

A series can match multiple trackers. Tenants without configured trackers emit no tracker metrics.

### Performance Considerations

- Matching runs once per update period (default 1min), not on every sample ingestion.
- The number of trackers is expected to be small (< 10).
- Compiled matchers are cached in the parsed Limits and only recompiled on config change.
