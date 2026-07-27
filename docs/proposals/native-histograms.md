# Native Histogram Support for All Production Metrics

## Problem

Cortex exposes ~56 histogram metrics across its components. Native histograms (introduced in Prometheus 2.40) offer significant advantages over classic histograms:

1. **Higher resolution** — Exponential bucket boundaries adapt to observed values, eliminating the need to pre-define bucket ranges.
2. **Lower storage cost** — Native histograms typically use fewer time series than classic histograms with many buckets.
3. **Better aggregation** — Native histograms can be merged across instances without information loss from mismatched bucket boundaries.

Previously, ~11 histograms were converted to dual-format (classic + native). The remaining ~45 production histograms were still classic-only, providing an inconsistent experience for operators who want to adopt native histograms.

## Design

### Dual-Format Histograms

All production histograms are configured as dual-format by adding three fields to each `prometheus.HistogramOpts`:

```go
NativeHistogramBucketFactor:     1.1,
NativeHistogramMaxBucketNumber:  100,
NativeHistogramMinResetDuration: time.Hour,
```

This means:
- **Classic scrapes** continue to work unchanged — the histogram exposes classic buckets as before.
- **Native-aware scrapes** (using `Accept: application/vnd.google.protobuf`) receive native histogram data instead.
- No behavioral change for existing deployments until they opt in to native histogram scraping.

### Configuration Values

| Field | Value | Rationale |
|-------|-------|-----------|
| `NativeHistogramBucketFactor` | 1.1 | ~10% relative resolution per bucket, providing good precision without excessive bucket count. Matches the pattern already established in the codebase. |
| `NativeHistogramMaxBucketNumber` | 100 | Upper bound on bucket count to prevent cardinality explosion from pathological distributions. |
| `NativeHistogramMinResetDuration` | `time.Hour` | Prevents premature schema resets during transient spikes, reducing unnecessary churn. |

These values match the existing pattern used by the ~11 histograms already converted (e.g., in `pkg/distributor`, `pkg/ingester`).

### Affected Components

| Component | Histograms |
|-----------|-----------|
| Ingester | 8 (WAL replay, appender add/commit, queried samples/exemplars/series/chunks) |
| Querier | 7 (store gateway client, instances hit, refetches, blocks scan, tenant federation) |
| Cache | 4 (instrumented cache value size/duration, memcached, redis) |
| API | 3 (request duration, message sizes) |
| Distributor | 2 (query duration, labels per sample) |
| Compactor | 2 (meta sync duration, garbage collection duration) |
| Storage/TSDB | 5 (bucket index load, multilevel cache fetch/backfill) |
| Store Gateway | 1 (blocks sync) |
| Frontend | 2 (queue duration, retries) |
| Scheduler | 1 (queue duration) |
| Alertmanager | 2 (client request duration, initial sync duration) |
| Ring/KV | 4 (KV request duration, consul, dynamodb, lifecycler shutdown) |
| Ruler | 2 (client pool request duration, frontend client pool) |
| HA Tracker | 1 (change propagation time) |
| Configs | 2 (client request duration, database request duration) |
| Parquet Converter | 1 (block conversion delay) |

### Backward Compatibility

This change is fully backward compatible:

- **No metric name changes** — all existing metric names, labels, and bucket boundaries are preserved.
- **No scrape format change** — classic format is served unless the scraper explicitly requests native histograms via content negotiation.
- **No configuration required** — dual-format is enabled automatically; operators opt in to native scraping at the Prometheus/agent level.

## Migration Path

1. **Deploy updated Cortex** — histograms begin emitting dual-format data. No observable change for classic scrapers.
2. **Enable native histogram ingestion** in Prometheus (or compatible agents) by configuring `scrape_protocols` to include `PrometheusProto`.
3. **Update dashboards/alerts** — native histograms use the base metric name (without `_bucket`/`_sum`/`_count` suffixes). For example:

   ```promql
   # Classic histogram query:
   histogram_quantile(0.99, rate(cortex_ingester_tsdb_appender_commit_duration_seconds_bucket[5m]))

   # Native histogram query (uses base name directly):
   histogram_quantile(0.99, rate(cortex_ingester_tsdb_appender_commit_duration_seconds[5m]))
   ```

   Native histograms also enable new PromQL functions not available with classic histograms: `histogram_avg()`, `histogram_fraction()`, `histogram_stddev()`, and `histogram_stdvar()`.

## Alternatives Considered

### Native-only (no classic buckets)

Rejected because it would break existing dashboards and alerts that rely on classic bucket boundaries. Dual-format ensures zero disruption.

### Per-histogram configuration

Rejected because uniform settings simplify operations and the chosen values (1.1 factor, 100 max buckets, 1h min reset) are broadly suitable for all Cortex histograms. Operators who need different settings can override at the scrape level.
