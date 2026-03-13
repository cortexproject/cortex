---
title: "Per-Tenant Cardinality API"
linkTitle: "Per-Tenant Cardinality API"
weight: 1
slug: per-tenant-cardinality-api
---

- Author: [Charlie Le](https://github.com/CharlieTLe)
- Date: March 2026
- Status: Draft

## Background

High-cardinality series is one of the most common operational challenges for Prometheus-based systems. When a tenant has too many active series, it can lead to increased resource usage in ingesters, slower queries, and ultimately hitting per-tenant series limits.

Currently, Cortex tenants lack visibility into which metrics, labels, and label-value pairs contribute the most series in ingesters. Without this information, debugging high-cardinality issues requires operators to inspect TSDB internals directly on ingester instances, which is impractical in a multi-tenant, distributed environment.

This proposal introduces a dedicated cardinality API for Cortex that works across both in-memory ingester data and long-term block storage.

## Goal

Expose per-tenant cardinality statistics via a REST API endpoint on the Cortex query path. The endpoint should:

1. Support two data sources: in-memory TSDB head data from ingesters and compacted blocks from long-term object storage via store gateways.
2. Aggregate statistics across all ingesters or store gateways that hold data for the requesting tenant.
3. Correctly account for replication factor when summing series counts from ingesters.
4. Respect multi-tenancy, ensuring tenants can only see their own data.

## Out of Scope

- **Automated cardinality limiting**: This is a read-only diagnostic endpoint; it does not enforce or suggest limits.
- **Cardinality reduction actions**: The endpoint reports statistics but does not provide mechanisms to drop or relabel series.

## Proposed Design

### Endpoint

```
GET <prometheus-http-prefix>/api/v1/cardinality?limit=N&source=head|blocks&start=T&end=T
```

The endpoint is also registered at `<legacy-http-prefix>/api/v1/cardinality`, following the pattern used by other querier endpoints.

- **Authentication**: Requires `X-Scope-OrgID` header (standard Cortex tenant authentication).
- **Query Parameters**:
  - `limit` (optional, default 10, max 512) - controls the number of top items returned per category. Values outside the `[1, 512]` range are rejected with HTTP 400.
  - `source` (optional, default `head`) - selects the data source. `head` queries ingester TSDB heads, `blocks` queries compacted blocks in long-term storage via store gateways. Invalid values are rejected with HTTP 400.
  - `start` (RFC3339 or Unix timestamp) - start of the time range to analyze. Required for `source=blocks`, not accepted for `source=head`.
  - `end` (RFC3339 or Unix timestamp) - end of the time range to analyze. Required for `source=blocks`, not accepted for `source=head`.
- **Time Range Behavior** (`source=blocks` only):
  - `start` and `end` are required. Only blocks whose time range overlaps with `[start, end]` are analyzed. A block is included if its `minTime < end` and its `maxTime > start`.
  - The requested time range (`end - start`) must not exceed the per-tenant `cardinality_max_query_range` limit (see [Per-Tenant Limits](#per-tenant-limits)).
  - `start` must be before `end`; inverted ranges are rejected with HTTP 400.

The head path does not accept `start`/`end` because the TSDB head cannot filter cardinality statistics by sub-range — it always returns stats for the full head. Rather than accept parameters that cannot be honored, the endpoint rejects them with HTTP 400.

### Error Responses

All error responses use the standard Prometheus API envelope:

```json
{"status": "error", "errorType": "bad_data", "error": "description of the problem"}
```

| Condition | HTTP Status | Error Message |
|---|---|---|
| Invalid `limit` (< 1, > 512, or non-integer) | 400 | `invalid limit: must be an integer between 1 and 512` |
| Invalid `source` (not `head` or `blocks`) | 400 | `invalid source: must be "head" or "blocks"` |
| `start`/`end` provided with `source=head` | 400 | `start and end parameters are not supported for source=head` |
| `start` or `end` missing with `source=blocks` | 400 | `start and end are required for source=blocks` |
| Malformed `start` or `end` | 400 | `invalid start/end: must be RFC3339 or Unix timestamp` |
| `start >= end` | 400 | `invalid time range: start must be before end` |
| Time range exceeds `cardinality_max_query_range` | 400 | `the query time range exceeds the limit (query length: %s, limit: %s)` |

### Architecture

The HTTP handler parses the `source` parameter and delegates to the appropriate backend. The endpoint is registered via `NewQuerierHandler` in `pkg/api/handlers.go` and does **not** go through the Query Frontend — it is served directly by the Querier. The Query Frontend's splitting, caching, and retry logic is designed for PromQL queries and does not apply to cardinality statistics. The Querier's own per-tenant concurrency limit provides sufficient request control (see [Per-Tenant Limits](#per-tenant-limits)).

#### Head Path (`source=head`)

The request flows through the Querier's HTTP handler, which delegates to the in-process Distributor for ingester fan-out:

```
Client → HTTP Handler (Querier) → In-process Distributor → gRPC Fan-out (Ingesters) → Aggregation (Distributor) → JSON Response
```

1. **HTTP Handler** (`CardinalityHandler` in `pkg/querier/cardinality_handler.go`): Registered via `NewQuerierHandler` in `pkg/api/handlers.go`. Parses the `limit` query parameter and calls the distributor's `Cardinality` method.
2. **Distributor Fan-out** (`Cardinality` in `pkg/distributor/distributor.go`): The Querier process holds an in-process Distributor instance (initialized via the `DistributorService` module). This instance uses `GetIngestersForMetadata` to discover all ingesters for the tenant, then sends a `CardinalityRequest` gRPC call to each ingester in the replication set with `MaxErrors = 0` — all ingesters must respond for the RF-based aggregation to be accurate. If any ingester in the tenant's replication set is unavailable, the request fails.
3. **Ingester** (`Cardinality` in `pkg/ingester/ingester.go`): Retrieves the tenant's TSDB head and calls `db.Head().Stats(labels.MetricName, limit)` to get cardinality statistics from the Prometheus TSDB library.
4. **Aggregation**: The distributor merges responses from all ingesters and returns the combined result.

**Rolling upgrade compatibility**: During rolling deployments, some ingesters may not yet support the `Cardinality` RPC. The distributor treats `Unimplemented` gRPC errors from old ingesters the same as any other error — since `MaxErrors = 0`, the request fails with an HTTP 500 indicating that not all ingesters support the cardinality API. This is acceptable during the upgrade window.

#### Blocks Path (`source=blocks`)

The request flows through the Querier's HTTP handler, which fans out to store gateways using the same `BlocksFinder` + `GetClientsFor` pattern used by `LabelNames`, `LabelValues`, and `Series`:

```
Client → HTTP Handler (Querier) → BlocksFinder (discover blocks) → GetClientsFor (route blocks to SGs) → gRPC Fan-out → Aggregation (Querier) → JSON Response
```

1. **HTTP Handler** (`CardinalityHandler` in `pkg/querier/cardinality_handler.go`): Parses `limit`, `start`, `end`, and `source=blocks`, then calls the blocks store's `Cardinality` method.
2. **Block Discovery**: The Querier uses `BlocksFinder.GetBlocks()` to discover all blocks for the tenant within the `[start, end]` time range, then calls `GetClientsFor()` to route each block to exactly one store gateway instance. Each block is sent to a single store gateway — there is no broadcast to all replicas.
3. **Store Gateway** (`Cardinality` in `pkg/storegateway/gateway.go`): Receives a request with specific block IDs. Locates the tenant's `BucketStore`, iterates over the specified blocks, and computes cardinality statistics from block indexes (see [Block Index Cardinality Computation](#block-index-cardinality-computation)).
4. **Consistency Check**: After receiving responses, the querier runs `BlocksConsistencyChecker.Check()` to detect missing blocks. If blocks are missing (e.g., a store gateway hasn't loaded a recently uploaded block), the querier retries those blocks on different store gateway replicas, up to 3 attempts. If blocks remain missing after all retries, the response includes partial results.
5. **Aggregation**: The querier merges responses from all store gateways and returns the combined result.

**Rolling upgrade compatibility**: During rolling deployments, store gateways that do not yet support the `Cardinality` RPC return `Unimplemented` errors. The querier retries affected blocks on other replicas. If no replica supports the RPC, those blocks are treated as missing and the response is partial.

### gRPC Definition

Shared protobuf messages are defined in `pkg/cortexpb/cardinality.proto` and imported by both the ingester and store gateway protos:

```protobuf
// pkg/cortexpb/cardinality.proto

message CardinalityStatItem {
  string name = 1;
  uint64 value = 2;
}
```

#### Ingester Service

A new `Cardinality` RPC is added to the Ingester service in `pkg/ingester/client/ingester.proto`:

```protobuf
rpc Cardinality(CardinalityRequest) returns (CardinalityResponse) {};

message CardinalityRequest {
  int32 limit = 1;
}

message CardinalityResponse {
  uint64 num_series = 1;
  repeated cortexpb.CardinalityStatItem series_count_by_metric_name = 2;
  repeated cortexpb.CardinalityStatItem label_value_count_by_label_name = 3;
  repeated cortexpb.CardinalityStatItem series_count_by_label_value_pair = 4;
}
```

#### Store Gateway Service

A new `Cardinality` RPC is added to the StoreGateway service in `pkg/storegateway/storegatewaypb/gateway.proto`:

```protobuf
rpc Cardinality(CardinalityRequest) returns (CardinalityResponse) {};

message CardinalityRequest {
  int32 limit = 1;
  int64 min_time = 2;
  int64 max_time = 3;
  repeated bytes block_ids = 4;
}

message CardinalityResponse {
  uint64 num_series = 1;
  repeated cortexpb.CardinalityStatItem series_count_by_metric_name = 2;
  repeated cortexpb.CardinalityStatItem label_value_count_by_label_name = 3;
  repeated cortexpb.CardinalityStatItem series_count_by_label_value_pair = 4;
}
```

The store gateway `CardinalityRequest` includes `min_time`, `max_time`, and `block_ids` so the store gateway can filter blocks server-side. This matches the pattern used by other store gateway RPCs (`SeriesRequest`, `LabelNamesRequest`) where the querier routes specific blocks to specific store gateways.

### Aggregation Logic

#### Head Path Aggregation

Because each series is replicated across multiple ingesters (controlled by the replication factor), the aggregation logic divides by the RF when merging responses. All ingesters must respond (`MaxErrors = 0`) for the RF division to be accurate.

| Field | Aggregation Strategy |
|---|---|
| `numSeries` | Sum across ingesters, divide by replication factor |
| `seriesCountByMetricName` | Sum per metric, divide by RF, return top N |
| `labelValueCountByLabelName` | Maximum per label (unique counts, not affected by replication) |
| `seriesCountByLabelValuePair` | Sum per pair, divide by RF, return top N |

The `topNStats` helper function handles the sort-and-truncate step: it divides values by the replication factor, sorts descending by value, and returns the top N items.

**Note on approximation**: The RF division is a best-effort approximation, matching the approach used by `UserStats`. It can undercount when ingesters are in non-ACTIVE states during ring changes, or when shuffle sharding with a lookback period causes uneven distribution. This is acceptable for a diagnostic endpoint — the goal is to identify the largest cardinality contributors, not to produce exact totals.

#### Blocks Path Aggregation

The blocks path uses `GetClientsFor` to route each block to exactly one store gateway instance. Since there is no broadcast to all replicas, **no RF division is applied** — each block's statistics are returned exactly once.

| Field | Aggregation Strategy |
|---|---|
| `numSeries` | Sum across store gateways (no RF division) |
| `seriesCountByMetricName` | Sum per metric, return top N |
| `labelValueCountByLabelName` | Maximum per label |
| `seriesCountByLabelValuePair` | Sum per pair, return top N |

**Note on block overlap**: Before compaction completes, a tenant may have multiple blocks covering the same time range. Series that appear in overlapping blocks are counted once per block they appear in, so the `numSeries` total may overcount compared to the true unique series count. In practice, with RF ingesters each uploading 2-hour blocks, a 24-hour query range before compaction could have up to `RF * 12` overlapping source blocks, making `numSeries` up to `RF`x the true value. The response includes an `approximated` field set to `true` when overlapping blocks are detected, so consumers know the results may be inflated. The top-N rankings remain useful regardless of overlap — the relative ordering of cardinality contributors is preserved.

### Block Index Cardinality Computation

The store gateway computes cardinality statistics from the block indexes already loaded for the tenant's `BucketStore`. Each block has an `indexheader.Reader` (memory-mapped binary index header) that provides cheap access to label metadata, and optionally a full `index.Reader` for posting list expansion.

The three cardinality dimensions have different cost profiles:

#### 1. Label Value Count by Label Name (Cheap)

This is computed entirely from the index header, with no object storage I/O:

```go
labelNames, _ := indexHeaderReader.LabelNames()
for _, name := range labelNames {
    values, _ := indexHeaderReader.LabelValues(name)
    // len(values) = number of distinct values for this label
}
```

The index header stores label name → label value → posting offset mappings in memory. Calling `LabelValues()` returns the distinct values directly. Across multiple blocks, the values are merged (set union) to produce the total distinct count per label.

#### 2. Series Count by Metric Name (Moderate)

To count the number of series per `__name__` value, we must determine the size of each posting list. Two approaches:

**Option A — Posting list expansion**: For each metric name, call `ExpandedPostings(ctx, "__name__", metricName)` on the full block index to get the posting list (series IDs). The list length equals the series count. This requires fetching posting list data from object storage.

**Option B — Posting offset estimation**: The index header stores the byte offset of each posting list in the index file. The byte length between consecutive posting offsets provides an estimate of the posting list size. Since posting lists are varint-encoded series IDs, the relationship between byte size and series count is approximately proportional. This avoids object storage I/O entirely but produces estimates rather than exact counts.

**Recommendation**: Use Option A (posting list expansion) for the `__name__` label only. The number of distinct metric names is typically bounded (hundreds to low thousands), making the cost manageable. Results should be cached per block since compacted blocks are immutable (see [Caching](#caching)).

#### 3. Series Count by Label-Value Pair (Expensive)

This requires expanding posting lists for every label=value combination, which is an order of magnitude more expensive than metric-name-only expansion. For a tenant with 100 label names and 1,000 values each, this means 100,000 posting list lookups.

**Recommendation**: This field is computed on-demand using Option A (posting list expansion). To bound the cost:
- Only expand posting lists for the top N label names by value count (already known from step 1).
- Within each label name, only expand posting lists for a bounded number of values.
- Apply the per-tenant `cardinality_query_timeout` (default 60s) so that very high-cardinality tenants get partial results rather than unbounded computation. When a timeout occurs, the response includes whatever results were computed before the deadline, with the `approximated` field set to `true`.

Results are cached per block (see [Caching](#caching)).

#### Block Selection

The store gateway filters blocks based on the required `start` and `end` parameters: a block is included only if its `minTime < end` and its `maxTime > start`. This scopes cardinality analysis to a specific time window, such as the last 24 hours or a particular incident period.

#### Caching

Compacted blocks are immutable — once a block is written to object storage, its contents never change. This means cardinality statistics computed from a block's index can be cached indefinitely (until the block is deleted by the compactor).

Each store gateway maintains a per-block cardinality cache. The cache stores the full (unlimited) result per block ULID, keyed by `(block ULID)`. The `limit` (top-N truncation) is applied at response time from the cached full result. This maximizes cache hit rates — a `limit=10` request followed by a `limit=20` request reuses the same cache entry.

The cache is populated on first request and invalidated when blocks are removed during compaction syncs. A safety TTL of 24 hours is applied as defense-in-depth for entries that survive block deletion.

**Cache hit/miss metrics**: The cache exposes `cortex_cardinality_cache_hits_total` and `cortex_cardinality_cache_misses_total` counters per store gateway (see [Observability](#observability)).

### Response Format

The JSON response uses the standard Prometheus API envelope. Both `source=head` and `source=blocks` return the same data fields:

```json
{
  "status": "success",
  "data": {
    "numSeries": 1500,
    "approximated": false,
    "seriesCountByMetricName": [
      {"name": "http_requests_total", "value": 500},
      {"name": "process_cpu_seconds_total", "value": 200}
    ],
    "labelValueCountByLabelName": [
      {"name": "instance", "value": 50},
      {"name": "job", "value": 10}
    ],
    "seriesCountByLabelValuePair": [
      {"name": "job=api-server", "value": 300},
      {"name": "instance=host1:9090", "value": 150}
    ]
  }
}
```

| Field | Description |
|---|---|
| `numSeries` | Total number of series (approximate — see notes on RF division and block overlap). |
| `approximated` | `true` when results may be inflated due to overlapping blocks, partial timeout, or missing blocks after consistency check retries. `false` when results are exact. |
| `seriesCountByMetricName` | Top N metrics by series count. |
| `labelValueCountByLabelName` | Top N label names by number of distinct values. |
| `seriesCountByLabelValuePair` | Top N label=value pairs by series count. |

### Multi-Tenancy

Tenant isolation is enforced through the existing Cortex authentication middleware. The `X-Scope-OrgID` header identifies the tenant, and the ingester only returns statistics from that tenant's TSDB head. No cross-tenant data leakage is possible because each tenant has a separate TSDB instance in the ingester.

### Per-Tenant Limits

To prevent expensive cardinality queries from overloading the system, the following per-tenant runtime-configurable limits are introduced:

| Limit | Flag | YAML | Default | Description |
|---|---|---|---|---|
| `cardinality_api_enabled` | `-querier.cardinality-api-enabled` | `cardinality_api_enabled` | `false` | Enables the cardinality API for this tenant. When disabled, the endpoint returns HTTP 403. |
| `cardinality_max_query_range` | `-querier.cardinality-max-query-range` | `cardinality_max_query_range` | `24h` | Maximum allowed time range (`end - start`) for `source=blocks` cardinality queries. |
| `cardinality_max_concurrent_requests` | `-querier.cardinality-max-concurrent-requests` | `cardinality_max_concurrent_requests` | `2` | Maximum number of concurrent cardinality requests per tenant. Excess requests are rejected with HTTP 429. |
| `cardinality_query_timeout` | `-querier.cardinality-query-timeout` | `cardinality_query_timeout` | `60s` | Per-request timeout for cardinality computation. On timeout, partial results are returned with `approximated: true`. |

When a `source=blocks` request exceeds the `cardinality_max_query_range` limit, the endpoint returns HTTP 400 with an error message following the pattern used by `max_query_length` violations: `"the query time range exceeds the limit (query length: %s, limit: %s)"`.

### Observability

The cardinality endpoint exposes the following metrics:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `cortex_cardinality_request_duration_seconds` | Histogram | `source`, `status_code` | End-to-end request duration. |
| `cortex_cardinality_requests_total` | Counter | `source`, `status_code` | Total requests by source and result. |
| `cortex_cardinality_inflight_requests` | Gauge | `source` | Current number of in-flight cardinality requests. |
| `cortex_cardinality_cache_hits_total` | Counter | — | Per-block cardinality cache hits (store gateway). |
| `cortex_cardinality_cache_misses_total` | Counter | — | Per-block cardinality cache misses (store gateway). |
| `cortex_cardinality_blocks_queried_total` | Counter | — | Number of blocks analyzed per request (store gateway). |

These metrics are registered with `promauto.With(reg)` following Cortex conventions — no global registerer is used.

## Rollout Plan

The cardinality API is introduced as an **experimental** feature behind the `cardinality_api_enabled` per-tenant flag (default `false`).

**Phase 1 — Head path**: Implement the `source=head` path (ingester fan-out, RF-based aggregation). This is lower risk since it only queries in-memory TSDB heads with bounded data. Enable for a small set of tenants for validation.

**Phase 2 — Blocks path**: Implement the `source=blocks` path (store gateway fan-out, block index analysis, caching). This is higher risk due to potential object storage I/O and larger data volumes. Enable selectively behind the same flag.

**Phase 3 — GA**: After validation, change `cardinality_api_enabled` default to `true` and graduate from experimental status.

## Design Alternatives

### Distributor vs Querier Routing (Head Path)

This design routes the endpoint through the **Querier**, which handles the HTTP request and delegates to the in-process Distributor for ingester fan-out and aggregation. An alternative is to route through the **Distributor** directly.

**Current approach (Querier):**
- Provides logical separation — this is a read-only diagnostic endpoint and belongs on the read path alongside other query APIs.
- Follows the pattern used by the `/api/v1/metadata` endpoint, which is registered via `NewQuerierHandler` and delegates to the Distributor's `MetricsMetadata` method.
- Requires adding `Cardinality` to the Querier's Distributor interface (`pkg/querier/distributor_queryable.go`) and a handler in the Querier package.

**Alternative (Distributor):**
- Follows the pattern used by the `UserStats` endpoint, which is registered directly on the Distributor.
- Slightly simpler — no need to thread the method through the Querier's Distributor interface.

Note that both approaches have the same number of network hops. Even in microservices mode, the Querier process initializes an in-process Distributor instance via the `DistributorService` module (a `UserInvisibleModule` dependency of `Queryable`). This in-process Distributor holds its own ingester client pool and connects directly to ingesters via gRPC. The choice between Querier and Distributor routing only affects which process serves the HTTP request, not the number of network hops.

### Posting List Expansion vs Offset Estimation (Blocks Path)

For computing series counts from block indexes, two strategies were considered:

**Posting list expansion (chosen):**
- Fetches and decodes the posting list for each label value from the full block index.
- Produces exact series counts.
- Requires object storage I/O on first access (cached thereafter).
- Cost is proportional to the number of distinct metric names (typically manageable).

**Posting offset estimation:**
- Uses byte offsets between consecutive entries in the posting offset table (available in the index header) to estimate posting list sizes.
- No object storage I/O required — uses only the memory-mapped index header.
- Produces approximate counts since varint encoding means byte size is not directly proportional to series count.
- Would require calibration or a conversion factor that varies by data characteristics.

Posting list expansion was chosen because exact counts are more useful for cardinality debugging, and the per-block caching strategy (blocks are immutable) amortizes the object storage I/O cost over repeated requests.

### Compactor-Based Precomputation (Blocks Path)

An alternative to on-demand computation is precomputing cardinality statistics during compaction and storing them in block `meta.json`:

**Pros:**
- Zero read-time cost — statistics are available immediately from block metadata.
- The compactor already reads the full block index during compaction and validation (`GatherIndexHealthStats`).

**Cons:**
- Statistics are only available after compaction runs. Freshly uploaded blocks from ingesters would have no cardinality data until the next compaction cycle.
- Increases `meta.json` size. With hundreds of metric names and label-value pairs, the cardinality data could be significant.
- The `limit` parameter cannot be applied at precomputation time — either store all data or pick a fixed limit.
- Adds complexity to the compaction pipeline for a diagnostic feature.

The on-demand approach was chosen because it works for all blocks immediately (not just compacted ones) and allows flexible `limit` parameters per request.

## Implementation

The implementation spans the following key files:

### Head Path (Ingester)

- `pkg/api/handlers.go` - Route registration in `NewQuerierHandler` (both prometheus and legacy prefixes)
- `pkg/querier/cardinality_handler.go` - HTTP handler (`CardinalityHandler`)
- `pkg/querier/cardinality_handler_test.go` - Handler unit tests
- `pkg/querier/distributor_queryable.go` - `Cardinality` added to the Distributor interface
- `pkg/distributor/distributor.go` - Fan-out to ingesters and aggregation logic (`Cardinality`, `topNStats`)
- `pkg/distributor/distributor_test.go` - Aggregation unit tests
- `pkg/ingester/ingester.go` - Per-tenant TSDB head stats retrieval (`Cardinality`, `statsToPB`)
- `pkg/ingester/client/ingester.proto` - gRPC message definitions (`CardinalityRequest`, `CardinalityResponse`)

### Blocks Path (Store Gateway)

- `pkg/querier/cardinality_handler.go` - HTTP handler routes `source=blocks` to store gateway path
- `pkg/querier/blocks_store_queryable.go` - `Cardinality` added to the store gateway query interface
- `pkg/storegateway/storegatewaypb/gateway.proto` - gRPC message definitions for store gateway `Cardinality` RPC
- `pkg/storegateway/gateway.go` - Store gateway `Cardinality` handler, delegates to `ThanosBucketStores`
- `pkg/storegateway/bucket_stores.go` - Per-tenant block iteration and cardinality computation from index headers and block indexes

### Shared

- `pkg/cortexpb/cardinality.proto` - Shared `CardinalityStatItem` message definition
- `pkg/util/validation/limits.go` - Per-tenant limit definitions (`cardinality_api_enabled`, `cardinality_max_query_range`, `cardinality_max_concurrent_requests`, `cardinality_query_timeout`)
- `pkg/util/validation/exporter.go` - Overrides exporter for new limits
- `docs/api/_index.md` - API documentation (updated with `source`, `start`, and `end` parameters)
- `integration/api_endpoints_test.go` - Integration tests for both head and blocks paths
