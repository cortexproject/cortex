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
3. Correctly account for replication factor when summing series counts.
4. Respect multi-tenancy, ensuring tenants can only see their own data.

## Out of Scope

- **Automated cardinality limiting**: This is a read-only diagnostic endpoint; it does not enforce or suggest limits.
- **Cardinality reduction actions**: The endpoint reports statistics but does not provide mechanisms to drop or relabel series.

## Proposed Design

### Endpoint

```
GET /api/v1/cardinality?limit=N&source=head|blocks&start=T&end=T
```

- **Authentication**: Requires `X-Scope-OrgID` header (standard Cortex tenant authentication).
- **Query Parameters**:
  - `limit` (optional, default 10) - controls the number of top items returned per category.
  - `source` (optional, default `head`) - selects the data source. `head` queries ingester TSDB heads, `blocks` queries compacted blocks in long-term storage via store gateways.
  - `start` (optional, RFC3339 or Unix timestamp) - start of the time range to analyze.
  - `end` (optional, RFC3339 or Unix timestamp) - end of the time range to analyze.
- **Time Range Behavior**:
  - **Blocks path**: Only blocks whose time range overlaps with `[start, end]` are analyzed. A block is included if its `minTime < end` and its `maxTime > start`.
  - **Head path**: Head stats are included if the head's time range overlaps with `[start, end]`. The TSDB head does not support sub-range cardinality filtering, so when included, stats reflect the full head.
  - When `start` and `end` are omitted, all available data is included.

### Architecture

The HTTP handler parses the `source` parameter and delegates to the appropriate backend.

#### Head Path (`source=head`)

The request flows through the Querier's HTTP handler, which delegates to the in-process Distributor for ingester fan-out:

```
Client → HTTP Handler (Querier) → In-process Distributor → gRPC Fan-out (Ingesters) → Aggregation (Distributor) → JSON Response
```

1. **HTTP Handler** (`TSDBStatusHandler` in `pkg/querier/tsdb_status_handler.go`): Registered via `NewQuerierHandler` in `pkg/api/handlers.go`. Parses the `limit` query parameter and calls the distributor's `TSDBStatus` method.
2. **Distributor Fan-out** (`TSDBStatus` in `pkg/distributor/distributor.go`): The Querier process holds an in-process Distributor instance (initialized via the `DistributorService` module). This instance uses `GetIngestersForMetadata` to discover all ingesters for the tenant, then sends a `TSDBStatusRequest` gRPC call to each ingester in the replication set.
3. **Ingester** (`TSDBStatus` in `pkg/ingester/ingester.go`): Retrieves the tenant's TSDB head and calls `db.Head().Stats(labels.MetricName, limit)` to get cardinality statistics from the Prometheus TSDB library.
4. **Aggregation**: The distributor merges responses from all ingesters and returns the combined result.

#### Blocks Path (`source=blocks`)

The request flows through the Querier's HTTP handler, which fans out to store gateways:

```
Client → HTTP Handler (Querier) → gRPC Fan-out (Store Gateways) → Per-Tenant Block Index Analysis → Aggregation (Querier) → JSON Response
```

1. **HTTP Handler** (`TSDBStatusHandler` in `pkg/querier/tsdb_status_handler.go`): Parses `limit` and `source=blocks`, then calls the blocks store's `TSDBStatus` method.
2. **Store Gateway Fan-out**: The Querier uses its existing store gateway client pool (`BlocksStoreSet`) to discover store gateways that hold blocks for the tenant, then sends a `TSDBStatus` gRPC call to each relevant store gateway instance.
3. **Store Gateway** (`TSDBStatus` in `pkg/storegateway/gateway.go`): Locates the tenant's `BucketStore`, iterates over the tenant's loaded blocks, and computes cardinality statistics from block indexes (see [Block Index Cardinality Computation](#block-index-cardinality-computation)).
4. **Aggregation**: The querier merges responses from all store gateways and returns the combined result.

### gRPC Definition

#### Ingester Service

A new `TSDBStatus` RPC is added to the Ingester service in `pkg/ingester/client/ingester.proto`:

```protobuf
rpc TSDBStatus(TSDBStatusRequest) returns (TSDBStatusResponse) {};

message TSDBStatusRequest {
  int32 limit = 1;
}

message TSDBStatusResponse {
  uint64 num_series = 1;
  repeated TSDBStatItem series_count_by_metric_name = 2;
  repeated TSDBStatItem label_value_count_by_label_name = 3;
  repeated TSDBStatItem series_count_by_label_value_pair = 4;
}

message TSDBStatItem {
  string name = 1;
  uint64 value = 2;
}
```

#### Store Gateway Service

A new `TSDBStatus` RPC is added to the StoreGateway service in `pkg/storegateway/storegatewaypb/gateway.proto`:

```protobuf
rpc TSDBStatus(TSDBStatusRequest) returns (TSDBStatusResponse) {};

message TSDBStatusRequest {
  int32 limit = 1;
}

message TSDBStatusResponse {
  uint64 num_series = 1;
  repeated TSDBStatItem series_count_by_metric_name = 2;
  repeated TSDBStatItem label_value_count_by_label_name = 3;
  repeated TSDBStatItem series_count_by_label_value_pair = 4;
}

message TSDBStatItem {
  string name = 1;
  uint64 value = 2;
}
```

Both the ingester and store gateway response messages share the same fields.

### Aggregation Logic

Because each series is replicated across multiple ingesters (controlled by the replication factor), the aggregation logic must account for this when merging responses:

#### Head Path Aggregation

| Field | Aggregation Strategy |
|---|---|
| `numSeries` | Sum across ingesters, divide by replication factor |
| `seriesCountByMetricName` | Sum per metric, divide by RF, return top N |
| `labelValueCountByLabelName` | Maximum per label (unique counts, not affected by replication) |
| `seriesCountByLabelValuePair` | Sum per pair, divide by RF, return top N |

The `topNStats` helper function handles the sort-and-truncate step: it divides values by the replication factor, sorts descending by value, and returns the top N items.

#### Blocks Path Aggregation

Store gateways use the store gateway ring for replication, so different store gateways may serve the same blocks. The aggregation handles this differently from ingesters:

| Field | Aggregation Strategy |
|---|---|
| `numSeries` | Sum across store gateways, divide by store gateway replication factor |
| `seriesCountByMetricName` | Sum per metric, divide by SG RF, return top N |
| `labelValueCountByLabelName` | Maximum per label |
| `seriesCountByLabelValuePair` | Sum per pair, divide by SG RF, return top N |

**Note on block overlap**: Before compaction completes, a tenant may have multiple blocks covering the same time range. Series that appear in overlapping blocks within a single store gateway are counted once per block they appear in, so the `numSeries` total may overcount compared to the true unique series count. This is an acceptable approximation — the primary use case is identifying which metrics and label-value pairs contribute the most cardinality, not producing an exact total.

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
- Apply a per-request timeout so that very high-cardinality tenants get partial results rather than unbounded computation.

Results are cached per block (see [Caching](#caching)).

#### Block Selection

When the `start` and `end` query parameters are provided, the store gateway filters blocks based on time range overlap: a block is included only if its `minTime < end` and its `maxTime > start`. This allows users to scope cardinality analysis to a specific time window, such as the last 24 hours or a particular incident period. When `start` and `end` are omitted, all blocks for the tenant are included.

#### Caching

Compacted blocks are immutable — once a block is written to object storage, its contents never change. This means cardinality statistics computed from a block's index can be cached indefinitely (until the block is deleted by the compactor). Each store gateway maintains a per-block cardinality cache keyed by `(block ULID, limit)`. This cache eliminates redundant index traversals when the endpoint is called repeatedly.

The cache is populated on first request and invalidated when blocks are removed during compaction syncs.

### Response Format

The JSON response uses a flat structure. Both `source=head` and `source=blocks` return the same fields:

```json
{
  "numSeries": 1500,
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
```

### Multi-Tenancy

Tenant isolation is enforced through the existing Cortex authentication middleware. The `X-Scope-OrgID` header identifies the tenant, and the ingester only returns statistics from that tenant's TSDB head. No cross-tenant data leakage is possible because each tenant has a separate TSDB instance in the ingester.

## Design Alternatives

### Distributor vs Querier Routing (Head Path)

This design routes the endpoint through the **Querier**, which handles the HTTP request and delegates to the in-process Distributor for ingester fan-out and aggregation. An alternative is to route through the **Distributor** directly.

**Current approach (Querier):**
- Provides logical separation — this is a read-only diagnostic endpoint and belongs on the read path alongside other query APIs.
- Follows the pattern used by the `/api/v1/metadata` endpoint, which is registered via `NewQuerierHandler` and delegates to the Distributor's `MetricsMetadata` method.
- Requires adding `TSDBStatus` to the Querier's Distributor interface (`pkg/querier/distributor_queryable.go`) and a handler in the Querier package.

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

- `pkg/api/handlers.go` - Route registration in `NewQuerierHandler`
- `pkg/querier/tsdb_status_handler.go` - HTTP handler (`TSDBStatusHandler`)
- `pkg/querier/distributor_queryable.go` - `TSDBStatus` added to the Distributor interface
- `pkg/distributor/distributor.go` - Fan-out to ingesters and aggregation logic (`TSDBStatus`, `topNStats`)
- `pkg/ingester/ingester.go` - Per-tenant TSDB head stats retrieval (`TSDBStatus`, `statsToPB`)
- `pkg/ingester/client/ingester.proto` - gRPC message definitions (`TSDBStatusRequest`, `TSDBStatusResponse`, `TSDBStatItem`)

### Blocks Path (Store Gateway)

- `pkg/querier/tsdb_status_handler.go` - HTTP handler routes `source=blocks` to store gateway path
- `pkg/querier/blocks_store_queryable.go` - `TSDBStatus` added to the store gateway query interface
- `pkg/storegateway/storegatewaypb/gateway.proto` - gRPC message definitions for store gateway `TSDBStatus` RPC
- `pkg/storegateway/gateway.go` - Store gateway `TSDBStatus` handler, delegates to `ThanosBucketStores`
- `pkg/storegateway/bucket_stores.go` - Per-tenant block iteration and cardinality computation from index headers and block indexes

### Shared

- `docs/api/_index.md` - API documentation (updated with `source`, `start`, and `end` parameters)
- `integration/api_endpoints_test.go` - Integration tests for both head and blocks paths
