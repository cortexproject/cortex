---
title: "Per-Tenant TSDB Status API"
linkTitle: "Per-Tenant TSDB Status API"
weight: 1
slug: per-tenant-tsdb-status-api
---

- Author: [Charlie Le](https://github.com/CharlieTLe)
- Date: March 2026
- Status: Draft

## Background

High-cardinality series is one of the most common operational challenges for Prometheus-based systems. When a tenant has too many active series, it can lead to increased resource usage in ingesters, slower queries, and ultimately hitting per-tenant series limits.

Currently, Cortex tenants lack visibility into which metrics, labels, and label-value pairs contribute the most series in ingesters. Without this information, debugging high-cardinality issues requires operators to inspect TSDB internals directly on ingester instances, which is impractical in a multi-tenant, distributed environment.

Prometheus itself exposes a `/api/v1/status/tsdb` endpoint that provides cardinality statistics from the TSDB head. This proposal brings equivalent functionality to Cortex as a multi-tenant, distributed API.

## Goal

Expose per-tenant TSDB head cardinality statistics via a REST API endpoint on the Cortex query path. The endpoint should:

1. Be compatible with the Prometheus `/api/v1/status/tsdb` response format.
2. Aggregate statistics across all ingesters that hold data for the requesting tenant.
3. Correctly account for replication factor when summing series counts and memory usage.
4. Respect multi-tenancy, ensuring tenants can only see their own data.

## Out of Scope

- **Long-term storage cardinality analysis**: This endpoint only covers in-memory TSDB head data in ingesters. Analyzing cardinality across compacted blocks in object storage is a separate concern. A future long-term cardinality API could reuse portable fields (see [Extensibility](#extensibility-to-long-term-storage)) or introduce a separate endpoint.
- **Automated cardinality limiting**: This is a read-only diagnostic endpoint; it does not enforce or suggest limits.
- **Cardinality reduction actions**: The endpoint reports statistics but does not provide mechanisms to drop or relabel series.

## Proposed Design

### Endpoint

```
GET /api/v1/status/tsdb?limit=N
```

- **Authentication**: Requires `X-Scope-OrgID` header (standard Cortex tenant authentication).
- **Query Parameter**: `limit` (optional, default 10) - controls the number of top items returned per category.
- **Legacy Path**: Also registered at `<legacy-prefix>/api/v1/status/tsdb`.

### Architecture

The request flows through the Querier's HTTP handler, which delegates to the in-process Distributor for ingester fan-out:

```
Client → HTTP Handler (Querier) → In-process Distributor → gRPC Fan-out (Ingesters) → Aggregation (Distributor) → JSON Response
```

1. **HTTP Handler** (`TSDBStatusHandler` in `pkg/querier/tsdb_status_handler.go`): Registered via `NewQuerierHandler` in `pkg/api/handlers.go`. Parses the `limit` query parameter and calls the distributor's `TSDBStatus` method.
2. **Distributor Fan-out** (`TSDBStatus` in `pkg/distributor/distributor.go`): The Querier process holds an in-process Distributor instance (initialized via the `DistributorService` module). This instance uses `GetIngestersForMetadata` to discover all ingesters for the tenant, then sends a `TSDBStatusRequest` gRPC call to each ingester in the replication set.
3. **Ingester** (`TSDBStatus` in `pkg/ingester/ingester.go`): Retrieves the tenant's TSDB head and calls `db.Head().Stats(labels.MetricName, limit)` to get cardinality statistics from the Prometheus TSDB library.
4. **Aggregation**: The distributor merges responses from all ingesters and returns the combined result.

### gRPC Definition

A new `TSDBStatus` RPC is added to the Ingester service in `pkg/ingester/client/ingester.proto`:

```protobuf
rpc TSDBStatus(TSDBStatusRequest) returns (TSDBStatusResponse) {};

message TSDBStatusRequest {
  int32 limit = 1;
}

message TSDBStatusResponse {
  uint64 num_series = 1;
  int64 min_time = 2;
  int64 max_time = 3;
  int32 num_label_pairs = 4;
  repeated TSDBStatItem series_count_by_metric_name = 5;
  repeated TSDBStatItem label_value_count_by_label_name = 6;
  repeated TSDBStatItem memory_in_bytes_by_label_name = 7;
  repeated TSDBStatItem series_count_by_label_value_pair = 8;
}

message TSDBStatItem {
  string name = 1;
  uint64 value = 2;
}
```

### Aggregation Logic

Because each series is replicated across multiple ingesters (controlled by the replication factor), the aggregation logic must account for this when merging responses:

| Field | Aggregation Strategy |
|---|---|
| `numSeries` | Sum across ingesters, divide by replication factor |
| `minTime` | Minimum across all ingesters |
| `maxTime` | Maximum across all ingesters |
| `numLabelPairs` | Maximum across ingesters |
| `seriesCountByMetricName` | Sum per metric, divide by RF, return top N |
| `labelValueCountByLabelName` | Maximum per label (unique counts, not affected by replication) |
| `memoryInBytesByLabelName` | Sum per label, divide by RF, return top N |
| `seriesCountByLabelValuePair` | Sum per pair, divide by RF, return top N |

The `topNStats` helper function handles the sort-and-truncate step: it divides values by the replication factor, sorts descending by value, and returns the top N items.

### Response Format

The JSON response uses a flat structure for head statistics:

```json
{
  "numSeries": 1500,
  "minTime": 1709740800000,
  "maxTime": 1709748000000,
  "numLabelPairs": 42,
  "seriesCountByMetricName": [
    {"name": "http_requests_total", "value": 500},
    {"name": "process_cpu_seconds_total", "value": 200}
  ],
  "labelValueCountByLabelName": [
    {"name": "instance", "value": 50},
    {"name": "job", "value": 10}
  ],
  "memoryInBytesByLabelName": [
    {"name": "instance", "value": 25600},
    {"name": "job", "value": 5120}
  ],
  "seriesCountByLabelValuePair": [
    {"name": "job=api-server", "value": 300},
    {"name": "instance=host1:9090", "value": 150}
  ]
}
```

### API Compatibility with Prometheus

The response format intentionally diverges from the upstream Prometheus `/api/v1/status/tsdb` endpoint in two ways:

1. **Flat structure vs nested `headStats`**: Prometheus wraps `numSeries`, `numLabelPairs`, `chunkCount`, `minTime`, and `maxTime` inside a `headStats` object. This proposal uses a flat structure at the top level instead, which is simpler for consumers but means existing Prometheus client libraries cannot parse the response directly.

2. **`chunkCount` omitted**: Prometheus includes a `chunkCount` field (from `prometheus_tsdb_head_chunks`). In a distributed system with replication, chunk counts across ingesters cannot be meaningfully aggregated — chunks are an ingester-local storage detail, and summing/dividing by the replication factor does not produce a useful number.

**Open question**: Should we adopt the `headStats` wrapper to maintain client compatibility with Prometheus tooling? The trade-off is compatibility vs simplicity — the flat format is easier to consume for Cortex-specific clients, but adopting the Prometheus format would allow reuse of existing client libraries.

### Extensibility to Long-Term Storage

Some fields in the response are inherently specific to the in-memory TSDB head and would not translate to a long-term storage cardinality API:

| Field | Head-specific? | Notes |
|---|---|---|
| `seriesCountByMetricName` | No | Portable to block storage |
| `labelValueCountByLabelName` | No | Portable to block storage |
| `seriesCountByLabelValuePair` | No | Portable to block storage |
| `memoryInBytesByLabelName` | **Yes** | In-memory byte usage has no analogue in object storage |
| `minTime` / `maxTime` | **Yes** | Reflects head time range, not total storage |
| `numSeries` | Partially | Head-only count; block storage would have a different count |
| `numLabelPairs` | Partially | Head-only count |

If a long-term storage cardinality API is added in the future, the portable fields (`seriesCountByMetricName`, `labelValueCountByLabelName`, `seriesCountByLabelValuePair`) could share a common response format. Head-specific fields like `memoryInBytesByLabelName` would remain scoped to this endpoint. This could be achieved by either adding a `source=head|blocks` query parameter to this endpoint or introducing a separate endpoint for block storage cardinality.

### Multi-Tenancy

Tenant isolation is enforced through the existing Cortex authentication middleware. The `X-Scope-OrgID` header identifies the tenant, and the ingester only returns statistics from that tenant's TSDB head. No cross-tenant data leakage is possible because each tenant has a separate TSDB instance in the ingester.

## Design Alternatives

### Distributor vs Querier Routing

This design routes the endpoint through the **Querier**, which handles the HTTP request and delegates to the in-process Distributor for ingester fan-out and aggregation. An alternative is to route through the **Distributor** directly.

**Current approach (Querier):**
- Provides logical separation — this is a read-only diagnostic endpoint and belongs on the read path alongside other query APIs.
- Follows the pattern used by the `/api/v1/metadata` endpoint, which is registered via `NewQuerierHandler` and delegates to the Distributor's `MetricsMetadata` method.
- Requires adding `TSDBStatus` to the Querier's Distributor interface (`pkg/querier/distributor_queryable.go`) and a handler in the Querier package.

**Alternative (Distributor):**
- Follows the pattern used by the `UserStats` endpoint, which is registered directly on the Distributor.
- Slightly simpler — no need to thread the method through the Querier's Distributor interface.

Note that both approaches have the same number of network hops. Even in microservices mode, the Querier process initializes an in-process Distributor instance via the `DistributorService` module (a `UserInvisibleModule` dependency of `Queryable`). This in-process Distributor holds its own ingester client pool and connects directly to ingesters via gRPC. The choice between Querier and Distributor routing only affects which process serves the HTTP request, not the number of network hops.

## Implementation

The implementation spans the following key files:

- `pkg/api/handlers.go` - Route registration in `NewQuerierHandler`
- `pkg/querier/tsdb_status_handler.go` - HTTP handler (`TSDBStatusHandler`)
- `pkg/querier/distributor_queryable.go` - `TSDBStatus` added to the Distributor interface
- `pkg/distributor/distributor.go` - Fan-out to ingesters and aggregation logic (`TSDBStatus`, `topNStats`)
- `pkg/ingester/ingester.go` - Per-tenant TSDB head stats retrieval (`TSDBStatus`, `statsToPB`)
- `pkg/ingester/client/ingester.proto` - gRPC message definitions (`TSDBStatusRequest`, `TSDBStatusResponse`, `TSDBStatItem`)
- `docs/api/_index.md` - API documentation
- `integration/api_endpoints_test.go` - Integration tests
