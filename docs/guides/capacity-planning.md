---
title: "Capacity Planning"
linkTitle: "Capacity Planning"
weight: 10
slug: capacity-planning
---

This guide provides capacity planning recommendations for Cortex using the blocks storage engine. The blocks storage is based on [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/) and stores data in object storage (S3, GCS, Azure, Swift, etc.).

You will want to estimate how many nodes are required, how many of each component to run, and how much storage space will be required. In practice, these will vary greatly depending on the metrics being sent to Cortex.

## Key Parameters

Some key parameters to consider:

1. **Number of active series**: If you have Prometheus already, you can query [`prometheus_tsdb_head_series`](https://prometheus.io/docs/prometheus/latest/querying/functions/#prometheus_tsdb_head_series) to see this number.
2. **Sampling rate**: e.g. a new sample for each series every minute (the default Prometheus [scrape_interval](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)). Multiply this by the number of active series to get the total rate at which samples will arrive at Cortex.
3. **Series churn rate**: The rate at which series are added and removed. This can be very high if you monitor objects that come and go - for example, if you run thousands of batch jobs lasting a minute or so and capture metrics with a unique ID for each one. [Read how to analyse this on Prometheus](https://www.robustperception.io/using-tsdb-analyze-to-investigate-churn-and-cardinality).
4. **Data retention period**: How long you want to retain data for, e.g. 1 month or 2 years.
5. **Query patterns**: Rate and complexity of queries, time ranges typically queried.

Other parameters which can become important if you have particularly high values:

6. Number of different series under one metric name.
7. Number of labels per series.
8. Compressibility of time-series data.

## Component Planning

### Distributors

- **CPU**: 20,000 samples/sec with 1 CPU core.
- **Memory**: 80,000 samples/sec with 1 GB memory.
- **Optimization**: Configure Prometheus [`max_samples_per_send`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write) to 1,000 samples to reduce CPU utilization.

**Note**: If you enable compression between distributors and ingesters (for example, to save on inter-zone bandwidth charges at AWS/GCP), they will use significantly more CPU (approx. 100% more for distributor and 50% more for ingester).

### Ingesters

Ingesters run per-tenant TSDB instances and create 2-hour blocks (by default) that are uploaded to object storage.

#### Memory Requirements

- **Rule of thumb**: Each million series in an ingester takes approximately 15GB of RAM. The total number of series in ingesters is the number of active series times the replication factor (typically 3).
- **Additional considerations**:
  - Memory increases during write-ahead log (WAL) replay. [See Prometheus issue #6934](https://github.com/prometheus/prometheus/issues/6934#issuecomment-726039115). If you do not have enough memory for WAL replay, the ingester will not be able to restart successfully without intervention.
  - Memory temporarily increases during resharding since timeseries are temporarily on both the new and old ingesters. Scale up the number of ingesters before memory utilization is too high.

#### Disk Requirements

- **Rule of thumb**: For [`-blocks-storage.tsdb.retention-period=24h`](../blocks-storage/store-gateway.md#blocks_storage_config), estimate 30KB per series after replication.
- **Example**: 20M active series replicated 3 ways = ~1.7TB total. Divide by the number of ingesters and allow margin for growth.
- **Storage type**: Requires persistent disk that can survive ingester failures (e.g., AWS EBS, GCP persistent disk) for WAL and local blocks.

### Store-Gateway

The store-gateway is responsible for querying blocks from object storage and is required when using blocks storage.

#### Memory Requirements

- **Index-headers**: Store-gateways keep block index-headers in memory (via mmap) to speed up queries.
- **Caching**: Enable [index, chunks, and metadata caching](../blocks-storage/store-gateway.md#caching) for production deployments.

#### Disk Requirements

- **Index-headers**: Store-gateways download and cache index-headers locally. Persistent disk is recommended but not required.
- **File descriptors**: Ensure a high number of max open file descriptors (recommended: 65536 or higher, ideally 1048576) as each block index-header uses a file descriptor.

#### Scaling

- **Sharding**: Enable [store-gateway sharding](../blocks-storage/store-gateway.md#blocks-sharding-and-replication) for horizontal scaling.
- **Replication**: Configure replication factor > 1 for high availability.

### Compactor

The compactor is **required** for blocks storage. It merges and deduplicates blocks, and maintains the bucket index.

#### Disk Requirements

- **Compaction space**: Significant disk space needed for downloading source blocks and storing compacted blocks before upload.
- **Rule of thumb**: `compactor.compaction-concurrency × max_compaction_range_blocks_size × 2`
- **Alternative estimate**: ~150GB of disk space for every 10M active series owned by the largest tenant.

### Queriers

Queriers remain stateless and query both ingesters (for recent data) and store-gateways (for historical data).

#### Configuration

- **Query range**: Configure [`-querier.query-ingesters-within`](../blocks-storage/querier.md#querier_config) and [`-querier.query-store-after`](../blocks-storage/querier.md#querier_config) to optimize query performance.
- **Store-gateway discovery**: Configure store-gateway addresses or ring when sharding is enabled.

## Storage Planning

### Object Storage

- **Primary storage**: All blocks are stored in object storage (S3, GCS, Azure, Swift, filesystem).
- **Capacity**: Depends on retention period, series count, and data compressibility.
- **Compaction benefits**: The compactor reduces storage usage by deduplicating samples and merging blocks.

### Local Storage

- **Ingesters**: Persistent disk for TSDB WAL and local blocks.
- **Store-gateways**: Optional persistent disk for index-header caching.
- **Compactor**: Temporary disk space for compaction operations.

## Production Recommendations

1. **Enable caching**: Configure [index, chunks, and metadata caching](../blocks-storage/production-tips.md#caching) for store-gateways and queriers.
2. **Enable bucket index**: Use [bucket index](../blocks-storage/bucket-index.md) to reduce object storage API calls.
3. **Avoid querying non-compacted blocks**: Configure appropriate query time ranges to let the compactor process blocks first. See [production tips](../blocks-storage/production-tips.md#avoid-querying-non-compacted-blocks).
4. **File descriptors**: Ensure high file descriptor limits for ingesters and store-gateways.
5. **Monitoring**: Monitor compaction lag, query performance, and storage usage.

For more detailed production guidance, see [Blocks Storage Production Tips](../blocks-storage/production-tips.md).
