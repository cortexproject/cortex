---
title: "Parquet Mode"
linkTitle: "Parquet Mode"
weight: 11
slug: parquet-mode
---

## Overview

Parquet mode in Cortex provides an experimental feature that converts TSDB blocks to Parquet format for improved query performance and storage efficiency on older data. This feature is particularly beneficial for long-term storage scenarios where data is accessed less frequently but needs to be queried efficiently.

The parquet mode consists of two main components:
- **Parquet Converter**: Converts TSDB blocks to Parquet format
- **Parquet Queryable**: Enables querying of Parquet files with fallback to TSDB blocks

## Why Parquet Mode?

Traditional TSDB format and Store Gateway architecture face significant challenges when dealing with long-term data storage on object storage:

### TSDB Format Limitations
- **Random Read Intensive**: TSDB index relies heavily on random reads, where each read becomes a separate request to object storage
- **Overfetching**: To reduce object storage requests, data that are close together are merged in a sigle request, leading to higher bandwidth usage and overfetching
- **High Cardinality Bottlenecks**: Index postings can become a major bottleneck for high cardinality data

### Store Gateway Operational Challenges
- **Resource Intensive**: Requires significant local disk space for index headers and high memory usage
- **Complex State Management**: Requires complex data sharding when scaling, which often leads to consistency and availability issues, as well as long startup times
- **Query Inefficiencies**: Single-threaded block processing leads to high latency for large blocks

### Parquet Advantages
[Apache Parquet](https://parquet.apache.org/) addresses these challenges through:
- **Columnar Storage**: Data organized by columns reduces object storage requests as only specific columns need to be fetched
- **Data Locality**: Series that are likely to be queried together are co-located to minimize I/O operations
- **Stateless Design**: Rich file metadata eliminates the need for local state like index headers
- **Advanced Compression**: Reduces storage costs and improves query performance
- **Parallel Processing**: Row groups enable parallel processing for better scalability

For more details on the design rationale, see the [Parquet Storage Proposal](../proposals/parquet-storage.md).

## Architecture

The parquet system works by:

1. **Block Conversion**: The parquet converter runs periodically to identify TSDB blocks that should be converted to Parquet format
2. **Storage**: Parquet files are stored alongside TSDB blocks in object storage
3. **Querying**: The parquet queryable attempts to query Parquet files first, falling back to TSDB blocks when necessary
4. **Marker System**: Conversion status is tracked using marker files to avoid duplicate conversions

## Configuration

### Enabling Parquet Converter

To enable the parquet converter service, add it to your target list:

```yaml
target: parquet-converter
```

Or include it in a multi-target deployment:

```yaml
target: all,parquet-converter
```

### Parquet Converter Configuration

Configure the parquet converter in your Cortex configuration:

```yaml
parquet_converter:
  # Data directory for caching blocks during conversion
  data_dir: "./data"

  # Frequency of conversion job execution
  conversion_interval: 1m

  # Maximum rows per parquet row group
  max_rows_per_row_group: 1000000

  # Number of concurrent meta file sync operations
  meta_sync_concurrency: 20

  # Enable file buffering to reduce memory usage
  file_buffer_enabled: true

  # Ring configuration for distributed conversion
  ring:
    kvstore:
      store: consul
      consul:
        host: localhost:8500
    heartbeat_period: 5s
    heartbeat_timeout: 1m
    instance_addr: 127.0.0.1
    instance_port: 9095
```

### Per-Tenant Parquet Settings

Enable parquet conversion per tenant using limits:

```yaml
limits:
  # Enable parquet converter for all tenants
  parquet_converter_enabled: true

  # Shard size for shuffle sharding (0 = disabled)
  parquet_converter_tenant_shard_size: 0.8
```

You can also configure per-tenant settings using runtime configuration:

```yaml
overrides:
  tenant-1:
    parquet_converter_enabled: true
    parquet_converter_tenant_shard_size: 2
  tenant-2:
    parquet_converter_enabled: false
```

### Enabling Parquet Queryable

To enable querying of Parquet files, configure the querier:

```yaml
querier:
  # Enable parquet queryable with fallback (experimental)
  enable_parquet_queryable: true

  # Cache size for parquet shards
  parquet_queryable_shard_cache_size: 512

  # Default block store: "tsdb" or "parquet"
  parquet_queryable_default_block_store: "parquet"

  # Disable fallback to TSDB blocks when parquet files are not available
  parquet_queryable_fallback_disabled: false
```

### Query Limits for Parquet

Configure query limits specific to parquet operations:

```yaml
limits:
  # Maximum number of rows that can be scanned per query
  parquet_max_fetched_row_count: 1000000

  # Maximum chunk bytes per query
  parquet_max_fetched_chunk_bytes: 100_000_000 # 100MB

  # Maximum data bytes per query
  parquet_max_fetched_data_bytes: 1_000_000_000 # 1GB
```

### Cache Configuration

Parquet mode supports dedicated caching for both chunks and labels to improve query performance. Configure caching in the blocks storage section:

```yaml
blocks_storage:
  bucket_store:
    # Chunks cache configuration for parquet data
    chunks_cache:
      backend: "memcached"  # Options: "", "inmemory", "memcached", "redis"
      subrange_size: 16000  # Size of each subrange for better caching
      max_get_range_requests: 3  # Max sub-GetRange requests per GetRange call
      attributes_ttl: 168h  # TTL for caching object attributes
      subrange_ttl: 24h     # TTL for caching individual chunk subranges

      # Memcached configuration (if using memcached backend)
      memcached:
        addresses: "memcached:11211"
        timeout: 500ms
        max_idle_connections: 16
        max_async_concurrency: 10
        max_async_buffer_size: 10000
        max_get_multi_concurrency: 100
        max_get_multi_batch_size: 0

    # Parquet labels cache configuration (experimental)
    parquet_labels_cache:
      backend: "memcached"  # Options: "", "inmemory", "memcached", "redis"
      subrange_size: 16000  # Size of each subrange for better caching
      max_get_range_requests: 3  # Max sub-GetRange requests per GetRange call
      attributes_ttl: 168h  # TTL for caching object attributes
      subrange_ttl: 24h     # TTL for caching individual label subranges

      # Memcached configuration (if using memcached backend)
      memcached:
        addresses: "memcached:11211"
        timeout: 500ms
        max_idle_connections: 16
```

#### Cache Backend Options

- **Empty string ("")**: Disables caching
- **inmemory**: Uses in-memory cache (suitable for single-instance deployments)
- **memcached**: Uses Memcached for distributed caching (recommended for production)
- **redis**: Uses Redis for distributed caching
- **Multi-level**: Comma-separated list for multi-tier caching (e.g., "inmemory,memcached")

#### Cache Performance Tuning

- **subrange_size**: Smaller values increase cache hit rates but create more cache entries
- **max_get_range_requests**: Higher values reduce object storage requests but increase memory usage
- **TTL values**: Balance between cache freshness and hit rates based on your data patterns
- **Multi-level caching**: Use "inmemory,memcached" for L1/L2 cache hierarchy

## Block Conversion Logic

The parquet converter determines which blocks to convert based on:

1. **Time Range**: Only blocks with time ranges larger than the base TSDB block duration (typically 2h) are converted
2. **Conversion Status**: Blocks are only converted once, tracked via marker files
3. **Tenant Settings**: Conversion must be enabled for the specific tenant

The conversion process:
- Downloads TSDB blocks from object storage
- Converts time series data to Parquet format
- Uploads Parquet files (chunks and labels) to object storage
- Creates conversion marker files to track completion

## Querying Behavior

When parquet queryable is enabled:

1. **Block Discovery**: The bucket index is used to discover available blocks
   * The bucket index now contains metadata indicating whether parquet files are available for querying
1. **Query Execution**: Queries prioritize parquet files when available, falling back to TSDB blocks when parquet conversion is incomplete
1. **Hybrid Queries**: Supports querying both parquet and TSDB blocks within the same query operation
1. **Fallback Control**: When `parquet_queryable_fallback_disabled` is set to `true`, queries will fail with a consistency check error if any required blocks are not available as parquet files, ensuring strict parquet-only querying

## Monitoring

### Parquet Converter Metrics

Monitor parquet converter operations:

```promql
# Blocks converted
cortex_parquet_converter_blocks_converted_total

# Conversion failures
cortex_parquet_converter_block_convert_failures_total

# Delay in minutes of Parquet block to be converted from the TSDB block being uploaded to object store
cortex_parquet_converter_convert_block_delay_minutes
```

### Parquet Queryable Metrics

Monitor parquet query performance:

```promql
# Blocks queried by type
cortex_parquet_queryable_blocks_queried_total

# Query operations
cortex_parquet_queryable_operations_total

# Cache metrics
cortex_parquet_queryable_cache_hits_total
cortex_parquet_queryable_cache_misses_total
```

## Best Practices

### Deployment Recommendations

1. **Dedicated Converters**: Run parquet converters on dedicated instances for better resource isolation
2. **Ring Configuration**: Use a distributed ring for high availability and load distribution
3. **Storage Considerations**: Ensure sufficient disk space in `data_dir` for block processing
4. **Network Bandwidth**: Consider network bandwidth for downloading/uploading blocks

### Performance Tuning

1. **Row Group Size**: Adjust `max_rows_per_row_group` based on your query patterns
2. **Cache Size**: Tune `parquet_queryable_shard_cache_size` based on available memory
3. **Concurrency**: Adjust `meta_sync_concurrency` based on object storage performance

### Fallback Configuration

1. **Gradual Migration**: Keep `parquet_queryable_fallback_disabled: false` (default) during initial deployment to allow queries to succeed even when parquet conversion is incomplete
2. **Strict Parquet Mode**: Set `parquet_queryable_fallback_disabled: true` only after ensuring all required blocks have been converted to parquet format
3. **Monitoring**: Monitor conversion progress and query failures before enabling strict parquet mode

## Limitations

1. **Experimental Feature**: Parquet mode is experimental and may have stability issues
2. **Storage Overhead**: Parquet files are stored in addition to TSDB blocks
3. **Conversion Latency**: There's a delay between block creation and parquet availability
4. **Shuffle Sharding Requirement**: Parquet mode only supports shuffle sharding as sharding strategy
5. **Bucket Index Dependency**: The bucket index must be enabled and properly configured as it provides essential metadata for parquet file discovery and query routing

## Migration Considerations

When enabling parquet mode:

1. **Gradual Rollout**: Enable for specific tenants first
2. **Monitor Resources**: Watch CPU, memory, and storage usage
3. **Backup Strategy**: Ensure TSDB blocks remain available as fallback
4. **Testing**: Thoroughly test query patterns before production deployment
