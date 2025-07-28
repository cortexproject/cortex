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
  parquet_converter_tenant_shard_size: 0
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
```

### Query Limits for Parquet

Configure query limits specific to parquet operations:

```yaml
limits:
  # Maximum number of rows that can be scanned per query
  parquet_max_fetched_row_count: 1000000
  
  # Maximum chunk bytes per query
  parquet_max_fetched_chunk_bytes: 100MB
  
  # Maximum data bytes per query  
  parquet_max_fetched_data_bytes: 1GB
```

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

1. **Primary Query Path**: Attempts to query Parquet files first
2. **Fallback Logic**: Falls back to TSDB blocks if:
   - Parquet files are not available
   - Query fails on Parquet files
   - Block hasn't been converted yet
3. **Hybrid Queries**: Can query both Parquet and TSDB blocks in the same query

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
4. **Conversion Interval**: Balance between conversion latency and system load

### Query Optimization

1. **Time Range Queries**: Parquet performs best with time-range based queries
2. **Label Filtering**: Use label filters to reduce data scanning
3. **Aggregation**: Leverage Parquet's columnar format for aggregation queries

## Limitations

1. **Experimental Feature**: Parquet mode is experimental and may have stability issues
2. **Storage Overhead**: Parquet files are stored in addition to TSDB blocks
3. **Conversion Latency**: There's a delay between block creation and parquet availability
4. **Query Compatibility**: Some advanced PromQL features may not be fully supported

## Migration Considerations

When enabling parquet mode:

1. **Gradual Rollout**: Enable for specific tenants first
2. **Monitor Resources**: Watch CPU, memory, and storage usage
3. **Backup Strategy**: Ensure TSDB blocks remain available as fallback
4. **Testing**: Thoroughly test query patterns before production deployment

## Future Enhancements

The parquet mode is under active development with planned improvements:
- Better query optimization
- Reduced storage overhead
- Enhanced monitoring and observability
- Improved conversion performance