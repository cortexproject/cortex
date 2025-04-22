---
title: "Parquet-based Storage"
linkTitle: "Parquet-based Storage"
weight: 1
slug: parquet-storage
---

- Author: [Alan Protasio](https://github.com/alanprot), [Ben Ye](https://github.com/yeya24)
- Date: April 2025
- Status: Proposed

## Background

Since the introduction of Block Storage in Cortex, TSDB format and Store Gateway is the de-facto way to query long term data on object storage. However, it presents several significant challenges:

### TSDB Format Limitations

TSDB format, while efficient for write-heavy workloads on local SSDs, is not designed for object storage:
- Index relies heavily on random reads to serve queries, where each random read becomes a request to object store
- In order to reduce requests to object store, requests needs to be merged, leading to higher overfetch
- Index relies on postings, which can be a huge bottleneck for high cardinality data

### Store Gateway Operational Challenges

Store Gateway is originally introduced in [Thanos](https://thanos.io/). Both Cortex and Thanos community have been collaborating to add a lot of optimizations to Store Gateway. However, it has its own problems related to the design.

1. Resource Intensive
   - Requires significant local disk space to store index headers
   - High memory utilization due to index header mmap
   - Often needs over-provisioning to handle query spikes

2. State Management and Scaling Difficulties
   - Requires complex data sharding when scaling. Often causing issues such as consistency check failure. Hard to configure for users
   - Initial sync causes long startup time. This affects service availability on both scaling and failure recovery scenario

3. Query Inefficiencies
   - Attempts to minimize storage requests often lead to overfetching, causing high bandwidth usage
   - Complex caching logic with varying effectiveness. Latency varies a lot when cache miss
   - Processes single block with one goroutine, leading to high latency for large blocks and cannot scale without complex data partitioning

### Why Parquet?

[Apache Parquet](https://parquet.apache.org/) is a columnar storage format designed specifically for efficient data storage and retrieval from object storage systems. It offers several key advantages that directly address the problems we face with TSDB and Store Gateway:

- Data organized by columns rather than rows, reduces number of requests to object storage as only limited IO is required to fetch the whole column
- Rich file metadata and index, no local state like index header required to query the data, making it stateless
- Advanced compression techniques reduce storage costs and improve query performance
- Parallel processing friendly using Parquet Row Group

There are other benefits of Parquet formats, but they are not directly related to the proposal:

- Wide ecosystem and tooling support
- Column pruning opportunity using projection pushdown

## Out of Scope

- Allow Ingester and Compactor to create Parquet files instead of TSDB blocks directly. This could be in the future roadmap but this proposal only focuses on converting and querying Parquet files.

## Proposed Design

### Components

There are 2 new Cortex components/modules introduced in this design.

#### 1. Parquet Converter

Parquet Converter is a new component that converts TSDB blocks on object store to Parquet file format.

It is similar to compactor, however, it only converts single block. The converted Parquet files will be stored in the same TSDB block folder so that the lifecycle of Parquet file will be managed together with the block.

Only certain blocks can be configured to convert to Parquet file and it can be block duration based, for example we only convert if block duration is >= 12h.

#### 2. Parquet Queryable

Similar to the existing `distributorQueryable` and `blockStorageQueryable`, Parquet queryable is a queryable implementation which allows Cortex to query parquet files and can be used in both Cortex Querier and Ruler.

If Parquet queryable is enabled, block storage queryable will be disabled and Cortex querier will not query Store Gateway anymore. `distributorQueryable` remains unchanged so it still queries Ingesters.

Parquet queryable uses bucket index to discovers  parquet files in object storage. The bucket index is the same as the existing TSDB bucket index file, but using a different name `bucket-index-parquet.json.gz`. It is updated periodically by Cortex Compactor/Parquet Converter if parquet storage is enabled.

Cortex querier remains a stateless component when Parquet queryable is enabled.

### Architecture

```
┌──────────┐    ┌─────────────┐    ┌──────────────┐
│ Ingester │───>│   TSDB      │───>│   Parquet    │
└──────────┘    │   Blocks    │    │  Converter   │
                └─────────────┘    └──────────────┘
                                          │
                                          v
┌──────────┐    ┌─────────────┐    ┌──────────────┐
│  Query   │───>│   Parquet   │───>│    Parquet   │
│ Frontend │    │   Querier   │    │    Files     │
└──────────┘    └─────────────┘    └──────────────┘
```

### Data Format

Following the current design of Cortex, each Parquet file contains at most 1 day of data.

#### Schema Overview

The Parquet format consists of two types of files:

1. **Labels Parquet File**
   - Each row represents a unique time series
   - Each column corresponds to a label name (e.g., `__name__`, `label1`, ..., `labelN`)
   - Row groups are sorted by `__name__` alphabetically in ascending order

2. **Chunks Parquet File**
   - Maintains row and row group order matching the Labels file
   - Contains multiple chunk columns for time-series data. Each column covering a time range of chunks: 0-8h, 8h-16h, 16-24h.

#### Column Specifications

| Column Name | Description | Type | Encoding/Compression/skipPageBounds | Required |
|------------|-------------|------|-----------------------------------|-----------|
| `s_hash` | Hash of all labels | INT64 | None/Zstd/Yes | No |
| `s_col_indexes` | Bitmap indicating which columns store the label set for this row (series) | ByteArray (bitmap) | DeltaByteArray/Zstd/Yes | Yes |
| `s_lbl_{labelName}` | Values for a given label name. Rows are sorted by metric name | ByteArray (string) | RLE_DICTIONARY/Zstd/No | Yes |
| `s_data_{n}` | Chunks columns (0 to data_cols_count). Each column contains data from `[n*duration, (n+1)*duration]` where duration is `24h/data_cols_count` | ByteArray (encoded chunks) | DeltaByteArray/Zstd/Yes | Yes |

data_cols_count_md will be a parquet file metadata and its value is usually 3 but it can be configurable to adjust for different usecases.

## Open Questions

1. Should we use Parquet Gateway to replace Store Gateway
   - Separate query engine and storage
   - We can make Parquet Gateway semi-stateful like data locality for better performance

## Acknowledgement

We'd like to give huge credits for people from the Thanos community who started this initiative.

- [Filip Petkovski](https://github.com/fpetkovski) and his initial [talk about Parquet](https://www.youtube.com/watch?v=V8Y4VuUwg8I)
- [Michael Hoffmann](https://github.com/MichaHoffmann) and his great work of [parquet poc](https://github.com/cloudflare/parquet-tsdb-poc)
