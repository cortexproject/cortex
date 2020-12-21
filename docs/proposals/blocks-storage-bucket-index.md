---
title: "Blocks storage bucket index"
linkTitle: "Blocks storage bucket index"
weight: 1
slug: blocks-storage-bucket-index
---

- Author: [Marco Pracucci](https://github.com/pracucci)
- Date: November 2020
- Status: draft

## Introduction

Queriers and store-gateways, at startup and periodically while running, need to scan the entire object store bucket to find tenants and blocks for each tenants.

For each discovered block, they need to fetch the `meta.json` file and check for the existence of `deletion-mark.json` (used to signal a block is "marked for deletion"). The `meta.json` file is immutable and gets cached, but `deletion-mark.json` can be created at any time and its non-existence can't be safely cached for long (actually our cache is pretty ineffective regarding this).

The number of blocks in the storage linearly increases with the number of tenants, and so does the number of bucket API calls required to scan the bucket.

For example, assuming 1 block / day / tenant and a 400d retention, we would have 400 blocks for a single-tenant cluster, but 4M blocks for a 10K tenants cluster (regardless of the number of active series or their QPS).

We're currently testing a cluster running with 6K tenants and we have faced the following issues:

- The cold start of queriers and store-gateways (empty caches) take tens of minutes.
- If we increase the concurrency of bucket operations run to scan the bucket, we hit into object store rate limits and/or the 5xx error rate increases
- The need to run "list objects" API call for queriers and store-gateways to discover blocks and the check for the existence of `deletion-mark.json` (which requires a read operation for each block and can't be cached for a long time) represents a significant % of bucket API calls baseline costs, regardless the tenants QPS (costs we have even if the cluster has 0 queries)

## Goal

The goal of this proposal is:

- The querier should be up and running without having to scan the bucket at all (zero startup time)
- The querier should not run any "list objects" operation at anytime
- The querier should require only 1 "get object" operation to get the entire view of a tenant's blocks

## Out of scope

We believe the same technique described in this proposal could be applied to optimise the store-gateway too (we've built a [PoC](https://github.com/cortexproject/cortex/compare/pracucci:experiment-object-store-based-blocks-index)), however to keep the design doc easier to discuss we suggest to keep the store-gateway out of scope and address it in a follow-up design doc.

## Proposal

We propose to introduce a per-tenant bucket index. The index is a single JSON file containing two main information: list of all completed (non partial) blocks in the bucket + list of all deletion marks. The bucket index is stored in the bucket within the tenant location (eg. `/user-1/bucket-index.json`) and is kept updated by the compactor.

The querier, at query time, checks whether the bucket index for the tenant has already been loaded in memory. If not, the querier will download it and cache it in memory. Given it's a small file, we expect the lazy download of the bucket index to not significantly impact first query performances.

While in-memory, a background process will keep it updated at periodic intervals (configurable), so that subsequent queries from the same tenant to the same querier instance will use the cached (and periodically updated) bucket index.

If a bucket index is unused for a long time (configurable), eg. because that querier instance is not receiving any query from the tenant, the querier will offload it, stopping to keep it updated at regular intervals. This is particularly useful when shuffle sharding is enabled, because a querier will only run queries for a subset of tenants and tenants can be re-sharded across queries in the event of a scale up/down or rolling updates.

The bucket index will be also cached on memcached for a short period of time, to reduce the number of "get object" operations in case multiple queriers fetch it in a short period of time (eg. 5 minutes).

### Bucket index structure

The `bucket-index.json` is a single file containing the following information:

- `Version`
- List of blocks
  - `ID`, `MinTime`, `MaxTime`, `UploadedAt`
  - `SegmentsFormat` (eg. "1-based-6-digits"), `SegmentsNum` (these two information will be used by the store-gateway)
- List of block deletion marks
  - `ID`, `DeletionTime`
- Timestamp of when the index has been updated

The bucket index intentionally stores a subset of data for each block's `meta.json`, in order to keep the size of the index small. The information in the index is enough to run the querier without having to load the full `meta.json` of each block.

**Size**: the expected size of the index is about 150 bytes per block. In the case of a tenant with 400 blocks, its bucket index would be 58KB. The size could be further reduced compressing it: experiments show a compression ratio of about 4x using gzip.

### Why the compactor writes the bucket index

There are few reasons why the compactor may be a good candidate to write the bucket index:

- The compactor already scans the bucket to have a consistent view of the blocks before running a compaction. Writing the bucket index in the compactor would require no additional bucket API calls.
- Queriers and store-gateways currently read only from the bucket (no writes). We believe that it may be a nice property to preserve.


### How to reduce bucket API calls required to discover deletion marks

The `deletion-mark.json` files are stored within each block location. This means that the compactor would still need to run a "get object" operation for every single block in order to find out which block has it, while updating the bucket index.

To reduce the number of operations required, we propose to store block deletion marks for all blocks in a per-tenant location (`markers/`). Discovering all blocks marked for deletion for a given tenant would only require a single "list objects" operation on the `/<tenant-id>/markers/` prefix. New markers introduced in the future (eg. tenant deletion mark) could be stored in the same location in order to discover all markers with a single "list objects" operation.

For example:

```
/user-1/markers/01ER1ZSYHF1FT6RBD8HTVQWX13-deletion-mark.json
/user-1/markers/01ER1ZGSX1Q4B41MK1QQ7RHD33-deletion-mark.json
/user-1/markers/01ER1ZGWKVFT60YMXT8D3XJMDB-deletion-mark.json
```

### What if a bucket index gets stale

Queriers and store-gateways don't need a strongly consistent view over the bucket. Even today, given queriers and store-gateways run a periodic scan of the bucket, their view could be stale up to the "scan interval" (defaults to 15m).

The maximum delay between a change into the bucket is picked up by queriers and store-gateways depends on the configuration and is the minimum time between:

- New blocks uploaded by ingester:
  `min(-querier.query-ingesters-within, -blocks-storage.tsdb.retention-period)` (default: 6h)
- New blocks uploaded/deleted by compactor:
  `-compactor.deletion-delay` (default: 12h)

In order to guarantee consistent query results, we propose to configure the bucket index max stale period in the querier and fail queries if, because of any issue, the bucket index `UpdatedAt` age exceeds it.

The staleness of each bucket index will be tracked via metrics, in order to be alert on it when it's close to expiration (but before it happens).

### Object store eventual consistency

Object stores like S3 or GCS do guarantee read-after-create consistency but not read-after-update.

However, given the analysis done in "[What if a bucket index gets stale](#what-if-a-bucket-index-gets-stale)" we expect this not to be a real issue, considering we do expect object store reads to not be out of sync for hours.
