---
title: "Production tips"
linkTitle: "Production tips"
weight: 4
slug: production-tips
---

This page shares some tips and things to take in consideration when setting up a production Cortex cluster based on the blocks storage.

## Querier

### Ensure caching is enabled

The querier rely on caching to reduce the number API calls to the storage bucket. Ensure [caching](./querier.md#caching) is properly configured.

### Avoid querying non compacted blocks

Querying non compacted blocks may be inefficient, when running a Cortex blocks storage cluster at scale, because of two reasons:

1. Non compacted blocks contain duplicated samples (as effect of the ingested samples replication)
2. Overhead introduced querying many small indexes

Because of this, we would suggest to avoid querying non compacted blocks. In order to do it, you should:

1. Run the [compactor](./compactor.md)
2. Configure ingesters `-experimental.tsdb.retention-period` large enough to give compactor enough time to compact newly uploaded blocks (_see below_)
3. Configure queriers `-querier.query-ingesters-within` equal to the blocks retention period in the ingesters
4. Configure queriers `-querier.query-store-after` equal to the blocks retention period minus 1h
5. Lower `-experimental.tsdb.bucket-store.ignore-deletion-marks-delay` to 1h, otherwise non compacted blocks could be queried anyway, even if their compacted replacement is available

#### How to estimate the retention period

The retention period should be set to a duration large enough to give compactor enough time to compact newly uploaded blocks, and queriers and store-gateways to discover and sync newly compacted blocks.

The following diagram shows all the timings involved in the estimation of the retention period, assuming the worst case scenario and that blocks compaction takes no longer then 3h.

![Avoid querying non compacted blocks](/images/blocks-storage/avoid-querying-non-compacted-blocks.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

## Store-gateway

### Ensure caching is enabled

The store-gateway heavily relies on caching both to speed up the queries and to reduce the number of API calls to the storage bucket. Ensure [caching](./store-gateway.md#caching) is properly configured.

### Ensure a high number of max open file descriptors

The store-gateway stores each block’s index-header on the local disk and loads it via mmap. This means that the store-gateway keeps a file descriptor open for each loaded block. If your Cortex cluster has many blocks in the bucket, the store-gateway may hit the **`file-max` ulimit** (maximum number of open file descriptions by a process); in such case, we recommend increasing the limit on your system.

## Compactor

### Ensure the compactor has enough disk space

The compactor generally needs a lot of disk space in order to download source blocks from the bucket and store the compacted block before uploading it to the storage. Please refer to [Compactor disk utilization](./compactor.md#compactor-disk-utilization) for more information about how to do capacity planning.
