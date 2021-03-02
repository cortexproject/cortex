---
title: "Production tips"
linkTitle: "Production tips"
weight: 4
slug: production-tips
---

This page shares some tips and things to take in consideration when setting up a production Cortex cluster based on the blocks storage.

## Ingester

### Ensure a high number of max open file descriptors

The ingester stores received series into per-tenant TSDB blocks. Both TSDB WAL, head and compacted blocks are composed by a relatively large number of files which gets loaded via mmap. This means that the ingester keeps file descriptors open for TSDB WAL segments, chunk files and compacted blocks which haven't reached the retention period yet.

If your Cortex cluster has many tenants or ingester is running with a long `-blocks-storage.tsdb.retention-period`, the ingester may hit the **`file-max` ulimit** (maximum number of open file descriptions by a process); in such case, we recommend increasing the limit on your system or enabling [shuffle sharding](../guides/shuffle-sharding.md).

The rule of thumb is that a production system shouldn't have the `file-max` ulimit below `65536`, but higher values are recommended (eg. `1048576`).

## Querier

### Ensure caching is enabled

The querier relies on caching to reduce the number API calls to the storage bucket. Ensure [caching](./querier.md#caching) is properly configured and [properly scaled](#ensure-memcached-is-properly-scaled).

### Ensure bucket index is enabled

The bucket index reduces the number of API calls to the storage bucket and, when enabled, the querier is up and running immediately after the startup (no need to run an initial bucket scan). Ensure [bucket index](./bucket-index.md) is enabled for the querier.

### Avoid querying non compacted blocks

When running Cortex blocks storage cluster at scale, querying non compacted blocks may be inefficient for two reasons:

1. Non compacted blocks contain duplicated samples (as effect of the ingested samples replication)
2. Overhead introduced querying many small indexes

Because of this, we would suggest to avoid querying non compacted blocks. In order to do it, you should:

1. Run the [compactor](./compactor.md)
1. Configure queriers `-querier.query-store-after` large enough to give compactor enough time to compact newly uploaded blocks (_see below_)
1. Configure queriers `-querier.query-ingesters-within` equal to `-querier.query-store-after` plus 5m (5 minutes is just a delta to query the boundary both from ingesters and queriers)
1. Configure ingesters `-blocks-storage.tsdb.retention-period` at least as `-querier.query-ingesters-within`
1. Lower `-blocks-storage.bucket-store.ignore-deletion-marks-delay` to 1h, otherwise non compacted blocks could be queried anyway, even if their compacted replacement is available

#### How to estimate `-querier.query-store-after`

The `-querier.query-store-after` should be set to a duration large enough to give compactor enough time to compact newly uploaded blocks, and queriers and store-gateways to discover and sync newly compacted blocks.

The following diagram shows all the timings involved in the estimation. This diagram should be used only as a template and you're expected to tweak the assumptions based on real measurements in your Cortex cluster. In this example, the following assumptions have been done:

- An ingester takes up to 30 minutes to upload a block to the storage
- The compactor takes up to 3 hours to compact 2h blocks shipped from all ingesters
- Querier and store-gateways take up to 15 minutes to discover and load a new compacted block

Given these assumptions, in the worst case scenario it would take up to 6h and 45m since when a sample has been ingested until that sample has been appended to a block flushed to the storage and that block has been [vertically compacted](./compactor.md) with all other overlapping 2h blocks shipped from ingesters.

![Avoid querying non compacted blocks](/images/blocks-storage/avoid-querying-non-compacted-blocks.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

## Store-gateway

### Ensure caching is enabled

The store-gateway heavily relies on caching both to speed up the queries and to reduce the number of API calls to the storage bucket. Ensure [caching](./store-gateway.md#caching) is properly configured and [properly scaled](#ensure-memcached-is-properly-scaled).

### Ensure bucket index is enabled

The bucket index reduces the number of API calls to the storage bucket and the startup time of the store-gateway. Ensure [bucket index](./bucket-index.md) is enabled for the store-gateway.

### Ensure a high number of max open file descriptors

The store-gateway stores each block’s index-header on the local disk and loads it via mmap. This means that the store-gateway keeps a file descriptor open for each loaded block. If your Cortex cluster has many blocks in the bucket, the store-gateway may hit the **`file-max` ulimit** (maximum number of open file descriptions by a process); in such case, we recommend increasing the limit on your system or running more store-gateway instances with blocks sharding enabled.

The rule of thumb is that a production system shouldn't have the `file-max` ulimit below `65536`, but higher values are recommended (eg. `1048576`).

## Compactor

### Ensure the compactor has enough disk space

The compactor generally needs a lot of disk space in order to download source blocks from the bucket and store the compacted block before uploading it to the storage. Please refer to [Compactor disk utilization](./compactor.md#compactor-disk-utilization) for more information about how to do capacity planning.

### Ensure deletion marks migration is disabled after first run

Cortex 1.7.0 introduced a change to store block deletion marks in a per-tenant global `markers/` location in the object store. If your Cortex cluster has been created with a Cortex version older than 1.7.0, the compactor needs to migrate deletion marks to the global location.

The migration is a one-time operation run at compactor startup. Running it at every compactor startup is a waste of resources and increases the compactor startup time so, once it successfully run once, you should disable it via `-compactor.block-deletion-marks-migration-enabled=false` (or its respective YAML config option).

You can see that the initial migration is done by looking for the following message in the compactor's logs:
`msg="migrated block deletion marks to the global markers location"`.

## Caching

### Ensure memcached is properly scaled

The rule of thumb to ensure memcached is properly scaled is to make sure evictions happen infrequently. When that's not the case and they affect query performances, the suggestion is to scale out the memcached cluster adding more nodes or increasing the memory limit of existing ones.

We also recommend to run a different memcached cluster for each cache type (metadata, index, chunks). It's not required, but suggested to not worry about the effect of memory pressure on a cache type against others.
