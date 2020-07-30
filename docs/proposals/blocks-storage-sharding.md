---
title: "Blocks storage sharding"
linkTitle: "Blocks storage sharding"
weight: 1
slug: blocks-storage-sharding
---

- Author: [Marco Pracucci](https://github.com/pracucci)
- Date: March 2020
- Status: accepted

## Problem

In Cortex, when using the experimental blocks storage, each querier internally runs the Thanos [`BucketStore`](https://github.com/thanos-io/thanos/blob/master/pkg/store/bucket.go). This means that each querier has a full view over all blocks in the long-term storage and all blocks index headers are loaded in each querier memory. The querier memory usage linearly increase with number and size of all blocks in the storage, imposing a scalability limit to the blocks storage.

In this proposal we want to solve this. In particular, we want to:

1. Shard blocks (index headers) across a pool of nodes
2. Do not compromise HA on the read path (if a node fails, queries should continue to work)
3. Do not compromise correctness (either the query result is correct or it fails)

## Proposed solution

The idea is to introduce a new Cortex service - `store-gateway` - internally running the Thanos [`BucketStore`](https://github.com/thanos-io/thanos/blob/master/pkg/store/bucket.go). At query time, a querier will run a query fetching the matching series both from ingesters and the subset of gateways holding the related blocks (based on the query time range). Blocks are replicated across the gateways in order to guarantee query results consistency and HA even in the case of a gateway instance failure.

### Ring-based sharding and replication

In order to build blocks sharding and replication, the `store-gateway` instances form a [ring](../architecture.md#the-hash-ring). Each gateway instance uses a custom [`MetaFetcherFilter`](https://github.com/thanos-io/thanos/blob/master/pkg/block/fetcher.go#L108) to filter blocks loaded on the instance itself, keeping only blocks whose `hash(block-id)` is within the tokens range assigned to the gateway instance within the ring.

Within a gateway, the blocks synchronization is triggered in two cases:

1. **Periodically**<br />
   to discover new blocks uploaded by ingesters or compactor, and delete old blocks removed due to retention or by the compactor
2. **On-demand**<br/>
   when the ring topology changes (the tokens ranges assigned to the gateway instance have changed)

It's important to outline that the sync takes time (typically will have to re-scan the bucket and download new blocks index headers) and Cortex needs to guarantee query results consistency at any given time (_see below_).

### Query execution

When a querier executes a query, it will need to fetch series both from ingesters and the store-gateway instances.

For a given query, the number of blocks to query is expected to be low, especially if the Cortex cluster is running the `query-frontend` with a `24h` query split interval. In this scenario, whatever is the client's query time range, the `query-frontend` will split the client's query into partitioned queries each with up to `24h` time range and the querier will likely hit not more than 1 block per partitioned query (except for the last 24h for which blocks may have not been compacted yet).

Given this assumption, we want to avoid sending every query to every store-gateway instance. The querier should be able to take an informed decision about the minimum subset of store-gateway instances which needs to query given a time range.

The idea is to run the [`MetaFetcher`](https://github.com/thanos-io/thanos/blob/master/pkg/block/fetcher.go#L127) also within the querier, but without any sharding filter (contrary to the store-gateway). At any given point in time, the querier knows the entire list of blocks in the storage. When the querier executes the `Select()` (or `SelectSorted()`) it does:

1. Compute the list of blocks by the query time range
2. Compute the minimum list of store-gateway instances containing the required blocks (using the information from the ring)
3. Fetch series from ingesters and the matching store-gateway instances
4. Merge and deduplicate received series
   - Optimization: can be skipped if the querier hits only 1 store-gateway

### Query results consistency

When a querier executes a query, it should guarantee that either all blocks matching the time range are queried or the query fails.

However, due to the (intentional) lack of a strong coordination between queriers and store-gateways, and the ring topology which can change any time, there's no guarantee that the blocks assigned to a store-gateway shard are effectively loaded on the store-gateway itself at any given point in time.

The idea is introduce a **consistency check in the querier**. When a store-gateway receives a request from the querier, the store-gateway includes in the response the list of block IDs currently loaded on the store-gateway itself. The querier can then merge the list of block IDs received from all store-gateway hit, and match it against the list of block IDs computed at the beginning of the query execution.

There are three possible scenarios:

1. The list match: all good
2. All the blocks known by the querier are within the list of blocks returned by store-gateway, but the store-gateway also included blocks unknown to the querier: all good (it means the store-gateways have discovered and loaded new blocks before the querier discovered them)
3. Some blocks known by the querier are **not** within the list of blocks returned by store-gateway: potential consistency issue

We want to protect from a partial results response which may occur in the case #3. However, there are some legit cases which, if not handled, would lead to frequent false positives. Given the querier and store-gateway instances independently scan the bucket at a regular interval (to find new blocks or deleted blocks), we may be in one of the following cases:

a. The querier has discovered new blocks before the store-gateway successfully discovered and loaded them
b. The store-gateway has offloaded blocks "marked for deletion" before the querier

To protect from case (a), we can exclude the blocks which have been uploaded in the last `X` time from the consistency check (same technique already used in other Thanos components). This `X` delay time is used to give the store-gateway enough time to discover and load new blocks, before the querier consider them for the consistency check. This value `X` should be greater than the `-experimental.blocks-storage.bucket-store.consistency-delay`, because we do expect the querier to consider a block for consistency check once it's reasonably safe to assume that its store-gateway already loaded it.

To protect from case (b) we need to understand how blocks are offloaded. The `BucketStore` (running within the store-gateway) offloads a block as soon as it's not returned by the `MetaFetcher`. This means we can configure the `MetaFetcher` with a [`IgnoreDeletionMarkFilter`](https://github.com/thanos-io/thanos/blob/4bd19b16a752e9ceb1836c21d4156bdeb517fe50/pkg/block/fetcher.go#L648) with a delay of `X` (could be the same value used for case (a)) and in the querier exclude the blocks which have been marked for deletion more than `X` time ago from the consistency check.

## Trade-offs

The proposed solution comes with the following trade-offs:

- A querier is not ready until it has completed an initial full scan of the bucket, downloading the `meta.json` file of every block
- A store-gateway is not ready until it has completed an initial full scan of the bucket, downloading the `meta.json` and index header of each block matching its shard
- If a querier hits 2+ store-gateways it may receive duplicated series if the 2+ store-gateways share some blocks due to the replication factor
