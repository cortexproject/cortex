---
title: "Bucket Index"
linkTitle: "Bucket Index"
weight: 5
slug: bucket-index
---

The bucket index is a **per-tenant file containing the list of blocks and block deletion marks** in the storage. The bucket index itself is stored in the backend object storage, is periodically updated by the compactor and used by queriers to discover blocks in the storage.

The bucket index usage is **optional** and can be enabled via `-blocks-storage.bucket-store.bucket-index.enabled=true` (or its respective YAML config option).

## Benefits

The [querier](./querier.md) needs to have an almost up-to-date view over the entire storage bucket, in order to find the right blocks to lookup at query time. Because of this, querier needs to periodically scan the bucket to look for new blocks uploaded by ingester or compactor, and blocks deleted (or marked for deletion) by compactor.

When this bucket index is enabled, the querier periodically look up the per-tenant bucket index instead of scanning the bucket via "list objects" operations. This brings few benefits:

1. Reduced number of API calls to the object storage by querier
2. No "list objects" storage API calls done by querier
3. The [querier](./querier.md) is up and running immediately after the startup (no need to run an initial bucket scan)

## Structure of the index

The `bucket-index.json.gz` contains:

- **`blocks`**<br />
  List of complete blocks of a tenant, including blocks marked for deletion (partial blocks are excluded from the index).
- **`block_deletion_marks`**<br />
  List of block deletion marks.
- **`updated_at`**<br />
  Unix timestamp (seconds precision) of when the index has been updated (written in the storage) the last time.

## How it gets updated

The [compactor](./compactor.md) periodically scans the bucket and uploads an updated bucket index to the storage. The frequency at which the bucket index is updated can be configured via `-compactor.cleanup-interval`.

Despite using the bucket index is optional, the index itself is built and updated by the compactor even if `-blocks-storage.bucket-store.bucket-index.enabled` has **not** been enabled. This is intentional, so that once a Cortex cluster operator decides to enable the bucket index in a live cluster, the bucket index for any tenant is already existing and query results consistency is guaranteed. The overhead introduced by keeping the bucket index updated is expected to be non significative.

## How it's used by the querier

The [querier](./querier.md), at query time, checks whether the bucket index for the tenant has already been loaded in memory. If not, the querier downloads it from the storage and cache it in memory.

_Given it's a small file, lazy downloading it doesn't significantly impact on first query performances, but allows to get a querier up and running without pre-downloading every tenant's bucket index. Moreover, if the [metadata cache](./querier.md#metadata-cache) is enabled, the bucket index will be cached for a short time in a shared cache, reducing the actual latency and number of API calls to the object storage in case multiple queriers will fetch the same tenant's bucket index in a short time._

![Querier - Bucket index](/images/blocks-storage/bucket-index-querier-logic.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

While in-memory, a background process will keep it **updated at periodic intervals**, so that subsequent queries from the same tenant to the same querier instance will use the cached (and periodically updated) bucket index. There are two config options involved:

- `-blocks-storage.bucket-store.bucket-index.update-on-stale-interval`<br />
  This option configures how frequently a cached bucket index should be refreshed.
- `-blocks-storage.bucket-store.bucket-index.update-on-error-interval`<br />
  If downloading a bucket index fails, the failure is cached for a short time in order to avoid hammering the backend storage. This option configures how frequently a bucket index, which previously failed to load, should be tried to load again.

If a bucket index is unused for a long time (configurable via `-blocks-storage.bucket-store.bucket-index.idle-timeout`), e.g. because that querier instance is not receiving any query from the tenant, the querier will offload it, stopping to keep it updated at regular intervals. This is particularly for tenants which are resharded to different queriers when [shuffle sharding](../guides/shuffle-sharding.md) is enabled.

Finally, the querier, at query time, checks how old is a bucket index (based on its `updated_at`) and fail a query if its age is older than `-blocks-storage.bucket-store.bucket-index.max-stale-period`. This circuit breaker is used to ensure queriers will not return any partial query results due to a stale view over the long-term storage.
