---
title: "Blocks storage (experimental)"
linkTitle: "Blocks storage (experimental)"
weight: 1
slug: blocks-storage
---

The blocks storage is an **experimental** Cortex storage engine based on [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/): it stores each tenant's time series into their own TSDB which write out their series to a on-disk Block (defaults to 2h block range periods). Each Block is composed by chunk files - containing the timestamp-value pairs for multiple series - and an index, which indexes metric names and labels to time series in the chunk files.

The supported backends for the blocks storage are:

* [Amazon S3](https://aws.amazon.com/s3)
* [Google Cloud Storage](https://cloud.google.com/storage/)

_Internally, this storage engine is based on [Thanos](https://thanos.io), but no Thanos knowledge is required in order to run it._

The rest of the document assumes you have read the [Cortex architecture](../architecture.md) documentation.

## How it works

When the blocks storage is used, each **ingester** creates a per-tenant TSDB and ships the TSDB Blocks - which by default are cut every 2 hours - to the long-term storage.

**Queriers** periodically iterate over the storage bucket to discover recently uploaded Blocks and - for each Block - download a subset of the block index - called "index header" - which is kept in memory and used to provide fast lookups.

### The write path

**Ingesters** receive incoming samples from the distributors. Each push request belongs to a tenant, and the ingester append the received samples to the specific per-tenant TSDB. The received samples are both kept in-memory and written to a write-ahead log (WAL) stored on the local disk and used to recover the in-memory series in case the ingester abruptly terminates. The per-tenant TSDB is lazily created in each ingester upon the first push request is received for that tenant.

The in-memory samples are periodically flushed to disk - and the WAL truncated - when a new TSDB Block is cut, which by default occurs every 2 hours. Each new Block cut is then uploaded to the long-term storage and kept in the ingester for some more time, in order to give queriers enough time to discover the new Block from the storage and download its index header.


In order to effectively use the **WAL** and being able to recover the in-memory series upon ingester abruptly termination, the WAL needs to be stored to a persistent local disk which can survive in the event of an ingester failure (ie. AWS EBS volume or GCP persistent disk when running in the cloud). For example, if you're running the Cortex cluster in Kubernetes, you may use a StatefulSet with a persistent volume claim for the ingesters.

### The read path

**Queriers** - at startup - iterate over the entire storage bucket to discover all tenants Blocks and - for each of them - download the index header. During this initial synchronization phase, a querier is not ready to handle incoming queries yet and its `/ready` readiness probe endpoint will fail.

Queriers also periodically re-iterate over the storage bucket to discover newly uploaded Blocks (by the ingesters) and find out Blocks deleted in the meanwhile, as effect of an optional retention policy.

The blocks chunks and the entire index is never fully downloaded by the queriers. In the read path, a querier lookups the series label names and values using the in-memory index header and then download the required segments of the index and chunks for the matching series directly from the long-term storage using byte-range requests.

The index header is also stored to the local disk, in order to avoid to re-download it on subsequent restarts of a querier. For this reason, it's recommended - but not required - to run the querier with a persistent local disk. For example, if you're running the Cortex cluster in Kubernetes, you may use a StatefulSet with a persistent volume claim for the queriers.

### Sharding and Replication

The series sharding and replication doesn't change based on the storage engine, so the general overview provided by the "[Cortex architecture](../architecture.md)" documentation applies to the blocks storage as well.

It's important to note that - differently than the [chunks storage](../architecture.md#chunks-storage-default) - time series are effectively written N times to the long-term storage, where N is the replication factor (typically 3). This may lead to a storage utilization N times more than the chunks storage, but is actually mitigated by the [compactor](#compactor) service (see "vertical compaction").

### Compactor

The **compactor** is an optional - but highly recommended - service which compacts multiple Blocks of a given tenant into a single optimized larger Block. The compactor has two main benefits:

1. Vertically compact Blocks uploaded by all ingesters for the same time range
2. Horizontally compact Blocks with small time ranges into a single larger Block

The **vertical compaction** compacts all the Blocks of a tenant uploaded by any ingester for the same Block range period (defaults to 2 hours) into a single Block, de-duplicating samples that are originally written to N Blocks as effect of the replication.

The **horizontal compaction** triggers after the vertical compaction and compacts several Blocks belonging to adjacent small range periods (2 hours) into a single larger Block. Despite the total block chunks size doesn't change after this compaction, it may have a significative impact on the reduction of the index size and its index header kept in memory by queriers.

The compactor is **stateless**.

## Configuration

The general [configuration documentation](../configuration/_index.md) also applied to a Cortex cluster running the blocks storage, with few differences:

- [`storage_config`](#storage-config)
- [`tsdb_config`](#tsdb-config)
- [`compactor_config`](#compactor-config)

### `storage_config`

The `storage_config` block configures the storage engine.

```yaml
storage:
  # The storage engine to use. Use "tsdb" for the blocks storage.
  # CLI flag: -store.engine
  engine: tsdb
```

### `tsdb_config`

The `tsdb_config` block configures the blocks storage engine (based on TSDB).

```yaml
tsdb:
  # Backend storage to use. Either "s3" or "gcs".
  # CLI flag: -experimental.tsdb.backend
  backend: <string>

  # Local directory to store TSDBs in the ingesters.
  # CLI flag: -experimental.tsdb.dir
  [dir: <string> | default = "tsdb"]

  # TSDB blocks range period.
  # CLI flag: -experimental.tsdb.block-ranges-period
  [ block_ranges_period: <list of duration> | default = [2h]]

  # TSDB blocks retention in the ingester before a block is removed. This
  # should be larger than the block_ranges_period and large enough to give
  # ingesters enough time to discover newly uploaded blocks.
  # CLI flag: -experimental.tsdb.retention-period
  [ retention_period: <duration> | default = 6h]

  # How frequently the TSDB blocks are scanned and new ones are shipped to
  # the storage. 0 means shipping is disabled.
  # CLI flag: -experimental.tsdb.ship-interval
  [ship_interval: <duration> | default = 1m]

  # Maximum number of tenants concurrently shipping blocks to the storage.
  # CLI flag: -experimental.tsdb.ship-concurrency
  [ship_concurrency: <int> | default = 10]

  # The bucket store configuration applies to queriers and configure how queriers
  # iteract with the long-term storage backend.
  bucket_store:
    # Directory to store synched TSDB index headers.
    # CLI flag: -experimental.tsdb.bucket-store.sync-dir
    [sync_dir: <string> | default = "tsdb-sync"]

    # How frequently scan the bucket to look for changes (new blocks shipped by
    # ingesters and blocks removed by retention or compaction).
    # CLI flag: -experimental.tsdb.bucket-store.sync-interval
    [sync_interval: <duration> | default = 5m]

    # Size - in bytes - of a per-tenant in-memory index cache used to speed up
    # blocks index lookups.
    # CLI flag: -experimental.tsdb.bucket-store.index-cache-size-bytes
    [ index_cache_size_bytes: <int> | default = 262144000]

    # Max size - in bytes - of a per-tenant chunk pool, used to reduce memory
    # allocations.
    # CLI flag: -experimental.tsdb.bucket-store.max-chunk-pool-bytes
    [max_chunk_pool_bytes: <int> | default = 2147483648]

    # Max number of samples per query when loading series from the long-term
    # storage. 0 disables the limit.
    # CLI flag: -experimental.tsdb.bucket-store.max-sample-count
    [max_sample_count: <int> | default = 0]
    
    # Max number of concurrent queries to execute against the long-term storage
    # on a per-tenant basis.
    # CLI flag: -experimental.tsdb.bucket-store.max-concurrent
    [max_concurrent: <int> | default = 20]

    # Maximum number of concurrent tenants synching blocks.
    # CLI flag: -experimental.tsdb.bucket-store.tenant-sync-concurrency
    [tenant_sync_concurrency: <int> | default = 10]

    # Maximum number of concurrent blocks synching per tenant.
    # CLI flag: -experimental.tsdb.bucket-store.block-sync-concurrency
    [block_sync_concurrency: <int> | default = 20]

  # Configures the S3 storage backend.
  # Required only when "s3" backend has been selected.
  s3:
    # S3 bucket name.
    # CLI flag: -experimental.tsdb.s3.bucket-name string
    bucket_name: <string>

    # S3 access key ID. If empty, fallbacks to AWS SDK default logic.
    # CLI flag: -experimental.tsdb.s3.access-key-id string
    [access_key_id: <string>]

    # S3 secret access key. If empty, fallbacks to AWS SDK default logic.
    # CLI flag: -experimental.tsdb.s3.secret-access-key string
    [secret_access_key: <string>]

    # S3 endpoint without schema. By defaults it use the AWS S3 endpoint.
    # CLI flag: -experimental.tsdb.s3.endpoint
    [endpoint: <string> | default = ""]

    # If enabled, use http:// for the S3 endpoint instead of https://.
    # This could be useful in local dev/test environments while using
    # an S3-compatible backend storage, like Minio.
    # CLI flag: -experimental.tsdb.s3.insecure
    [insecure: <boolean> | default = false]

  # Configures the GCS storage backend.
  # Required only when "gcs" backend has been selected.
  gcs:
    # GCS bucket name.
    # CLI flag: -experimental.tsdb.gcs.bucket-name
    bucket_name: <string>

    # JSON representing either a Google Developers Console client_credentials.json
    # file or a Google Developers service account key file. If empty, fallbacks to
    # Google SDK default logic.
    # CLI flag: -experimental.tsdb.gcs.service-account string
    [ service_account: <string>]
```

### `compactor_config`

The `compactor_config` block configures the optional compactor service.

```yaml
compactor:
    # List of compaction time ranges.
    # CLI flag: -compactor.block-ranges
    [block_ranges: <list of duration> | default = [2h,12h,24h]]

    # Number of Go routines to use when syncing block metadata from the long-term
    # storage.
    # CLI flag: -compactor.block-sync-concurrency
    [block_sync_concurrency: <int> | default = 20]

    # Minimum age of fresh (non-compacted) blocks before they are being processed,
    # in order to skip blocks that are still uploading from ingesters. Malformed
    # blocks older than the maximum of consistency-delay and 30m will be removed.
    # CLI flag: -compactor.consistency-delay
    [consistency_delay: <duration> | default = 30m]

    # Directory in which to cache downloaded blocks to compact and to store the
    # newly created block during the compaction process.
    # CLI flag: -compactor.data-dir
    [data_dir: <string> | default = "./data"]

    # The frequency at which the compactor should look for new blocks eligible for
    # compaction and trigger their compaction.
    # CLI flag: -compactor.compaction-interval
    [compaction_interval: <duration> | default = 1h]

    # How many times to retry a failed compaction during a single compaction
    # interval.
    # CLI flag: -compactor.compaction-retries
    [compaction_retries: <int> | default = 3]
```

## Known issues

### Horizontal scalability

The compactor currently doesn't support horizontal scalability and only 1 replica of the compactor should run at any given time within a Cortex cluster.

### Migrating from the chunks to the blocks storage

Currently, no smooth migration path is provided to migrate from chunks to blocks storage. For this reason, the blocks storage can only be enabled in new Cortex clusters.
