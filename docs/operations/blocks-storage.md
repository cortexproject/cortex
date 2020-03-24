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
* [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)
* [Local Filesystem](https://thanos.io/storage.md/#filesystem) (single node only)

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

### Series sharding and replication

The series sharding and replication doesn't change based on the storage engine, so the general overview provided by the "[Cortex architecture](../architecture.md)" documentation applies to the blocks storage as well.

It's important to note that - differently than the [chunks storage](../architecture.md#chunks-storage-default) - time series are effectively written N times to the long-term storage, where N is the replication factor (typically 3). This may lead to a storage utilization N times more than the chunks storage, but is actually mitigated by the [compactor](#compactor) service (see "vertical compaction").

### Compactor

The **compactor** is an optional - but highly recommended - service which compacts multiple Blocks of a given tenant into a single optimized larger Block. The compactor has two main benefits:

1. Vertically compact Blocks uploaded by all ingesters for the same time range
2. Horizontally compact Blocks with small time ranges into a single larger Block

The **vertical compaction** compacts all the Blocks of a tenant uploaded by any ingester for the same Block range period (defaults to 2 hours) into a single Block, de-duplicating samples that are originally written to N Blocks as effect of the replication.

The **horizontal compaction** triggers after the vertical compaction and compacts several Blocks belonging to adjacent small range periods (2 hours) into a single larger Block. Despite the total block chunks size doesn't change after this compaction, it may have a significative impact on the reduction of the index size and its index header kept in memory by queriers.

The compactor is **stateless**.

#### Compactor sharding

The compactor optionally supports sharding. When sharding is enabled, multiple compactor instances can coordinate to split the workload and shard blocks by tenant. All the blocks of a tenant are processed by a single compactor instance at any given time, but compaction for different tenants may simultaneously run on different compactor instances.

Whenever the pool of compactors increase or decrease (ie. following up a scale up/down), tenants are resharded across the available compactor instances without any manual intervention. Compactors coordinate via the Cortex [hash ring](../architecture.md#the-hash-ring).

#### Compactor HTTP endpoints

- `GET /compactor/ring`<br />
  Displays the status of the compactors ring, including the tokens owned by each compactor and an option to remove (forget) instances from the ring.

## Index cache

The querier supports a cache to speed up postings and series lookups from TSDB blocks indexes. Two backends are supported:

- `inmemory`
- `memcached`

### In-memory index cache

The `inmemory` index cache is **enabled by default** and its max size can be configured through the flag `-experimental.tsdb.bucket-store.index-cache.inmemory.max-size-bytes` (or config file). The trade-off of using the in-memory index cache is:

- Pros: zero latency
- Cons: increased querier memory usage, not shared across multiple querier replicas

### Memcached index cache

The `memcached` index cache allows to use [Memcached](https://memcached.org/) as cache backend. This cache backend is configured using `-experimental.tsdb.bucket-store.index-cache.backend=memcached` and requires the Memcached server(s) addresses via `-experimental.tsdb.bucket-store.index-cache.memcached.addresses` (or config file). The addresses are resolved using the [DNS service provider](../configuration/arguments.md#dns-service-discovery).

The trade-off of using the Memcached index cache is:

- Pros: can scale beyond a single node memory (Memcached cluster), shared across multiple querier instances
- Cons: higher latency in the cache round trip compared to the in-memory one

The Memcached client uses a jump hash algorithm to shard cached entries across a cluster of Memcached servers. For this reason, you should make sure memcached servers are **not** behind any kind of load balancer and their address is configured so that servers are added/removed to the end of the list whenever a scale up/down occurs.

For example, if you're running Memcached in Kubernetes, you may:

1. Deploy your Memcached cluster using a [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
2. Create an [headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) for Memcached StatefulSet
3. Configure the Cortex's Memcached client address using the `dnssrvnoa+` [service discovery](../configuration/arguments.md#dns-service-discovery)

## Configuration

The general [configuration documentation](../configuration/_index.md) also applied to a Cortex cluster running the blocks storage, with few differences:

- [`storage_config`](#storage-config)
- [`tsdb_config`](#tsdb-config)
- [`compactor_config`](#compactor-config)

### `storage_config`

The `storage_config` configures the storage engine.

```yaml
storage:
  # The storage engine to use. Use "tsdb" for the blocks storage.
  # CLI flag: -store.engine
  engine: tsdb
```

### `tsdb_config`

The `tsdb_config` configures the experimental blocks storage.

```yaml
tsdb:
  # Local directory to store TSDBs in the ingesters.
  # CLI flag: -experimental.tsdb.dir
  [dir: <string> | default = "tsdb"]

  # TSDB blocks range period.
  # CLI flag: -experimental.tsdb.block-ranges-period
  [block_ranges_period: <list of duration> | default = 2h0m0s]

  # TSDB blocks retention in the ingester before a block is removed. This should
  # be larger than the block_ranges_period and large enough to give queriers
  # enough time to discover newly uploaded blocks.
  # CLI flag: -experimental.tsdb.retention-period
  [retention_period: <duration> | default = 6h0m0s]

  # How frequently the TSDB blocks are scanned and new ones are shipped to the
  # storage. 0 means shipping is disabled.
  # CLI flag: -experimental.tsdb.ship-interval
  [ship_interval: <duration> | default = 1m0s]

  # Maximum number of tenants concurrently shipping blocks to the storage.
  # CLI flag: -experimental.tsdb.ship-concurrency
  [ship_concurrency: <int> | default = 10]

  # Backend storage to use. Supported backends are: s3, gcs, azure, filesystem.
  # CLI flag: -experimental.tsdb.backend
  [backend: <string> | default = "s3"]

  bucket_store:
    # Directory to store synchronized TSDB index headers.
    # CLI flag: -experimental.tsdb.bucket-store.sync-dir
    [sync_dir: <string> | default = "tsdb-sync"]

    # How frequently scan the bucket to look for changes (new blocks shipped by
    # ingesters and blocks removed by retention or compaction). 0 disables it.
    # CLI flag: -experimental.tsdb.bucket-store.sync-interval
    [sync_interval: <duration> | default = 5m0s]

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

    # Number of Go routines to use when syncing block meta files from object
    # storage per tenant.
    # CLI flag: -experimental.tsdb.bucket-store.meta-sync-concurrency
    [meta_sync_concurrency: <int> | default = 20]

    # Whether the bucket store should use the binary index header. If false, it
    # uses the JSON index header.
    # CLI flag: -experimental.tsdb.bucket-store.binary-index-header-enabled
    [binary_index_header_enabled: <boolean> | default = true]

    # Minimum age of a block before it's being read. Set it to safe value (e.g
    # 30m) if your object storage is eventually consistent. GCS and S3 are
    # (roughly) strongly consistent.
    # CLI flag: -experimental.tsdb.bucket-store.consistency-delay
    [consistency_delay: <duration> | default = 0s]

    index_cache:
      # The index cache backend type. Supported values: inmemory, memcached.
      # CLI flag: -experimental.tsdb.bucket-store.index-cache.backend
      [backend: <string> | default = "inmemory"]

      inmemory:
        # Maximum size in bytes of in-memory index cache used to speed up blocks
        # index lookups (shared between all tenants).
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.inmemory.max-size-bytes
        [max_size_bytes: <int> | default = 1073741824]

      memcached:
        # Comma separated list of memcached addresses. Supported prefixes are:
        # dns+ (looked up as an A/AAAA query), dnssrv+ (looked up as a SRV
        # query, dnssrvnoa+ (looked up as a SRV query, with no A/AAAA lookup
        # made after that).
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.addresses
        [addresses: <string> | default = ""]

        # The socket read/write timeout.
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.timeout
        [timeout: <duration> | default = 100ms]

        # The maximum number of idle connections that will be maintained per
        # address.
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-idle-connections
        [max_idle_connections: <int> | default = 16]

        # The maximum number of concurrent asynchronous operations can occur.
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-async-concurrency
        [max_async_concurrency: <int> | default = 50]

        # The maximum number of enqueued asynchronous operations allowed.
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-async-buffer-size
        [max_async_buffer_size: <int> | default = 10000]

        # The maximum number of concurrent connections running get operations.
        # If set to 0, concurrency is unlimited.
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-get-multi-concurrency
        [max_get_multi_concurrency: <int> | default = 100]

        # The maximum number of keys a single underlying get operation should
        # run. If more keys are specified, internally keys are splitted into
        # multiple batches and fetched concurrently, honoring the max
        # concurrency. If set to 0, the max batch size is unlimited.
        # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-get-multi-batch-size
        [max_get_multi_batch_size: <int> | default = 0]

  # How frequently does Cortex try to compact TSDB head. Block is only created
  # if data covers smallest block range. Must be greater than 0 and max 5
  # minutes.
  # CLI flag: -experimental.tsdb.head-compaction-interval
  [head_compaction_interval: <duration> | default = 1m0s]

  # Maximum number of tenants concurrently compacting TSDB head into a new block
  # CLI flag: -experimental.tsdb.head-compaction-concurrency
  [head_compaction_concurrency: <int> | default = 5]

  # The number of shards of series to use in TSDB (must be a power of 2).
  # Reducing this will decrease memory footprint, but can negatively impact
  # performance.
  # CLI flag: -experimental.tsdb.stripe-size
  [stripe_size: <int> | default = 16384]

  # limit the number of concurrently opening TSDB's on startup
  # CLI flag: -experimental.tsdb.max-tsdb-opening-concurrency-on-startup
  [max_tsdb_opening_concurrency_on_startup: <int> | default = 10]

  s3:
    # S3 endpoint without schema
    # CLI flag: -experimental.tsdb.s3.endpoint
    [endpoint: <string> | default = ""]

    # S3 bucket name
    # CLI flag: -experimental.tsdb.s3.bucket-name
    [bucket_name: <string> | default = ""]

    # S3 secret access key
    # CLI flag: -experimental.tsdb.s3.secret-access-key
    [secret_access_key: <string> | default = ""]

    # S3 access key ID
    # CLI flag: -experimental.tsdb.s3.access-key-id
    [access_key_id: <string> | default = ""]

    # If enabled, use http:// for the S3 endpoint instead of https://. This
    # could be useful in local dev/test environments while using an
    # S3-compatible backend storage, like Minio.
    # CLI flag: -experimental.tsdb.s3.insecure
    [insecure: <boolean> | default = false]

  gcs:
    # GCS bucket name
    # CLI flag: -experimental.tsdb.gcs.bucket-name
    [bucket_name: <string> | default = ""]

    # JSON representing either a Google Developers Console
    # client_credentials.json file or a Google Developers service account key
    # file. If empty, fallback to Google default logic.
    # CLI flag: -experimental.tsdb.gcs.service-account
    [service_account: <string> | default = ""]

  azure:
    # Azure storage account name
    # CLI flag: -experimental.tsdb.azure.account-name
    [account_name: <string> | default = ""]

    # Azure storage account key
    # CLI flag: -experimental.tsdb.azure.account-key
    [account_key: <string> | default = ""]

    # Azure storage container name
    # CLI flag: -experimental.tsdb.azure.container-name
    [container_name: <string> | default = ""]

    # Azure storage endpoint suffix without schema. The account name will be
    # prefixed to this value to create the FQDN
    # CLI flag: -experimental.tsdb.azure.endpoint-suffix
    [endpoint_suffix: <string> | default = ""]

    # Number of retries for recoverable errors
    # CLI flag: -experimental.tsdb.azure.max-retries
    [max_retries: <int> | default = 20]

  filesystem:
    # Local filesystem storage directory.
    # CLI flag: -experimental.tsdb.filesystem.dir
    [dir: <string> | default = ""]
```
### `compactor_config`

The `compactor_config` configures the compactor for the experimental blocks storage.

```yaml
compactor:
  # List of compaction time ranges.
  # CLI flag: -compactor.block-ranges
  [block_ranges: <list of duration> | default = 2h0m0s,12h0m0s,24h0m0s]

  # Number of Go routines to use when syncing block index and chunks files from
  # the long term storage.
  # CLI flag: -compactor.block-sync-concurrency
  [block_sync_concurrency: <int> | default = 20]

  # Number of Go routines to use when syncing block meta files from the long
  # term storage.
  # CLI flag: -compactor.meta-sync-concurrency
  [meta_sync_concurrency: <int> | default = 20]

  # Minimum age of fresh (non-compacted) blocks before they are being processed.
  # Malformed blocks older than the maximum of consistency-delay and 48h0m0s
  # will be removed.
  # CLI flag: -compactor.consistency-delay
  [consistency_delay: <duration> | default = 30m0s]

  # Data directory in which to cache blocks and process compactions
  # CLI flag: -compactor.data-dir
  [data_dir: <string> | default = "./data"]

  # The frequency at which the compaction runs
  # CLI flag: -compactor.compaction-interval
  [compaction_interval: <duration> | default = 1h0m0s]

  # How many times to retry a failed compaction during a single compaction
  # interval
  # CLI flag: -compactor.compaction-retries
  [compaction_retries: <int> | default = 3]

  # Shard tenants across multiple compactor instances. Sharding is required if
  # you run multiple compactor instances, in order to coordinate compactions and
  # avoid race conditions leading to the same tenant blocks simultaneously
  # compacted by different instances.
  # CLI flag: -compactor.sharding-enabled
  [sharding_enabled: <boolean> | default = false]

  sharding_ring:
    kvstore:
      # Backend storage to use for the ring. Supported values are: consul, etcd,
      # inmemory, multi, memberlist (experimental).
      # CLI flag: -compactor.ring.store
      [store: <string> | default = "consul"]

      # The prefix for the keys in the store. Should end with a /.
      # CLI flag: -compactor.ring.prefix
      [prefix: <string> | default = "collectors/"]

      # The consul_config configures the consul client.
      # The CLI flags prefix for this block config is: compactor.ring
      [consul: <consul_config>]

      # The etcd_config configures the etcd client.
      # The CLI flags prefix for this block config is: compactor.ring
      [etcd: <etcd_config>]

      multi:
        # Primary backend storage used by multi-client.
        # CLI flag: -compactor.ring.multi.primary
        [primary: <string> | default = ""]

        # Secondary backend storage used by multi-client.
        # CLI flag: -compactor.ring.multi.secondary
        [secondary: <string> | default = ""]

        # Mirror writes to secondary store.
        # CLI flag: -compactor.ring.multi.mirror-enabled
        [mirror_enabled: <boolean> | default = false]

        # Timeout for storing value to secondary store.
        # CLI flag: -compactor.ring.multi.mirror-timeout
        [mirror_timeout: <duration> | default = 2s]

    # Period at which to heartbeat to the ring.
    # CLI flag: -compactor.ring.heartbeat-period
    [heartbeat_period: <duration> | default = 5s]

    # The heartbeat timeout after which compactors are considered unhealthy
    # within the ring.
    # CLI flag: -compactor.ring.heartbeat-timeout
    [heartbeat_timeout: <duration> | default = 1m0s]
```

## Known issues

### Migrating from the chunks to the blocks storage

Currently, no smooth migration path is provided to migrate from chunks to blocks storage. For this reason, the blocks storage can only be enabled in new Cortex clusters.
