---
title: "Caching"
linkTitle: "Caching"
weight: 4
slug: caching
---

**Warning: the chunks storage is deprecated. You're encouraged to use the [blocks storage](../blocks-storage/_index.md).**

Correctly configured caching is important for a production-ready Cortex cluster.
Cortex has many opportunities for using caching to accelerate queries and reduce cost.  Cortex can use a cache for:

* The results of a whole query

And for the chunk storage:

* Individual chunks
* Index lookups for one label on one day
* Reducing duplication of writes.

This doc aims to describe what each cache does, how to configure them and how to tune them.

## Cortex Caching Options

Cortex can use various different technologies for caching - Memcached, Redis or an in-process FIFO cache.
The recommended caching technology for production workloads is [Memcached](https://memcached.org/).
Using Memcached in your Cortex install means results from one process can be re-used by another.
In-process caching can cut fetch times slightly and reduce the load on Memcached, but can only be used by a single process.

If multiple caches are enabled for each caching opportunities, they will be tiered – writes will go to all caches, but reads will first go to the in-memory FIFO cache, then memcached, then redis.

### Memcached

For small deployments you can use a single memcached cluster for all the caching opportunities – the keys do not collide.

For large deployments we recommend separate memcached deployments for each of the caching opportunities, as this allows more sophisticated sizing, monitoring and configuration of each cache.
For help provisioning and monitoring memcached clusters using [tanka](https://github.com/grafana/tanka), see the [memcached jsonnet module](https://github.com/grafana/jsonnet-libs/tree/master/memcached) and the [memcached-mixin](https://github.com/grafana/jsonnet-libs/tree/master/memcached-mixin).

Cortex uses DNS SRV records to find the various memcached servers in a cluster.
You should ensure your memcached servers are not behind any kind of load balancer.
If deploying Cortex on Kubernetes, Cortex should be pointed at a memcached [headless service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services).

The flags used to configure memcached are common for each caching caching opportunity, differentiated by a prefix:

```
-<prefix>.cache.write-back-buffer int
    How many chunks to buffer for background write back. (default 10000)
-<prefix>.cache.write-back-goroutines int
    How many goroutines to use to write back to memcache. (default 10)
-<prefix>.memcached.batchsize int
    How many keys to fetch in each batch.
-<prefix>.memcached.consistent-hash
    Use consistent hashing to distribute to memcache servers.
-<prefix>.memcached.expiration duration
    How long keys stay in the memcache.
-<prefix>.memcached.hostname string
    Hostname for memcached service to use when caching chunks. If empty, no memcached will be used.
-<prefix>.memcached.max-idle-conns int
    Maximum number of idle connections in pool. (default 16)
-<prefix>.memcached.parallelism int
    Maximum active requests to memcache. (default 100)
-<prefix>.memcached.service string
    SRV service used to discover memcache servers. (default "memcached")
-<prefix>.memcached.timeout duration
    Maximum time to wait before giving up on memcached requests. (default 100ms)
-<prefix>.memcached.update-interval duration
    Period with which to poll DNS for memcache servers. (default 1m0s)
```

See the [`memcached_config`](../configuration/config-file-reference.md#memcached_config) and [`memcached_client_config`](../configuration/config-file-reference.md#memcached_client_config) documentation if you use a config file with Cortex.

### FIFO Cache (Experimental)

The FIFO cache is an in-memory, in-process (non-shared) cache that uses a First-In-First-Out (FIFO) eviction strategy.
The FIFO cache is useful for simple scenarios where deploying an additional memcached server is too much work, such as when experimenting with the Query Frontend.
The FIFO cache can also be used in front of Memcached to reduce latency for commonly accessed keys.
The FIFO cache stores a fixed number of entries, and therefore it’s memory usage depends on the caches value’s size.

To enable the FIFO cache, use the following flags:

```
-<prefix>.cache.enable-fifocache
    Enable in-memory cache.
-<prefix>.fifocache.duration duration
    The expiry duration for the cache.
-<prefix>.fifocache.max-size-bytes int
    Maximum memory size of the cache.
-<prefix>.fifocache.max-size-items int
    Maximum number of entries in the cache.
```

See [`fifo_cache_config` documentation](../configuration/config-file-reference.md#fifo-cache-config) if you use a config file with Cortex.


### Redis (Experimental)

You can also use [Redis](https://redis.io/) for out-of-process caching; this is a relatively new addition to Cortex and is under active development.

```
-<prefix>.redis.endpoint string
    Redis endpoint to use when caching chunks. If empty, no redis will be used.
    For Redis Server - Redis service endpoint
    For Redis Cluster - comma-separated list of Redis node's endpoints
    For Redis Sentinel - comma-separated list of Redis Sentinel endpoints
-<prefix>.redis.master-name
    Redis Sentinel master group name.
    An empty string for Redis Server or Redis Cluster
-<prefix>.redis.tls-enabled
    Enable connecting to redis with TLS.
-<prefix>.redis.tls-insecure-skip-verify
    Skip validating server certificate.
-<prefix>.redis.expiration duration
    How long keys stay in the redis.
-<prefix>.redis.db int
    Database index. (default 0)
-<prefix>.redis.pool-size int
    Maximum number of socket connections in pool.
-<prefix>.redis.password value
    Password to use when connecting to redis.
-<prefix>.redis.timeout duration
    Maximum time to wait before giving up on redis requests. (default 100ms)
-<prefix>.redis.idle-timeout duration
    Amount of time after which client closes idle connections.
-<prefix>.redis.max-connection-age duration
    Amount of time after which client closes connections.
```

See [`redis_config` documentation](../configuration/config-file-reference.md#redis-config) if you use a config file with Cortex.

## Cortex Caching Opportunities

### Chunks Cache

The chunk cache stores immutable compressed chunks.
The cache is used by queries to reduce load on the chunk store.
These are typically a few KB in size, and depend mostly on the duration and encoding of your chunks.
The chunk cache is a write-through cache - chunks are written to the cache as they are flushed to the chunk store.  This ensures the cache always contains the most recent chunks.
Items stay in the cache indefinitely.

The chunk cache should be configured on the **ingester**, **querier** and **ruler** using the flags with the prefix `-store.chunks-cache`.

It is best practice to ensure the chunk cache is big enough to accommodate at least 24 hours of chunk data.
You can use the following query (from the [cortex-mixin](https://github.com/grafana/cortex-jsonnet)) to estimate the required number of memcached replicas:

```promql
// 4 x in-memory series size = 24hrs of data.
    (
        4 *
        sum by(cluster, namespace) (
                cortex_ingester_memory_series{job=~".+/ingester"}
            *
                cortex_ingester_chunk_size_bytes_sum{job=~".+/ingester"}
            /
                cortex_ingester_chunk_size_bytes_count{job=~".+/ingester"}
        )
        / 1e9
    )
>
    (
        sum by (cluster, namespace) (memcached_limit_bytes{job=~".+/memcached"}) / 1e9
    )
```

### Index Read Cache

The index read cache stores entire rows from the inverted label index.
The cache is used by queries to reduce load on the index.
These are typically only a few KB in size, but can grow up to many MB for very high cardinality metrics.
The index read cache is populated when there is a cache miss.

The index read cache should be configured on the **querier** and **ruler**, using the flags with the `-store.index-cache-read` prefix.

### Query Results Cache

The query results cache contains protobuf & snappy encoded query results.
These query results can potentially be very large, and as such the maximum value size in memcached should be increased beyond the default `1M`.
The cache is populated when there is a cache miss.
Items stay in the cache indefinitely.

The query results cache should be configured on the **query-frontend** using flags with `-frontend` prefix:

- `-frontend.memcached.*` flags to use Memcached backend
- `-frontend.redis.*` flags to use Redis backend
- `-frontend.fifocache.*` and `-frontend.cache.enable-fifocache` flags to use the per-process in-memory cache (not shared across multiple query-frontend instances)

Please keep in mind to also enable `-querier.cache-results=true` and configure `-querier.split-queries-by-interval=24h` (`24h` is a good starting point).


### Index Write Cache

The index write cache is used to avoid re-writing index and chunk data which has already been stored in the back-end database, aka “deduplication”.
This can reduce write load on your backend-database by around 12x.

You should not use in-process caching for the index write cache - most of the deduplication comes from replication between ingesters.

The index write cache contains row and column keys written to the index.
If an entry is in the index write cache it will not be written to the index.
As such, entries are only written to the index write cache _after_ being successfully written to the index.
Data stays in the index indefinitely or until it is evicted by newer entries.

The index write cache should be configures on the **ingesters** using flags with the `-store.index-cache-write` prefix.
