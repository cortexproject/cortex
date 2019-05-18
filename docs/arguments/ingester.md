# Ingester Arguments

#### Ring Lifecycler Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.num-tokens` | Number of tokens for each ingester. | `128` |
| `-ingester.heartbeat-period` | Period at which to heartbeat to consul. | `5s` |
| `-ingester.join-after` | Period to wait for a claim from another ingester; will join automatically after this. | `0s` |
| `-ingester.min-ready-duration` | Minimum duration to wait before becoming ready. This is to work around race conditions with ingesters exiting and updating the ring. | `1m` |
| `-ingester.claim-on-rollout` | Send chunks to PENDING ingesters on exit. | `false` |
| `-ingester.normalise-tokens` | Write out "normalised" tokens to the ring. Normalised tokens consume less memory to encode and decode; as the ring is unmarshalled regularly, this significantly reduces memory usage of anything that watches the ring. Before enabling, rollout a version of Cortex that supports normalised token for all jobs that interact with the ring, then rollout with this flag set to `true` on the ingesters. The new ring code can still read and write the old ring format, so is backwards compatible. | `false` |
| `-ingester.final-sleep` | Duration to sleep for before exiting, to ensure metrics are scraped. | `30s` |
| `-ingester.interface` | Name of network interface to read address from. | (no default) |
| `-ingester.addr` | IP address to advertise in consul. | (empty string) |
| `-ingester.port` | Port to advertise in consul (defaults to server.grpc-listen-port). | `0` |
| `-ingester.id` | ID to register into consul. | (OS hostname) |

#### General Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-event.sample-rate` | How often to sample observability events (0 = never). | `0` |
| `-ingester.max-transfer-retries` | Number of times to try and transfer chunks before falling back to flushing. | `10` |
| `-ingester.flush-period` | Period with which to attempt to flush chunks. | `1m` |
| `-ingester.retain-period` | Period chunks will remain in memory after flushing. | `5m` |
| `-ingester.flush-op-timeout` | Timeout for individual flush operations. | `1m` |
| `-ingester.max-chunk-idle` | Maximum chunk idle time before flushing. | `5m` |
| `-ingester.max-chunk-age` | How long chunks can stay in memory before they must be flushed into the underlying database. | `12h` |
| `-ingester.chunk-age-jitter` | Range of time to subtract from MaxChunkAge to spread out flushes. | `20m` |
| `-ingester.concurrent-flushes` | Number of concurrent goroutines flushing to the underlying time series database (DynamoDB, BigTable, etc). | `50` |
| `-ingester.rate-update-period` | Period with which to update the per-user ingestion rates. | `15s` |

#### Server Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-server.http-listen-port` | HTTP server listen port. | `80` |
| `-server.grpc-listen-port` | gRPC server listen port. | `9095` |
| `-server.register-instrumentation` | Register the intrumentation handlers (/metrics etc). | `true` |
| `-server.graceful-shutdown-timeout` | Timeout for graceful shutdowns | `30s` |
| `-server.http-read-timeout` | Read timeout for HTTP server | `30s` |
| `-server.http-write-timeout` | Write timeout for HTTP server | `30s` |
| `-server.http-idle-timeout` | Idle timeout for HTTP server | `120s` |
| `-server.grpc-max-recv-msg-size-bytes` | Limit on the size of a gRPC message this server can receive (bytes). | `4*1024*1024` |
| `-server.grpc-max-send-msg-size-bytes` | Limit on the size of a gRPC message this server can send (bytes). | `4*1024*1024` |
| `-server.grpc-max-concurrent-streams` | Limit on the number of concurrent streams for gRPC calls (0 = unlimited) | `100` |
| `-server.path-prefix` | Base path to serve all API routes from (e.g. /v1/) | (empty string) |
| `-log.level` | Only log messages with the given severity or above. Valid levels: [debug, info, warn, error] | `info` |

#### Chunk Encoding Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.chunk-encoding` | Encoding version to use for chunks (0 = Delta, 1 = DoubleDelta, 2 = Varbit, 3 = Bigchunk). | `1` |
| `-store.fullsize-chunks` | When saving varbit chunks, pad to 1024 bytes. | `true` |
| `-store.bigchunk-size-cap-bytes` | When using bigchunk encoding, start a new bigchunk if over this size (0 = unlimited). | `0` |

#### Chunk Store Flags

Flags with the prefix `-store.index-cache-read.` represent the cache config for reading index entries.

| Flag | Description | Default |
| --- | --- | --- |
| `-store.min-chunk-age` | Minimum time between chunk update and being saved to the store. | `0` |
| `-store.index-cache-read.memcache.write-back-goroutines` | How many goroutines to use to write back to memcache. | `10` |
| `-store.index-cache-read.memcache.write-back-buffer` | How many chunks to buffer for background write back. | `10000` |
| `-store.index-cache-read.memcached.expiration` | How long keys stay in the memcache. | `0` |
| `-store.index-cache-read.memcached.batchsize` | How many keys to fetch in each batch. | `0` |
| `-store.index-cache-read.memcached.parallelism` | Maximum active requests to memcache. | `100` |
| `-store.index-cache-read.memcached.hostname` | Hostname for memcached service to use when caching chunks. If empty, no memcached will be used. | (empty string) |
| `-store.index-cache-read.memcached.service` | SRV service used to discover memcache servers. | `memcached` |
| `-store.index-cache-read.memcached.max-idle-conns` | Maximum number of idle connections in pool. | `16` |
| `-store.index-cache-read.memcached.timeout` | Maximum time to wait before giving up on memcached requests. | `100ms` |
| `-store.index-cache-read.memcached.update-interval` | Period with which to poll DNS for memcache servers. | `1m` |
| `-store.index-cache-read.diskcache.path` | Path to file used to cache chunks. | `/var/run/chunks` |
| `-store.index-cache-read.diskcache.size` | Size of file (bytes). | `1024*1024*1024` |
| `-store.index-cache-read.fifocache.size` | The number of entries to cache. | `0` |
| `-store.index-cache-read.fifocache.duration` | The expiry duration for the cache. | `0` |
| `-store.index-cache-read.cache.enable-diskcache` | Enable on-disk cache. | `false` |
| `-store.index-cache-read.cache.enable-fifocache` | Enable in-memory cache. | `false` |
| `-store.index-cache-read.cache.default-validity` | The default validity of entries for caches unless overridden. | `0` |
| `-store.cache-lookups-older-than` | Cache index entries older than this period. 0 to disable. | `0` |

**AWS Config**

If you want to use AWS Products such as S3 and/or DynamoDB these flags can be set to configure the storage.

| Flag | Description | Default |
| --- | --- | --- |
| `-dynamodb.url` | DynamoDB endpoint URL with escaped Key and Secret encoded. If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<table-name> to use a mock in-memory implementation. | (no default) |
| `-dynamodb.api-limit` | DynamoDB table management requests per second limit. | `2.0` |
| `-applicationautoscaling.url` | ApplicationAutoscaling endpoint URL with escaped Key and Secret encoded. | (no default) |
| `-dynamodb.chunk.gang.size` | Number of chunks to group together to parallelise fetches (zero to disable). | `10` |
| `-dynamodb.chunk.get.max.parallelism` | Max number of chunk-get operations to start in parallel. | `32` |
| `-dynamodb.min-backoff` | Minimum backoff time. | `100ms` |
| `-dynamodb.max-backoff` | Maximum backoff time. | `50s` |
| `-dynamodb.max-retries` | Maximum number of times to retry an operation. | `20` |
| `-dynamodb.min-backoff` | Minimum backoff time | `100ms` |
| `-s3.url` | S3 endpoint URL with escaped Key and Secret encoded. If only region is specified as a host, proper endpoint will be deduced. Use inmemory:///<bucket-name> to use a mock in-memory implementation. | (no default) |

**GCP Config**

If you want to use Google Cloud Productssuch as GCS and/or BigTable these flags can be set to configure the storage.

| Flag | Description | Default |
| --- | --- | --- |
| `-bigtable.project` | Bigtable project ID. | (no default) |
| `-bigtable.instance` | Bigtable instance ID. | (no default) |
| `-bigtable.grpc-max-recv-msg-size` | gRPC client max receive message size (bytes). | `100<<20` |
| `-bigtable.grpc-max-send-msg-size` | gRPC client max send message size (bytes). | `16<<20` |
| `-bigtable.grpc-use-gzip-compression` | Use compression when sending messages. | `false` |
| `-bigtable.grpc-client-rate-limit` | Rate limit for gRPC client; 0 means disabled. | `0` |
| `-bigtable.grpc-client-rate-limit-burst` | Rate limit burst for gRPC client. | `0` |
| `-bigtable.backoff-on-ratelimits` | Enable backoff and retry when we hit ratelimits. | `false` |
| `-bigtable.backoff-min-period` | Minimum delay when backing off. | `100ms` |
| `-bigtable.backoff-max-period` | Maximum delay when backing off. | `10s` |
| `-bigtable.backoff-retries` | Number of times to backoff and retry before failing. | `10` |
| `-gcs.bucketname` | Name of GCS bucket to put chunks in. | (empty string) |
| `-gcs.chunk-buffer-size` | The size of the buffer that GCS client for each PUT request. 0 to disable buffering. | `0` |

**Cassandra Config**

| Flag | Description | Default |
| --- | --- | --- |
| `-cassandra.addresses` | Comma-separated hostnames or ips of Cassandra instances. | (empty string) |
| `-cassandra.port` | Port that Cassandra is running on. | `9042` |
| `-cassandra.keyspace` | Keyspace to use in Cassandra. | (empty string) |
| `-cassandra.consistency` | Consistency level for Cassandra. | `QUORUM` |
| `-cassandra.replication-factor` | Replication factor to use in Cassandra. | `1` |
| `-cassandra.disable-initial-host-lookup` | Instruct the cassandra driver to not attempt to get host info from the system.peers table. | `false` |
| `-cassandra.ssl` | Use SSL when connecting to cassandra instances. | `false` |
| `-cassandra.host-verification` | Require SSL certificate validation. | `false` |
| `-cassandra.ca-path` | Path to certificate file to verify the peer. | (empty string) |
| `-cassandra.auth` | Enable password authentication when connecting to cassandra. | `false` |
| `-cassandra.username` | Username to use when connecting to cassandra. | (empty string) |
| `-cassandra.password` | Password to use when connecting to cassandra. | (empty string) |
| `-cassandra.timeout` | Timeout when connecting to cassandra. | 600ms |

**File System Config**

| Flag                     | Description                   | Default        |
| ------------------------ | ----------------------------- | -------------- |
| `-local.chunk-directory` | Directory to store chunks in. | (empty string) |

#### Chunk Schema Flags

In the course of time while developing Cortex we figured out there are better ways (more efficient, resilient against failures, etc.) to store data in the underlying Database. By then you had billings of records on disk with the one of the older schemas. Therefore we introduced the "from" tag which indicates the timeframe in which each schema has been used. You can move to a new schema by switching to it at midnight UTC and updating your configuration accordingly.

| Flag | Description | Default |
| --- | --- | --- |
| `-chunk.storage-client` | Which storage client to use (one of `aws` (aliases: `aws-dynamo`, `dynamo`), `gcp`, `gcp-columnkey` (alias: `bigtable`), `bigtable-hashed`, `cassandra`, `boltdb`, `inmemory` ) | `aws` |
| `-store.cache-lookups-older-than` | Cache index entries older than this period. 0 to disable. | 0 |

#### Validation Limits

Limits describe all the limits for users (see [Ingester, Distributor & Querier limits](#ingester-distributor--querier-limits)); can be used to describe global default limits via flags, or per-user limits via yaml config.

| Flag | Description | Default |
| --- | --- | --- |
| `-distributor.ingestion-rate-limit` | Per-user ingestion rate limit in samples per second. | `25000` |
| `-distributor.ingestion-burst-size` | Per-user allowed ingestion burst size (in number of samples). Will be reset every -distributor.limiter-reload-period. | `50000` |
| `-distributor.accept-ha-samples` | Per-user flag to enable handling of samples with external labels for identifying replicas in an HA Prometheus setup. | `false` |
| `-ha-tracker.replica` | Prometheus label to look for in samples to identify a Proemtheus HA replica. | `__replica__` |
| `-ha-tracker.cluster` | Prometheus label to look for in samples to identify a Poemtheus HA cluster. | `cluster` |
| `-validation.max-length-label-name` | Maximum length accepted for label names. | `1024` |
| `-validation.max-length-label-value` | Maximum length accepted for label value. This setting also applies to the metric name. | `2048` |
| `-validation.max-label-names-per-series` | Maximum number of label names per series. | `30` |
| `-validation.reject-old-sample` | Reject old samples. | `false` |
| `-validation.reject-old-samples.max-age` | Maximum accepted sample age before rejecting. | `14*24h` |
| `-validation.create-grace-period` | Duration which table will be created/deleted before/after it's needed; we won't accept sample from before this time. | `10m` |
| `-validation.enforce-metric-name` | Enforce every sample has a metric name. | `true` |
| `-ingester.max-series-per-query` | The maximum number of series that a query can return. | `100000` |
| `-ingester.max-samples-per-query` | The maximum number of samples that a query can return. | `1000000` |
| `-ingester.max-series-per-user` | Maximum number of active series per user. | `5000000` |
| `-ingester.max-series-per-metric` | "Maximum number of active series per metric name. | `50000` |
| `-store.query-chunk-limit` | Maximum number of chunks that can be fetched in a single query. | `2e6` |
| `-store.max-query-length` | Limit to length of chunk store queries, 0 to disable. | `0` |
| `-store.max-query-parallelism` | Maximum number of queries will be scheduled in parallel by the frontend. | `14` |
| `-store.cardinality-limit` | Cardinality limit for index queries. | `1e5` |
| `-limits.per-user-override-config` | File name of per-user overrides. | (empty string) |
| `-limits.per-user-override-period` | Period with this to reload the overrides. | `10s` |

#### Client Prealloc Config

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester-client.expected-timeseries` | Expected number of timeseries per request, use for preallocations. | `100` |
| `-ingester-client.expected-labels` | Expected number of labels per timeseries, used for preallocations. | `20` |
| `-ingester-client.expected-samples-per-series` | Expected number of samples per timeseries, used for preallocations. | `10` |

#### Client Config

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.client.grpc-max-recv-msg-size` | gRPC client max receive message size (bytes). | `100<<20` |
| `-ingester.client.grpc-max-send-msg-size` | gRPC client max send message size (bytes). | `16<<20` |
| `-ingester.client.grpc-use-gzip-compression` | Use compression when sending messages. | `false` |
| `-ingester.client.grpc-client-rate-limit` | Rate limit for gRPC client; 0 means disabled. | `0` |
| `-ingester.client.grpc-client-rate-limit-burst` | Rate limit burst for gRPC client. | `0` |
| `-ingester.client.backoff-on-ratelimits` | Enable backoff and retry when we hit ratelimits. | `false` |
| `-ingester.client.backoff-min-period` | Minimum delay when backing off. | `100ms` |
| `-ingester.client.backoff-max-period` | Maximum delay when backing off. | `10s` |
| `-ingester.client.backoff-retries` | Number of times to backoff and retry before failing. | `10` |
