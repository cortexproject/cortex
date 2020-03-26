---
title: "Configuration file"
linkTitle: "Configuration file"
weight: 1
slug: configuration-file
---

Cortex can be configured using a YAML file - specified using the `-config.file` flag - or CLI flags. In case you combine both, CLI flags take precedence over the YAML config file.

The current configuration of any Cortex component can be seen by visiting the `/config` HTTP path.
Passwords are filtered out of this endpoint.

## Reference

To specify which configuration file to load, pass the `-config.file` flag at the command line. The file is written in [YAML format](https://en.wikipedia.org/wiki/YAML), defined by the scheme below. Brackets indicate that a parameter is optional.

### Generic placeholders

* `<boolean>`: a boolean that can take the values `true` or `false`
* `<int>`: any integer matching the regular expression `[1-9]+[0-9]*`
* `<duration>`: a duration matching the regular expression `[0-9]+(ns|us|µs|ms|s|m|h|d|w|y)` where y = 365 days.
* `<string>`: a regular string
* `<url>`: an URL
* `<prefix>`: a CLI flag prefix based on the context (look at the parent configuration block to see which CLI flags prefix should be used)

### Use environment variables in the configuration

You can use environment variable references in the config file to set values that need to be configurable during deployment by using the `-config.expand-env` flag.
To do this, use:

```
${VAR}
```

Where VAR is the name of the environment variable.

Each variable reference is replaced at startup by the value of the environment variable.
The replacement is case-sensitive and occurs before the YAML file is parsed.
References to undefined variables are replaced by empty strings unless you specify a default value or custom error text.

To specify a default value, use:

```
${VAR:default_value}
```

Where default_value is the value to use if the environment variable is undefined.

### Supported contents and default values of the config file

```yaml
# The Cortex service to run. Supported values are: all, distributor, ingester,
# querier, query-frontend, table-manager, ruler, alertmanager, configs.
# CLI flag: -target
[target: <string> | default = "all"]

# Set to false to disable auth.
# CLI flag: -auth.enabled
[auth_enabled: <boolean> | default = true]

# HTTP path prefix for Cortex API.
# CLI flag: -http.prefix
[http_prefix: <string> | default = "/api/prom"]

# The server_config configures the HTTP and gRPC server of the launched
# service(s).
[server: <server_config>]

# The distributor_config configures the Cortex distributor.
[distributor: <distributor_config>]

# The querier_config configures the Cortex querier.
[querier: <querier_config>]

# The ingester_client_config configures how the Cortex distributors connect to
# the ingesters.
[ingester_client: <ingester_client_config>]

# The ingester_config configures the Cortex ingester.
[ingester: <ingester_config>]

flusher:
  # Directory to read WAL from.
  # CLI flag: -flusher.wal-dir
  [wal_dir: <string> | default = "wal"]

  # Number of concurrent goroutines flushing to dynamodb.
  # CLI flag: -flusher.concurrent-flushes
  [concurrent_flushes: <int> | default = 50]

  # Timeout for individual flush operations.
  # CLI flag: -flusher.flush-op-timeout
  [flush_op_timeout: <duration> | default = 2m0s]

# The storage_config configures where Cortex stores the data (chunks storage
# engine).
[storage: <storage_config>]

# The chunk_store_config configures how Cortex stores the data (chunks storage
# engine).
[chunk_store: <chunk_store_config>]

# The limits_config configures default and per-tenant limits imposed by Cortex
# services (ie. distributor, ingester, ...).
[limits: <limits_config>]

# The frontend_worker_config configures the worker - running within the Cortex
# querier - picking up and executing queries enqueued by the query-frontend.
[frontend_worker: <frontend_worker_config>]

# The query_frontend_config configures the Cortex query-frontend.
[frontend: <query_frontend_config>]

# The queryrange_config configures the query splitting and caching in the Cortex
# query-frontend.
[query_range: <queryrange_config>]

# The table_manager_config configures the Cortex table-manager.
[table_manager: <table_manager_config>]

# The tsdb_config configures the experimental blocks storage.
[tsdb: <tsdb_config>]

# The compactor_config configures the compactor for the experimental blocks
# storage.
[compactor: <compactor_config>]

store_gateway:
  # Shard blocks across multiple store gateway instances.
  # CLI flag: -store-gateway.sharding-enabled
  [sharding_enabled: <boolean> | default = false]

  sharding_ring:
    kvstore:
      # Backend storage to use for the ring. Supported values are: consul, etcd,
      # inmemory, multi, memberlist (experimental).
      # CLI flag: -store-gateway.ring.store
      [store: <string> | default = "consul"]

      # The prefix for the keys in the store. Should end with a /.
      # CLI flag: -store-gateway.ring.prefix
      [prefix: <string> | default = "collectors/"]

      # The consul_config configures the consul client.
      # The CLI flags prefix for this block config is: store-gateway.ring
      [consul: <consul_config>]

      # The etcd_config configures the etcd client.
      # The CLI flags prefix for this block config is: store-gateway.ring
      [etcd: <etcd_config>]

      multi:
        # Primary backend storage used by multi-client.
        # CLI flag: -store-gateway.ring.multi.primary
        [primary: <string> | default = ""]

        # Secondary backend storage used by multi-client.
        # CLI flag: -store-gateway.ring.multi.secondary
        [secondary: <string> | default = ""]

        # Mirror writes to secondary store.
        # CLI flag: -store-gateway.ring.multi.mirror-enabled
        [mirror_enabled: <boolean> | default = false]

        # Timeout for storing value to secondary store.
        # CLI flag: -store-gateway.ring.multi.mirror-timeout
        [mirror_timeout: <duration> | default = 2s]

    # Period at which to heartbeat to the ring.
    # CLI flag: -store-gateway.ring.heartbeat-period
    [heartbeat_period: <duration> | default = 5s]

    # The heartbeat timeout after which store gateways are considered unhealthy
    # within the ring.
    # CLI flag: -store-gateway.ring.heartbeat-timeout
    [heartbeat_timeout: <duration> | default = 1m0s]

    # The replication factor to use when sharding blocks.
    # CLI flag: -store-gateway.replication-factor
    [replication_factor: <int> | default = 2]

# The purger_config configures the purger which takes care of delete requests
[purger: <purger_config>]

# The ruler_config configures the Cortex ruler.
[ruler: <ruler_config>]

# The configs_config configures the Cortex Configs DB and API.
[configs: <configs_config>]

# The alertmanager_config configures the Cortex alertmanager.
[alertmanager: <alertmanager_config>]

runtime_config:
  # How often to check runtime config file.
  # CLI flag: -runtime-config.reload-period
  [period: <duration> | default = 10s]

  # File with the configuration that can be updated in runtime.
  # CLI flag: -runtime-config.file
  [file: <string> | default = ""]

# The memberlist_config configures the Gossip memberlist.
[memberlist: <memberlist_config>]
```

### `server_config`

The `server_config` configures the HTTP and gRPC server of the launched service(s).

```yaml
# HTTP server listen address.
# CLI flag: -server.http-listen-address
[http_listen_address: <string> | default = ""]

# HTTP server listen port.
# CLI flag: -server.http-listen-port
[http_listen_port: <int> | default = 80]

# Maximum number of simultaneous http connections, <=0 to disable
# CLI flag: -server.http-conn-limit
[http_listen_conn_limit: <int> | default = 0]

# gRPC server listen address.
# CLI flag: -server.grpc-listen-address
[grpc_listen_address: <string> | default = ""]

# gRPC server listen port.
# CLI flag: -server.grpc-listen-port
[grpc_listen_port: <int> | default = 9095]

# Maximum number of simultaneous grpc connections, <=0 to disable
# CLI flag: -server.grpc-conn-limit
[grpc_listen_conn_limit: <int> | default = 0]

# Register the intrumentation handlers (/metrics etc).
# CLI flag: -server.register-instrumentation
[register_instrumentation: <boolean> | default = true]

# Timeout for graceful shutdowns
# CLI flag: -server.graceful-shutdown-timeout
[graceful_shutdown_timeout: <duration> | default = 30s]

# Read timeout for HTTP server
# CLI flag: -server.http-read-timeout
[http_server_read_timeout: <duration> | default = 30s]

# Write timeout for HTTP server
# CLI flag: -server.http-write-timeout
[http_server_write_timeout: <duration> | default = 30s]

# Idle timeout for HTTP server
# CLI flag: -server.http-idle-timeout
[http_server_idle_timeout: <duration> | default = 2m0s]

# Limit on the size of a gRPC message this server can receive (bytes).
# CLI flag: -server.grpc-max-recv-msg-size-bytes
[grpc_server_max_recv_msg_size: <int> | default = 4194304]

# Limit on the size of a gRPC message this server can send (bytes).
# CLI flag: -server.grpc-max-send-msg-size-bytes
[grpc_server_max_send_msg_size: <int> | default = 4194304]

# Limit on the number of concurrent streams for gRPC calls (0 = unlimited)
# CLI flag: -server.grpc-max-concurrent-streams
[grpc_server_max_concurrent_streams: <int> | default = 100]

# The duration after which an idle connection should be closed. Default:
# infinity
# CLI flag: -server.grpc.keepalive.max-connection-idle
[grpc_server_max_connection_idle: <duration> | default = 2562047h47m16.854775807s]

# The duration for the maximum amount of time a connection may exist before it
# will be closed. Default: infinity
# CLI flag: -server.grpc.keepalive.max-connection-age
[grpc_server_max_connection_age: <duration> | default = 2562047h47m16.854775807s]

# An additive period after max-connection-age after which the connection will be
# forcibly closed. Default: infinity
# CLI flag: -server.grpc.keepalive.max-connection-age-grace
[grpc_server_max_connection_age_grace: <duration> | default = 2562047h47m16.854775807s]

# Duration after which a keepalive probe is sent in case of no activity over the
# connection., Default: 2h
# CLI flag: -server.grpc.keepalive.time
[grpc_server_keepalive_time: <duration> | default = 2h0m0s]

# After having pinged for keepalive check, the duration after which an idle
# connection should be closed, Default: 20s
# CLI flag: -server.grpc.keepalive.timeout
[grpc_server_keepalive_timeout: <duration> | default = 20s]

# Only log messages with the given severity or above. Valid levels: [debug,
# info, warn, error]
# CLI flag: -log.level
[log_level: <string> | default = "info"]

# Base path to serve all API routes from (e.g. /v1/)
# CLI flag: -server.path-prefix
[http_path_prefix: <string> | default = ""]
```

### `distributor_config`

The `distributor_config` configures the Cortex distributor.

```yaml
pool:
  # How frequently to clean up clients for ingesters that have gone away.
  # CLI flag: -distributor.client-cleanup-period
  [client_cleanup_period: <duration> | default = 15s]

  # Run a health check on each ingester client during periodic cleanup.
  # CLI flag: -distributor.health-check-ingesters
  [health_check_ingesters: <boolean> | default = false]

ha_tracker:
  # Enable the distributors HA tracker so that it can accept samples from
  # Prometheus HA replicas gracefully (requires labels).
  # CLI flag: -distributor.ha-tracker.enable
  [enable_ha_tracker: <boolean> | default = false]

  # Update the timestamp in the KV store for a given cluster/replica only after
  # this amount of time has passed since the current stored timestamp.
  # CLI flag: -distributor.ha-tracker.update-timeout
  [ha_tracker_update_timeout: <duration> | default = 15s]

  # Maximum jitter applied to the update timeout, in order to spread the HA
  # heartbeats over time.
  # CLI flag: -distributor.ha-tracker.update-timeout-jitter-max
  [ha_tracker_update_timeout_jitter_max: <duration> | default = 5s]

  # If we don't receive any samples from the accepted replica for a cluster in
  # this amount of time we will failover to the next replica we receive a sample
  # from. This value must be greater than the update timeout
  # CLI flag: -distributor.ha-tracker.failover-timeout
  [ha_tracker_failover_timeout: <duration> | default = 30s]

  kvstore:
    # Backend storage to use for the ring. Supported values are: consul, etcd,
    # inmemory, multi, memberlist (experimental).
    # CLI flag: -distributor.ha-tracker.store
    [store: <string> | default = "consul"]

    # The prefix for the keys in the store. Should end with a /.
    # CLI flag: -distributor.ha-tracker.prefix
    [prefix: <string> | default = "ha-tracker/"]

    # The consul_config configures the consul client.
    # The CLI flags prefix for this block config is: distributor.ha-tracker
    [consul: <consul_config>]

    # The etcd_config configures the etcd client.
    # The CLI flags prefix for this block config is: distributor.ha-tracker
    [etcd: <etcd_config>]

    multi:
      # Primary backend storage used by multi-client.
      # CLI flag: -distributor.ha-tracker.multi.primary
      [primary: <string> | default = ""]

      # Secondary backend storage used by multi-client.
      # CLI flag: -distributor.ha-tracker.multi.secondary
      [secondary: <string> | default = ""]

      # Mirror writes to secondary store.
      # CLI flag: -distributor.ha-tracker.multi.mirror-enabled
      [mirror_enabled: <boolean> | default = false]

      # Timeout for storing value to secondary store.
      # CLI flag: -distributor.ha-tracker.multi.mirror-timeout
      [mirror_timeout: <duration> | default = 2s]

# remote_write API max receive message size (bytes).
# CLI flag: -distributor.max-recv-msg-size
[max_recv_msg_size: <int> | default = 104857600]

# Timeout for downstream ingesters.
# CLI flag: -distributor.remote-timeout
[remote_timeout: <duration> | default = 2s]

# Time to wait before sending more than the minimum successful query requests.
# CLI flag: -distributor.extra-query-delay
[extra_queue_delay: <duration> | default = 0s]

# Distribute samples based on all labels, as opposed to solely by user and
# metric name.
# CLI flag: -distributor.shard-by-all-labels
[shard_by_all_labels: <boolean> | default = false]

ring:
  kvstore:
    # Backend storage to use for the ring. Supported values are: consul, etcd,
    # inmemory, multi, memberlist (experimental).
    # CLI flag: -distributor.ring.store
    [store: <string> | default = "consul"]

    # The prefix for the keys in the store. Should end with a /.
    # CLI flag: -distributor.ring.prefix
    [prefix: <string> | default = "collectors/"]

    # The consul_config configures the consul client.
    # The CLI flags prefix for this block config is: distributor.ring
    [consul: <consul_config>]

    # The etcd_config configures the etcd client.
    # The CLI flags prefix for this block config is: distributor.ring
    [etcd: <etcd_config>]

    multi:
      # Primary backend storage used by multi-client.
      # CLI flag: -distributor.ring.multi.primary
      [primary: <string> | default = ""]

      # Secondary backend storage used by multi-client.
      # CLI flag: -distributor.ring.multi.secondary
      [secondary: <string> | default = ""]

      # Mirror writes to secondary store.
      # CLI flag: -distributor.ring.multi.mirror-enabled
      [mirror_enabled: <boolean> | default = false]

      # Timeout for storing value to secondary store.
      # CLI flag: -distributor.ring.multi.mirror-timeout
      [mirror_timeout: <duration> | default = 2s]

  # Period at which to heartbeat to the ring.
  # CLI flag: -distributor.ring.heartbeat-period
  [heartbeat_period: <duration> | default = 5s]

  # The heartbeat timeout after which distributors are considered unhealthy
  # within the ring.
  # CLI flag: -distributor.ring.heartbeat-timeout
  [heartbeat_timeout: <duration> | default = 1m0s]
```

### `ingester_config`

The `ingester_config` configures the Cortex ingester.

```yaml
walconfig:
  # Enable writing of ingested data into WAL.
  # CLI flag: -ingester.wal-enabled
  [wal_enabled: <boolean> | default = false]

  # Enable checkpointing of in-memory chunks.
  # CLI flag: -ingester.checkpoint-enabled
  [checkpoint_enabled: <boolean> | default = false]

  # Recover data from existing WAL irrespective of WAL enabled/disabled.
  # CLI flag: -ingester.recover-from-wal
  [recover_from_wal: <boolean> | default = false]

  # Directory to store the WAL and/or recover from WAL.
  # CLI flag: -ingester.wal-dir
  [wal_dir: <string> | default = "wal"]

  # Interval at which checkpoints should be created.
  # CLI flag: -ingester.checkpoint-duration
  [checkpoint_duration: <duration> | default = 30m0s]

lifecycler:
  ring:
    kvstore:
      # Backend storage to use for the ring. Supported values are: consul, etcd,
      # inmemory, multi, memberlist (experimental).
      # CLI flag: -ring.store
      [store: <string> | default = "consul"]

      # The prefix for the keys in the store. Should end with a /.
      # CLI flag: -ring.prefix
      [prefix: <string> | default = "collectors/"]

      # The consul_config configures the consul client.
      [consul: <consul_config>]

      # The etcd_config configures the etcd client.
      [etcd: <etcd_config>]

      multi:
        # Primary backend storage used by multi-client.
        # CLI flag: -multi.primary
        [primary: <string> | default = ""]

        # Secondary backend storage used by multi-client.
        # CLI flag: -multi.secondary
        [secondary: <string> | default = ""]

        # Mirror writes to secondary store.
        # CLI flag: -multi.mirror-enabled
        [mirror_enabled: <boolean> | default = false]

        # Timeout for storing value to secondary store.
        # CLI flag: -multi.mirror-timeout
        [mirror_timeout: <duration> | default = 2s]

    # The heartbeat timeout after which ingesters are skipped for reads/writes.
    # CLI flag: -ring.heartbeat-timeout
    [heartbeat_timeout: <duration> | default = 1m0s]

    # The number of ingesters to write to and read from.
    # CLI flag: -distributor.replication-factor
    [replication_factor: <int> | default = 3]

  # Number of tokens for each ingester.
  # CLI flag: -ingester.num-tokens
  [num_tokens: <int> | default = 128]

  # Period at which to heartbeat to consul.
  # CLI flag: -ingester.heartbeat-period
  [heartbeat_period: <duration> | default = 5s]

  # Observe tokens after generating to resolve collisions. Useful when using
  # gossiping ring.
  # CLI flag: -ingester.observe-period
  [observe_period: <duration> | default = 0s]

  # Period to wait for a claim from another member; will join automatically
  # after this.
  # CLI flag: -ingester.join-after
  [join_after: <duration> | default = 0s]

  # Minimum duration to wait before becoming ready. This is to work around race
  # conditions with ingesters exiting and updating the ring.
  # CLI flag: -ingester.min-ready-duration
  [min_ready_duration: <duration> | default = 1m0s]

  # Name of network interface to read address from.
  # CLI flag: -ingester.lifecycler.interface
  [interface_names: <list of string> | default = [eth0 en0]]

  # Duration to sleep for before exiting, to ensure metrics are scraped.
  # CLI flag: -ingester.final-sleep
  [final_sleep: <duration> | default = 30s]

  # File path where tokens are stored. If empty, tokens are not stored at
  # shutdown and restored at startup.
  # CLI flag: -ingester.tokens-file-path
  [tokens_file_path: <string> | default = ""]

# Number of times to try and transfer chunks before falling back to flushing.
# Negative value or zero disables hand-over.
# CLI flag: -ingester.max-transfer-retries
[max_transfer_retries: <int> | default = 10]

# Period with which to attempt to flush chunks.
# CLI flag: -ingester.flush-period
[flush_period: <duration> | default = 1m0s]

# Period chunks will remain in memory after flushing.
# CLI flag: -ingester.retain-period
[retain_period: <duration> | default = 5m0s]

# Maximum chunk idle time before flushing.
# CLI flag: -ingester.max-chunk-idle
[max_chunk_idle_time: <duration> | default = 5m0s]

# Maximum chunk idle time for chunks terminating in stale markers before
# flushing. 0 disables it and a stale series is not flushed until the
# max-chunk-idle timeout is reached.
# CLI flag: -ingester.max-stale-chunk-idle
[max_stale_chunk_idle_time: <duration> | default = 0s]

# Timeout for individual flush operations.
# CLI flag: -ingester.flush-op-timeout
[flush_op_timeout: <duration> | default = 1m0s]

# Maximum chunk age before flushing.
# CLI flag: -ingester.max-chunk-age
[max_chunk_age: <duration> | default = 12h0m0s]

# Range of time to subtract from -ingester.max-chunk-age to spread out flushes
# CLI flag: -ingester.chunk-age-jitter
[chunk_age_jitter: <duration> | default = 20m0s]

# Number of concurrent goroutines flushing to dynamodb.
# CLI flag: -ingester.concurrent-flushes
[concurrent_flushes: <int> | default = 50]

# If true, spread series flushes across the whole period of
# -ingester.max-chunk-age.
# CLI flag: -ingester.spread-flushes
[spread_flushes: <boolean> | default = false]

# Period with which to update the per-user ingestion rates.
# CLI flag: -ingester.rate-update-period
[rate_update_period: <duration> | default = 15s]
```

### `querier_config`

The `querier_config` configures the Cortex querier.

```yaml
# The maximum number of concurrent queries.
# CLI flag: -querier.max-concurrent
[max_concurrent: <int> | default = 20]

# The timeout for a query.
# CLI flag: -querier.timeout
[timeout: <duration> | default = 2m0s]

# Use iterators to execute query, as opposed to fully materialising the series
# in memory.
# CLI flag: -querier.iterators
[iterators: <boolean> | default = false]

# Use batch iterators to execute query, as opposed to fully materialising the
# series in memory.  Takes precedent over the -querier.iterators flag.
# CLI flag: -querier.batch-iterators
[batch_iterators: <boolean> | default = false]

# Use streaming RPCs to query ingester.
# CLI flag: -querier.ingester-streaming
[ingester_streaming: <boolean> | default = false]

# Maximum number of samples a single query can load into memory.
# CLI flag: -querier.max-samples
[max_samples: <int> | default = 50000000]

# Maximum lookback beyond which queries are not sent to ingester. 0 means all
# queries are sent to ingester.
# CLI flag: -querier.query-ingesters-within
[query_ingesters_within: <duration> | default = 0s]

# The time after which a metric should only be queried from storage and not just
# ingesters. 0 means all queries are sent to store.
# CLI flag: -querier.query-store-after
[query_store_after: <duration> | default = 0s]

# Maximum duration into the future you can query. 0 to disable.
# CLI flag: -querier.max-query-into-future
[max_query_into_future: <duration> | default = 10m0s]

# The default evaluation interval or step size for subqueries.
# CLI flag: -querier.default-evaluation-interval
[default_evaluation_interval: <duration> | default = 1m0s]

# Active query tracker monitors active queries, and writes them to the file in
# given directory. If Cortex discovers any queries in this log during startup,
# it will log them to the log file. Setting to empty value disables active query
# tracker, which also disables -querier.max-concurrent option.
# CLI flag: -querier.active-query-tracker-dir
[active_query_tracker_dir: <string> | default = "./active-query-tracker"]
```

### `query_frontend_config`

The `query_frontend_config` configures the Cortex query-frontend.

```yaml
# Maximum number of outstanding requests per tenant per frontend; requests
# beyond this error with HTTP 429.
# CLI flag: -querier.max-outstanding-requests-per-tenant
[max_outstanding_per_tenant: <int> | default = 100]

# Compress HTTP responses.
# CLI flag: -querier.compress-http-responses
[compress_responses: <boolean> | default = false]

# URL of downstream Prometheus.
# CLI flag: -frontend.downstream-url
[downstream_url: <string> | default = ""]

# Log queries that are slower than the specified duration. 0 to disable.
# CLI flag: -frontend.log-queries-longer-than
[log_queries_longer_than: <duration> | default = 0s]
```

### `queryrange_config`

The `queryrange_config` configures the query splitting and caching in the Cortex query-frontend.

```yaml
# Split queries by an interval and execute in parallel, 0 disables it. You
# should use an a multiple of 24 hours (same as the storage bucketing scheme),
# to avoid queriers downloading and processing the same chunks. This also
# determines how cache keys are chosen when result caching is enabled
# CLI flag: -querier.split-queries-by-interval
[split_queries_by_interval: <duration> | default = 0s]

# Deprecated: Split queries by day and execute in parallel.
# CLI flag: -querier.split-queries-by-day
[split_queries_by_day: <boolean> | default = false]

# Mutate incoming queries to align their start and end with their step.
# CLI flag: -querier.align-querier-with-step
[align_queries_with_step: <boolean> | default = false]

results_cache:
  cache:
    # Enable in-memory cache.
    # CLI flag: -frontend.cache.enable-fifocache
    [enable_fifocache: <boolean> | default = false]

    # The default validity of entries for caches unless overridden.
    # CLI flag: -frontend.default-validity
    [default_validity: <duration> | default = 0s]

    background:
      # At what concurrency to write back to cache.
      # CLI flag: -frontend.background.write-back-concurrency
      [writeback_goroutines: <int> | default = 10]

      # How many key batches to buffer for background write-back.
      # CLI flag: -frontend.background.write-back-buffer
      [writeback_buffer: <int> | default = 10000]

    # The memcached_config block configures how data is stored in Memcached (ie.
    # expiration).
    # The CLI flags prefix for this block config is: frontend
    [memcached: <memcached_config>]

    # The memcached_client_config configures the client used to connect to
    # Memcached.
    # The CLI flags prefix for this block config is: frontend
    [memcached_client: <memcached_client_config>]

    # The redis_config configures the Redis backend cache.
    # The CLI flags prefix for this block config is: frontend
    [redis: <redis_config>]

    # The fifo_cache_config configures the local in-memory cache.
    # The CLI flags prefix for this block config is: frontend
    [fifocache: <fifo_cache_config>]

  # Most recent allowed cacheable result, to prevent caching very recent results
  # that might still be in flux.
  # CLI flag: -frontend.max-cache-freshness
  [max_freshness: <duration> | default = 1m0s]

# Cache query results.
# CLI flag: -querier.cache-results
[cache_results: <boolean> | default = false]

# Maximum number of retries for a single request; beyond this, the downstream
# error is returned.
# CLI flag: -querier.max-retries-per-request
[max_retries: <int> | default = 5]

# Perform query parallelisations based on storage sharding configuration and
# query ASTs. This feature is supported only by the chunks storage engine.
# CLI flag: -querier.parallelise-shardable-queries
[parallelise_shardable_queries: <boolean> | default = false]
```

### `ruler_config`

The `ruler_config` configures the Cortex ruler.

```yaml
# URL of alerts return path.
# CLI flag: -ruler.external.url
[external_url: <url> | default = ]

# How frequently to evaluate rules
# CLI flag: -ruler.evaluation-interval
[evaluation_interval: <duration> | default = 1m0s]

# How frequently to poll for rule changes
# CLI flag: -ruler.poll-interval
[poll_interval: <duration> | default = 1m0s]

storage:
  # Method to use for backend rule storage (configdb, azure, gcs, s3)
  # CLI flag: -ruler.storage.type
  [type: <string> | default = "configdb"]

  # The configstore_config configures the config database storing rules and
  # alerts, and is used by the Cortex alertmanager.
  # The CLI flags prefix for this block config is: ruler
  [configdb: <configstore_config>]

  azure:
    # Name of the blob container used to store chunks. Defaults to `cortex`.
    # This container must be created before running cortex.
    # CLI flag: -ruler.storage.azure.container-name
    [container_name: <string> | default = "cortex"]

    # The Microsoft Azure account name to be used
    # CLI flag: -ruler.storage.azure.account-name
    [account_name: <string> | default = ""]

    # The Microsoft Azure account key to use.
    # CLI flag: -ruler.storage.azure.account-key
    [account_key: <string> | default = ""]

    # Preallocated buffer size for downloads (default is 512KB)
    # CLI flag: -ruler.storage.azure.download-buffer-size
    [download_buffer_size: <int> | default = 512000]

    # Preallocated buffer size for up;oads (default is 256KB)
    # CLI flag: -ruler.storage.azure.upload-buffer-size
    [upload_buffer_size: <int> | default = 256000]

    # Number of buffers used to used to upload a chunk. (defaults to 1)
    # CLI flag: -ruler.storage.azure.download-buffer-count
    [upload_buffer_count: <int> | default = 1]

    # Timeout for requests made against azure blob storage. Defaults to 30
    # seconds.
    # CLI flag: -ruler.storage.azure.request-timeout
    [request_timeout: <duration> | default = 30s]

    # Number of retries for a request which times out.
    # CLI flag: -ruler.storage.azure.max-retries
    [max_retries: <int> | default = 5]

    # Minimum time to wait before retrying a request.
    # CLI flag: -ruler.storage.azure.min-retry-delay
    [min_retry_delay: <duration> | default = 10ms]

    # Maximum time to wait before retrying a request.
    # CLI flag: -ruler.storage.azure.max-retry-delay
    [max_retry_delay: <duration> | default = 500ms]

  gcs:
    # Name of GCS bucket to put chunks in.
    # CLI flag: -ruler.storage.gcs.bucketname
    [bucket_name: <string> | default = ""]

    # The size of the buffer that GCS client for each PUT request. 0 to disable
    # buffering.
    # CLI flag: -ruler.storage.gcs.chunk-buffer-size
    [chunk_buffer_size: <int> | default = 0]

    # The duration after which the requests to GCS should be timed out.
    # CLI flag: -ruler.storage.gcs.request-timeout
    [request_timeout: <duration> | default = 0s]

  s3:
    # S3 endpoint URL with escaped Key and Secret encoded. If only region is
    # specified as a host, proper endpoint will be deduced. Use
    # inmemory:///<bucket-name> to use a mock in-memory implementation.
    # CLI flag: -ruler.storage.s3.url
    [s3: <url> | default = ]

    # Comma separated list of bucket names to evenly distribute chunks over.
    # Overrides any buckets specified in s3.url flag
    # CLI flag: -ruler.storage.s3.buckets
    [bucketnames: <string> | default = ""]

    # Set this to `true` to force the request to use path-style addressing.
    # CLI flag: -ruler.storage.s3.force-path-style
    [s3forcepathstyle: <boolean> | default = false]

# file path to store temporary rule files for the prometheus rule managers
# CLI flag: -ruler.rule-path
[rule_path: <string> | default = "/rules"]

# URL of the Alertmanager to send notifications to.
# CLI flag: -ruler.alertmanager-url
[alertmanager_url: <url> | default = ]

# Use DNS SRV records to discover alertmanager hosts.
# CLI flag: -ruler.alertmanager-discovery
[enable_alertmanager_discovery: <boolean> | default = false]

# How long to wait between refreshing alertmanager hosts.
# CLI flag: -ruler.alertmanager-refresh-interval
[alertmanager_refresh_interval: <duration> | default = 1m0s]

# If enabled requests to alertmanager will utilize the V2 API.
# CLI flag: -ruler.alertmanager-use-v2
[enable_alertmanager_v2: <boolean> | default = false]

# Capacity of the queue for notifications to be sent to the Alertmanager.
# CLI flag: -ruler.notification-queue-capacity
[notification_queue_capacity: <int> | default = 10000]

# HTTP timeout duration when sending notifications to the Alertmanager.
# CLI flag: -ruler.notification-timeout
[notification_timeout: <duration> | default = 10s]

# Distribute rule evaluation using ring backend
# CLI flag: -ruler.enable-sharding
[enable_sharding: <boolean> | default = false]

# Time to spend searching for a pending ruler when shutting down.
# CLI flag: -ruler.search-pending-for
[search_pending_for: <duration> | default = 5m0s]

ring:
  kvstore:
    # Backend storage to use for the ring. Supported values are: consul, etcd,
    # inmemory, multi, memberlist (experimental).
    # CLI flag: -ruler.ring.store
    [store: <string> | default = "consul"]

    # The prefix for the keys in the store. Should end with a /.
    # CLI flag: -ruler.ring.prefix
    [prefix: <string> | default = "rulers/"]

    # The consul_config configures the consul client.
    # The CLI flags prefix for this block config is: ruler.ring
    [consul: <consul_config>]

    # The etcd_config configures the etcd client.
    # The CLI flags prefix for this block config is: ruler.ring
    [etcd: <etcd_config>]

    multi:
      # Primary backend storage used by multi-client.
      # CLI flag: -ruler.ring.multi.primary
      [primary: <string> | default = ""]

      # Secondary backend storage used by multi-client.
      # CLI flag: -ruler.ring.multi.secondary
      [secondary: <string> | default = ""]

      # Mirror writes to secondary store.
      # CLI flag: -ruler.ring.multi.mirror-enabled
      [mirror_enabled: <boolean> | default = false]

      # Timeout for storing value to secondary store.
      # CLI flag: -ruler.ring.multi.mirror-timeout
      [mirror_timeout: <duration> | default = 2s]

  # Period at which to heartbeat to the ring.
  # CLI flag: -ruler.ring.heartbeat-period
  [heartbeat_period: <duration> | default = 5s]

  # The heartbeat timeout after which rulers are considered unhealthy within the
  # ring.
  # CLI flag: -ruler.ring.heartbeat-timeout
  [heartbeat_timeout: <duration> | default = 1m0s]

  # Number of tokens for each ingester.
  # CLI flag: -ruler.ring.num-tokens
  [num_tokens: <int> | default = 128]

# Period with which to attempt to flush rule groups.
# CLI flag: -ruler.flush-period
[flush_period: <duration> | default = 1m0s]

# Enable the ruler api
# CLI flag: -experimental.ruler.enable-api
[enable_api: <boolean> | default = false]
```

### `alertmanager_config`

The `alertmanager_config` configures the Cortex alertmanager.

```yaml
# Base path for data storage.
# CLI flag: -alertmanager.storage.path
[data_dir: <string> | default = "data/"]

# How long to keep data for.
# CLI flag: -alertmanager.storage.retention
[retention: <duration> | default = 120h0m0s]

# The URL under which Alertmanager is externally reachable (for example, if
# Alertmanager is served via a reverse proxy). Used for generating relative and
# absolute links back to Alertmanager itself. If the URL has a path portion, it
# will be used to prefix all HTTP endpoints served by Alertmanager. If omitted,
# relevant URL components will be derived automatically.
# CLI flag: -alertmanager.web.external-url
[external_url: <url> | default = ]

# How frequently to poll Cortex configs
# CLI flag: -alertmanager.configs.poll-interval
[poll_interval: <duration> | default = 15s]

# Listen address for cluster.
# CLI flag: -cluster.listen-address
[cluster_bind_address: <string> | default = "0.0.0.0:9094"]

# Explicit address to advertise in cluster.
# CLI flag: -cluster.advertise-address
[cluster_advertise_address: <string> | default = ""]

# Initial peers (may be repeated).
# CLI flag: -cluster.peer
[peers: <list of string> | default = ]

# Time to wait between peers to send notifications.
# CLI flag: -cluster.peer-timeout
[peer_timeout: <duration> | default = 15s]

# Filename of fallback config to use if none specified for instance.
# CLI flag: -alertmanager.configs.fallback
[fallback_config_file: <string> | default = ""]

# Root of URL to generate if config is http://internal.monitor
# CLI flag: -alertmanager.configs.auto-webhook-root
[auto_webhook_root: <string> | default = ""]

storage:
  # Type of backend to use to store alertmanager configs. Supported values are:
  # "configdb", "local".
  # CLI flag: -alertmanager.storage.type
  [type: <string> | default = "configdb"]

  # The configstore_config configures the config database storing rules and
  # alerts, and is used by the Cortex alertmanager.
  # The CLI flags prefix for this block config is: alertmanager
  [configdb: <configstore_config>]

  local:
    # Path at which alertmanager configurations are stored.
    # CLI flag: -alertmanager.storage.local.path
    [path: <string> | default = ""]
```

### `table_manager_config`

The `table_manager_config` configures the Cortex table-manager.

```yaml
# If true, disable all changes to DB capacity
# CLI flag: -table-manager.throughput-updates-disabled
[throughput_updates_disabled: <boolean> | default = false]

# If true, enables retention deletes of DB tables
# CLI flag: -table-manager.retention-deletes-enabled
[retention_deletes_enabled: <boolean> | default = false]

# Tables older than this retention period are deleted. Note: This setting is
# destructive to data!(default: 0, which disables deletion)
# CLI flag: -table-manager.retention-period
[retention_period: <duration> | default = 0s]

# How frequently to poll DynamoDB to learn our capacity.
# CLI flag: -dynamodb.poll-interval
[dynamodb_poll_interval: <duration> | default = 2m0s]

# DynamoDB periodic tables grace period (duration which table will be
# created/deleted before/after it's needed).
# CLI flag: -dynamodb.periodic-table.grace-period
[creation_grace_period: <duration> | default = 10m0s]

index_tables_provisioning:
  # Enables on demand throughput provisioning for the storage provider (if
  # supported). Applies only to tables which are not autoscaled
  # CLI flag: -dynamodb.periodic-table.enable-ondemand-throughput-mode
  [provisioned_throughput_on_demand_mode: <boolean> | default = false]

  # DynamoDB table default write throughput.
  # CLI flag: -dynamodb.periodic-table.write-throughput
  [provisioned_write_throughput: <int> | default = 1000]

  # DynamoDB table default read throughput.
  # CLI flag: -dynamodb.periodic-table.read-throughput
  [provisioned_read_throughput: <int> | default = 300]

  # Enables on demand throughput provisioning for the storage provider (if
  # supported). Applies only to tables which are not autoscaled
  # CLI flag: -dynamodb.periodic-table.inactive-enable-ondemand-throughput-mode
  [inactive_throughput_on_demand_mode: <boolean> | default = false]

  # DynamoDB table write throughput for inactive tables.
  # CLI flag: -dynamodb.periodic-table.inactive-write-throughput
  [inactive_write_throughput: <int> | default = 1]

  # DynamoDB table read throughput for inactive tables.
  # CLI flag: -dynamodb.periodic-table.inactive-read-throughput
  [inactive_read_throughput: <int> | default = 300]

  write_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.periodic-table.write-throughput.scale.target-value
    [target: <float> | default = 80]

  inactive_write_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale.target-value
    [target: <float> | default = 80]

  # Number of last inactive tables to enable write autoscale.
  # CLI flag: -dynamodb.periodic-table.inactive-write-throughput.scale-last-n
  [inactive_write_scale_lastn: <int> | default = 4]

  read_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.periodic-table.read-throughput.scale.target-value
    [target: <float> | default = 80]

  inactive_read_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale.target-value
    [target: <float> | default = 80]

  # Number of last inactive tables to enable read autoscale.
  # CLI flag: -dynamodb.periodic-table.inactive-read-throughput.scale-last-n
  [inactive_read_scale_lastn: <int> | default = 4]

chunk_tables_provisioning:
  # Enables on demand throughput provisioning for the storage provider (if
  # supported). Applies only to tables which are not autoscaled
  # CLI flag: -dynamodb.chunk-table.enable-ondemand-throughput-mode
  [provisioned_throughput_on_demand_mode: <boolean> | default = false]

  # DynamoDB table default write throughput.
  # CLI flag: -dynamodb.chunk-table.write-throughput
  [provisioned_write_throughput: <int> | default = 1000]

  # DynamoDB table default read throughput.
  # CLI flag: -dynamodb.chunk-table.read-throughput
  [provisioned_read_throughput: <int> | default = 300]

  # Enables on demand throughput provisioning for the storage provider (if
  # supported). Applies only to tables which are not autoscaled
  # CLI flag: -dynamodb.chunk-table.inactive-enable-ondemand-throughput-mode
  [inactive_throughput_on_demand_mode: <boolean> | default = false]

  # DynamoDB table write throughput for inactive tables.
  # CLI flag: -dynamodb.chunk-table.inactive-write-throughput
  [inactive_write_throughput: <int> | default = 1]

  # DynamoDB table read throughput for inactive tables.
  # CLI flag: -dynamodb.chunk-table.inactive-read-throughput
  [inactive_read_throughput: <int> | default = 300]

  write_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.chunk-table.write-throughput.scale.target-value
    [target: <float> | default = 80]

  inactive_write_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale.target-value
    [target: <float> | default = 80]

  # Number of last inactive tables to enable write autoscale.
  # CLI flag: -dynamodb.chunk-table.inactive-write-throughput.scale-last-n
  [inactive_write_scale_lastn: <int> | default = 4]

  read_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.chunk-table.read-throughput.scale.target-value
    [target: <float> | default = 80]

  inactive_read_scale:
    # Should we enable autoscale for the table.
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.enabled
    [enabled: <boolean> | default = false]

    # AWS AutoScaling role ARN
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.role-arn
    [role_arn: <string> | default = ""]

    # DynamoDB minimum provision capacity.
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.min-capacity
    [min_capacity: <int> | default = 3000]

    # DynamoDB maximum provision capacity.
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.max-capacity
    [max_capacity: <int> | default = 6000]

    # DynamoDB minimum seconds between each autoscale up.
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.out-cooldown
    [out_cooldown: <int> | default = 1800]

    # DynamoDB minimum seconds between each autoscale down.
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.in-cooldown
    [in_cooldown: <int> | default = 1800]

    # DynamoDB target ratio of consumed capacity to provisioned capacity.
    # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale.target-value
    [target: <float> | default = 80]

  # Number of last inactive tables to enable read autoscale.
  # CLI flag: -dynamodb.chunk-table.inactive-read-throughput.scale-last-n
  [inactive_read_scale_lastn: <int> | default = 4]
```

### `storage_config`

The `storage_config` configures where Cortex stores the data (chunks storage engine).

```yaml
# The storage engine to use: chunks or tsdb. Be aware tsdb is experimental and
# shouldn't be used in production.
# CLI flag: -store.engine
[engine: <string> | default = "chunks"]

aws:
  dynamodb:
    # DynamoDB endpoint URL with escaped Key and Secret encoded. If only region
    # is specified as a host, proper endpoint will be deduced. Use
    # inmemory:///<table-name> to use a mock in-memory implementation.
    # CLI flag: -dynamodb.url
    [dynamodb_url: <url> | default = ]

    # DynamoDB table management requests per second limit.
    # CLI flag: -dynamodb.api-limit
    [api_limit: <float> | default = 2]

    # DynamoDB rate cap to back off when throttled.
    # CLI flag: -dynamodb.throttle-limit
    [throttle_limit: <float> | default = 10]

    # ApplicationAutoscaling endpoint URL with escaped Key and Secret encoded.
    # CLI flag: -applicationautoscaling.url
    [application_autoscaling_url: <url> | default = ]

    metrics:
      # Use metrics-based autoscaling, via this query URL
      # CLI flag: -metrics.url
      [url: <string> | default = ""]

      # Queue length above which we will scale up capacity
      # CLI flag: -metrics.target-queue-length
      [target_queue_length: <int> | default = 100000]

      # Scale up capacity by this multiple
      # CLI flag: -metrics.scale-up-factor
      [scale_up_factor: <float> | default = 1.3]

      # Ignore throttling below this level (rate per second)
      # CLI flag: -metrics.ignore-throttle-below
      [ignore_throttle_below: <float> | default = 1]

      # query to fetch ingester queue length
      # CLI flag: -metrics.queue-length-query
      [queue_length_query: <string> | default = "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))"]

      # query to fetch throttle rates per table
      # CLI flag: -metrics.write-throttle-query
      [write_throttle_query: <string> | default = "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) > 0"]

      # query to fetch write capacity usage per table
      # CLI flag: -metrics.usage-query
      [write_usage_query: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) > 0"]

      # query to fetch read capacity usage per table
      # CLI flag: -metrics.read-usage-query
      [read_usage_query: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) > 0"]

      # query to fetch read errors per table
      # CLI flag: -metrics.read-error-query
      [read_error_query: <string> | default = "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) > 0"]

    # Number of chunks to group together to parallelise fetches (zero to
    # disable)
    # CLI flag: -dynamodb.chunk-gang-size
    [chunk_gang_size: <int> | default = 10]

    # Max number of chunk-get operations to start in parallel
    # CLI flag: -dynamodb.chunk.get-max-parallelism
    [chunk_get_max_parallelism: <int> | default = 32]

  # S3 endpoint URL with escaped Key and Secret encoded. If only region is
  # specified as a host, proper endpoint will be deduced. Use
  # inmemory:///<bucket-name> to use a mock in-memory implementation.
  # CLI flag: -s3.url
  [s3: <url> | default = ]

  # Comma separated list of bucket names to evenly distribute chunks over.
  # Overrides any buckets specified in s3.url flag
  # CLI flag: -s3.buckets
  [bucketnames: <string> | default = ""]

  # Set this to `true` to force the request to use path-style addressing.
  # CLI flag: -s3.force-path-style
  [s3forcepathstyle: <boolean> | default = false]

azure:
  # Name of the blob container used to store chunks. Defaults to `cortex`. This
  # container must be created before running cortex.
  # CLI flag: -azure.container-name
  [container_name: <string> | default = "cortex"]

  # The Microsoft Azure account name to be used
  # CLI flag: -azure.account-name
  [account_name: <string> | default = ""]

  # The Microsoft Azure account key to use.
  # CLI flag: -azure.account-key
  [account_key: <string> | default = ""]

  # Preallocated buffer size for downloads (default is 512KB)
  # CLI flag: -azure.download-buffer-size
  [download_buffer_size: <int> | default = 512000]

  # Preallocated buffer size for up;oads (default is 256KB)
  # CLI flag: -azure.upload-buffer-size
  [upload_buffer_size: <int> | default = 256000]

  # Number of buffers used to used to upload a chunk. (defaults to 1)
  # CLI flag: -azure.download-buffer-count
  [upload_buffer_count: <int> | default = 1]

  # Timeout for requests made against azure blob storage. Defaults to 30
  # seconds.
  # CLI flag: -azure.request-timeout
  [request_timeout: <duration> | default = 30s]

  # Number of retries for a request which times out.
  # CLI flag: -azure.max-retries
  [max_retries: <int> | default = 5]

  # Minimum time to wait before retrying a request.
  # CLI flag: -azure.min-retry-delay
  [min_retry_delay: <duration> | default = 10ms]

  # Maximum time to wait before retrying a request.
  # CLI flag: -azure.max-retry-delay
  [max_retry_delay: <duration> | default = 500ms]

bigtable:
  # Bigtable project ID.
  # CLI flag: -bigtable.project
  [project: <string> | default = ""]

  # Bigtable instance ID.
  # CLI flag: -bigtable.instance
  [instance: <string> | default = ""]

  grpc_client_config:
    # gRPC client max receive message size (bytes).
    # CLI flag: -bigtable.grpc-max-recv-msg-size
    [max_recv_msg_size: <int> | default = 104857600]

    # gRPC client max send message size (bytes).
    # CLI flag: -bigtable.grpc-max-send-msg-size
    [max_send_msg_size: <int> | default = 16777216]

    # Use compression when sending messages.
    # CLI flag: -bigtable.grpc-use-gzip-compression
    [use_gzip_compression: <boolean> | default = false]

    # Rate limit for gRPC client; 0 means disabled.
    # CLI flag: -bigtable.grpc-client-rate-limit
    [rate_limit: <float> | default = 0]

    # Rate limit burst for gRPC client.
    # CLI flag: -bigtable.grpc-client-rate-limit-burst
    [rate_limit_burst: <int> | default = 0]

    # Enable backoff and retry when we hit ratelimits.
    # CLI flag: -bigtable.backoff-on-ratelimits
    [backoff_on_ratelimits: <boolean> | default = false]

    backoff_config:
      # Minimum delay when backing off.
      # CLI flag: -bigtable.backoff-min-period
      [min_period: <duration> | default = 100ms]

      # Maximum delay when backing off.
      # CLI flag: -bigtable.backoff-max-period
      [max_period: <duration> | default = 10s]

      # Number of times to backoff and retry before failing.
      # CLI flag: -bigtable.backoff-retries
      [max_retries: <int> | default = 10]

  # If enabled, once a tables info is fetched, it is cached.
  # CLI flag: -bigtable.table-cache.enabled
  [table_cache_enabled: <boolean> | default = true]

  # Duration to cache tables before checking again.
  # CLI flag: -bigtable.table-cache.expiration
  [table_cache_expiration: <duration> | default = 30m0s]

gcs:
  # Name of GCS bucket to put chunks in.
  # CLI flag: -gcs.bucketname
  [bucket_name: <string> | default = ""]

  # The size of the buffer that GCS client for each PUT request. 0 to disable
  # buffering.
  # CLI flag: -gcs.chunk-buffer-size
  [chunk_buffer_size: <int> | default = 0]

  # The duration after which the requests to GCS should be timed out.
  # CLI flag: -gcs.request-timeout
  [request_timeout: <duration> | default = 0s]

cassandra:
  # Comma-separated hostnames or IPs of Cassandra instances.
  # CLI flag: -cassandra.addresses
  [addresses: <string> | default = ""]

  # Port that Cassandra is running on
  # CLI flag: -cassandra.port
  [port: <int> | default = 9042]

  # Keyspace to use in Cassandra.
  # CLI flag: -cassandra.keyspace
  [keyspace: <string> | default = ""]

  # Consistency level for Cassandra.
  # CLI flag: -cassandra.consistency
  [consistency: <string> | default = "QUORUM"]

  # Replication factor to use in Cassandra.
  # CLI flag: -cassandra.replication-factor
  [replication_factor: <int> | default = 1]

  # Instruct the cassandra driver to not attempt to get host info from the
  # system.peers table.
  # CLI flag: -cassandra.disable-initial-host-lookup
  [disable_initial_host_lookup: <boolean> | default = false]

  # Use SSL when connecting to cassandra instances.
  # CLI flag: -cassandra.ssl
  [SSL: <boolean> | default = false]

  # Require SSL certificate validation.
  # CLI flag: -cassandra.host-verification
  [host_verification: <boolean> | default = true]

  # Path to certificate file to verify the peer.
  # CLI flag: -cassandra.ca-path
  [CA_path: <string> | default = ""]

  # Enable password authentication when connecting to cassandra.
  # CLI flag: -cassandra.auth
  [auth: <boolean> | default = false]

  # Username to use when connecting to cassandra.
  # CLI flag: -cassandra.username
  [username: <string> | default = ""]

  # Password to use when connecting to cassandra.
  # CLI flag: -cassandra.password
  [password: <string> | default = ""]

  # File containing password to use when connecting to cassandra.
  # CLI flag: -cassandra.password-file
  [password_file: <string> | default = ""]

  # If set, when authenticating with cassandra a custom authenticator will be
  # expected during the handshake. This flag can be set multiple times.
  # CLI flag: -cassandra.custom-authenticator
  [custom_authenticators: <list of string> | default = ]

  # Timeout when connecting to cassandra.
  # CLI flag: -cassandra.timeout
  [timeout: <duration> | default = 2s]

  # Initial connection timeout, used during initial dial to server.
  # CLI flag: -cassandra.connect-timeout
  [connect_timeout: <duration> | default = 5s]

  # Number of retries to perform on a request. (Default is 0: no retries)
  # CLI flag: -cassandra.max-retries
  [max_retries: <int> | default = 0]

  # Maximum time to wait before retrying a failed request. (Default = 10s)
  # CLI flag: -cassandra.retry-max-backoff
  [retry_max_backoff: <duration> | default = 10s]

  # Minimum time to wait before retrying a failed request. (Default = 100ms)
  # CLI flag: -cassandra.retry-min-backoff
  [retry_min_backoff: <duration> | default = 100ms]

boltdb:
  # Location of BoltDB index files.
  # CLI flag: -boltdb.dir
  [directory: <string> | default = ""]

filesystem:
  # Directory to store chunks in.
  # CLI flag: -local.chunk-directory
  [directory: <string> | default = ""]

# Cache validity for active index entries. Should be no higher than
# -ingester.max-chunk-idle.
# CLI flag: -store.index-cache-validity
[index_cache_validity: <duration> | default = 5m0s]

index_queries_cache_config:
  # Cache config for index entry reading. Enable in-memory cache.
  # CLI flag: -store.index-cache-read.cache.enable-fifocache
  [enable_fifocache: <boolean> | default = false]

  # Cache config for index entry reading. The default validity of entries for
  # caches unless overridden.
  # CLI flag: -store.index-cache-read.default-validity
  [default_validity: <duration> | default = 0s]

  background:
    # Cache config for index entry reading. At what concurrency to write back to
    # cache.
    # CLI flag: -store.index-cache-read.background.write-back-concurrency
    [writeback_goroutines: <int> | default = 10]

    # Cache config for index entry reading. How many key batches to buffer for
    # background write-back.
    # CLI flag: -store.index-cache-read.background.write-back-buffer
    [writeback_buffer: <int> | default = 10000]

  # The memcached_config block configures how data is stored in Memcached (ie.
  # expiration).
  # The CLI flags prefix for this block config is: store.index-cache-read
  [memcached: <memcached_config>]

  # The memcached_client_config configures the client used to connect to
  # Memcached.
  # The CLI flags prefix for this block config is: store.index-cache-read
  [memcached_client: <memcached_client_config>]

  # The redis_config configures the Redis backend cache.
  # The CLI flags prefix for this block config is: store.index-cache-read
  [redis: <redis_config>]

  # The fifo_cache_config configures the local in-memory cache.
  # The CLI flags prefix for this block config is: store.index-cache-read
  [fifocache: <fifo_cache_config>]

delete_store:
  # Store for keeping delete request
  # CLI flag: -deletes.store
  [store: <string> | default = ""]

  # Name of the table which stores delete requests
  # CLI flag: -deletes.requests-table-name
  [requests_table_name: <string> | default = "delete_requests"]
```

### `chunk_store_config`

The `chunk_store_config` configures how Cortex stores the data (chunks storage engine).

```yaml
chunk_cache_config:
  # Cache config for chunks. Enable in-memory cache.
  # CLI flag: -store.chunks-cache.cache.enable-fifocache
  [enable_fifocache: <boolean> | default = false]

  # Cache config for chunks. The default validity of entries for caches unless
  # overridden.
  # CLI flag: -store.chunks-cache.default-validity
  [default_validity: <duration> | default = 0s]

  background:
    # Cache config for chunks. At what concurrency to write back to cache.
    # CLI flag: -store.chunks-cache.background.write-back-concurrency
    [writeback_goroutines: <int> | default = 10]

    # Cache config for chunks. How many key batches to buffer for background
    # write-back.
    # CLI flag: -store.chunks-cache.background.write-back-buffer
    [writeback_buffer: <int> | default = 10000]

  # The memcached_config block configures how data is stored in Memcached (ie.
  # expiration).
  # The CLI flags prefix for this block config is: store.chunks-cache
  [memcached: <memcached_config>]

  # The memcached_client_config configures the client used to connect to
  # Memcached.
  # The CLI flags prefix for this block config is: store.chunks-cache
  [memcached_client: <memcached_client_config>]

  # The redis_config configures the Redis backend cache.
  # The CLI flags prefix for this block config is: store.chunks-cache
  [redis: <redis_config>]

  # The fifo_cache_config configures the local in-memory cache.
  # The CLI flags prefix for this block config is: store.chunks-cache
  [fifocache: <fifo_cache_config>]

write_dedupe_cache_config:
  # Cache config for index entry writing. Enable in-memory cache.
  # CLI flag: -store.index-cache-write.cache.enable-fifocache
  [enable_fifocache: <boolean> | default = false]

  # Cache config for index entry writing. The default validity of entries for
  # caches unless overridden.
  # CLI flag: -store.index-cache-write.default-validity
  [default_validity: <duration> | default = 0s]

  background:
    # Cache config for index entry writing. At what concurrency to write back to
    # cache.
    # CLI flag: -store.index-cache-write.background.write-back-concurrency
    [writeback_goroutines: <int> | default = 10]

    # Cache config for index entry writing. How many key batches to buffer for
    # background write-back.
    # CLI flag: -store.index-cache-write.background.write-back-buffer
    [writeback_buffer: <int> | default = 10000]

  # The memcached_config block configures how data is stored in Memcached (ie.
  # expiration).
  # The CLI flags prefix for this block config is: store.index-cache-write
  [memcached: <memcached_config>]

  # The memcached_client_config configures the client used to connect to
  # Memcached.
  # The CLI flags prefix for this block config is: store.index-cache-write
  [memcached_client: <memcached_client_config>]

  # The redis_config configures the Redis backend cache.
  # The CLI flags prefix for this block config is: store.index-cache-write
  [redis: <redis_config>]

  # The fifo_cache_config configures the local in-memory cache.
  # The CLI flags prefix for this block config is: store.index-cache-write
  [fifocache: <fifo_cache_config>]

# Cache index entries older than this period. 0 to disable.
# CLI flag: -store.cache-lookups-older-than
[cache_lookups_older_than: <duration> | default = 0s]

# Limit how long back data can be queried
# CLI flag: -store.max-look-back-period
[max_look_back_period: <duration> | default = 0s]
```

### `ingester_client_config`

The `ingester_client_config` configures how the Cortex distributors connect to the ingesters.

```yaml
grpc_client_config:
  # gRPC client max receive message size (bytes).
  # CLI flag: -ingester.client.grpc-max-recv-msg-size
  [max_recv_msg_size: <int> | default = 104857600]

  # gRPC client max send message size (bytes).
  # CLI flag: -ingester.client.grpc-max-send-msg-size
  [max_send_msg_size: <int> | default = 16777216]

  # Use compression when sending messages.
  # CLI flag: -ingester.client.grpc-use-gzip-compression
  [use_gzip_compression: <boolean> | default = false]

  # Rate limit for gRPC client; 0 means disabled.
  # CLI flag: -ingester.client.grpc-client-rate-limit
  [rate_limit: <float> | default = 0]

  # Rate limit burst for gRPC client.
  # CLI flag: -ingester.client.grpc-client-rate-limit-burst
  [rate_limit_burst: <int> | default = 0]

  # Enable backoff and retry when we hit ratelimits.
  # CLI flag: -ingester.client.backoff-on-ratelimits
  [backoff_on_ratelimits: <boolean> | default = false]

  backoff_config:
    # Minimum delay when backing off.
    # CLI flag: -ingester.client.backoff-min-period
    [min_period: <duration> | default = 100ms]

    # Maximum delay when backing off.
    # CLI flag: -ingester.client.backoff-max-period
    [max_period: <duration> | default = 10s]

    # Number of times to backoff and retry before failing.
    # CLI flag: -ingester.client.backoff-retries
    [max_retries: <int> | default = 10]
```

### `frontend_worker_config`

The `frontend_worker_config` configures the worker - running within the Cortex querier - picking up and executing queries enqueued by the query-frontend.

```yaml
# Address of query frontend service, in host:port format.
# CLI flag: -querier.frontend-address
[frontend_address: <string> | default = ""]

# Number of simultaneous queries to process.
# CLI flag: -querier.worker-parallelism
[parallelism: <int> | default = 10]

# How often to query DNS.
# CLI flag: -querier.dns-lookup-period
[dns_lookup_duration: <duration> | default = 10s]

grpc_client_config:
  # gRPC client max receive message size (bytes).
  # CLI flag: -querier.frontend-client.grpc-max-recv-msg-size
  [max_recv_msg_size: <int> | default = 104857600]

  # gRPC client max send message size (bytes).
  # CLI flag: -querier.frontend-client.grpc-max-send-msg-size
  [max_send_msg_size: <int> | default = 16777216]

  # Use compression when sending messages.
  # CLI flag: -querier.frontend-client.grpc-use-gzip-compression
  [use_gzip_compression: <boolean> | default = false]

  # Rate limit for gRPC client; 0 means disabled.
  # CLI flag: -querier.frontend-client.grpc-client-rate-limit
  [rate_limit: <float> | default = 0]

  # Rate limit burst for gRPC client.
  # CLI flag: -querier.frontend-client.grpc-client-rate-limit-burst
  [rate_limit_burst: <int> | default = 0]

  # Enable backoff and retry when we hit ratelimits.
  # CLI flag: -querier.frontend-client.backoff-on-ratelimits
  [backoff_on_ratelimits: <boolean> | default = false]

  backoff_config:
    # Minimum delay when backing off.
    # CLI flag: -querier.frontend-client.backoff-min-period
    [min_period: <duration> | default = 100ms]

    # Maximum delay when backing off.
    # CLI flag: -querier.frontend-client.backoff-max-period
    [max_period: <duration> | default = 10s]

    # Number of times to backoff and retry before failing.
    # CLI flag: -querier.frontend-client.backoff-retries
    [max_retries: <int> | default = 10]
```

### `etcd_config`

The `etcd_config` configures the etcd client. The supported CLI flags `<prefix>` used to reference this config block are:

- _no prefix_
- `compactor.ring`
- `distributor.ha-tracker`
- `distributor.ring`
- `ruler.ring`
- `store-gateway.ring`

&nbsp;

```yaml
# The etcd endpoints to connect to.
# CLI flag: -<prefix>.etcd.endpoints
[endpoints: <list of string> | default = []]

# The dial timeout for the etcd connection.
# CLI flag: -<prefix>.etcd.dial-timeout
[dial_timeout: <duration> | default = 10s]

# The maximum number of retries to do for failed ops.
# CLI flag: -<prefix>.etcd.max-retries
[max_retries: <int> | default = 10]
```

### `consul_config`

The `consul_config` configures the consul client. The supported CLI flags `<prefix>` used to reference this config block are:

- _no prefix_
- `compactor.ring`
- `distributor.ha-tracker`
- `distributor.ring`
- `ruler.ring`
- `store-gateway.ring`

&nbsp;

```yaml
# Hostname and port of Consul.
# CLI flag: -<prefix>.consul.hostname
[host: <string> | default = "localhost:8500"]

# ACL Token used to interact with Consul.
# CLI flag: -<prefix>.consul.acl-token
[acl_token: <string> | default = ""]

# HTTP timeout when talking to Consul
# CLI flag: -<prefix>.consul.client-timeout
[http_client_timeout: <duration> | default = 20s]

# Enable consistent reads to Consul.
# CLI flag: -<prefix>.consul.consistent-reads
[consistent_reads: <boolean> | default = true]

# Rate limit when watching key or prefix in Consul, in requests per second. 0
# disables the rate limit.
# CLI flag: -<prefix>.consul.watch-rate-limit
[watch_rate_limit: <float> | default = 0]

# Burst size used in rate limit. Values less than 1 are treated as 1.
# CLI flag: -<prefix>.consul.watch-burst-size
[watch_burst_size: <int> | default = 1]
```

### `memberlist_config`

The `memberlist_config` configures the Gossip memberlist.

```yaml
# Name of the node in memberlist cluster. Defaults to hostname.
# CLI flag: -memberlist.nodename
[node_name: <string> | default = ""]

# The timeout for establishing a connection with a remote node, and for
# read/write operations. Uses memberlist LAN defaults if 0.
# CLI flag: -memberlist.stream-timeout
[stream_timeout: <duration> | default = 0s]

# Multiplication factor used when sending out messages (factor * log(N+1)).
# CLI flag: -memberlist.retransmit-factor
[retransmit_factor: <int> | default = 0]

# How often to use pull/push sync. Uses memberlist LAN defaults if 0.
# CLI flag: -memberlist.pullpush-interval
[pull_push_interval: <duration> | default = 0s]

# How often to gossip. Uses memberlist LAN defaults if 0.
# CLI flag: -memberlist.gossip-interval
[gossip_interval: <duration> | default = 0s]

# How many nodes to gossip to. Uses memberlist LAN defaults if 0.
# CLI flag: -memberlist.gossip-nodes
[gossip_nodes: <int> | default = 0]

# How long to keep gossiping to dead nodes, to give them chance to refute their
# death. Uses memberlist LAN defaults if 0.
# CLI flag: -memberlist.gossip-to-dead-nodes-time
[gossip_to_dead_nodes_time: <duration> | default = 0s]

# How soon can dead node's name be reclaimed with new address. Defaults to 0,
# which is disabled.
# CLI flag: -memberlist.dead-node-reclaim-time
[dead_node_reclaim_time: <duration> | default = 0s]

# Other cluster members to join. Can be specified multiple times. Memberlist
# store is EXPERIMENTAL.
# CLI flag: -memberlist.join
[join_members: <list of string> | default = ]

# If this node fails to join memberlist cluster, abort.
# CLI flag: -memberlist.abort-if-join-fails
[abort_if_cluster_join_fails: <boolean> | default = true]

# How long to keep LEFT ingesters in the ring.
# CLI flag: -memberlist.left-ingesters-timeout
[left_ingesters_timeout: <duration> | default = 5m0s]

# Timeout for leaving memberlist cluster.
# CLI flag: -memberlist.leave-timeout
[leave_timeout: <duration> | default = 5s]

# IP address to listen on for gossip messages. Multiple addresses may be
# specified. Defaults to 0.0.0.0
# CLI flag: -memberlist.bind-addr
[bind_addr: <list of string> | default = ]

# Port to listen on for gossip messages.
# CLI flag: -memberlist.bind-port
[bind_port: <int> | default = 7946]

# Timeout used when connecting to other nodes to send packet.
# CLI flag: -memberlist.packet-dial-timeout
[packet_dial_timeout: <duration> | default = 5s]

# Timeout for writing 'packet' data.
# CLI flag: -memberlist.packet-write-timeout
[packet_write_timeout: <duration> | default = 5s]
```

### `limits_config`

The `limits_config` configures default and per-tenant limits imposed by Cortex services (ie. distributor, ingester, ...).

```yaml
# Per-user ingestion rate limit in samples per second.
# CLI flag: -distributor.ingestion-rate-limit
[ingestion_rate: <float> | default = 25000]

# Whether the ingestion rate limit should be applied individually to each
# distributor instance (local), or evenly shared across the cluster (global).
# CLI flag: -distributor.ingestion-rate-limit-strategy
[ingestion_rate_strategy: <string> | default = "local"]

# Per-user allowed ingestion burst size (in number of samples).
# CLI flag: -distributor.ingestion-burst-size
[ingestion_burst_size: <int> | default = 50000]

# Flag to enable, for all users, handling of samples with external labels
# identifying replicas in an HA Prometheus setup.
# CLI flag: -distributor.ha-tracker.enable-for-all-users
[accept_ha_samples: <boolean> | default = false]

# Prometheus label to look for in samples to identify a Prometheus HA cluster.
# CLI flag: -distributor.ha-tracker.cluster
[ha_cluster_label: <string> | default = "cluster"]

# Prometheus label to look for in samples to identify a Prometheus HA replica.
# CLI flag: -distributor.ha-tracker.replica
[ha_replica_label: <string> | default = "__replica__"]

# This flag can be used to specify label names that to drop during sample
# ingestion within the distributor and can be repeated in order to drop multiple
# labels.
# CLI flag: -distributor.drop-label
[drop_labels: <list of string> | default = ]

# Maximum length accepted for label names
# CLI flag: -validation.max-length-label-name
[max_label_name_length: <int> | default = 1024]

# Maximum length accepted for label value. This setting also applies to the
# metric name
# CLI flag: -validation.max-length-label-value
[max_label_value_length: <int> | default = 2048]

# Maximum number of label names per series.
# CLI flag: -validation.max-label-names-per-series
[max_label_names_per_series: <int> | default = 30]

# Reject old samples.
# CLI flag: -validation.reject-old-samples
[reject_old_samples: <boolean> | default = false]

# Maximum accepted sample age before rejecting.
# CLI flag: -validation.reject-old-samples.max-age
[reject_old_samples_max_age: <duration> | default = 336h0m0s]

# Duration which table will be created/deleted before/after it's needed; we
# won't accept sample from before this time.
# CLI flag: -validation.create-grace-period
[creation_grace_period: <duration> | default = 10m0s]

# Enforce every sample has a metric name.
# CLI flag: -validation.enforce-metric-name
[enforce_metric_name: <boolean> | default = true]

# Per-user subring to shard metrics to ingesters. 0 is disabled.
# CLI flag: -experimental.distributor.user-subring-size
[user_subring_size: <int> | default = 0]

# The maximum number of series that a query can return.
# CLI flag: -ingester.max-series-per-query
[max_series_per_query: <int> | default = 100000]

# The maximum number of samples that a query can return.
# CLI flag: -ingester.max-samples-per-query
[max_samples_per_query: <int> | default = 1000000]

# The maximum number of active series per user, per ingester. 0 to disable.
# CLI flag: -ingester.max-series-per-user
[max_series_per_user: <int> | default = 5000000]

# The maximum number of active series per metric name, per ingester. 0 to
# disable.
# CLI flag: -ingester.max-series-per-metric
[max_series_per_metric: <int> | default = 50000]

# The maximum number of active series per user, across the cluster. 0 to
# disable. Supported only if -distributor.shard-by-all-labels is true.
# CLI flag: -ingester.max-global-series-per-user
[max_global_series_per_user: <int> | default = 0]

# The maximum number of active series per metric name, across the cluster. 0 to
# disable.
# CLI flag: -ingester.max-global-series-per-metric
[max_global_series_per_metric: <int> | default = 0]

# Minimum number of samples in an idle chunk to flush it to the store. Use with
# care, if chunks are less than this size they will be discarded.
# CLI flag: -ingester.min-chunk-length
[min_chunk_length: <int> | default = 0]

# Maximum number of chunks that can be fetched in a single query.
# CLI flag: -store.query-chunk-limit
[max_chunks_per_query: <int> | default = 2000000]

# Limit to length of chunk store queries, 0 to disable.
# CLI flag: -store.max-query-length
[max_query_length: <duration> | default = 0s]

# Maximum number of queries will be scheduled in parallel by the frontend.
# CLI flag: -querier.max-query-parallelism
[max_query_parallelism: <int> | default = 14]

# Cardinality limit for index queries.
# CLI flag: -store.cardinality-limit
[cardinality_limit: <int> | default = 100000]

# File name of per-user overrides. [deprecated, use -runtime-config.file
# instead]
# CLI flag: -limits.per-user-override-config
[per_tenant_override_config: <string> | default = ""]

# Period with which to reload the overrides. [deprecated, use
# -runtime-config.reload-period instead]
# CLI flag: -limits.per-user-override-period
[per_tenant_override_period: <duration> | default = 10s]
```

### `redis_config`

The `redis_config` configures the Redis backend cache. The supported CLI flags `<prefix>` used to reference this config block are:

- `frontend`
- `store.chunks-cache`
- `store.index-cache-read`
- `store.index-cache-write`

&nbsp;

```yaml
# Redis service endpoint to use when caching chunks. If empty, no redis will be
# used.
# CLI flag: -<prefix>.redis.endpoint
[endpoint: <string> | default = ""]

# Maximum time to wait before giving up on redis requests.
# CLI flag: -<prefix>.redis.timeout
[timeout: <duration> | default = 100ms]

# How long keys stay in the redis.
# CLI flag: -<prefix>.redis.expiration
[expiration: <duration> | default = 0s]

# Maximum number of idle connections in pool.
# CLI flag: -<prefix>.redis.max-idle-conns
[max_idle_conns: <int> | default = 80]

# Maximum number of active connections in pool.
# CLI flag: -<prefix>.redis.max-active-conns
[max_active_conns: <int> | default = 0]

# Password to use when connecting to redis.
# CLI flag: -<prefix>.redis.password
[password: <string> | default = ""]

# Enables connecting to redis with TLS.
# CLI flag: -<prefix>.redis.enable-tls
[enable_tls: <boolean> | default = false]
```

### `memcached_config`

The `memcached_config` block configures how data is stored in Memcached (ie. expiration). The supported CLI flags `<prefix>` used to reference this config block are:

- `frontend`
- `store.chunks-cache`
- `store.index-cache-read`
- `store.index-cache-write`

&nbsp;

```yaml
# How long keys stay in the memcache.
# CLI flag: -<prefix>.memcached.expiration
[expiration: <duration> | default = 0s]

# How many keys to fetch in each batch.
# CLI flag: -<prefix>.memcached.batchsize
[batch_size: <int> | default = 0]

# Maximum active requests to memcache.
# CLI flag: -<prefix>.memcached.parallelism
[parallelism: <int> | default = 100]
```

### `memcached_client_config`

The `memcached_client_config` configures the client used to connect to Memcached. The supported CLI flags `<prefix>` used to reference this config block are:

- `frontend`
- `store.chunks-cache`
- `store.index-cache-read`
- `store.index-cache-write`

&nbsp;

```yaml
# Hostname for memcached service to use. If empty and if addresses is unset, no
# memcached will be used.
# CLI flag: -<prefix>.memcached.hostname
[host: <string> | default = ""]

# SRV service used to discover memcache servers.
# CLI flag: -<prefix>.memcached.service
[service: <string> | default = "memcached"]

# EXPERIMENTAL: Comma separated addresses list in DNS Service Discovery format:
# https://cortexmetrics.io/docs/configuration/arguments/#dns-service-discovery
# CLI flag: -<prefix>.memcached.addresses
[addresses: <string> | default = ""]

# Maximum time to wait before giving up on memcached requests.
# CLI flag: -<prefix>.memcached.timeout
[timeout: <duration> | default = 100ms]

# Maximum number of idle connections in pool.
# CLI flag: -<prefix>.memcached.max-idle-conns
[max_idle_conns: <int> | default = 16]

# Period with which to poll DNS for memcache servers.
# CLI flag: -<prefix>.memcached.update-interval
[update_interval: <duration> | default = 1m0s]

# Use consistent hashing to distribute to memcache servers.
# CLI flag: -<prefix>.memcached.consistent-hash
[consistent_hash: <boolean> | default = false]
```

### `fifo_cache_config`

The `fifo_cache_config` configures the local in-memory cache. The supported CLI flags `<prefix>` used to reference this config block are:

- `frontend`
- `store.chunks-cache`
- `store.index-cache-read`
- `store.index-cache-write`

&nbsp;

```yaml
# The number of entries to cache.
# CLI flag: -<prefix>.fifocache.size
[size: <int> | default = 0]

# The expiry duration for the cache.
# CLI flag: -<prefix>.fifocache.duration
[validity: <duration> | default = 0s]
```

### `configs_config`

The `configs_config` configures the Cortex Configs DB and API.

```yaml
database:
  # URI where the database can be found (for dev you can use memory://)
  # CLI flag: -configs.database.uri
  [uri: <string> | default = "postgres://postgres@configs-db.weave.local/configs?sslmode=disable"]

  # Path where the database migration files can be found
  # CLI flag: -configs.database.migrations-dir
  [migrations_dir: <string> | default = ""]

  # File containing password (username goes in URI)
  # CLI flag: -configs.database.password-file
  [password_file: <string> | default = ""]

api:
  notifications:
    # Disable Email notifications for Alertmanager.
    # CLI flag: -configs.notifications.disable-email
    [disable_email: <boolean> | default = false]

    # Disable WebHook notifications for Alertmanager.
    # CLI flag: -configs.notifications.disable-webhook
    [disable_webhook: <boolean> | default = false]
```

### `configstore_config`

The `configstore_config` configures the config database storing rules and alerts, and is used by the Cortex alertmanager. The supported CLI flags `<prefix>` used to reference this config block are:

- `alertmanager`
- `ruler`

&nbsp;

```yaml
# URL of configs API server.
# CLI flag: -<prefix>.configs.url
[configs_api_url: <url> | default = ]

# Timeout for requests to Weave Cloud configs service.
# CLI flag: -<prefix>.configs.client-timeout
[client_timeout: <duration> | default = 5s]
```

### `tsdb_config`

The `tsdb_config` configures the experimental blocks storage.

```yaml
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

  # Max number of concurrent queries to execute against the long-term storage on
  # a per-tenant basis.
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
      # dns+ (looked up as an A/AAAA query), dnssrv+ (looked up as a SRV query,
      # dnssrvnoa+ (looked up as a SRV query, with no A/AAAA lookup made after
      # that).
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

      # The maximum number of concurrent connections running get operations. If
      # set to 0, concurrency is unlimited.
      # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-get-multi-concurrency
      [max_get_multi_concurrency: <int> | default = 100]

      # The maximum number of keys a single underlying get operation should run.
      # If more keys are specified, internally keys are splitted into multiple
      # batches and fetched concurrently, honoring the max concurrency. If set
      # to 0, the max batch size is unlimited.
      # CLI flag: -experimental.tsdb.bucket-store.index-cache.memcached.max-get-multi-batch-size
      [max_get_multi_batch_size: <int> | default = 0]

# How frequently does Cortex try to compact TSDB head. Block is only created if
# data covers smallest block range. Must be greater than 0 and max 5 minutes.
# CLI flag: -experimental.tsdb.head-compaction-interval
[head_compaction_interval: <duration> | default = 1m0s]

# Maximum number of tenants concurrently compacting TSDB head into a new block
# CLI flag: -experimental.tsdb.head-compaction-concurrency
[head_compaction_concurrency: <int> | default = 5]

# The number of shards of series to use in TSDB (must be a power of 2). Reducing
# this will decrease memory footprint, but can negatively impact performance.
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

  # If enabled, use http:// for the S3 endpoint instead of https://. This could
  # be useful in local dev/test environments while using an S3-compatible
  # backend storage, like Minio.
  # CLI flag: -experimental.tsdb.s3.insecure
  [insecure: <boolean> | default = false]

gcs:
  # GCS bucket name
  # CLI flag: -experimental.tsdb.gcs.bucket-name
  [bucket_name: <string> | default = ""]

  # JSON representing either a Google Developers Console client_credentials.json
  # file or a Google Developers service account key file. If empty, fallback to
  # Google default logic.
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
# List of compaction time ranges.
# CLI flag: -compactor.block-ranges
[block_ranges: <list of duration> | default = 2h0m0s,12h0m0s,24h0m0s]

# Number of Go routines to use when syncing block index and chunks files from
# the long term storage.
# CLI flag: -compactor.block-sync-concurrency
[block_sync_concurrency: <int> | default = 20]

# Number of Go routines to use when syncing block meta files from the long term
# storage.
# CLI flag: -compactor.meta-sync-concurrency
[meta_sync_concurrency: <int> | default = 20]

# Minimum age of fresh (non-compacted) blocks before they are being processed.
# Malformed blocks older than the maximum of consistency-delay and 48h0m0s will
# be removed.
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

# Shard tenants across multiple compactor instances. Sharding is required if you
# run multiple compactor instances, in order to coordinate compactions and avoid
# race conditions leading to the same tenant blocks simultaneously compacted by
# different instances.
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

  # The heartbeat timeout after which compactors are considered unhealthy within
  # the ring.
  # CLI flag: -compactor.ring.heartbeat-timeout
  [heartbeat_timeout: <duration> | default = 1m0s]
```

### `purger_config`

The `purger_config` configures the purger which takes care of delete requests

```yaml
# Enable purger to allow deletion of series. Be aware that Delete series feature
# is still experimental
# CLI flag: -purger.enable
[enable: <boolean> | default = false]

# Number of workers executing delete plans in parallel
# CLI flag: -purger.num-workers
[num_workers: <int> | default = 2]

# Name of the object store to use for storing delete plans
# CLI flag: -purger.object-store-type
[object_store_type: <string> | default = ""]
```
