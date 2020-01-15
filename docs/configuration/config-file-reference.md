---
title: "Configuration file"
linkTitle: "Configuration file"
weight: 1
slug: configuration-file
---

Cortex can be configured using a YAML file - specified using the `-config.file` flag - or CLI flags. In case you combine both, CLI flags take precedence over the YAML config file.

The current configuration of any Cortex component can be seen by visiting the `/config` HTTP path.

## Reference

To specify which configuration file to load, pass the `-config.file` flag at the command line. The file is written in [YAML format](https://en.wikipedia.org/wiki/YAML), defined by the scheme below. Brackets indicate that a parameter is optional.

### Generic placeholders

* `<boolean>`: a boolean that can take the values `true` or `false`
* `<int>`: any integer matching the regular expression `[1-9]+[0-9]*`
* `<duration>`: a duration matching the regular expression `[0-9]+(ns|us|µs|ms|[smh])`
* `<string>`: a regular string
* `<url>`: an URL
* `<prefix>`: a CLI flag prefix based on the context (look at the parent configuration block to see which CLI flags prefix should be used)

### Use environment variables in the configuration

You can use environment variable references in the config file to set values that need to be configurable during deployment.
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

# The ruler_config configures the Cortex ruler.
[ruler: <ruler_config>]

# The configdb_config configures the config database storing rules and alerts,
# and used by the 'configs' service to expose APIs to manage them.
[configdb: <configdb_config>]

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

## `server_config`

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

## `distributor_config`

The `distributor_config` configures the Cortex distributor.

```yaml
# Report number of ingested samples to billing system.
# CLI flag: -distributor.enable-billing
[enable_billing: <boolean> | default = false]

billing:
  # Maximum number of billing events to buffer in memory
  # CLI flag: -billing.max-buffered-events
  [maxbufferedevents: <int> | default = 1024]

  # How often to retry sending events to the billing ingester.
  # CLI flag: -billing.retry-delay
  [retrydelay: <duration> | default = 500ms]

  # points to the billing ingester sidecar (should be on localhost)
  # CLI flag: -billing.ingester
  [ingesterhostport: <string> | default = "localhost:24225"]

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

## `ingester_config`

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
[flushcheckperiod: <duration> | default = 1m0s]

# Period chunks will remain in memory after flushing.
# CLI flag: -ingester.retain-period
[retainperiod: <duration> | default = 5m0s]

# Maximum chunk idle time before flushing.
# CLI flag: -ingester.max-chunk-idle
[maxchunkidle: <duration> | default = 5m0s]

# Maximum chunk idle time for chunks terminating in stale markers before
# flushing. 0 disables it and a stale series is not flushed until the
# max-chunk-idle timeout is reached.
# CLI flag: -ingester.max-stale-chunk-idle
[maxstalechunkidle: <duration> | default = 0s]

# Timeout for individual flush operations.
# CLI flag: -ingester.flush-op-timeout
[flushoptimeout: <duration> | default = 1m0s]

# Maximum chunk age before flushing.
# CLI flag: -ingester.max-chunk-age
[maxchunkage: <duration> | default = 12h0m0s]

# Range of time to subtract from MaxChunkAge to spread out flushes
# CLI flag: -ingester.chunk-age-jitter
[chunkagejitter: <duration> | default = 20m0s]

# Number of concurrent goroutines flushing to dynamodb.
# CLI flag: -ingester.concurrent-flushes
[concurrentflushes: <int> | default = 50]

# If true, spread series flushes across the whole period of MaxChunkAge
# CLI flag: -ingester.spread-flushes
[spreadflushes: <boolean> | default = false]

# Period with which to update the per-user ingestion rates.
# CLI flag: -ingester.rate-update-period
[rateupdateperiod: <duration> | default = 15s]
```

## `querier_config`

The `querier_config` configures the Cortex querier.

```yaml
# The maximum number of concurrent queries.
# CLI flag: -querier.max-concurrent
[maxconcurrent: <int> | default = 20]

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
[batchiterators: <boolean> | default = false]

# Use streaming RPCs to query ingester.
# CLI flag: -querier.ingester-streaming
[ingesterstreaming: <boolean> | default = false]

# Maximum number of samples a single query can load into memory.
# CLI flag: -querier.max-samples
[maxsamples: <int> | default = 50000000]

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
[defaultevaluationinterval: <duration> | default = 1m0s]

# Active query tracker monitors active queries, and writes them to the file in
# given directory. If Cortex discovers any queries in this log during startup,
# it will log them to the log file. Setting to empty value disables active query
# tracker, which also disables -querier.max-concurrent option.
# CLI flag: -querier.active-query-tracker-dir
[active_query_tracker_dir: <string> | default = "./active-query-tracker"]
```

## `query_frontend_config`

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
[downstream: <string> | default = ""]

# Log queries that are slower than the specified duration. 0 to disable.
# CLI flag: -frontend.log-queries-longer-than
[log_queries_longer_than: <duration> | default = 0s]
```

## `queryrange_config`

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
      # How many goroutines to use to write back to memcache.
      # CLI flag: -frontend.memcache.write-back-goroutines
      [writeback_goroutines: <int> | default = 10]

      # How many chunks to buffer for background write back.
      # CLI flag: -frontend.memcache.write-back-buffer
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

## `ruler_config`

The `ruler_config` configures the Cortex ruler.

```yaml
# URL of alerts return path.
# CLI flag: -ruler.external.url
[externalurl: <url> | default = ]

# How frequently to evaluate rules
# CLI flag: -ruler.evaluation-interval
[evaluationinterval: <duration> | default = 1m0s]

# How frequently to poll for rule changes
# CLI flag: -ruler.poll-interval
[pollinterval: <duration> | default = 1m0s]

storeconfig:
  # Method to use for backend rule storage (configdb)
  # CLI flag: -ruler.storage.type
  [type: <string> | default = "configdb"]

  # The configstore_config configures the config database storing rules and
  # alerts, and is used by the Cortex alertmanager.
  # The CLI flags prefix for this block config is: ruler
  [configdb: <configstore_config>]

# file path to store temporary rule files for the prometheus rule managers
# CLI flag: -ruler.rule-path
[rulepath: <string> | default = "/rules"]

# URL of the Alertmanager to send notifications to.
# CLI flag: -ruler.alertmanager-url
[alertmanagerurl: <url> | default = ]

# Use DNS SRV records to discover alertmanager hosts.
# CLI flag: -ruler.alertmanager-discovery
[alertmanagerdiscovery: <boolean> | default = false]

# How long to wait between refreshing alertmanager hosts.
# CLI flag: -ruler.alertmanager-refresh-interval
[alertmanagerrefreshinterval: <duration> | default = 1m0s]

# If enabled requests to alertmanager will utilize the V2 API.
# CLI flag: -ruler.alertmanager-use-v2
[alertmanangerenablev2api: <boolean> | default = false]

# Capacity of the queue for notifications to be sent to the Alertmanager.
# CLI flag: -ruler.notification-queue-capacity
[notificationqueuecapacity: <int> | default = 10000]

# HTTP timeout duration when sending notifications to the Alertmanager.
# CLI flag: -ruler.notification-timeout
[notificationtimeout: <duration> | default = 10s]

# Distribute rule evaluation using ring backend
# CLI flag: -ruler.enable-sharding
[enablesharding: <boolean> | default = false]

# Time to spend searching for a pending ruler when shutting down.
# CLI flag: -ruler.search-pending-for
[searchpendingfor: <duration> | default = 5m0s]

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
[flushcheckperiod: <duration> | default = 1m0s]

# Enable the ruler api
# CLI flag: -experimental.ruler.enable-api
[enable_api: <boolean> | default = false]
```

## `alertmanager_config`

The `alertmanager_config` configures the Cortex alertmanager.

```yaml
# Base path for data storage.
# CLI flag: -alertmanager.storage.path
[datadir: <string> | default = "data/"]

# How long to keep data for.
# CLI flag: -alertmanager.storage.retention
[retention: <duration> | default = 120h0m0s]

# The URL under which Alertmanager is externally reachable (for example, if
# Alertmanager is served via a reverse proxy). Used for generating relative and
# absolute links back to Alertmanager itself. If the URL has a path portion, it
# will be used to prefix all HTTP endpoints served by Alertmanager. If omitted,
# relevant URL components will be derived automatically.
# CLI flag: -alertmanager.web.external-url
[externalurl: <url> | default = ]

# How frequently to poll Cortex configs
# CLI flag: -alertmanager.configs.poll-interval
[pollinterval: <duration> | default = 15s]

# Listen address for cluster.
# CLI flag: -cluster.listen-address
[clusterbindaddr: <string> | default = "0.0.0.0:9094"]

# Explicit address to advertise in cluster.
# CLI flag: -cluster.advertise-address
[clusteradvertiseaddr: <string> | default = ""]

# Initial peers (may be repeated).
# CLI flag: -cluster.peer
[peers: <list of string> | default = ]

# Time to wait between peers to send notifications.
# CLI flag: -cluster.peer-timeout
[peertimeout: <duration> | default = 15s]

# Filename of fallback config to use if none specified for instance.
# CLI flag: -alertmanager.configs.fallback
[fallbackconfigfile: <string> | default = ""]

# Root of URL to generate if config is http://internal.monitor
# CLI flag: -alertmanager.configs.auto-webhook-root
[autowebhookroot: <string> | default = ""]

store:
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

## `table_manager_config`

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

## `storage_config`

The `storage_config` configures where Cortex stores the data (chunks storage engine).

```yaml
# The storage engine to use: chunks or tsdb. Be aware tsdb is experimental and
# shouldn't be used in production.
# CLI flag: -store.engine
[engine: <string> | default = "chunks"]

aws:
  dynamodbconfig:
    # DynamoDB endpoint URL with escaped Key and Secret encoded. If only region
    # is specified as a host, proper endpoint will be deduced. Use
    # inmemory:///<table-name> to use a mock in-memory implementation.
    # CLI flag: -dynamodb.url
    [dynamodb: <url> | default = ]

    # DynamoDB table management requests per second limit.
    # CLI flag: -dynamodb.api-limit
    [apilimit: <float> | default = 2]

    # DynamoDB rate cap to back off when throttled.
    # CLI flag: -dynamodb.throttle-limit
    [throttlelimit: <float> | default = 10]

    # ApplicationAutoscaling endpoint URL with escaped Key and Secret encoded.
    # CLI flag: -applicationautoscaling.url
    [applicationautoscaling: <url> | default = ]

    metrics:
      # Use metrics-based autoscaling, via this query URL
      # CLI flag: -metrics.url
      [url: <string> | default = ""]

      # Queue length above which we will scale up capacity
      # CLI flag: -metrics.target-queue-length
      [targetqueuelen: <int> | default = 100000]

      # Scale up capacity by this multiple
      # CLI flag: -metrics.scale-up-factor
      [scaleupfactor: <float> | default = 1.3]

      # Ignore throttling below this level (rate per second)
      # CLI flag: -metrics.ignore-throttle-below
      [minthrottling: <float> | default = 1]

      # query to fetch ingester queue length
      # CLI flag: -metrics.queue-length-query
      [queuelengthquery: <string> | default = "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))"]

      # query to fetch throttle rates per table
      # CLI flag: -metrics.write-throttle-query
      [throttlequery: <string> | default = "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) > 0"]

      # query to fetch write capacity usage per table
      # CLI flag: -metrics.usage-query
      [usagequery: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) > 0"]

      # query to fetch read capacity usage per table
      # CLI flag: -metrics.read-usage-query
      [readusagequery: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) > 0"]

      # query to fetch read errors per table
      # CLI flag: -metrics.read-error-query
      [readerrorquery: <string> | default = "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) > 0"]

    # Number of chunks to group together to parallelise fetches (zero to
    # disable)
    # CLI flag: -dynamodb.chunk.gang.size
    [chunkgangsize: <int> | default = 10]

    # Max number of chunk-get operations to start in parallel
    # CLI flag: -dynamodb.chunk.get.max.parallelism
    [chunkgetmaxparallelism: <int> | default = 32]

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
      [minbackoff: <duration> | default = 100ms]

      # Maximum delay when backing off.
      # CLI flag: -bigtable.backoff-max-period
      [maxbackoff: <duration> | default = 10s]

      # Number of times to backoff and retry before failing.
      # CLI flag: -bigtable.backoff-retries
      [maxretries: <int> | default = 10]

  # If enabled, once a tables info is fetched, it is cached.
  # CLI flag: -bigtable.table-cache.enabled
  [tablecacheenabled: <boolean> | default = true]

  # Duration to cache tables before checking again.
  # CLI flag: -bigtable.table-cache.expiration
  [tablecacheexpiration: <duration> | default = 30m0s]

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
[indexcachevalidity: <duration> | default = 5m0s]

index_queries_cache_config:
  # Cache config for index entry reading. Enable in-memory cache.
  # CLI flag: -store.index-cache-read.cache.enable-fifocache
  [enable_fifocache: <boolean> | default = false]

  # Cache config for index entry reading. The default validity of entries for
  # caches unless overridden.
  # CLI flag: -store.index-cache-read.default-validity
  [default_validity: <duration> | default = 0s]

  background:
    # Cache config for index entry reading. How many goroutines to use to write
    # back to memcache.
    # CLI flag: -store.index-cache-read.memcache.write-back-goroutines
    [writeback_goroutines: <int> | default = 10]

    # Cache config for index entry reading. How many chunks to buffer for
    # background write back.
    # CLI flag: -store.index-cache-read.memcache.write-back-buffer
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
```

## `chunk_store_config`

The `chunk_store_config` configures how Cortex stores the data (chunks storage engine).

```yaml
chunk_cache_config:
  # Cache config for chunks. Enable in-memory cache.
  # CLI flag: -cache.enable-fifocache
  [enable_fifocache: <boolean> | default = false]

  # Cache config for chunks. The default validity of entries for caches unless
  # overridden.
  # CLI flag: -default-validity
  [default_validity: <duration> | default = 0s]

  background:
    # Cache config for chunks. How many goroutines to use to write back to
    # memcache.
    # CLI flag: -memcache.write-back-goroutines
    [writeback_goroutines: <int> | default = 10]

    # Cache config for chunks. How many chunks to buffer for background write
    # back.
    # CLI flag: -memcache.write-back-buffer
    [writeback_buffer: <int> | default = 10000]

  # The memcached_config block configures how data is stored in Memcached (ie.
  # expiration).
  [memcached: <memcached_config>]

  # The memcached_client_config configures the client used to connect to
  # Memcached.
  [memcached_client: <memcached_client_config>]

  # The redis_config configures the Redis backend cache.
  [redis: <redis_config>]

  # The fifo_cache_config configures the local in-memory cache.
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
    # Cache config for index entry writing. How many goroutines to use to write
    # back to memcache.
    # CLI flag: -store.index-cache-write.memcache.write-back-goroutines
    [writeback_goroutines: <int> | default = 10]

    # Cache config for index entry writing. How many chunks to buffer for
    # background write back.
    # CLI flag: -store.index-cache-write.memcache.write-back-buffer
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

## `ingester_client_config`

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
    [minbackoff: <duration> | default = 100ms]

    # Maximum delay when backing off.
    # CLI flag: -ingester.client.backoff-max-period
    [maxbackoff: <duration> | default = 10s]

    # Number of times to backoff and retry before failing.
    # CLI flag: -ingester.client.backoff-retries
    [maxretries: <int> | default = 10]
```

## `frontend_worker_config`

The `frontend_worker_config` configures the worker - running within the Cortex querier - picking up and executing queries enqueued by the query-frontend.

```yaml
# Address of query frontend service.
# CLI flag: -querier.frontend-address
[address: <string> | default = ""]

# Number of simultaneous queries to process.
# CLI flag: -querier.worker-parallelism
[parallelism: <int> | default = 10]

# How often to query DNS.
# CLI flag: -querier.dns-lookup-period
[dnslookupduration: <duration> | default = 10s]

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
    [minbackoff: <duration> | default = 100ms]

    # Maximum delay when backing off.
    # CLI flag: -querier.frontend-client.backoff-max-period
    [maxbackoff: <duration> | default = 10s]

    # Number of times to backoff and retry before failing.
    # CLI flag: -querier.frontend-client.backoff-retries
    [maxretries: <int> | default = 10]
```

## `etcd_config`

The `etcd_config` configures the etcd client.

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

## `consul_config`

The `consul_config` configures the consul client.

```yaml
# Hostname and port of Consul.
# CLI flag: -<prefix>.consul.hostname
[host: <string> | default = "localhost:8500"]

# ACL Token used to interact with Consul.
# CLI flag: -<prefix>.consul.acltoken
[acltoken: <string> | default = ""]

# HTTP timeout when talking to Consul
# CLI flag: -<prefix>.consul.client-timeout
[httpclienttimeout: <duration> | default = 20s]

# Enable consistent reads to Consul.
# CLI flag: -<prefix>.consul.consistent-reads
[consistentreads: <boolean> | default = true]

# Rate limit when watching key or prefix in Consul, in requests per second. 0
# disables the rate limit.
# CLI flag: -<prefix>.consul.watch-rate-limit
[watchkeyratelimit: <float> | default = 0]

# Burst size used in rate limit. Values less than 1 are treated as 1.
# CLI flag: -<prefix>.consul.watch-burst-size
[watchkeyburstsize: <int> | default = 1]
```

## `memberlist_config`

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

## `limits_config`

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

## `redis_config`

The `redis_config` configures the Redis backend cache.

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

## `memcached_config`

The `memcached_config` block configures how data is stored in Memcached (ie. expiration).

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

## `memcached_client_config`

The `memcached_client_config` configures the client used to connect to Memcached.

```yaml
# Hostname for memcached service to use when caching chunks. If empty, no
# memcached will be used.
# CLI flag: -<prefix>.memcached.hostname
[host: <string> | default = ""]

# SRV service used to discover memcache servers.
# CLI flag: -<prefix>.memcached.service
[service: <string> | default = "memcached"]

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

## `fifo_cache_config`

The `fifo_cache_config` configures the local in-memory cache.

```yaml
# The number of entries to cache.
# CLI flag: -<prefix>.fifocache.size
[size: <int> | default = 0]

# The expiry duration for the cache.
# CLI flag: -<prefix>.fifocache.duration
[validity: <duration> | default = 0s]
```

## `configdb_config`

The `configdb_config` configures the config database storing rules and alerts, and used by the 'configs' service to expose APIs to manage them.

```yaml
# URI where the database can be found (for dev you can use memory://)
# CLI flag: -database.uri
[uri: <string> | default = "postgres://postgres@configs-db.weave.local/configs?sslmode=disable"]

# Path where the database migration files can be found
# CLI flag: -database.migrations
[migrationsdir: <string> | default = ""]

# File containing password (username goes in URI)
# CLI flag: -database.password-file
[passwordfile: <string> | default = ""]
```

## `configstore_config`

The `configstore_config` configures the config database storing rules and alerts, and is used by the Cortex alertmanager.

```yaml
# URL of configs API server.
# CLI flag: -<prefix>.configs.url
[configsapiurl: <url> | default = ]

# Timeout for requests to Weave Cloud configs service.
# CLI flag: -<prefix>.configs.client-timeout
[clienttimeout: <duration> | default = 5s]
```
