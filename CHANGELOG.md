# Changelog

## master / unreleased

* [CHANGE] Don't support mixed time units anymore for duration. For example, 168h5m0s doesn't work anymore, please use just one unit (s|m|h|d|w|y). #2252
* [CHANGE] Utilize separate protos for rule state and storage. Experimental ruler API will not be functional until the rollout is complete. #2226
* [CHANGE] Frontend worker in querier now starts after all Querier module dependencies are started. This fixes issue where frontend worker started to send queries to querier before it was ready to serve them (mostly visible when using experimental blocks storage). #2246
* [CHANGE] Lifecycler component now enters Failed state on errors, and doesn't exit the process. (Important if you're vendoring Cortex and use Lifecycler) #2251
* [FEATURE] Flusher target to flush the WAL.
  * `-flusher.wal-dir` for the WAL directory to recover from.
  * `-flusher.concurrent-flushes` for number of concurrent flushes.
  * `-flusher.flush-op-timeout` is duration after which a flush should timeout.
* [ENHANCEMENT] Better re-use of connections to DynamoDB and S3. #2268
* [ENHANCEMENT] Experimental TSDB: Add support for local `filesystem` backend. #2245
* [ENHANCEMENT] Experimental TSDB: Added memcached support for the TSDB index cache. #2290
* [ENHANCEMENT] Allow 1w (where w denotes week) and 1y (where y denotes year) when setting table period and retention. #2252 
* [ENHANCEMENT] Added FIFO cache metrics for current number of entries and memory usage. #2270
* [ENHANCEMENT] Output all config fields to /config API, including those with empty value. #2209
* [BUGFIX] Fixed etcd client keepalive settings. #2278
* [BUGFIX] Fixed bug in updating last element of FIFO cache. #2270


## 0.7.0 / 2020-03-16

Cortex `0.7.0` is a major step forward the upcoming `1.0` release. In this release, we've got 164 contributions from 26 authors. Thanks to all contributors! ❤️

Please be aware that Cortex `0.7.0` introduces some **breaking changes**. You're encouraged to read all the `[CHANGE]` entries below before upgrading your Cortex cluster. In particular:

- Cleaned up some configuration options in preparation for the Cortex `1.0.0` release (see also the [annotated config file breaking changes](#annotated-config-file-breaking-changes) below):
  - Removed CLI flags support to configure the schema (see [how to migrate from flags to schema file](https://cortexmetrics.io/docs/configuration/schema-configuration/#migrating-from-flags-to-schema-file))
  - Renamed CLI flag `-config-yaml` to `-schema-config-file`
  - Removed CLI flag `-store.min-chunk-age` in favor of `-querier.query-store-after`. The corresponding YAML config option `ingestermaxquerylookback` has been renamed to [`query_ingesters_within`](https://cortexmetrics.io/docs/configuration/configuration-file/#querier-config)
  - Deprecated CLI flag `-frontend.cache-split-interval` in favor of `-querier.split-queries-by-interval`
  - Renamed the YAML config option `defaul_validity` to `default_validity`
  - Removed the YAML config option `config_store` (in the [`alertmanager YAML config`](https://cortexmetrics.io/docs/configuration/configuration-file/#alertmanager-config)) in favor of `store`
  - Removed the YAML config root block `configdb` in favor of [`configs`](https://cortexmetrics.io/docs/configuration/configuration-file/#configs-config). This change is also reflected in the following CLI flags renaming:
      * `-database.*` -> `-configs.database.*`
      * `-database.migrations` -> `-configs.database.migrations-dir`
  - Removed the fluentd-based billing infrastructure including the CLI flags:
      * `-distributor.enable-billing`
      * `-billing.max-buffered-events`
      * `-billing.retry-delay`
      * `-billing.ingester`
- Removed support for using denormalised tokens in the ring. Before upgrading, make sure your Cortex cluster is already running `v0.6.0` or an earlier version with `-ingester.normalise-tokens=true`

### Full changelog

* [CHANGE] Removed support for flags to configure schema. Further, the flag for specifying the config file (`-config-yaml`) has been deprecated. Please use `-schema-config-file`. See the [Schema Configuration documentation](https://cortexmetrics.io/docs/configuration/schema-configuration/) for more details on how to configure the schema using the YAML file. #2221
* [CHANGE] In the config file, the root level `config_store` config option has been moved to `alertmanager` > `store` > `configdb`. #2125
* [CHANGE] Removed unnecessary `frontend.cache-split-interval` in favor of `querier.split-queries-by-interval` both to reduce configuration complexity and guarantee alignment of these two configs. Starting from now, `-querier.cache-results` may only be enabled in conjunction with `-querier.split-queries-by-interval` (previously the cache interval default was `24h` so if you want to preserve the same behaviour you should set `-querier.split-queries-by-interval=24h`). #2040
* [CHANGE] Renamed Configs configuration options. #2187
  * configuration options
    * `-database.*` -> `-configs.database.*`
    * `-database.migrations` -> `-configs.database.migrations-dir`
  * config file
    * `configdb.uri:` -> `configs.database.uri:`
    * `configdb.migrationsdir:` -> `configs.database.migrations_dir:`
    * `configdb.passwordfile:` -> `configs.database.password_file:`
* [CHANGE] Moved `-store.min-chunk-age` to the Querier config as `-querier.query-store-after`, allowing the store to be skipped during query time if the metrics wouldn't be found. The YAML config option `ingestermaxquerylookback` has been renamed to `query_ingesters_within` to match its CLI flag. #1893
* [CHANGE] Renamed the cache configuration setting `defaul_validity` to `default_validity`. #2140
* [CHANGE] Remove fluentd-based billing infrastructure and flags such as `-distributor.enable-billing`. #1491
* [CHANGE] Removed remaining support for using denormalised tokens in the ring. If you're still running ingesters with denormalised tokens (Cortex 0.4 or earlier, with `-ingester.normalise-tokens=false`), such ingesters will now be completely invisible to distributors and need to be either switched to Cortex 0.6.0 or later, or be configured to use normalised tokens. #2034
* [CHANGE] The frontend http server will now send 502 in case of deadline exceeded and 499 if the user requested cancellation. #2156
* [CHANGE] We now enforce queries to be up to `-querier.max-query-into-future` into the future (defaults to 10m). #1929
  * `-store.min-chunk-age` has been removed
  * `-querier.query-store-after` has been added in it's place.
* [CHANGE] Removed unused `/validate_expr endpoint`. #2152
* [CHANGE] Updated Prometheus dependency to v2.16.0. This Prometheus version uses Active Query Tracker to limit concurrent queries. In order to keep `-querier.max-concurrent` working, Active Query Tracker is enabled by default, and is configured to store its data to `active-query-tracker` directory (relative to current directory when Cortex started). This can be changed by using `-querier.active-query-tracker-dir` option. Purpose of Active Query Tracker is to log queries that were running when Cortex crashes. This logging happens on next Cortex start. #2088
* [CHANGE] Default to BigChunk encoding; may result in slightly higher disk usage if many timeseries have a constant value, but should generally result in fewer, bigger chunks. #2207
* [CHANGE] WAL replays are now done while the rest of Cortex is starting, and more specifically, when HTTP server is running. This makes it possible to scrape metrics during WAL replays. Applies to both chunks and experimental blocks storage. #2222
* [CHANGE] Cortex now has `/ready` probe for all services, not just ingester and querier as before. In single-binary mode, /ready reports 204 only if all components are running properly. #2166
* [CHANGE] If you are vendoring Cortex and use its components in your project, be aware that many Cortex components no longer start automatically when they are created. You may want to review PR and attached document. #2166
* [CHANGE] Experimental TSDB: the querier in-memory index cache used by the experimental blocks storage shifted from per-tenant to per-querier. The `-experimental.tsdb.bucket-store.index-cache-size-bytes` now configures the per-querier index cache max size instead of a per-tenant cache and its default has been increased to 1GB. #2189
* [CHANGE] Experimental TSDB: TSDB head compaction interval and concurrency is now configurable (defaults to 1 min interval and 5 concurrent head compactions). New options: `-experimental.tsdb.head-compaction-interval` and `-experimental.tsdb.head-compaction-concurrency`. #2172
* [CHANGE] Experimental TSDB: switched the blocks storage index header to the binary format. This change is expected to have no visible impact, except lower startup times and memory usage in the queriers. It's possible to switch back to the old JSON format via the flag `-experimental.tsdb.bucket-store.binary-index-header-enabled=false`. #2223
* [CHANGE] Experimental Memberlist KV store can now be used in single-binary Cortex. Attempts to use it previously would fail with panic. This change also breaks existing binary protocol used to exchange gossip messages, so this version will not be able to understand gossiped Ring when used in combination with the previous version of Cortex. Easiest way to upgrade is to shutdown old Cortex installation, and restart it with new version. Incremental rollout works too, but with reduced functionality until all components run the same version. #2016
* [FEATURE] Added a read-only local alertmanager config store using files named corresponding to their tenant id. #2125
* [FEATURE] Added flag `-experimental.ruler.enable-api` to enable the ruler api which implements the Prometheus API `/api/v1/rules` and `/api/v1/alerts` endpoints under the configured `-http.prefix`. #1999
* [FEATURE] Added sharding support to compactor when using the experimental TSDB blocks storage. #2113
* [FEATURE] Added ability to override YAML config file settings using environment variables. #2147
  * `-config.expand-env`
* [FEATURE] Added flags to disable Alertmanager notifications methods. #2187
  * `-configs.notifications.disable-email`
  * `-configs.notifications.disable-webhook`
* [FEATURE] Add /config HTTP endpoint which exposes the current Cortex configuration as YAML. #2165
* [FEATURE] Allow Prometheus remote write directly to ingesters. #1491
* [FEATURE] Introduced new standalone service `query-tee` that can be used for testing purposes to send the same Prometheus query to multiple backends (ie. two Cortex clusters ingesting the same metrics) and compare the performances. #2203
* [FEATURE] Fan out parallelizable queries to backend queriers concurrently. #1878
  * `querier.parallelise-shardable-queries` (bool)
  * Requires a shard-compatible schema (v10+)
  * This causes the number of traces to increase accordingly.
  * The query-frontend now requires a schema config to determine how/when to shard queries, either from a file or from flags (i.e. by the `config-yaml` CLI flag). This is the same schema config the queriers consume. The schema is only required to use this option.
  * It's also advised to increase downstream concurrency controls as well:
    * `querier.max-outstanding-requests-per-tenant`
    * `querier.max-query-parallelism`
    * `querier.max-concurrent`
    * `server.grpc-max-concurrent-streams` (for both query-frontends and queriers)
* [FEATURE] Added user sub rings to distribute users to a subset of ingesters. #1947
  * `-experimental.distributor.user-subring-size`
* [FEATURE] Add flag `-experimental.tsdb.stripe-size` to expose TSDB stripe size option. #2185
* [FEATURE] Experimental Delete Series: Added support for Deleting Series with Prometheus style API. Needs to be enabled first by setting `-purger.enable` to `true`. Deletion only supported when using `boltdb` and `filesystem` as index and object store respectively. Support for other stores to follow in separate PRs #2103
* [ENHANCEMENT] Alertmanager: Expose Per-tenant alertmanager metrics #2124
* [ENHANCEMENT] Add `status` label to `cortex_alertmanager_configs` metric to gauge the number of valid and invalid configs. #2125
* [ENHANCEMENT] Cassandra Authentication: added the `custom_authenticators` config option that allows users to authenticate with cassandra clusters using password authenticators that are not approved by default in [gocql](https://github.com/gocql/gocql/blob/81b8263d9fe526782a588ef94d3fa5c6148e5d67/conn.go#L27) #2093
* [ENHANCEMENT] Cassandra Storage: added `max_retries`, `retry_min_backoff` and `retry_max_backoff` configuration options to enable retrying recoverable errors. #2054
* [ENHANCEMENT] Allow to configure HTTP and gRPC server listen address, maximum number of simultaneous connections and connection keepalive settings.
  * `-server.http-listen-address`
  * `-server.http-conn-limit`
  * `-server.grpc-listen-address`
  * `-server.grpc-conn-limit`
  * `-server.grpc.keepalive.max-connection-idle`
  * `-server.grpc.keepalive.max-connection-age`
  * `-server.grpc.keepalive.max-connection-age-grace`
  * `-server.grpc.keepalive.time`
  * `-server.grpc.keepalive.timeout`
* [ENHANCEMENT] PostgreSQL: Bump up `github.com/lib/pq` from `v1.0.0` to `v1.3.0` to support PostgreSQL SCRAM-SHA-256 authentication. #2097
* [ENHANCEMENT] Cassandra Storage: User no longer need `CREATE` privilege on `<all keyspaces>` if given keyspace exists. #2032
* [ENHANCEMENT] Cassandra Storage: added `password_file` configuration options to enable reading Cassandra password from file. #2096
* [ENHANCEMENT] Configs API: Allow GET/POST configs in YAML format. #2181
* [ENHANCEMENT] Background cache writes are batched to improve parallelism and observability. #2135
* [ENHANCEMENT] Add automatic repair for checkpoint and WAL. #2105
* [ENHANCEMENT] Support `lastEvaluation` and `evaluationTime` in `/api/v1/rules` endpoints and make order of groups stable. #2196
* [ENHANCEMENT] Skip expired requests in query-frontend scheduling. #2082
* [ENHANCEMENT] Add ability to configure gRPC keepalive settings. #2066
* [ENHANCEMENT] Experimental TSDB: Export TSDB Syncer metrics from Compactor component, they are prefixed with `cortex_compactor_`. #2023
* [ENHANCEMENT] Experimental TSDB: Added dedicated flag `-experimental.tsdb.bucket-store.tenant-sync-concurrency` to configure the maximum number of concurrent tenants for which blocks are synched. #2026
* [ENHANCEMENT] Experimental TSDB: Expose metrics for objstore operations (prefixed with `cortex_<component>_thanos_objstore_`, component being one of `ingester`, `querier` and `compactor`). #2027
* [ENHANCEMENT] Experimental TSDB: Added support for Azure Storage to be used for block storage, in addition to S3 and GCS. #2083
* [ENHANCEMENT] Experimental TSDB: Reduced memory allocations in the ingesters when using the experimental blocks storage. #2057
* [ENHANCEMENT] Experimental Memberlist KV: expose `-memberlist.gossip-to-dead-nodes-time` and `-memberlist.dead-node-reclaim-time` options to control how memberlist library handles dead nodes and name reuse. #2131
* [BUGFIX] Alertmanager: fixed panic upon applying a new config, caused by duplicate metrics registration in the `NewPipelineBuilder` function. #211
* [BUGFIX] Azure Blob ChunkStore: Fixed issue causing `invalid chunk checksum` errors. #2074
* [BUGFIX] The gauge `cortex_overrides_last_reload_successful` is now only exported by components that use a `RuntimeConfigManager`. Previously, for components that do not initialize a `RuntimeConfigManager` (such as the compactor) the gauge was initialized with 0 (indicating error state) and then never updated, resulting in a false-negative permanent error state. #2092
* [BUGFIX] Fixed WAL metric names, added the `cortex_` prefix.
* [BUGFIX] Restored histogram `cortex_configs_request_duration_seconds` #2138
* [BUGFIX] Fix wrong syntax for `url` in config-file-reference. #2148
* [BUGFIX] Fixed some 5xx status code returned by the query-frontend when they should actually be 4xx. #2122
* [BUGFIX] Fixed leaked goroutines in the querier. #2070
* [BUGFIX] Experimental TSDB: fixed `/all_user_stats` and `/api/prom/user_stats` endpoints when using the experimental TSDB blocks storage. #2042
* [BUGFIX] Experimental TSDB: fixed ruler to correctly work with the experimental TSDB blocks storage. #2101

### Changes to denormalised tokens in the ring

Cortex 0.4.0 is the last version that can *write* denormalised tokens. Cortex 0.5.0 and above always write normalised tokens.

Cortex 0.6.0 is the last version that can *read* denormalised tokens. Starting with Cortex 0.7.0 only normalised tokens are supported, and ingesters writing denormalised tokens to the ring (running Cortex 0.4.0 or earlier with `-ingester.normalise-tokens=false`) are ignored by distributors. Such ingesters should either switch to using normalised tokens, or be upgraded to Cortex 0.5.0 or later.

### Known issues

- The gRPC streaming for ingesters doesn't work when using the experimental TSDB blocks storage. Please do not enable `-querier.ingester-streaming` if you're using the TSDB blocks storage. If you want to enable it, you can build Cortex from `master` given the issue has been fixed after Cortex `0.7` branch has been cut and the fix wasn't included in the `0.7` because related to an experimental feature.

### Annotated config file breaking changes

In this section you can find a config file diff showing the breaking changes introduced in Cortex `0.7`. You can also find the [full configuration file reference doc](https://cortexmetrics.io/docs/configuration/configuration-file/) in the website.

 ```diff
### Root level config

 # "configdb" has been moved to "alertmanager > store > configdb".
-[configdb: <configdb_config>]

 # "config_store" has been renamed to "configs".
-[config_store: <configstore_config>]
+[configs: <configs_config>]


### `distributor_config`

 # The support to hook an external billing system has been removed.
-[enable_billing: <boolean> | default = false]
-billing:
-  [maxbufferedevents: <int> | default = 1024]
-  [retrydelay: <duration> | default = 500ms]
-  [ingesterhostport: <string> | default = "localhost:24225"]


### `querier_config`

 # "ingestermaxquerylookback" has been renamed to "query_ingesters_within".
-[ingestermaxquerylookback: <duration> | default = 0s]
+[query_ingesters_within: <duration> | default = 0s]


### `queryrange_config`

results_cache:
  cache:
     # "defaul_validity" has been renamed to "default_validity".
-    [defaul_validity: <duration> | default = 0s]
+    [default_validity: <duration> | default = 0s]

   # "cache_split_interval" has been deprecated in favor of "split_queries_by_interval".
-  [cache_split_interval: <duration> | default = 24h0m0s]


### `alertmanager_config`

# The "store" config block has been added. This includes "configdb" which previously
# was the "configdb" root level config block.
+store:
+  [type: <string> | default = "configdb"]
+  [configdb: <configstore_config>]
+  local:
+    [path: <string> | default = ""]


### `storage_config`

index_queries_cache_config:
   # "defaul_validity" has been renamed to "default_validity".
-  [defaul_validity: <duration> | default = 0s]
+  [default_validity: <duration> | default = 0s]


### `chunk_store_config`

chunk_cache_config:
   # "defaul_validity" has been renamed to "default_validity".
-  [defaul_validity: <duration> | default = 0s]
+  [default_validity: <duration> | default = 0s]

write_dedupe_cache_config:
   # "defaul_validity" has been renamed to "default_validity".
-  [defaul_validity: <duration> | default = 0s]
+  [default_validity: <duration> | default = 0s]

 # "min_chunk_age" has been removed in favor of "querier > query_store_after".
-[min_chunk_age: <duration> | default = 0s]


### `configs_config`

-# "uri" has been moved to "database > uri".
-[uri: <string> | default = "postgres://postgres@configs-db.weave.local/configs?sslmode=disable"]

-# "migrationsdir" has been moved to "database > migrations_dir".
-[migrationsdir: <string> | default = ""]

-# "passwordfile" has been moved to "database > password_file".
-[passwordfile: <string> | default = ""]

+database:
+  [uri: <string> | default = "postgres://postgres@configs-db.weave.local/configs?sslmode=disable"]
+  [migrations_dir: <string> | default = ""]
+  [password_file: <string> | default = ""]
```

## 0.6.1 / 2020-02-05

* [BUGFIX] Fixed parsing of the WAL configuration when specified in the YAML config file. #2071

## 0.6.0 / 2020-01-28

Note that the ruler flags need to be changed in this upgrade. You're moving from a single node ruler to something that might need to be sharded.
Further, if you're using the configs service, we've upgraded the migration library and this requires some manual intervention. See full instructions below to upgrade your PostgreSQL.

* [CHANGE] The frontend component now does not cache results if it finds a `Cache-Control` header and if one of its values is `no-store`. #1974
* [CHANGE] Flags changed with transition to upstream Prometheus rules manager:
  * `-ruler.client-timeout` is now `ruler.configs.client-timeout` in order to match `ruler.configs.url`.
  * `-ruler.group-timeout`has been removed.
  * `-ruler.num-workers` has been removed.
  * `-ruler.rule-path` has been added to specify where the prometheus rule manager will sync rule files.
  * `-ruler.storage.type` has beem added to specify the rule store backend type, currently only the configdb.
  * `-ruler.poll-interval` has been added to specify the interval in which to poll new rule groups.
  * `-ruler.evaluation-interval` default value has changed from `15s` to `1m` to match the default evaluation interval in Prometheus.
  * Ruler sharding requires a ring which can be configured via the ring flags prefixed by `ruler.ring.`. #1987
* [CHANGE] Use relative links from /ring page to make it work when used behind reverse proxy. #1896
* [CHANGE] Deprecated `-distributor.limiter-reload-period` flag. #1766
* [CHANGE] Ingesters now write only normalised tokens to the ring, although they can still read denormalised tokens used by other ingesters. `-ingester.normalise-tokens` is now deprecated, and ignored. If you want to switch back to using denormalised tokens, you need to downgrade to Cortex 0.4.0. Previous versions don't handle claiming tokens from normalised ingesters correctly. #1809
* [CHANGE] Overrides mechanism has been renamed to "runtime config", and is now separate from limits. Runtime config is simply a file that is reloaded by Cortex every couple of seconds. Limits and now also multi KV use this mechanism.<br />New arguments were introduced: `-runtime-config.file` (defaults to empty) and `-runtime-config.reload-period` (defaults to 10 seconds), which replace previously used `-limits.per-user-override-config` and `-limits.per-user-override-period` options. Old options are still used if `-runtime-config.file` is not specified. This change is also reflected in YAML configuration, where old `limits.per_tenant_override_config` and `limits.per_tenant_override_period` fields are replaced with `runtime_config.file` and `runtime_config.period` respectively. #1749
* [CHANGE] Cortex now rejects data with duplicate labels. Previously, such data was accepted, with duplicate labels removed with only one value left. #1964
* [CHANGE] Changed the default value for `-distributor.ha-tracker.prefix` from `collectors/` to `ha-tracker/` in order to not clash with other keys (ie. ring) stored in the same key-value store. #1940
* [FEATURE] Experimental: Write-Ahead-Log added in ingesters for more data reliability against ingester crashes. #1103
  * `--ingester.wal-enabled`: Setting this to `true` enables writing to WAL during ingestion.
  * `--ingester.wal-dir`: Directory where the WAL data should be stored and/or recovered from.
  * `--ingester.checkpoint-enabled`: Set this to `true` to enable checkpointing of in-memory chunks to disk.
  * `--ingester.checkpoint-duration`: This is the interval at which checkpoints should be created.
  * `--ingester.recover-from-wal`: Set this to `true` to recover data from an existing WAL.
  * For more information, please checkout the ["Ingesters with WAL" guide](https://cortexmetrics.io/docs/guides/ingesters-with-wal/).
* [FEATURE] The distributor can now drop labels from samples (similar to the removal of the replica label for HA ingestion) per user via the `distributor.drop-label` flag. #1726
* [FEATURE] Added flag `debug.mutex-profile-fraction` to enable mutex profiling #1969
* [FEATURE] Added `global` ingestion rate limiter strategy. Deprecated `-distributor.limiter-reload-period` flag. #1766
* [FEATURE] Added support for Microsoft Azure blob storage to be used for storing chunk data. #1913
* [FEATURE] Added readiness probe endpoint`/ready` to queriers. #1934
* [FEATURE] Added "multi" KV store that can interact with two other KV stores, primary one for all reads and writes, and secondary one, which only receives writes. Primary/secondary store can be modified in runtime via runtime-config mechanism (previously "overrides"). #1749
* [FEATURE] Added support to store ring tokens to a file and read it back on startup, instead of generating/fetching the tokens to/from the ring. This feature can be enabled with the flag `-ingester.tokens-file-path`. #1750
* [FEATURE] Experimental TSDB: Added `/series` API endpoint support with TSDB blocks storage. #1830
* [FEATURE] Experimental TSDB: Added TSDB blocks `compactor` component, which iterates over users blocks stored in the bucket and compact them according to the configured block ranges. #1942
* [ENHANCEMENT] metric `cortex_ingester_flush_reasons` gets a new `reason` value: `Spread`, when `-ingester.spread-flushes` option is enabled. #1978
* [ENHANCEMENT] Added `password` and `enable_tls` options to redis cache configuration. Enables usage of Microsoft Azure Cache for Redis service. #1923
* [ENHANCEMENT] Upgraded Kubernetes API version for deployments from `extensions/v1beta1` to `apps/v1`. #1941
* [ENHANCEMENT] Experimental TSDB: Open existing TSDB on startup to prevent ingester from becoming ready before it can accept writes. The max concurrency is set via `--experimental.tsdb.max-tsdb-opening-concurrency-on-startup`. #1917
* [ENHANCEMENT] Experimental TSDB: Querier now exports aggregate metrics from Thanos bucket store and in memory index cache (many metrics to list, but all have `cortex_querier_bucket_store_` or `cortex_querier_blocks_index_cache_` prefix). #1996
* [ENHANCEMENT] Experimental TSDB: Improved multi-tenant bucket store. #1991
  * Allowed to configure the blocks sync interval via `-experimental.tsdb.bucket-store.sync-interval` (0 disables the sync)
  * Limited the number of tenants concurrently synched by `-experimental.tsdb.bucket-store.block-sync-concurrency`
  * Renamed `cortex_querier_sync_seconds` metric to `cortex_querier_blocks_sync_seconds`
  * Track `cortex_querier_blocks_sync_seconds` metric for the initial sync too
* [BUGFIX] Fixed unnecessary CAS operations done by the HA tracker when the jitter is enabled. #1861
* [BUGFIX] Fixed ingesters getting stuck in a LEAVING state after coming up from an ungraceful exit. #1921
* [BUGFIX] Reduce memory usage when ingester Push() errors. #1922
* [BUGFIX] Table Manager: Fixed calculation of expected tables and creation of tables from next active schema considering grace period. #1976
* [BUGFIX] Experimental TSDB: Fixed ingesters consistency during hand-over when using experimental TSDB blocks storage. #1854 #1818
* [BUGFIX] Experimental TSDB: Fixed metrics when using experimental TSDB blocks storage. #1981 #1982 #1990 #1983
* [BUGFIX] Experimental memberlist: Use the advertised address when sending packets to other peers of the Gossip memberlist. #1857

### Upgrading PostgreSQL (if you're using configs service)

Reference: <https://github.com/golang-migrate/migrate/tree/master/database/postgres#upgrading-from-v1>

1. Install the migrate package cli tool: <https://github.com/golang-migrate/migrate/tree/master/cmd/migrate#installation>
2. Drop the `schema_migrations` table: `DROP TABLE schema_migrations;`.
2. Run the migrate command:

```bash
migrate  -path <absolute_path_to_cortex>/cmd/cortex/migrations -database postgres://localhost:5432/database force 2
```

### Known issues

- The `cortex_prometheus_rule_group_last_evaluation_timestamp_seconds` metric, tracked by the ruler, is not unregistered for rule groups not being used anymore. This issue will be fixed in the next Cortex release (see [2033](https://github.com/cortexproject/cortex/issues/2033)).

- Write-Ahead-Log (WAL) does not have automatic repair of corrupt checkpoint or WAL segments, which is possible if ingester crashes abruptly or the underlying disk corrupts. Currently the only way to resolve this is to manually delete the affected checkpoint and/or WAL segments. Automatic repair will be added in the future releases.

## 0.4.0 / 2019-12-02

* [CHANGE] The frontend component has been refactored to be easier to re-use. When upgrading the frontend, cache entries will be discarded and re-created with the new protobuf schema. #1734
* [CHANGE] Removed direct DB/API access from the ruler. `-ruler.configs.url` has been now deprecated. #1579
* [CHANGE] Removed `Delta` encoding. Any old chunks with `Delta` encoding cannot be read anymore. If `ingester.chunk-encoding` is set to `Delta` the ingester will fail to start. #1706
* [CHANGE] Setting `-ingester.max-transfer-retries` to 0 now disables hand-over when ingester is shutting down. Previously, zero meant infinite number of attempts. #1771
* [CHANGE] `dynamo` has been removed as a valid storage name to make it consistent for all components. `aws` and `aws-dynamo` remain as valid storage names.
* [CHANGE/FEATURE] The frontend split and cache intervals can now be configured using the respective flag `--querier.split-queries-by-interval` and `--frontend.cache-split-interval`.
  * If `--querier.split-queries-by-interval` is not provided request splitting is disabled by default.
  * __`--querier.split-queries-by-day` is still accepted for backward compatibility but has been deprecated. You should now use `--querier.split-queries-by-interval`. We recommend a to use a multiple of 24 hours.__
* [FEATURE] Global limit on the max series per user and metric #1760
  * `-ingester.max-global-series-per-user`
  * `-ingester.max-global-series-per-metric`
  * Requires `-distributor.replication-factor` and `-distributor.shard-by-all-labels` set for the ingesters too
* [FEATURE] Flush chunks with stale markers early with `ingester.max-stale-chunk-idle`. #1759
* [FEATURE] EXPERIMENTAL: Added new KV Store backend based on memberlist library. Components can gossip about tokens and ingester states, instead of using Consul or Etcd. #1721
* [FEATURE] EXPERIMENTAL: Use TSDB in the ingesters & flush blocks to S3/GCS ala Thanos. This will let us use an Object Store more efficiently and reduce costs. #1695
* [FEATURE] Allow Query Frontend to log slow queries with `frontend.log-queries-longer-than`. #1744
* [FEATURE] Add HTTP handler to trigger ingester flush & shutdown - used when running as a stateful set with the WAL enabled.  #1746
* [FEATURE] EXPERIMENTAL: Added GCS support to TSDB blocks storage. #1772
* [ENHANCEMENT] Reduce memory allocations in the write path. #1706
* [ENHANCEMENT] Consul client now follows recommended practices for blocking queries wrt returned Index value. #1708
* [ENHANCEMENT] Consul client can optionally rate-limit itself during Watch (used e.g. by ring watchers) and WatchPrefix (used by HA feature) operations. Rate limiting is disabled by default. New flags added: `--consul.watch-rate-limit`, and `--consul.watch-burst-size`. #1708
* [ENHANCEMENT] Added jitter to HA deduping heartbeats, configure using `distributor.ha-tracker.update-timeout-jitter-max` #1534
* [ENHANCEMENT] Add ability to flush chunks with stale markers early. #1759
* [BUGFIX] Stop reporting successful actions as 500 errors in KV store metrics. #1798
* [BUGFIX] Fix bug where duplicate labels can be returned through metadata APIs. #1790
* [BUGFIX] Fix reading of old, v3 chunk data. #1779
* [BUGFIX] Now support IAM roles in service accounts in AWS EKS. #1803
* [BUGFIX] Fixed duplicated series returned when querying both ingesters and store with the experimental TSDB blocks storage. #1778

In this release we updated the following dependencies:

- gRPC v1.25.0  (resulted in a drop of 30% CPU usage when compression is on)
- jaeger-client v2.20.0
- aws-sdk-go to v1.25.22

## 0.3.0 / 2019-10-11

This release adds support for Redis as an alternative to Memcached, and also includes many optimisations which reduce CPU and memory usage.

* [CHANGE] Gauge metrics were renamed to drop the `_total` suffix. #1685
  * In Alertmanager, `alertmanager_configs_total` is now `alertmanager_configs`
  * In Ruler, `scheduler_configs_total` is now `scheduler_configs`
  * `scheduler_groups_total` is now `scheduler_groups`.
* [CHANGE] `--alertmanager.configs.auto-slack-root` flag was dropped as auto Slack root is not supported anymore. #1597
* [CHANGE] In table-manager, default DynamoDB capacity was reduced from 3,000 units to 1,000 units. We recommend you do not run with the defaults: find out what figures are needed for your environment and set that via `-dynamodb.periodic-table.write-throughput` and `-dynamodb.chunk-table.write-throughput`.
* [FEATURE] Add Redis support for caching #1612
* [FEATURE] Allow spreading chunk writes across multiple S3 buckets #1625
* [FEATURE] Added `/shutdown` endpoint for ingester to shutdown all operations of the ingester. #1746
* [ENHANCEMENT] Upgraded Prometheus to 2.12.0 and Alertmanager to 0.19.0. #1597
* [ENHANCEMENT] Cortex is now built with Go 1.13 #1675, #1676, #1679
* [ENHANCEMENT] Many optimisations, mostly impacting ingester and querier: #1574, #1624, #1638, #1644, #1649, #1654, #1702

Full list of changes: <https://github.com/cortexproject/cortex/compare/v0.2.0...v0.3.0>

## 0.2.0 / 2019-09-05

This release has several exciting features, the most notable of them being setting `-ingester.spread-flushes` to potentially reduce your storage space by upto 50%.

* [CHANGE] Flags changed due to changes upstream in Prometheus Alertmanager #929:
  * `alertmanager.mesh.listen-address` is now `cluster.listen-address`
  * `alertmanager.mesh.peer.host` and `alertmanager.mesh.peer.service` can be replaced by `cluster.peer`
  * `alertmanager.mesh.hardware-address`, `alertmanager.mesh.nickname`, `alertmanager.mesh.password`, and `alertmanager.mesh.peer.refresh-interval` all disappear.
* [CHANGE] --claim-on-rollout flag deprecated; feature is now always on #1566
* [CHANGE] Retention period must now be a multiple of periodic table duration #1564
* [CHANGE] The value for the name label for the chunks memcache in all `cortex_cache_` metrics is now `chunksmemcache` (before it was `memcache`) #1569
* [FEATURE] Makes the ingester flush each timeseries at a specific point in the max-chunk-age cycle with `-ingester.spread-flushes`. This means multiple replicas of a chunk are very likely to contain the same contents which cuts chunk storage space by up to 66%. #1578
* [FEATURE] Make minimum number of chunk samples configurable per user #1620
* [FEATURE] Honor HTTPS for custom S3 URLs #1603
* [FEATURE] You can now point the query-frontend at a normal Prometheus for parallelisation and caching #1441
* [FEATURE] You can now specify `http_config` on alert receivers #929
* [FEATURE] Add option to use jump hashing to load balance requests to memcached #1554
* [FEATURE] Add status page for HA tracker to distributors #1546
* [FEATURE] The distributor ring page is now easier to read with alternate rows grayed out #1621

## 0.1.0 / 2019-08-07

* [CHANGE] HA Tracker flags were renamed to provide more clarity #1465
  * `distributor.accept-ha-labels` is now `distributor.ha-tracker.enable`
  * `distributor.accept-ha-samples` is now `distributor.ha-tracker.enable-for-all-users`
  * `ha-tracker.replica` is now `distributor.ha-tracker.replica`
  * `ha-tracker.cluster` is now `distributor.ha-tracker.cluster`
* [FEATURE] You can specify "heap ballast" to reduce Go GC Churn #1489
* [BUGFIX] HA Tracker no longer always makes a request to Consul/Etcd when a request is not from the active replica #1516
* [BUGFIX] Queries are now correctly cancelled by the query-frontend #1508
