# Changelog

## master / unreleased

* [CHANGE] Metric `cortex_kv_request_duration_seconds` now includes `name` label to denote which client is being used as well as the `backend` label to denote the KV backend implementation in use. #2648
* [CHANGE] Experimental Ruler: Rule groups persisted to object storage using the experimental API have an updated object key encoding to better handle special characters. Rule groups previously-stored using object storage must be renamed to the new format. #2646
* [CHANGE] Query Frontend now uses Round Robin to choose a tenant queue to service next. #2553
* [CHANGE] `-promql.lookback-delta` is now deprecated and has been replaced by `-querier.lookback-delta` along with `lookback_delta` entry under `querier` in the config file. `-promql.lookback-delta` will be removed in v1.4.0. #2604
* [CHANGE] Experimental TSDB: removed `-experimental.tsdb.bucket-store.binary-index-header-enabled` flag. Now the binary index-header is always enabled.
* [CHANGE] Experimental TSDB: Renamed index-cache metrics to use original metric names from Thanos, as Cortex is not aggregating them in any way: #2627
  * `cortex_<service>_blocks_index_cache_items_evicted_total` => `thanos_store_index_cache_items_evicted_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items_added_total` => `thanos_store_index_cache_items_added_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_requests_total` => `thanos_store_index_cache_requests_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items_overflowed_total` => `thanos_store_index_cache_items_overflowed_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_hits_total` => `thanos_store_index_cache_hits_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items` => `thanos_store_index_cache_items{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_items_size_bytes` => `thanos_store_index_cache_items_size_bytes{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_total_size_bytes` => `thanos_store_index_cache_total_size_bytes{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operations_total` =>  `thanos_memcached_operations_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operation_failures_total` =>  `thanos_memcached_operation_failures_total{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operation_duration_seconds` =>  `thanos_memcached_operation_duration_seconds{name="index-cache"}`
  * `cortex_<service>_blocks_index_cache_memcached_operation_skipped_total` =>  `thanos_memcached_operation_skipped_total{name="index-cache"}`
* [CHANGE] Experimental TSDB: Renamed metrics in bucket stores: #2627
  * `cortex_<service>_blocks_meta_syncs_total` => `cortex_blocks_meta_syncs_total{component="<service>"}`
  * `cortex_<service>_blocks_meta_sync_failures_total` => `cortex_blocks_meta_sync_failures_total{component="<service>"}`
  * `cortex_<service>_blocks_meta_sync_duration_seconds` => `cortex_blocks_meta_sync_duration_seconds{component="<service>"}`
  * `cortex_<service>_blocks_meta_sync_consistency_delay_seconds` => `cortex_blocks_meta_sync_consistency_delay_seconds{component="<service>"}`
  * `cortex_<service>_blocks_meta_synced` => `cortex_blocks_meta_synced{component="<service>"}`
  * `cortex_<service>_bucket_store_block_loads_total` => `cortex_bucket_store_block_loads_total{component="<service>"}`
  * `cortex_<service>_bucket_store_block_load_failures_total` => `cortex_bucket_store_block_load_failures_total{component="<service>"}`
  * `cortex_<service>_bucket_store_block_drops_total` => `cortex_bucket_store_block_drops_total{component="<service>"}`
  * `cortex_<service>_bucket_store_block_drop_failures_total` => `cortex_bucket_store_block_drop_failures_total{component="<service>"}`
  * `cortex_<service>_bucket_store_blocks_loaded` => `cortex_bucket_store_blocks_loaded{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_touched` => `cortex_bucket_store_series_data_touched{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_fetched` => `cortex_bucket_store_series_data_fetched{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_size_touched_bytes` => `cortex_bucket_store_series_data_size_touched_bytes{component="<service>"}`
  * `cortex_<service>_bucket_store_series_data_size_fetched_bytes` => `cortex_bucket_store_series_data_size_fetched_bytes{component="<service>"}`
  * `cortex_<service>_bucket_store_series_blocks_queried` => `cortex_bucket_store_series_blocks_queried{component="<service>"}`
  * `cortex_<service>_bucket_store_series_get_all_duration_seconds` => `cortex_bucket_store_series_get_all_duration_seconds{component="<service>"}`
  * `cortex_<service>_bucket_store_series_merge_duration_seconds` => `cortex_bucket_store_series_merge_duration_seconds{component="<service>"}`
  * `cortex_<service>_bucket_store_series_refetches_total` => `cortex_bucket_store_series_refetches_total{component="<service>"}`
  * `cortex_<service>_bucket_store_series_result_series` => `cortex_bucket_store_series_result_series{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compressions_total` => `cortex_bucket_store_cached_postings_compressions_total{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compression_errors_total` => `cortex_bucket_store_cached_postings_compression_errors_total{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compression_time_seconds` => `cortex_bucket_store_cached_postings_compression_time_seconds{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_original_size_bytes_total` => `cortex_bucket_store_cached_postings_original_size_bytes_total{component="<service>"}`
  * `cortex_<service>_bucket_store_cached_postings_compressed_size_bytes_total` => `cortex_bucket_store_cached_postings_compressed_size_bytes_total{component="<service>"}`
  * `cortex_<service>_blocks_sync_seconds` => `cortex_bucket_stores_blocks_sync_seconds{component="<service>"}`
  * `cortex_<service>_blocks_last_successful_sync_timestamp_seconds` => `cortex_bucket_stores_blocks_last_successful_sync_timestamp_seconds{component="<service>"}`
* [CHANGE] Available command-line flags are printed to stdout, and only when requested via `-help`. Using invalid flag no longer causes printing of all available flags. #2691
* [CHANGE] Experimental Memberlist ring: randomize gossip node names to avoid conflicts when running multiple clients on the same host, or reusing host names (eg. pods in statefulset). Node name randomization can be disabled by using `-memberlist.randomize-node-name=false`. #2715
* [FEATURE] TLS config options added for GRPC clients in Querier (Query-frontend client & Ingester client), Ruler, Store Gateway, as well as HTTP client in Config store client. #2502
* [FEATURE] The flag `-frontend.max-cache-freshness` is now supported within the limits overrides, to specify per-tenant max cache freshness values. The corresponding YAML config parameter has been changed from `results_cache.max_freshness` to `limits_config.max_cache_freshness`. The legacy YAML config parameter (`results_cache.max_freshness`) will continue to be supported till Cortex release `v1.4.0`. #2609
* [FEATURE] Experimental gRPC Store: Added support to 3rd parties index and chunk stores using gRPC client/server plugin mechanism. #2220
* [ENHANCEMENT] Experimental TSDB: added the following metrics to the ingester: #2580 #2583 #2589 #2654
  * `cortex_ingester_tsdb_appender_add_duration_seconds`
  * `cortex_ingester_tsdb_appender_commit_duration_seconds`
  * `cortex_ingester_tsdb_refcache_purge_duration_seconds`
  * `cortex_ingester_tsdb_compactions_total`
  * `cortex_ingester_tsdb_compaction_duration_seconds`
  * `cortex_ingester_tsdb_wal_fsync_duration_seconds`
  * `cortex_ingester_tsdb_wal_page_flushes_total`
  * `cortex_ingester_tsdb_wal_completed_pages_total`
  * `cortex_ingester_tsdb_wal_truncations_failed_total`
  * `cortex_ingester_tsdb_wal_truncations_total`
  * `cortex_ingester_tsdb_wal_writes_failed_total`
  * `cortex_ingester_tsdb_checkpoint_deletions_failed_total`
  * `cortex_ingester_tsdb_checkpoint_deletions_total`
  * `cortex_ingester_tsdb_checkpoint_creations_failed_total`
  * `cortex_ingester_tsdb_checkpoint_creations_total`
  * `cortex_ingester_tsdb_wal_truncate_duration_seconds`
  * `cortex_ingester_tsdb_head_active_appenders`
  * `cortex_ingester_tsdb_head_series_not_found_total`
  * `cortex_ingester_tsdb_head_chunks`
  * `cortex_ingester_tsdb_mmap_chunk_corruptions_total`
  * `cortex_ingester_tsdb_head_chunks_created_total`
  * `cortex_ingester_tsdb_head_chunks_removed_total`
* [ENHANCEMENT] Experimental TSDB: added metrics useful to alert on critical conditions of the blocks storage: #2573
  * `cortex_compactor_last_successful_run_timestamp_seconds`
  * `cortex_querier_blocks_last_successful_sync_timestamp_seconds` (when store-gateway is disabled)
  * `cortex_querier_blocks_last_successful_scan_timestamp_seconds` (when store-gateway is enabled)
  * `cortex_storegateway_blocks_last_successful_sync_timestamp_seconds`
* [ENHANCEMENT] Experimental TSDB: added the flag `-experimental.tsdb.wal-compression-enabled` to allow to enable TSDB WAL compression. #2585
* [ENHANCEMENT] Experimental TSDB: Querier and store-gateway components can now use so-called "caching bucket", which can currently cache fetched chunks into shared memcached server. #2572
* [ENHANCEMENT] Ruler: Automatically remove unhealthy rulers from the ring. #2587
* [ENHANCEMENT] Query-tee: added support to `/metadata`, `/alerts`, and `/rules` endpoints #2600
* [ENHANCEMENT] Query-tee: added support to query results comparison between two different backends. The comparison is disabled by default and can be enabled via `-proxy.compare-responses=true`. #2611
* [ENHANCEMENT] Query-tee: improved the query-tee to not wait all backend responses before sending back the response to the client. The query-tee now sends back to the client first successful response, while honoring the `-backend.preferred` option. #2702
* [ENHANCEMENT] Thanos and Prometheus upgraded. #2604 #2634 #2686
  * TSDB now supports isolation of append and queries.
  * TSDB now holds less WAL files after Head Truncation.
  * TSDB now does memory-mapping of Head chunks and reduces memory usage.
* [ENHANCEMENT] Experimental TSDB: decoupled blocks deletion from blocks compaction in the compactor, so that blocks deletion is not blocked by a busy compactor. The following metrics have been added: #2623
  * `cortex_compactor_block_cleanup_started_total`
  * `cortex_compactor_block_cleanup_completed_total`
  * `cortex_compactor_block_cleanup_failed_total`
  * `cortex_compactor_block_cleanup_last_successful_run_timestamp_seconds`
* [ENHANCEMENT] Experimental TSDB: Use shared cache for metadata. This is especially useful when running multiple querier and store-gateway components to reduce number of object store API calls. #2626 #2640
* [ENHANCEMENT] Experimental TSDB: when `-querier.query-store-after` is configured and running the experimental blocks storage, the time range of the query sent to the store is now manipulated to ensure the query end time is not more recent than 'now - query-store-after'. #2642
* [ENHANCEMENT] Experimental TSDB: small performance improvement in concurrent usage of RefCache, used during samples ingestion. #2651
* [ENHANCEMENT] The following endpoints now respond appropriately to an `Accepts` header with the value `application/json` #2673
  * `/distributor/all_user_stats`
  * `/distributor/ha_tracker`
  * `/ingester/ring`
  * `/store-gateway/ring`
  * `/compactor/ring`
  * `/ruler/ring`
  * `/services`
* [ENHANCEMENT] Add `-cassandra.num-connections` to allow increasing the number of TCP connections to each Cassandra server. #2666
* [ENHANCEMENT] Use separate Cassandra clients and connections for reads and writes. #2666
* [ENHANCEMENT] Add `-cassandra.reconnect-interval` to allow specifying the reconnect interval to a Cassandra server that has been marked `DOWN` by the gocql driver. Also change the default value of the reconnect interval from `60s` to `1s`. #2687
* [ENHANCEMENT] Experimental TSDB: Applied a jitter to the period bucket scans in order to better distribute bucket operations over the time and increase the probability of hitting the shared cache (if configured). #2693
* [ENHANCEMENT] Experimental TSDB: Series limit per user and per metric now work in TSDB blocks. #2676
* [BUGFIX] Ruler: Ensure temporary rule files with special characters are properly mapped and cleaned up. #2506
* [BUGFIX] Fixes #2411, Ensure requests are properly routed to the prometheus api embedded in the query if `-server.path-prefix` is set. #2372
* [BUGFIX] Experimental TSDB: fixed chunk data corruption when querying back series using the experimental blocks storage. #2400
* [BUGFIX] Fixed collection of tracing spans from Thanos components used internally. #2655
* [BUGFIX] Experimental TSDB: fixed memory leak in ingesters. #2586
* [BUGFIX] QueryFrontend: fixed a situation where HTTP error is ignored and an incorrect status code is set. #2590
* [BUGFIX] Ingester: Fix an ingester starting up in the JOINING state and staying there forever. #2565
* [BUGFIX] QueryFrontend: fixed a panic (`integer divide by zero`) in the query-frontend. The query-frontend now requires the `-querier.default-evaluation-interval` config to be set to the same value of the querier. #2603
* [BUGFIX] Experimental TSDB: when the querier receives a `/series` request with a time range older than the data stored in the ingester, it now ignores the requested time range and returns known series anyway instead of returning an empty response. This aligns the behaviour with the chunks storage. #2617
* [BUGFIX] Cassandra: fixed an edge case leading to an invalid CQL query when querying the index on a Cassandra store. #2639
* [BUGFIX] Ingester: increment series per metric when recovering from WAL or transfer. #2674
* [BUGFIX] Fixed `wrong number of arguments for 'mget' command` Redis error when a query has no chunks to lookup from storage. #2700
* [BUGFIX] Ingester: Automatically remove old tmp checkpoints, fixing a potential disk space leak after an ingester crashes.

## 1.1.0 / 2020-05-21

This release brings the usual mix of bugfixes and improvements. The biggest change is that WAL support for chunks is now considered to be production-ready!

Please make sure to review renamed metrics, and update your dashboards and alerts accordingly.

* [CHANGE] Added v1 API routes documented in #2327. #2372
  * Added `-http.alertmanager-http-prefix` flag which allows the configuration of the path where the Alertmanager API and UI can be reached. The default is set to `/alertmanager`.
  * Added `-http.prometheus-http-prefix` flag which allows the configuration of the path where the Prometheus API and UI can be reached. The default is set to `/prometheus`.
  * Updated the index hosted at the root prefix to point to the updated routes.
  * Legacy routes hardcoded with the `/api/prom` prefix now respect the `-http.prefix` flag.
* [CHANGE] The metrics `cortex_distributor_ingester_appends_total` and `distributor_ingester_append_failures_total` now include a `type` label to differentiate between `samples` and `metadata`. #2336
* [CHANGE] The metrics for number of chunks and bytes flushed to the chunk store are renamed. Note that previous metrics were counted pre-deduplication, while new metrics are counted after deduplication. #2463
  * `cortex_ingester_chunks_stored_total` > `cortex_chunk_store_stored_chunks_total`
  * `cortex_ingester_chunk_stored_bytes_total` > `cortex_chunk_store_stored_chunk_bytes_total`
* [CHANGE] Experimental TSDB: renamed blocks meta fetcher metrics: #2375
  * `cortex_querier_bucket_store_blocks_meta_syncs_total` > `cortex_querier_blocks_meta_syncs_total`
  * `cortex_querier_bucket_store_blocks_meta_sync_failures_total` > `cortex_querier_blocks_meta_sync_failures_total`
  * `cortex_querier_bucket_store_blocks_meta_sync_duration_seconds` > `cortex_querier_blocks_meta_sync_duration_seconds`
  * `cortex_querier_bucket_store_blocks_meta_sync_consistency_delay_seconds` > `cortex_querier_blocks_meta_sync_consistency_delay_seconds`
* [CHANGE] Experimental TSDB: Modified default values for `compactor.deletion-delay` option from 48h to 12h and `-experimental.tsdb.bucket-store.ignore-deletion-marks-delay` from 24h to 6h. #2414
* [CHANGE] WAL: Default value of `-ingester.checkpoint-enabled` changed to `true`. #2416
* [CHANGE] `trace_id` field in log files has been renamed to `traceID`. #2518
* [CHANGE] Slow query log has a different output now. Previously used `url` field has been replaced with `host` and `path`, and query parameters are logged as individual log fields with `qs_` prefix. #2520
* [CHANGE] WAL: WAL and checkpoint compression is now disabled. #2436
* [CHANGE] Update in dependency `go-kit/kit` from `v0.9.0` to `v0.10.0`. HTML escaping disabled in JSON Logger. #2535
* [CHANGE] Experimental TSDB: Removed `cortex_<service>_` prefix from Thanos objstore metrics and added `component` label to distinguish which Cortex component is doing API calls to the object storage when running in single-binary mode: #2568
  - `cortex_<service>_thanos_objstore_bucket_operations_total` renamed to `thanos_objstore_bucket_operations_total{component="<name>"}`
  - `cortex_<service>_thanos_objstore_bucket_operation_failures_total` renamed to `thanos_objstore_bucket_operation_failures_total{component="<name>"}`
  - `cortex_<service>_thanos_objstore_bucket_operation_duration_seconds` renamed to `thanos_objstore_bucket_operation_duration_seconds{component="<name>"}`
  - `cortex_<service>_thanos_objstore_bucket_last_successful_upload_time` renamed to `thanos_objstore_bucket_last_successful_upload_time{component="<name>"}`
* [CHANGE] FIFO cache: The `-<prefix>.fifocache.size` CLI flag has been renamed to `-<prefix>.fifocache.max-size-items` as well as its YAML config option `size` renamed to `max_size_items`. #2319
* [FEATURE] Ruler: The `-ruler.evaluation-delay` flag was added to allow users to configure a default evaluation delay for all rules in cortex. The default value is 0 which is the current behavior. #2423
* [FEATURE] Experimental: Added a new object storage client for OpenStack Swift. #2440
* [FEATURE] TLS config options added to the Server. #2535
* [FEATURE] Experimental: Added support for `/api/v1/metadata` Prometheus-based endpoint. #2549
* [FEATURE] Add ability to limit concurrent queries to Cassandra with `-cassandra.query-concurrency` flag. #2562
* [FEATURE] Add `-cassandra.table-options` flag to customize table options of Cassandra when creating the index or chunk table. #2575
* [ENHANCEMENT] Experimental TSDB: sample ingestion errors are now reported via existing `cortex_discarded_samples_total` metric. #2370
* [ENHANCEMENT] Failures on samples at distributors and ingesters return the first validation error as opposed to the last. #2383
* [ENHANCEMENT] Experimental TSDB: Added `cortex_querier_blocks_meta_synced`, which reflects current state of synced blocks over all tenants. #2392
* [ENHANCEMENT] Added `cortex_distributor_latest_seen_sample_timestamp_seconds` metric to see how far behind Prometheus servers are in sending data. #2371
* [ENHANCEMENT] FIFO cache to support eviction based on memory usage. Added `-<prefix>.fifocache.max-size-bytes` CLI flag and YAML config option `max_size_bytes` to specify memory limit of the cache. #2319, #2527
* [ENHANCEMENT] Added `-querier.worker-match-max-concurrent`. Force worker concurrency to match the `-querier.max-concurrent` option.  Overrides `-querier.worker-parallelism`.  #2456
* [ENHANCEMENT] Added the following metrics for monitoring delete requests: #2445
  - `cortex_purger_delete_requests_received_total`: Number of delete requests received per user.
  - `cortex_purger_delete_requests_processed_total`: Number of delete requests processed per user.
  - `cortex_purger_delete_requests_chunks_selected_total`: Number of chunks selected while building delete plans per user.
  - `cortex_purger_delete_requests_processing_failures_total`: Number of delete requests processing failures per user.
* [ENHANCEMENT] Single Binary: Added query-frontend to the single binary.  Single binary users will now benefit from various query-frontend features.  Primarily: sharding, parallelization, load shedding, additional caching (if configured), and query retries. #2437
* [ENHANCEMENT] Allow 1w (where w denotes week) and 1y (where y denotes year) when setting `-store.cache-lookups-older-than` and `-store.max-look-back-period`. #2454
* [ENHANCEMENT] Optimize index queries for matchers using "a|b|c"-type regex. #2446 #2475
* [ENHANCEMENT] Added per tenant metrics for queries and chunks and bytes read from chunk store: #2463
  * `cortex_chunk_store_fetched_chunks_total` and `cortex_chunk_store_fetched_chunk_bytes_total`
  * `cortex_query_frontend_queries_total` (per tenant queries counted by the frontend)
* [ENHANCEMENT] WAL: New metrics `cortex_ingester_wal_logged_bytes_total` and `cortex_ingester_checkpoint_logged_bytes_total` added to track total bytes logged to disk for WAL and checkpoints. #2497
* [ENHANCEMENT] Add de-duplicated chunks counter `cortex_chunk_store_deduped_chunks_total` which counts every chunk not sent to the store because it was already sent by another replica. #2485
* [ENHANCEMENT] Query-frontend now also logs the POST data of long queries. #2481
* [ENHANCEMENT] WAL: Ingester WAL records now have type header and the custom WAL records have been replaced by Prometheus TSDB's WAL records. Old records will not be supported from 1.3 onwards. Note: once this is deployed, you cannot downgrade without data loss. #2436
* [ENHANCEMENT] Redis Cache: Added `idle_timeout`, `wait_on_pool_exhaustion` and `max_conn_lifetime` options to redis cache configuration. #2550
* [ENHANCEMENT] WAL: the experimental tag has been removed on the WAL in ingesters. #2560
* [ENHANCEMENT] Use newer AWS API for paginated queries - removes 'Deprecated' message from logfiles. #2452
* [ENHANCEMENT] Experimental memberlist: Add retry with backoff on memberlist join other members. #2705
* [BUGFIX] Ruler: Ensure temporary rule files with special characters are properly mapped and cleaned up. #2506
* [BUGFIX] Ensure requests are properly routed to the prometheus api embedded in the query if `-server.path-prefix` is set. Fixes #2411. #2372
* [BUGFIX] Experimental TSDB: Fixed chunk data corruption when querying back series using the experimental blocks storage. #2400
* [BUGFIX] Cassandra Storage: Fix endpoint TLS host verification. #2109
* [BUGFIX] Experimental TSDB: Fixed response status code from `422` to `500` when an error occurs while iterating chunks with the experimental blocks storage. #2402
* [BUGFIX] Ring: Fixed a situation where upgrading from pre-1.0 cortex with a rolling strategy caused new 1.0 ingesters to lose their zone value in the ring until manually forced to re-register. #2404
* [BUGFIX] Distributor: `/all_user_stats` now show API and Rule Ingest Rate correctly. #2457
* [BUGFIX] Fixed `version`, `revision` and `branch` labels exported by the `cortex_build_info` metric. #2468
* [BUGFIX] QueryFrontend: fixed a situation where span context missed when downstream_url is used. #2539
* [BUGFIX] Querier: Fixed a situation where querier would crash because of an unresponsive frontend instance. #2569

## 1.0.1 / 2020-04-23

* [BUGFIX] Fix gaps when querying ingesters with replication factor = 3 and 2 ingesters in the cluster. #2503

## 1.0.0 / 2020-04-02

This is the first major release of Cortex. We made a lot of **breaking changes** in this release which have been detailed below. Please also see the stability guarantees we provide as part of a major release: https://cortexmetrics.io/docs/configuration/v1guarantees/

* [CHANGE] Remove the following deprecated flags: #2339
  - `-metrics.error-rate-query` (use `-metrics.write-throttle-query` instead).
  - `-store.cardinality-cache-size` (use `-store.index-cache-read.enable-fifocache` and `-store.index-cache-read.fifocache.size` instead).
  - `-store.cardinality-cache-validity` (use `-store.index-cache-read.enable-fifocache` and `-store.index-cache-read.fifocache.duration` instead).
  - `-distributor.limiter-reload-period` (flag unused)
  - `-ingester.claim-on-rollout` (flag unused)
  - `-ingester.normalise-tokens` (flag unused)
* [CHANGE] Renamed YAML file options to be more consistent. See [full config file changes below](#config-file-breaking-changes). #2273
* [CHANGE] AWS based autoscaling has been removed. You can only use metrics based autoscaling now. `-applicationautoscaling.url` has been removed. See https://cortexmetrics.io/docs/production/aws/#dynamodb-capacity-provisioning on how to migrate. #2328
* [CHANGE] Renamed the `memcache.write-back-goroutines` and `memcache.write-back-buffer` flags to `background.write-back-concurrency` and `background.write-back-buffer`. This affects the following flags: #2241
  - `-frontend.memcache.write-back-buffer` --> `-frontend.background.write-back-buffer`
  - `-frontend.memcache.write-back-goroutines` --> `-frontend.background.write-back-concurrency`
  - `-store.index-cache-read.memcache.write-back-buffer` --> `-store.index-cache-read.background.write-back-buffer`
  - `-store.index-cache-read.memcache.write-back-goroutines` --> `-store.index-cache-read.background.write-back-concurrency`
  - `-store.index-cache-write.memcache.write-back-buffer` --> `-store.index-cache-write.background.write-back-buffer`
  - `-store.index-cache-write.memcache.write-back-goroutines` --> `-store.index-cache-write.background.write-back-concurrency`
  - `-memcache.write-back-buffer` --> `-store.chunks-cache.background.write-back-buffer`. Note the next change log for the difference.
  - `-memcache.write-back-goroutines` --> `-store.chunks-cache.background.write-back-concurrency`. Note the next change log for the difference.

* [CHANGE] Renamed the chunk cache flags to have `store.chunks-cache.` as prefix. This means the following flags have been changed: #2241
  - `-cache.enable-fifocache` --> `-store.chunks-cache.cache.enable-fifocache`
  - `-default-validity` --> `-store.chunks-cache.default-validity`
  - `-fifocache.duration` --> `-store.chunks-cache.fifocache.duration`
  - `-fifocache.size` --> `-store.chunks-cache.fifocache.size`
  - `-memcache.write-back-buffer` --> `-store.chunks-cache.background.write-back-buffer`. Note the previous change log for the difference.
  - `-memcache.write-back-goroutines` --> `-store.chunks-cache.background.write-back-concurrency`. Note the previous change log for the difference.
  - `-memcached.batchsize` --> `-store.chunks-cache.memcached.batchsize`
  - `-memcached.consistent-hash` --> `-store.chunks-cache.memcached.consistent-hash`
  - `-memcached.expiration` --> `-store.chunks-cache.memcached.expiration`
  - `-memcached.hostname` --> `-store.chunks-cache.memcached.hostname`
  - `-memcached.max-idle-conns` --> `-store.chunks-cache.memcached.max-idle-conns`
  - `-memcached.parallelism` --> `-store.chunks-cache.memcached.parallelism`
  - `-memcached.service` --> `-store.chunks-cache.memcached.service`
  - `-memcached.timeout` --> `-store.chunks-cache.memcached.timeout`
  - `-memcached.update-interval` --> `-store.chunks-cache.memcached.update-interval`
  - `-redis.enable-tls` --> `-store.chunks-cache.redis.enable-tls`
  - `-redis.endpoint` --> `-store.chunks-cache.redis.endpoint`
  - `-redis.expiration` --> `-store.chunks-cache.redis.expiration`
  - `-redis.max-active-conns` --> `-store.chunks-cache.redis.max-active-conns`
  - `-redis.max-idle-conns` --> `-store.chunks-cache.redis.max-idle-conns`
  - `-redis.password` --> `-store.chunks-cache.redis.password`
  - `-redis.timeout` --> `-store.chunks-cache.redis.timeout`
* [CHANGE] Rename the `-store.chunk-cache-stubs` to `-store.chunks-cache.cache-stubs` to be more inline with above. #2241
* [CHANGE] Change prefix of flags `-dynamodb.periodic-table.*` to `-table-manager.index-table.*`. #2359
* [CHANGE] Change prefix of flags `-dynamodb.chunk-table.*` to `-table-manager.chunk-table.*`. #2359
* [CHANGE] Change the following flags: #2359
  - `-dynamodb.poll-interval` --> `-table-manager.poll-interval`
  - `-dynamodb.periodic-table.grace-period` --> `-table-manager.periodic-table.grace-period`
* [CHANGE] Renamed the following flags: #2273
  - `-dynamodb.chunk.gang.size` --> `-dynamodb.chunk-gang-size`
  - `-dynamodb.chunk.get.max.parallelism` --> `-dynamodb.chunk-get-max-parallelism`
* [CHANGE] Don't support mixed time units anymore for duration. For example, 168h5m0s doesn't work anymore, please use just one unit (s|m|h|d|w|y). #2252
* [CHANGE] Utilize separate protos for rule state and storage. Experimental ruler API will not be functional until the rollout is complete. #2226
* [CHANGE] Frontend worker in querier now starts after all Querier module dependencies are started. This fixes issue where frontend worker started to send queries to querier before it was ready to serve them (mostly visible when using experimental blocks storage). #2246
* [CHANGE] Lifecycler component now enters Failed state on errors, and doesn't exit the process. (Important if you're vendoring Cortex and use Lifecycler) #2251
* [CHANGE] `/ready` handler now returns 200 instead of 204. #2330
* [CHANGE] Better defaults for the following options: #2344
  - `-<prefix>.consul.consistent-reads`: Old default: `true`, new default: `false`. This reduces the load on Consul.
  - `-<prefix>.consul.watch-rate-limit`: Old default: 0, new default: 1. This rate limits the reads to 1 per second. Which is good enough for ring watches.
  - `-distributor.health-check-ingesters`: Old default: `false`, new default: `true`.
  - `-ingester.max-stale-chunk-idle`: Old default: 0, new default: 2m. This lets us expire series that we know are stale early.
  - `-ingester.spread-flushes`: Old default: false, new default: true. This allows to better de-duplicate data and use less space.
  - `-ingester.chunk-age-jitter`: Old default: 20mins, new default: 0. This is to enable the `-ingester.spread-flushes` to true.
  - `-<prefix>.memcached.batchsize`: Old default: 0, new default: 1024. This allows batching of requests and keeps the concurrent requests low.
  - `-<prefix>.memcached.consistent-hash`: Old default: false, new default: true. This allows for better cache hits when the memcaches are scaled up and down.
  - `-querier.batch-iterators`: Old default: false, new default: true.
  - `-querier.ingester-streaming`: Old default: false, new default: true.
* [CHANGE] Experimental TSDB: Added `-experimental.tsdb.bucket-store.postings-cache-compression-enabled` to enable postings compression when storing to cache. #2335
* [CHANGE] Experimental TSDB: Added `-compactor.deletion-delay`, which is time before a block marked for deletion is deleted from bucket. If not 0, blocks will be marked for deletion and compactor component will delete blocks marked for deletion from the bucket. If delete-delay is 0, blocks will be deleted straight away. Note that deleting blocks immediately can cause query failures, if store gateway / querier still has the block loaded, or compactor is ignoring the deletion because it's compacting the block at the same time. Default value is 48h. #2335
* [CHANGE] Experimental TSDB: Added `-experimental.tsdb.bucket-store.index-cache.postings-compression-enabled`, to set duration after which the blocks marked for deletion will be filtered out while fetching blocks used for querying. This option allows querier to ignore blocks that are marked for deletion with some delay. This ensures store can still serve blocks that are meant to be deleted but do not have a replacement yet. Default is 24h, half of the default value for `-compactor.deletion-delay`. #2335
* [CHANGE] Experimental TSDB: Added `-experimental.tsdb.bucket-store.index-cache.memcached.max-item-size` to control maximum size of item that is stored to memcached. Defaults to 1 MiB. #2335
* [FEATURE] Added experimental storage API to the ruler service that is enabled when the `-experimental.ruler.enable-api` is set to true #2269
  * `-ruler.storage.type` flag now allows `s3`,`gcs`, and `azure` values
  * `-ruler.storage.(s3|gcs|azure)` flags exist to allow the configuration of object clients set for rule storage
* [CHANGE] Renamed table manager metrics. #2307 #2359
  * `cortex_dynamo_sync_tables_seconds` -> `cortex_table_manager_sync_duration_seconds`
  * `cortex_dynamo_table_capacity_units` -> `cortex_table_capacity_units`
* [FEATURE] Flusher target to flush the WAL. #2075
  * `-flusher.wal-dir` for the WAL directory to recover from.
  * `-flusher.concurrent-flushes` for number of concurrent flushes.
  * `-flusher.flush-op-timeout` is duration after which a flush should timeout.
* [FEATURE] Ingesters can now have an optional availability zone set, to ensure metric replication is distributed across zones. This is set via the `-ingester.availability-zone` flag or the `availability_zone` field in the config file. #2317
* [ENHANCEMENT] Better re-use of connections to DynamoDB and S3. #2268
* [ENHANCEMENT] Reduce number of goroutines used while executing a single index query. #2280
* [ENHANCEMENT] Experimental TSDB: Add support for local `filesystem` backend. #2245
* [ENHANCEMENT] Experimental TSDB: Added memcached support for the TSDB index cache. #2290
* [ENHANCEMENT] Experimental TSDB: Removed gRPC server to communicate between querier and BucketStore. #2324
* [ENHANCEMENT] Allow 1w (where w denotes week) and 1y (where y denotes year) when setting table period and retention. #2252
* [ENHANCEMENT] Added FIFO cache metrics for current number of entries and memory usage. #2270
* [ENHANCEMENT] Output all config fields to /config API, including those with empty value. #2209
* [ENHANCEMENT] Add "missing_metric_name" and "metric_name_invalid" reasons to cortex_discarded_samples_total metric. #2346
* [ENHANCEMENT] Experimental TSDB: sample ingestion errors are now reported via existing `cortex_discarded_samples_total` metric. #2370
* [BUGFIX] Ensure user state metrics are updated if a transfer fails. #2338
* [BUGFIX] Fixed etcd client keepalive settings. #2278
* [BUGFIX] Register the metrics of the WAL. #2295
* [BUXFIX] Experimental TSDB: fixed error handling when ingesting out of bound samples. #2342

### Known issues

- This experimental blocks storage in Cortex `1.0.0` has a bug which may lead to the error `cannot iterate chunk for series` when running queries. This bug has been fixed in #2400. If you're running the experimental blocks storage, please build Cortex from `master`.

### Config file breaking changes

In this section you can find a config file diff showing the breaking changes introduced in Cortex. You can also find the [full configuration file reference doc](https://cortexmetrics.io/docs/configuration/configuration-file/) in the website.

```diff
### ingester_config

 # Period with which to attempt to flush chunks.
 # CLI flag: -ingester.flush-period
-[flushcheckperiod: <duration> | default = 1m0s]
+[flush_period: <duration> | default = 1m0s]

 # Period chunks will remain in memory after flushing.
 # CLI flag: -ingester.retain-period
-[retainperiod: <duration> | default = 5m0s]
+[retain_period: <duration> | default = 5m0s]

 # Maximum chunk idle time before flushing.
 # CLI flag: -ingester.max-chunk-idle
-[maxchunkidle: <duration> | default = 5m0s]
+[max_chunk_idle_time: <duration> | default = 5m0s]

 # Maximum chunk idle time for chunks terminating in stale markers before
 # flushing. 0 disables it and a stale series is not flushed until the
 # max-chunk-idle timeout is reached.
 # CLI flag: -ingester.max-stale-chunk-idle
-[maxstalechunkidle: <duration> | default = 0s]
+[max_stale_chunk_idle_time: <duration> | default = 2m0s]

 # Timeout for individual flush operations.
 # CLI flag: -ingester.flush-op-timeout
-[flushoptimeout: <duration> | default = 1m0s]
+[flush_op_timeout: <duration> | default = 1m0s]

 # Maximum chunk age before flushing.
 # CLI flag: -ingester.max-chunk-age
-[maxchunkage: <duration> | default = 12h0m0s]
+[max_chunk_age: <duration> | default = 12h0m0s]

-# Range of time to subtract from MaxChunkAge to spread out flushes
+# Range of time to subtract from -ingester.max-chunk-age to spread out flushes
 # CLI flag: -ingester.chunk-age-jitter
-[chunkagejitter: <duration> | default = 20m0s]
+[chunk_age_jitter: <duration> | default = 0]

 # Number of concurrent goroutines flushing to dynamodb.
 # CLI flag: -ingester.concurrent-flushes
-[concurrentflushes: <int> | default = 50]
+[concurrent_flushes: <int> | default = 50]

-# If true, spread series flushes across the whole period of MaxChunkAge
+# If true, spread series flushes across the whole period of
+# -ingester.max-chunk-age.
 # CLI flag: -ingester.spread-flushes
-[spreadflushes: <boolean> | default = false]
+[spread_flushes: <boolean> | default = true]

 # Period with which to update the per-user ingestion rates.
 # CLI flag: -ingester.rate-update-period
-[rateupdateperiod: <duration> | default = 15s]
+[rate_update_period: <duration> | default = 15s]


### querier_config

 # The maximum number of concurrent queries.
 # CLI flag: -querier.max-concurrent
-[maxconcurrent: <int> | default = 20]
+[max_concurrent: <int> | default = 20]

 # Use batch iterators to execute query, as opposed to fully materialising the
 # series in memory.  Takes precedent over the -querier.iterators flag.
 # CLI flag: -querier.batch-iterators
-[batchiterators: <boolean> | default = false]
+[batch_iterators: <boolean> | default = true]

 # Use streaming RPCs to query ingester.
 # CLI flag: -querier.ingester-streaming
-[ingesterstreaming: <boolean> | default = false]
+[ingester_streaming: <boolean> | default = true]

 # Maximum number of samples a single query can load into memory.
 # CLI flag: -querier.max-samples
-[maxsamples: <int> | default = 50000000]
+[max_samples: <int> | default = 50000000]

 # The default evaluation interval or step size for subqueries.
 # CLI flag: -querier.default-evaluation-interval
-[defaultevaluationinterval: <duration> | default = 1m0s]
+[default_evaluation_interval: <duration> | default = 1m0s]

### query_frontend_config

 # URL of downstream Prometheus.
 # CLI flag: -frontend.downstream-url
-[downstream: <string> | default = ""]
+[downstream_url: <string> | default = ""]


### ruler_config

 # URL of alerts return path.
 # CLI flag: -ruler.external.url
-[externalurl: <url> | default = ]
+[external_url: <url> | default = ]

 # How frequently to evaluate rules
 # CLI flag: -ruler.evaluation-interval
-[evaluationinterval: <duration> | default = 1m0s]
+[evaluation_interval: <duration> | default = 1m0s]

 # How frequently to poll for rule changes
 # CLI flag: -ruler.poll-interval
-[pollinterval: <duration> | default = 1m0s]
+[poll_interval: <duration> | default = 1m0s]

-storeconfig:
+storage:

 # file path to store temporary rule files for the prometheus rule managers
 # CLI flag: -ruler.rule-path
-[rulepath: <string> | default = "/rules"]
+[rule_path: <string> | default = "/rules"]

 # URL of the Alertmanager to send notifications to.
 # CLI flag: -ruler.alertmanager-url
-[alertmanagerurl: <url> | default = ]
+[alertmanager_url: <url> | default = ]

 # Use DNS SRV records to discover alertmanager hosts.
 # CLI flag: -ruler.alertmanager-discovery
-[alertmanagerdiscovery: <boolean> | default = false]
+[enable_alertmanager_discovery: <boolean> | default = false]

 # How long to wait between refreshing alertmanager hosts.
 # CLI flag: -ruler.alertmanager-refresh-interval
-[alertmanagerrefreshinterval: <duration> | default = 1m0s]
+[alertmanager_refresh_interval: <duration> | default = 1m0s]

 # If enabled requests to alertmanager will utilize the V2 API.
 # CLI flag: -ruler.alertmanager-use-v2
-[alertmanangerenablev2api: <boolean> | default = false]
+[enable_alertmanager_v2: <boolean> | default = false]

 # Capacity of the queue for notifications to be sent to the Alertmanager.
 # CLI flag: -ruler.notification-queue-capacity
-[notificationqueuecapacity: <int> | default = 10000]
+[notification_queue_capacity: <int> | default = 10000]

 # HTTP timeout duration when sending notifications to the Alertmanager.
 # CLI flag: -ruler.notification-timeout
-[notificationtimeout: <duration> | default = 10s]
+[notification_timeout: <duration> | default = 10s]

 # Distribute rule evaluation using ring backend
 # CLI flag: -ruler.enable-sharding
-[enablesharding: <boolean> | default = false]
+[enable_sharding: <boolean> | default = false]

 # Time to spend searching for a pending ruler when shutting down.
 # CLI flag: -ruler.search-pending-for
-[searchpendingfor: <duration> | default = 5m0s]
+[search_pending_for: <duration> | default = 5m0s]

 # Period with which to attempt to flush rule groups.
 # CLI flag: -ruler.flush-period
-[flushcheckperiod: <duration> | default = 1m0s]
+[flush_period: <duration> | default = 1m0s]

### alertmanager_config

 # Base path for data storage.
 # CLI flag: -alertmanager.storage.path
-[datadir: <string> | default = "data/"]
+[data_dir: <string> | default = "data/"]

 # will be used to prefix all HTTP endpoints served by Alertmanager. If omitted,
 # relevant URL components will be derived automatically.
 # CLI flag: -alertmanager.web.external-url
-[externalurl: <url> | default = ]
+[external_url: <url> | default = ]

 # How frequently to poll Cortex configs
 # CLI flag: -alertmanager.configs.poll-interval
-[pollinterval: <duration> | default = 15s]
+[poll_interval: <duration> | default = 15s]

 # Listen address for cluster.
 # CLI flag: -cluster.listen-address
-[clusterbindaddr: <string> | default = "0.0.0.0:9094"]
+[cluster_bind_address: <string> | default = "0.0.0.0:9094"]

 # Explicit address to advertise in cluster.
 # CLI flag: -cluster.advertise-address
-[clusteradvertiseaddr: <string> | default = ""]
+[cluster_advertise_address: <string> | default = ""]

 # Time to wait between peers to send notifications.
 # CLI flag: -cluster.peer-timeout
-[peertimeout: <duration> | default = 15s]
+[peer_timeout: <duration> | default = 15s]

 # Filename of fallback config to use if none specified for instance.
 # CLI flag: -alertmanager.configs.fallback
-[fallbackconfigfile: <string> | default = ""]
+[fallback_config_file: <string> | default = ""]

 # Root of URL to generate if config is http://internal.monitor
 # CLI flag: -alertmanager.configs.auto-webhook-root
-[autowebhookroot: <string> | default = ""]
+[auto_webhook_root: <string> | default = ""]

### table_manager_config

-store:
+storage:

-# How frequently to poll DynamoDB to learn our capacity.
-# CLI flag: -dynamodb.poll-interval
-[dynamodb_poll_interval: <duration> | default = 2m0s]
+# How frequently to poll backend to learn our capacity.
+# CLI flag: -table-manager.poll-interval
+[poll_interval: <duration> | default = 2m0s]

-# DynamoDB periodic tables grace period (duration which table will be
-# created/deleted before/after it's needed).
-# CLI flag: -dynamodb.periodic-table.grace-period
+# Periodic tables grace period (duration which table will be created/deleted
+# before/after it's needed).
+# CLI flag: -table-manager.periodic-table.grace-period
 [creation_grace_period: <duration> | default = 10m0s]

 index_tables_provisioning:
   # Enables on demand throughput provisioning for the storage provider (if
-  # supported). Applies only to tables which are not autoscaled
-  # CLI flag: -dynamodb.periodic-table.enable-ondemand-throughput-mode
-  [provisioned_throughput_on_demand_mode: <boolean> | default = false]
+  # supported). Applies only to tables which are not autoscaled. Supported by
+  # DynamoDB
+  # CLI flag: -table-manager.index-table.enable-ondemand-throughput-mode
+  [enable_ondemand_throughput_mode: <boolean> | default = false]


   # Enables on demand throughput provisioning for the storage provider (if
-  # supported). Applies only to tables which are not autoscaled
-  # CLI flag: -dynamodb.periodic-table.inactive-enable-ondemand-throughput-mode
-  [inactive_throughput_on_demand_mode: <boolean> | default = false]
+  # supported). Applies only to tables which are not autoscaled. Supported by
+  # DynamoDB
+  # CLI flag: -table-manager.index-table.inactive-enable-ondemand-throughput-mode
+  [enable_inactive_throughput_on_demand_mode: <boolean> | default = false]


 chunk_tables_provisioning:
   # Enables on demand throughput provisioning for the storage provider (if
-  # supported). Applies only to tables which are not autoscaled
-  # CLI flag: -dynamodb.chunk-table.enable-ondemand-throughput-mode
-  [provisioned_throughput_on_demand_mode: <boolean> | default = false]
+  # supported). Applies only to tables which are not autoscaled. Supported by
+  # DynamoDB
+  # CLI flag: -table-manager.chunk-table.enable-ondemand-throughput-mode
+  [enable_ondemand_throughput_mode: <boolean> | default = false]

### storage_config

 aws:
-  dynamodbconfig:
+  dynamodb:
     # DynamoDB endpoint URL with escaped Key and Secret encoded. If only region
     # is specified as a host, proper endpoint will be deduced. Use
     # inmemory:///<table-name> to use a mock in-memory implementation.
     # CLI flag: -dynamodb.url
-    [dynamodb: <url> | default = ]
+    [dynamodb_url: <url> | default = ]

     # DynamoDB table management requests per second limit.
     # CLI flag: -dynamodb.api-limit
-    [apilimit: <float> | default = 2]
+    [api_limit: <float> | default = 2]

     # DynamoDB rate cap to back off when throttled.
     # CLI flag: -dynamodb.throttle-limit
-    [throttlelimit: <float> | default = 10]
+    [throttle_limit: <float> | default = 10]
-
-    # ApplicationAutoscaling endpoint URL with escaped Key and Secret encoded.
-    # CLI flag: -applicationautoscaling.url
-    [applicationautoscaling: <url> | default = ]


       # Queue length above which we will scale up capacity
       # CLI flag: -metrics.target-queue-length
-      [targetqueuelen: <int> | default = 100000]
+      [target_queue_length: <int> | default = 100000]

       # Scale up capacity by this multiple
       # CLI flag: -metrics.scale-up-factor
-      [scaleupfactor: <float> | default = 1.3]
+      [scale_up_factor: <float> | default = 1.3]

       # Ignore throttling below this level (rate per second)
       # CLI flag: -metrics.ignore-throttle-below
-      [minthrottling: <float> | default = 1]
+      [ignore_throttle_below: <float> | default = 1]

       # query to fetch ingester queue length
       # CLI flag: -metrics.queue-length-query
-      [queuelengthquery: <string> | default = "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))"]
+      [queue_length_query: <string> | default = "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))"]

       # query to fetch throttle rates per table
       # CLI flag: -metrics.write-throttle-query
-      [throttlequery: <string> | default = "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) > 0"]
+      [write_throttle_query: <string> | default = "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) > 0"]

       # query to fetch write capacity usage per table
       # CLI flag: -metrics.usage-query
-      [usagequery: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) > 0"]
+      [write_usage_query: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) > 0"]

       # query to fetch read capacity usage per table
       # CLI flag: -metrics.read-usage-query
-      [readusagequery: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) > 0"]
+      [read_usage_query: <string> | default = "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) > 0"]

       # query to fetch read errors per table
       # CLI flag: -metrics.read-error-query
-      [readerrorquery: <string> | default = "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) > 0"]
+      [read_error_query: <string> | default = "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) > 0"]

     # Number of chunks to group together to parallelise fetches (zero to
     # disable)
-    # CLI flag: -dynamodb.chunk.gang.size
-    [chunkgangsize: <int> | default = 10]
+    # CLI flag: -dynamodb.chunk-gang-size
+    [chunk_gang_size: <int> | default = 10]

     # Max number of chunk-get operations to start in parallel
-    # CLI flag: -dynamodb.chunk.get.max.parallelism
-    [chunkgetmaxparallelism: <int> | default = 32]
+    # CLI flag: -dynamodb.chunk.get-max-parallelism
+    [chunk_get_max_parallelism: <int> | default = 32]

     backoff_config:
       # Minimum delay when backing off.
       # CLI flag: -bigtable.backoff-min-period
-      [minbackoff: <duration> | default = 100ms]
+      [min_period: <duration> | default = 100ms]

       # Maximum delay when backing off.
       # CLI flag: -bigtable.backoff-max-period
-      [maxbackoff: <duration> | default = 10s]
+      [max_period: <duration> | default = 10s]

       # Number of times to backoff and retry before failing.
       # CLI flag: -bigtable.backoff-retries
-      [maxretries: <int> | default = 10]
+      [max_retries: <int> | default = 10]

   # If enabled, once a tables info is fetched, it is cached.
   # CLI flag: -bigtable.table-cache.enabled
-  [tablecacheenabled: <boolean> | default = true]
+  [table_cache_enabled: <boolean> | default = true]

   # Duration to cache tables before checking again.
   # CLI flag: -bigtable.table-cache.expiration
-  [tablecacheexpiration: <duration> | default = 30m0s]
+  [table_cache_expiration: <duration> | default = 30m0s]

 # Cache validity for active index entries. Should be no higher than
 # -ingester.max-chunk-idle.
 # CLI flag: -store.index-cache-validity
-[indexcachevalidity: <duration> | default = 5m0s]
+[index_cache_validity: <duration> | default = 5m0s]

### ingester_client_config

 grpc_client_config:
   backoff_config:
     # Minimum delay when backing off.
     # CLI flag: -ingester.client.backoff-min-period
-    [minbackoff: <duration> | default = 100ms]
+    [min_period: <duration> | default = 100ms]

     # Maximum delay when backing off.
     # CLI flag: -ingester.client.backoff-max-period
-    [maxbackoff: <duration> | default = 10s]
+    [max_period: <duration> | default = 10s]

     # Number of times to backoff and retry before failing.
     # CLI flag: -ingester.client.backoff-retries
-    [maxretries: <int> | default = 10]
+    [max_retries: <int> | default = 10]

### frontend_worker_config

-# Address of query frontend service.
+# Address of query frontend service, in host:port format.
 # CLI flag: -querier.frontend-address
-[address: <string> | default = ""]
+[frontend_address: <string> | default = ""]

 # How often to query DNS.
 # CLI flag: -querier.dns-lookup-period
-[dnslookupduration: <duration> | default = 10s]
+[dns_lookup_duration: <duration> | default = 10s]

 grpc_client_config:
   backoff_config:
     # Minimum delay when backing off.
     # CLI flag: -querier.frontend-client.backoff-min-period
-    [minbackoff: <duration> | default = 100ms]
+    [min_period: <duration> | default = 100ms]

     # Maximum delay when backing off.
     # CLI flag: -querier.frontend-client.backoff-max-period
-    [maxbackoff: <duration> | default = 10s]
+    [max_period: <duration> | default = 10s]

     # Number of times to backoff and retry before failing.
     # CLI flag: -querier.frontend-client.backoff-retries
-    [maxretries: <int> | default = 10]
+    [max_retries: <int> | default = 10]

### consul_config

 # ACL Token used to interact with Consul.
-# CLI flag: -<prefix>.consul.acltoken
-[acltoken: <string> | default = ""]
+# CLI flag: -<prefix>.consul.acl-token
+[acl_token: <string> | default = ""]

 # HTTP timeout when talking to Consul
 # CLI flag: -<prefix>.consul.client-timeout
-[httpclienttimeout: <duration> | default = 20s]
+[http_client_timeout: <duration> | default = 20s]

 # Enable consistent reads to Consul.
 # CLI flag: -<prefix>.consul.consistent-reads
-[consistentreads: <boolean> | default = true]
+[consistent_reads: <boolean> | default = false]

 # Rate limit when watching key or prefix in Consul, in requests per second. 0
 # disables the rate limit.
 # CLI flag: -<prefix>.consul.watch-rate-limit
-[watchkeyratelimit: <float> | default = 0]
+[watch_rate_limit: <float> | default = 1]

 # Burst size used in rate limit. Values less than 1 are treated as 1.
 # CLI flag: -<prefix>.consul.watch-burst-size
-[watchkeyburstsize: <int> | default = 1]
+[watch_burst_size: <int> | default = 1]


### configstore_config
 # URL of configs API server.
 # CLI flag: -<prefix>.configs.url
-[configsapiurl: <url> | default = ]
+[configs_api_url: <url> | default = ]

 # Timeout for requests to Weave Cloud configs service.
 # CLI flag: -<prefix>.configs.client-timeout
-[clienttimeout: <duration> | default = 5s]
+[client_timeout: <duration> | default = 5s]
```

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
* [BUGFIX] Experimental TSDB: Fixed incorrect query results introduced in #2604 caused by a buffer incorrectly reused while iterating samples. #2697

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
