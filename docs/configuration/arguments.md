---
title: "Cortex Arguments"
linkTitle: "Cortex Arguments Explained"
weight: 2
slug: arguments
---

## General Notes

Cortex has evolved over several years, and the command-line options sometimes reflect this heritage. In some cases the default value for options is not the recommended value, and in some cases names do not reflect the true meaning. We do intend to clean this up, but it requires a lot of care to avoid breaking existing installations. In the meantime we regret the inconvenience.

Duration arguments should be specified with a unit like `5s` or `3h`. Valid time units are "ms", "s", "m", "h".

## Querier

- `-querier.max-concurrent`

   The maximum number of top-level PromQL queries that will execute at the same time, per querier process.
   If using the query frontend, this should be set to at least (`-querier.worker-parallelism` * number of query frontend replicas). Otherwise queries may queue in the queriers and not the frontend, which will affect QoS.  Alternatively, consider using `-querier.worker-match-max-concurrent` to force worker parallelism to match `-querier.max-concurrent`.

- `-querier.timeout`

   The timeout for a top-level PromQL query.

- `-querier.max-samples`

   Maximum number of samples a single query can load into memory, to avoid blowing up on enormous queries.

The next three options only apply when the querier is used together with the Query Frontend or Query Scheduler:

- `-querier.frontend-address`

   Address of query frontend service, used by workers to find the frontend which will give them queries to execute.

- `-querier.scheduler-address`

   Address of query scheduler service, used by workers to find the scheduler which will give them queries to execute. If set, `-querier.frontend-address` is ignored, and querier will use query scheduler.

- `-querier.dns-lookup-period`

   How often the workers will query DNS to re-check where the query frontend or query scheduler is.

- `-querier.worker-parallelism`

   Number of simultaneous queries to process, per query frontend or scheduler.
   See note on `-querier.max-concurrent`

- `-querier.worker-match-max-concurrent`

   Force worker concurrency to match the -querier.max-concurrent option.  Overrides `-querier.worker-parallelism`.
   See note on `-querier.max-concurrent`


## Querier and Ruler

The ingester query API was improved over time, but defaults to the old behaviour for backwards-compatibility. For best results both of these next two flags should be set to `true`:

- `-querier.batch-iterators`

   This uses iterators to execute query, as opposed to fully materialising the series in memory, and fetches multiple results per loop.

- `-querier.ingester-streaming`

   Use streaming RPCs to query ingester, to reduce memory pressure in the ingester.

- `-querier.iterators`

   This is similar to `-querier.batch-iterators` but less efficient.
   If both `iterators` and `batch-iterators` are `true`, `batch-iterators` will take precedence.

- `-promql.lookback-delta`

   Time since the last sample after which a time series is considered stale and ignored by expression evaluations.

## Query Frontend

- `-querier.parallelise-shardable-queries`

   If set to true, will cause the query frontend to mutate incoming queries when possible by turning `sum` operations into sharded `sum` operations. This requires a shard-compatible schema (v10+). An abridged example:
   `sum by (foo) (rate(bar{baz=”blip”}[1m]))` ->
   ```
   sum by (foo) (
    sum by (foo) (rate(bar{baz=”blip”,__cortex_shard__=”0of16”}[1m])) or
    sum by (foo) (rate(bar{baz=”blip”,__cortex_shard__=”1of16”}[1m])) or
    ...
    sum by (foo) (rate(bar{baz=”blip”,__cortex_shard__=”15of16”}[1m]))
   )
   ```
   When enabled, the query-frontend requires a schema config to determine how/when to shard queries, either from a file or from flags (i.e. by the `-schema-config-file` CLI flag). This is the same schema config the queriers consume.
   It's also advised to increase downstream concurrency controls as well to account for more queries of smaller sizes:

   - `querier.max-outstanding-requests-per-tenant`
   - `querier.max-query-parallelism`
   - `querier.max-concurrent`
   - `server.grpc-max-concurrent-streams` (for both query-frontends and queriers)

   Furthermore, both querier and query-frontend components require the `querier.query-ingesters-within` parameter to know when to start sharding requests (ingester queries are not sharded). It's recommended to align this with `ingester.max-chunk-age`.

   Instrumentation (traces) also scale with the number of sharded queries and it's suggested to account for increased throughput there as well (for instance via `JAEGER_REPORTER_MAX_QUEUE_SIZE`).

- `-querier.align-querier-with-step`

   If set to true, will cause the query frontend to mutate incoming queries and align their start and end parameters to the step parameter of the query.  This improves the cacheability of the query results.

- `-querier.split-queries-by-day`

   If set to true, will cause the query frontend to split multi-day queries into multiple single-day queries and execute them in parallel.

- `-querier.cache-results`

   If set to true, will cause the querier to cache query results.  The cache will be used to answer future, overlapping queries.  The query frontend calculates extra queries required to fill gaps in the cache.

- `-frontend.forward-headers-list`

   Request headers  forwarded by query frontend to downstream queriers. Multiple headers may be specified. Defaults to empty.

- `-frontend.max-cache-freshness`

   When caching query results, it is desirable to prevent the caching of very recent results that might still be in flux.  Use this parameter to configure the age of results that should be excluded.

- `-frontend.memcached.{hostname, service, timeout}`

   Use these flags to specify the location and timeout of the memcached cluster used to cache query results.

- `-frontend.redis.{endpoint, timeout}`

   Use these flags to specify the location and timeout of the Redis service used to cache query results.

## Distributor

- `-distributor.shard-by-all-labels`

   In the original Cortex design, samples were sharded amongst distributors by the combination of (userid, metric name).  Sharding by metric name was designed to reduce the number of ingesters you need to hit on the read path; the downside was that you could hotspot the write path.

   In hindsight, this seems like the wrong choice: we do many orders of magnitude more writes than reads, and ingester reads are in-memory and cheap. It seems the right thing to do is to use all the labels to shard, improving load balancing and support for very high cardinality metrics.

   Set this flag to `true` for the new behaviour.

   Important to note is that when setting this flag to `true`, it has to be set on both the distributor and the querier (called `-distributor.shard-by-all-labels` on Querier as well). If the flag is only set on the distributor and not on the querier, you will get incomplete query results because not all ingesters are queried.

   **Upgrade notes**: As this flag also makes all queries always read from all ingesters, the upgrade path is pretty trivial; just enable the flag. When you do enable it, you'll see a spike in the number of active series as the writes are "reshuffled" amongst the ingesters, but over the next stale period all the old series will be flushed, and you should end up with much better load balancing. With this flag enabled in the queriers, reads will always catch all the data from all ingesters.

   **Warning**: disabling this flag can lead to a much less balanced distribution of load among the ingesters.

- `-distributor.extra-query-delay`
   This is used by a component with an embedded distributor (Querier and Ruler) to control how long to wait until sending more than the minimum amount of queries needed for a successful response.

- `distributor.ha-tracker.enable-for-all-users`
   Flag to enable, for all users, handling of samples with external labels identifying replicas in an HA Prometheus setup. This defaults to false, and is technically defined in the Distributor limits.

- `distributor.ha-tracker.enable`
   Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels). Global (for distributors), this ensures that the necessary internal data structures for the HA handling are created. The option `enable-for-all-users` is still needed to enable ingestion of HA samples for all users.

- `distributor.drop-label`
   This flag can be used to specify label names that to drop during sample ingestion within the distributor and can be repeated in order to drop multiple labels.

### Ring/HA Tracker Store

The KVStore client is used by both the Ring and HA Tracker (HA Tracker doesn't support memberlist as KV store).
- `{ring,distributor.ha-tracker}.prefix`
   The prefix for the keys in the store. Should end with a /. For example with a prefix of foo/, the key bar would be stored under foo/bar.
- `{ring,distributor.ha-tracker}.store`
   Backend storage to use for the HA Tracker (consul, etcd, inmemory, multi).
- `{ring,distributor.ring}.store`
   Backend storage to use for the Ring (consul, etcd, inmemory, memberlist, multi).

#### Consul

By default these flags are used to configure Consul used for the ring. To configure Consul for the HA tracker,
prefix these flags with `distributor.ha-tracker.`

- `consul.hostname`
   Hostname and port of Consul.
- `consul.acl-token`
   ACL token used to interact with Consul.
- `consul.client-timeout`
   HTTP timeout when talking to Consul.
- `consul.consistent-reads`
   Enable consistent reads to Consul.

#### etcd

By default these flags are used to configure etcd used for the ring. To configure etcd for the HA tracker,
prefix these flags with `distributor.ha-tracker.`

- `etcd.endpoints`
   The etcd endpoints to connect to.
- `etcd.dial-timeout`
   The timeout for the etcd connection.
- `etcd.max-retries`
   The maximum number of retries to do for failed ops.
- `etcd.tls-enabled`
   Enable TLS.
- `etcd.tls-cert-path`
   The TLS certificate file path.
- `etcd.tls-key-path`
   The TLS private key file path.
- `etcd.tls-ca-path`
   The trusted CA file path.
- `etcd.tls-insecure-skip-verify`
   Skip validating server certificate.

#### memberlist

Warning: memberlist KV works only for the [hash ring](../architecture.md#the-hash-ring), not for the HA Tracker, because propagation of changes is too slow for HA Tracker purposes.

When using memberlist-based KV store, each node maintains its own copy of the hash ring.
Updates generated locally, and received from other nodes are merged together to form the current state of the ring on the node.
Updates are also propagated to other nodes.
All nodes run the following two loops:

1. Every "gossip interval", pick random "gossip nodes" number of nodes, and send recent ring updates to them.
2. Every "push/pull sync interval", choose random single node, and exchange full ring information with it (push/pull sync). After this operation, rings on both nodes are the same.

When a node receives a ring update, node will merge it into its own ring state, and if that resulted in a change, node will add that update to the list of gossiped updates.
Such update will be gossiped `R * log(N+1)` times by this node (R = retransmit multiplication factor, N = number of gossiping nodes in the cluster).

If you find the propagation to be too slow, there are some tuning possibilities (default values are memberlist settings for LAN networks):
- Decrease gossip interval (default: 200ms)
- Increase gossip nodes (default 3)
- Decrease push/pull sync interval (default 30s)
- Increase retransmit multiplication factor (default 4)

To find propagation delay, you can use `cortex_ring_oldest_member_timestamp{state="ACTIVE"}` metric.

Flags for configuring KV store based on memberlist library:

- `memberlist.nodename`
   Name of the node in memberlist cluster. Defaults to hostname.
- `memberlist.randomize-node-name`
   This flag adds extra random suffix to the node name used by memberlist. Defaults to true. Using random suffix helps to prevent issues when running multiple memberlist nodes on the same machine, or when node names are reused (eg. in stateful sets).
- `memberlist.retransmit-factor`
   Multiplication factor used when sending out messages (factor * log(N+1)). If not set, default value is used.
- `memberlist.join`
   Other cluster members to join. Can be specified multiple times.
- `memberlist.min-join-backoff`, `memberlist.max-join-backoff`, `memberlist.max-join-retries`
   These flags control backoff settings when joining the cluster.
- `memberlist.abort-if-join-fails`
   If this node fails to join memberlist cluster, abort.
- `memberlist.rejoin-interval`
   How often to try to rejoin the memberlist cluster. Defaults to 0, no rejoining. Occasional rejoin may be useful in some configurations, and is otherwise harmless.
- `memberlist.left-ingesters-timeout`
   How long to keep LEFT ingesters in the ring. Note: this is only used for gossiping, LEFT ingesters are otherwise invisible.
- `memberlist.leave-timeout`
   Timeout for leaving memberlist cluster.
- `memberlist.gossip-interval`
   How often to gossip with other cluster members. Uses memberlist LAN defaults if 0.
- `memberlist.gossip-nodes`
   How many nodes to gossip with in each gossip interval. Uses memberlist LAN defaults if 0.
- `memberlist.pullpush-interval`
   How often to use pull/push sync. Uses memberlist LAN defaults if 0.
- `memberlist.bind-addr`
   IP address to listen on for gossip messages. Multiple addresses may be specified. Defaults to 0.0.0.0.
- `memberlist.bind-port`
   Port to listen on for gossip messages. Defaults to 7946.
- `memberlist.packet-dial-timeout`
   Timeout used when connecting to other nodes to send packet.
- `memberlist.packet-write-timeout`
   Timeout for writing 'packet' data.
- `memberlist.transport-debug`
   Log debug transport messages. Note: global log.level must be at debug level as well.
- `memberlist.gossip-to-dead-nodes-time`
   How long to keep gossiping to the nodes that seem to be dead. After this time, dead node is removed from list of nodes. If "dead" node appears again, it will simply join the cluster again, if its name is not reused by other node in the meantime. If the name has been reused, such a reanimated node will be ignored by other members.
- `memberlist.dead-node-reclaim-time`
   How soon can dead's node name be reused by a new node (using different IP). Disabled by default, name reclaim is not allowed until `gossip-to-dead-nodes-time` expires. This can be useful to set to low numbers when reusing node names, eg. in stateful sets.
   If memberlist library detects that new node is trying to reuse the name of previous node, it will log message like this: `Conflicting address for ingester-6. Mine: 10.44.12.251:7946 Theirs: 10.44.12.54:7946 Old state: 2`. Node states are: "alive" = 0, "suspect" = 1 (doesn't respond, will be marked as dead if it doesn't respond), "dead" = 2.

#### Multi KV

This is a special key-value implementation that uses two different KV stores (eg. consul, etcd or memberlist). One of them is always marked as primary, and all reads and writes go to primary store. Other one, secondary, is only used for writes. The idea is that operator can use multi KV store to migrate from primary to secondary store in runtime.

For example, migration from Consul to Etcd would look like this:

- Set `ring.store` to use `multi` store. Set `-multi.primary=consul` and `-multi.secondary=etcd`. All consul and etcd settings must still be specified.
- Start all Cortex microservices. They will still use Consul as primary KV, but they will also write share ring via etcd.
- Operator can now use "runtime config" mechanism to switch primary store to etcd.
- After all Cortex microservices have picked up new primary store, and everything looks correct, operator can now shut down Consul, and modify Cortex configuration to use `-ring.store=etcd` only.
- At this point, Consul can be shut down.

Multi KV has following parameters:

- `multi.primary` - name of primary KV store. Same values as in `ring.store` are supported, except `multi`.
- `multi.secondary` - name of secondary KV store.
- `multi.mirror-enabled` - enable mirroring of values to secondary store, defaults to true
- `multi.mirror-timeout` - wait max this time to write to secondary store to finish. Default to 2 seconds. Errors writing to secondary store are not reported to caller, but are logged and also reported via `cortex_multikv_mirror_write_errors_total` metric.

Multi KV also reacts on changes done via runtime configuration. It uses this section:

```yaml
multi_kv_config:
    mirror_enabled: false
    primary: memberlist
```

Note that runtime configuration values take precedence over command line options.

### HA Tracker

HA tracking has two of its own flags:
- `distributor.ha-tracker.cluster`
   Prometheus label to look for in samples to identify a Prometheus HA cluster. (default "cluster")
- `distributor.ha-tracker.replica`
   Prometheus label to look for in samples to identify a Prometheus HA replica. (default "`__replica__`")

It's reasonable to assume people probably already have a `cluster` label, or something similar. If not, they should add one along with `__replica__` via external labels in their Prometheus config. If you stick to these default values your Prometheus config could look like this (`POD_NAME` is an environment variable which must be set by you):

```yaml
global:
  external_labels:
    cluster: clustername
    __replica__: $POD_NAME
```

HA Tracking looks for the two labels (which can be overwritten per user)

It also talks to a KVStore and has it's own copies of the same flags used by the Distributor to connect to for the ring.
- `distributor.ha-tracker.failover-timeout`
   If we don't receive any samples from the accepted replica for a cluster in this amount of time we will failover to the next replica we receive a sample from. This value must be greater than the update timeout (default 30s)
- `distributor.ha-tracker.store`
   Backend storage to use for the ring (consul, etcd, inmemory, multi). Inmemory only works if there is a single distributor and ingester running in the same process (for testing purposes). (default "consul")
- `distributor.ha-tracker.update-timeout`
   Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed since the current stored timestamp. (default 15s)

## Ingester

- `-ingester.max-chunk-age`

  The maximum duration of a timeseries chunk in memory. If a timeseries runs for longer than this the current chunk will be flushed to the store and a new chunk created. (default 12h)

- `-ingester.max-chunk-idle`

  If a series doesn't receive a sample for this duration, it is flushed and removed from memory.

- `-ingester.max-stale-chunk-idle`

  If a series receives a [staleness marker](https://www.robustperception.io/staleness-and-promql), then we wait for this duration to get another sample before we close and flush this series, removing it from memory. You want it to be at least 2x the scrape interval as you don't want a single failed scrape to cause a chunk flush.

- `-ingester.chunk-age-jitter`

  To reduce load on the database exactly 12 hours after starting, the age limit is reduced by a varying amount up to this. Don't enable this along with `-ingester.spread-flushes` (default 0m)

- `-ingester.spread-flushes`

  Makes the ingester flush each timeseries at a specific point in the `max-chunk-age` cycle. This means multiple replicas of a chunk are very likely to contain the same contents which cuts chunk storage space by up to 66%. Set `-ingester.chunk-age-jitter` to `0` when using this option. If a chunk cache is configured (via `-store.chunks-cache.memcached.hostname`) then duplicate chunk writes are skipped which cuts write IOPs.

- `-ingester.join-after`

   How long to wait in PENDING state during the hand-over process (supported only by the chunks storage). (default 0s)

- `-ingester.max-transfer-retries`

   How many times a LEAVING ingester tries to find a PENDING ingester during the hand-over process (supported only by the chunks storage). Negative value or zero disables hand-over process completely. (default 10)

- `-ingester.normalise-tokens`

   Deprecated. New ingesters always write "normalised" tokens to the ring. Normalised tokens consume less memory to encode and decode; as the ring is unmarshalled regularly, this significantly reduces memory usage of anything that watches the ring.

   Cortex 0.4.0 is the last version that can *write* denormalised tokens. Cortex 0.5.0 and above always write normalised tokens.

   Cortex 0.6.0 is the last version that can *read* denormalised tokens. Starting with Cortex 0.7.0 only normalised tokens are supported, and ingesters writing denormalised tokens to the ring (running Cortex 0.4.0 or earlier with `-ingester.normalise-tokens=false`) are ignored by distributors. Such ingesters should either switch to using normalised tokens, or be upgraded to Cortex 0.5.0 or later.

- `-ingester.chunk-encoding`

  Pick one of the encoding formats for timeseries data, which have different performance characteristics.
  `Bigchunk` uses the Prometheus V2 code, and expands in memory to arbitrary length.
  `Varbit`, `Delta` and `DoubleDelta` use Prometheus V1 code, and are fixed at 1K per chunk.
  Defaults to `Bigchunk` starting version 0.7.0.

- `-ingester-client.expected-timeseries`

   When `push` requests arrive, pre-allocate this many slots to decode them. Tune this setting to reduce memory allocations and garbage. This should match the `max_samples_per_send` in your `queue_config` for Prometheus.

- `-ingester-client.expected-samples-per-series`

   When `push` requests arrive, pre-allocate this many slots to decode them. Tune this setting to reduce memory allocations and garbage. Under normal conditions, Prometheus scrapes should arrive with one sample per series.

- `-ingester-client.expected-labels`

   When `push` requests arrive, pre-allocate this many slots to decode them. Tune this setting to reduce memory allocations and garbage. The optimum value will depend on how many labels are sent with your timeseries samples.

- `-store.chunk-cache.cache-stubs`

   Where you don't want to cache every chunk written by ingesters, but you do want to take advantage of chunk write deduplication, this option will make ingesters write a placeholder to the cache for each chunk.
   Make sure you configure ingesters with a different cache to queriers, which need the whole value.

#### Flusher

- `-flusher.wal-dir`
   Directory where the WAL data should be recovered from.

- `-flusher.concurrent-flushes`
   Number of concurrent flushes.

- `-flusher.flush-op-timeout`
   Duration after which a flush should timeout.

## Runtime Configuration file

Cortex has a concept of "runtime config" file, which is simply a file that is reloaded while Cortex is running. It is used by some Cortex components to allow operator to change some aspects of Cortex configuration without restarting it. File is specified by using `-runtime-config.file=<filename>` flag and reload period (which defaults to 10 seconds) can be changed by `-runtime-config.reload-period=<duration>` flag. Previously this mechanism was only used by limits overrides, and flags were called `-limits.per-user-override-config=<filename>` and `-limits.per-user-override-period=10s` respectively. These are still used, if `-runtime-config.file=<filename>` is not specified.

At the moment runtime configuration may contain per-user limits, multi KV store, and ingester instance limits.

Example runtime configuration file:

```yaml
overrides:
  tenant1:
    ingestion_rate: 10000
    max_series_per_metric: 100000
    max_series_per_query: 100000
  tenant2:
    max_samples_per_query: 1000000
    max_series_per_metric: 100000
    max_series_per_query: 100000

multi_kv_config:
    mirror_enabled: false
    primary: memberlist

ingester_limits:
  max_ingestion_rate: 42000
  max_inflight_push_requests: 10000
```

When running Cortex on Kubernetes, store this file in a config map and mount it in each services' containers.  When changing the values there is no need to restart the services, unless otherwise specified.

The `/runtime_config` endpoint returns the whole runtime configuration, including the overrides. In case you want to get only the non-default values of the configuration you can pass the `mode` parameter with the `diff` value.

## Ingester, Distributor & Querier limits.

Cortex implements various limits on the requests it can process, in order to prevent a single tenant overwhelming the cluster.  There are various default global limits which apply to all tenants which can be set on the command line.  These limits can also be overridden on a per-tenant basis by using `overrides` field of runtime configuration file.

The `overrides` field is a map of tenant ID (same values as passed in the `X-Scope-OrgID` header) to the various limits.  An example could look like:

```yaml
overrides:
  tenant1:
    ingestion_rate: 10000
    max_series_per_metric: 100000
    max_series_per_query: 100000
  tenant2:
    max_samples_per_query: 1000000
    max_series_per_metric: 100000
    max_series_per_query: 100000
```

Valid per-tenant limits are (with their corresponding flags for default values):

- `ingestion_rate_strategy` / `-distributor.ingestion-rate-limit-strategy`
- `ingestion_rate` / `-distributor.ingestion-rate-limit`
- `ingestion_burst_size` / `-distributor.ingestion-burst-size`

  The per-tenant rate limit (and burst size), in samples per second. It supports two strategies: `local` (default) and `global`.

  The `local` strategy enforces the limit on a per distributor basis, actual effective rate limit will be N times higher, where N is the number of distributor replicas.

  The `global` strategy enforces the limit globally, configuring a per-distributor local rate limiter as `ingestion_rate / N`, where N is the number of distributor replicas (it's automatically adjusted if the number of replicas change). The `ingestion_burst_size` refers to the per-distributor local rate limiter (even in the case of the `global` strategy) and should be set at least to the maximum number of samples expected in a single push request. For this reason, the `global` strategy requires that push requests are evenly distributed across the pool of distributors; if you use a load balancer in front of the distributors you should be already covered, while if you have a custom setup (ie. an authentication gateway in front) make sure traffic is evenly balanced across distributors.

  The `global` strategy requires the distributors to form their own ring, which is used to keep track of the current number of healthy distributor replicas. The ring is configured by `distributor: { ring: {}}` / `-distributor.ring.*`.

- `max_label_name_length` / `-validation.max-length-label-name`
- `max_label_value_length` / `-validation.max-length-label-value`
- `max_label_names_per_series` / `-validation.max-label-names-per-series`

  Also enforced by the distributor, limits on the on length of labels and their values, and the total number of labels allowed per series.

- `reject_old_samples` / `-validation.reject-old-samples`
- `reject_old_samples_max_age` / `-validation.reject-old-samples.max-age`
- `creation_grace_period` / `-validation.create-grace-period`

  Also enforce by the distributor, limits on how far in the past (and future) timestamps that we accept can be.

- `max_series_per_user` / `-ingester.max-series-per-user`
- `max_series_per_metric` / `-ingester.max-series-per-metric`

  Enforced by the ingesters; limits the number of active series a user (or a given metric) can have.  When running with `-distributor.shard-by-all-labels=false` (the default), this limit will enforce the maximum number of series a metric can have 'globally', as all series for a single metric will be sent to the same replication set of ingesters.  This is not the case when running with `-distributor.shard-by-all-labels=true`, so the actual limit will be N/RF times higher, where N is number of ingester replicas and RF is configured replication factor.

  An active series is a series to which a sample has been written in the last `-ingester.max-chunk-idle` duration, which defaults to 5 minutes.

- `max_global_series_per_user` / `-ingester.max-global-series-per-user`
- `max_global_series_per_metric` / `-ingester.max-global-series-per-metric`

   Like `max_series_per_user` and `max_series_per_metric`, but the limit is enforced across the cluster. Each ingester is configured with a local limit based on the replication factor, the `-distributor.shard-by-all-labels` setting and the current number of healthy ingesters, and is kept updated whenever the number of ingesters change.

   Requires `-distributor.replication-factor`, `-distributor.shard-by-all-labels`, `-distributor.sharding-strategy` and `-distributor.zone-awareness-enabled` set for the ingesters too.

- `max_series_per_query` / `-ingester.max-series-per-query`

- `max_samples_per_query` / `-ingester.max-samples-per-query`

  Limits on the number of timeseries and samples returns by a single ingester during a query.

- `max_metadata_per_user` / `-ingester.max-metadata-per-user`
- `max_metadata_per_metric` / `-ingester.max-metadata-per-metric`
  Enforced by the ingesters; limits the number of active metadata a user (or a given metric) can have.  When running with `-distributor.shard-by-all-labels=false` (the default), this limit will enforce the maximum number of metadata a metric can have 'globally', as all metadata for a single metric will be sent to the same replication set of ingesters.  This is not the case when running with `-distributor.shard-by-all-labels=true`, so the actual limit will be N/RF times higher, where N is number of ingester replicas and RF is configured replication factor.

- `max_fetched_series_per_query` / `querier.max-fetched-series-per-query`
  When running Cortex with blocks storage this limit is enforced in the queriers on unique series fetched from ingesters and store-gateways (long-term storage).

- `max_global_metadata_per_user` / `-ingester.max-global-metadata-per-user`
- `max_global_metadata_per_metric` / `-ingester.max-global-metadata-per-metric`

   Like `max_metadata_per_user` and `max_metadata_per_metric`, but the limit is enforced across the cluster. Each ingester is configured with a local limit based on the replication factor, the `-distributor.shard-by-all-labels` setting and the current number of healthy ingesters, and is kept updated whenever the number of ingesters change.

   Requires `-distributor.replication-factor`, `-distributor.shard-by-all-labels`, `-distributor.sharding-strategy` and `-distributor.zone-awareness-enabled` set for the ingesters too.

## Ingester Instance Limits

Cortex ingesters support limits that are applied per-instance, meaning they apply to each ingester process. These can be used to ensure individual ingesters are not overwhelmed regardless of any per-user limits. These limits can be set under the `ingester.instance_limits` block in the global configuration file, with command line flags, or under the `ingester_limits` field in the runtime configuration file.

An example as part of the runtime configuration file:

```yaml
ingester_limits:
  max_ingestion_rate: 20000
  max_series: 1500000
  max_tenants: 1000
  max_inflight_push_requests: 30000
```

Valid ingester instance limits are (with their corresponding flags):

- `max_ingestion_rate` \ `--ingester.instance-limits.max-ingestion-rate`

  Limit the ingestion rate in samples per second for an ingester. When this limit is reached, new requests will fail with an HTTP 500 error.

- `max_series` \ `-ingester.instance-limits.max-series`

  Limit the total number of series that an ingester keeps in memory, across all users. When this limit is reached, requests that create new series will fail with an HTTP 500 error.

- `max_tenants` \ `-ingester.instance-limits.max-tenants`

  Limit the maximum number of users an ingester will accept metrics for. When this limit is reached, requests from new users will fail with an HTTP 500 error.

- `max_inflight_push_requests` \ `-ingester.instance-limits.max-inflight-push-requests`

  Limit the maximum number of requests being handled by an ingester at once. This setting is critical for preventing ingesters from using an excessive amount of memory during high load or temporary slow downs. When this limit is reached, new requests will fail with an HTTP 500 error.

## Storage

- `s3.force-path-style`

  Set this to `true` to force the request to use path-style addressing (`http://s3.amazonaws.com/BUCKET/KEY`). By default, the S3 client will use virtual hosted bucket addressing when possible (`http://BUCKET.s3.amazonaws.com/KEY`).

## DNS Service Discovery

Some clients in Cortex support service discovery via DNS to find addresses of backend servers to connect to (ie. caching servers). The clients supporting it are:

- [Blocks storage's memcached cache](../blocks-storage/store-gateway.md#caching)
- [All caching memcached servers](./config-file-reference.md#memcached-client-config)
- [Memberlist KV store](./config-file-reference.md#memberlist-config)

### Supported discovery modes

The DNS service discovery, inspired from Thanos DNS SD, supports different discovery modes. A discovery mode is selected adding a specific prefix to the address. The supported prefixes are:

- **`dns+`**<br />
  The domain name after the prefix is looked up as an A/AAAA query. For example: `dns+memcached.local:11211`
- **`dnssrv+`**<br />
  The domain name after the prefix is looked up as a SRV query, and then each SRV record is resolved as an A/AAAA record. For example: `dnssrv+_memcached._tcp.memcached.namespace.svc.cluster.local`
- **`dnssrvnoa+`**<br />
  The domain name after the prefix is looked up as a SRV query, with no A/AAAA lookup made after that. For example: `dnssrvnoa+_memcached._tcp.memcached.namespace.svc.cluster.local`

If **no prefix** is provided, the provided IP or hostname will be used straightaway without pre-resolving it.

If you are using a managed memcached service from [Google Cloud](https://cloud.google.com/memorystore/docs/memcached/auto-discovery-overview), or [AWS](https://docs.aws.amazon.com/AmazonElastiCache/latest/mem-ug/AutoDiscovery.HowAutoDiscoveryWorks.html), use the [auto-discovery](./config-file-reference.md#memcached-client-config) flag instead of DNS discovery, then use the discovery/configuration endpoint as the domain name without any prefix.

## Logging of IP of reverse proxy

If a reverse proxy is used in front of Cortex it might be diffult to troubleshoot errors. The following 3 settings can be used to log the IP address passed along by the reverse proxy in headers like X-Forwarded-For.

- `-server.log_source_ips_enabled`

  Set this to `true` to add logging of the IP when a Forwarded, X-Real-IP or X-Forwarded-For header is used. A field called `sourceIPs` will be added to error logs when data is pushed into Cortex.

- `-server.log-source-ips-header`

  Header field storing the source IPs. It is only used if `-server.log-source-ips-enabled` is true and if `-server.log-source-ips-regex` is set. If not set the default Forwarded, X-Real-IP or X-Forwarded-For headers are searched.

- `-server.log-source-ips-regex`

  Regular expression for matching the source IPs. It should contain at least one capturing group the first of which will be returned. Only used if `-server.log-source-ips-enabled` is true and if `-server.log-source-ips-header` is set. If not set the default Forwarded, X-Real-IP or X-Forwarded-For headers are searched.
