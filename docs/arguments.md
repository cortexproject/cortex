# Cortex Arguments Explained

## General Notes

Cortex has evolved over several years, and the command-line options sometimes reflect this heritage. In some cases the default value for options is not the recommended value, and in some cases names do not reflect the true meaning. We do intend to clean this up, but it requires a lot of care to avoid breaking existing installations. In the meantime we regret the inconvenience.

Duration arguments should be specified with a unit like `5s` or `3h`. Valid time units are "ms", "s", "m", "h".

## Querier

- `-querier.max-concurrent`

   The maximum number of top-level PromQL queries that will execute at the same time, per querier process.
   If using the query frontend, this should be set to at least (`querier.worker-parallelism` * number of query frontend replicas). Otherwise queries may queue in the queriers and not the frontend, which will affect QoS.

- `-querier.query-parallelism`

   This refers to database queries against the store (e.g. Bigtable or DynamoDB).  This is the max subqueries run in parallel per higher-level query.

- `-querier.timeout`

   The timeout for a top-level PromQL query.

- `-querier.max-samples`

   Maximum number of samples a single query can load into memory, to avoid blowing up on enormous queries.

The next three options only apply when the querier is used together with the Query Frontend:

- `-querier.frontend-address`

   Address of query frontend service, used by workers to find the frontend which will give them queries to execute.

- `-querier.dns-lookup-period`

   How often the workers will query DNS to re-check where the frontend is.

- `-querier.worker-parallelism`

   Number of simultaneous queries to process, per worker process.
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

- `-querier.align-querier-with-step`

   If set to true, will cause the query frontend to mutate incoming queries and align their start and end parameters to the step parameter of the query.  This improves the cacheability of the query results.

- `-querier.split-queries-by-day`

   If set to true, will case the query frontend to split multi-day queries into multiple single-day queries and execute them in parallel.

- `-querier.cache-results`

   If set to true, will cause the querier to cache query results.  The cache will be used to answer future, overlapping queries.  The query frontend calculates extra queries required to fill gaps in the cache.

- `-frontend.max-cache-freshness`

   When caching query results, it is desirable to prevent the caching of very recent results that might still be in flux.  Use this parameter to configure the age of results that should be excluded.

- `-memcached.{hostname, service, timeout}`

   Use these flags to specify the location and timeout of the memcached cluster used to cache query results.

- `-redis.{endpoint, timeout}`

   Use these flags to specify the location and timeout of the Redis service used to cache query results.

## Distributor

- `-distributor.shard-by-all-labels`

   In the original Cortex design, samples were sharded amongst distributors by the combination of (userid, metric name).  Sharding by metric name was designed to reduce the number of ingesters you need to hit on the read path; the downside was that you could hotspot the write path.

   In hindsight, this seems like the wrong choice: we do many orders of magnitude more writes than reads, and ingester reads are in-memory and cheap. It seems the right thing to do is to use all the labels to shard, improving load balancing and support for very high cardinality metrics.

   Set this flag to `true` for the new behaviour.

   **Upgrade notes**: As this flag also makes all queries always read from all ingesters, the upgrade path is pretty trivial; just enable the flag. When you do enable it, you'll see a spike in the number of active series as the writes are "reshuffled" amongst the ingesters, but over the next stale period all the old series will be flushed, and you should end up with much better load balancing. With this flag enabled in the queriers, reads will always catch all the data from all ingesters.

- `-distributor.extra-query-delay`
   This is used by a component with an embedded distributor (Querier and Ruler) to control how long to wait until sending more than the minimum amount of queries needed for a successful response.

- `distributor.ha-tracker.enable-for-all-users`
   Flag to enable, for all users, handling of samples with external labels identifying replicas in an HA Prometheus setup. This defaults to false, and is technically defined in the Distributor limits.

- `distributor.ha-tracker.enable` 
   Enable the distributors HA tracker so that it can accept samples from Prometheus HA replicas gracefully (requires labels). Global (for distributors), this ensures that the necessary internal data structures for the HA handling are created. The option `enable-for-all-users` is still needed to enable ingestion of HA samples for all users.

- `distributor.drop-label`
   This flag can be used to specify label names that to drop during sample ingestion within the distributor and can be repeated in order to drop multiple labels.

### Ring/HA Tracker Store

The KVStore client is used by both the Ring and HA Tracker.
- `{ring,distributor.ha-tracker}.prefix`
   The prefix for the keys in the store. Should end with a /. For example with a prefix of foo/, the key bar would be stored under foo/bar.
- `{ring,distributor.ha-tracker}.store`
   Backend storage to use for the ring (consul, etcd, inmemory).

#### Consul

By default these flags are used to configure Consul used for the ring. To configure Consul for the HA tracker,
prefix these flags with `distributor.ha-tracker.`

- `consul.hostname`
   Hostname and port of Consul.
- `consul.acltoken`
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
   Backend storage to use for the ring (consul, etcd, inmemory). (default "consul")
- `distributor.ha-tracker.update-timeout`
   Update the timestamp in the KV store for a given cluster/replica only after this amount of time has passed since the current stored timestamp. (default 15s)

## Ingester

- `-ingester.max-chunk-age`

  The maximum duration of a timeseries chunk in memory. If a timeseries runs for longer than this the current chunk will be flushed to the store and a new chunk created. (default 12h)

- `-ingester.chunk-age-jitter`

  To reduce load on the database exactly 12 hours after starting, the age limit is reduced by a varying amount up to this. (default 20m)

- `-ingester.spread-flushes`

  Makes the ingester flush each timeseries at a specific point in the `max-chunk-age` cycle. This means multiple replicas of a chunk are very likely to contain the same contents which cuts chunk storage space by up to 66%. Set `-ingester.chunk-age-jitter` to `0` when using this option. If a chunk cache is configured (via `-memcached.hostname`) then duplicate chunk writes are skipped which cuts write IOPs.

- `-ingester.join-after`

   How long to wait in PENDING state during the [hand-over process](ingester-handover.md). (default 0s)

- `-ingester.ingester.max-transfer-retries`

   How many times a LEAVING ingester tries to find a PENDING ingester during the [hand-over process](ingester-handover.md). Each attempt takes a second or so. (default 10)

- `-ingester.normalise-tokens`

   Write out "normalised" tokens to the ring.  Normalised tokens consume less memory to encode and decode; as the ring is unmarshalled regularly, this significantly reduces memory usage of anything that watches the ring.

   Before enabling, rollout a version of Cortex that supports normalised token for all jobs that interact with the ring, then rollout with this flag set to `true` on the ingesters.  The new ring code can still read and write the old ring format, so is backwards compatible.

- `-ingester.chunk-encoding`

  Pick one of the encoding formats for timeseries data, which have different performance characteristics.
  `Bigchunk` uses the Prometheus V2 code, and expands in memory to arbitrary length.
  `Varbit`, `Delta` and `DoubleDelta` use Prometheus V1 code, and are fixed at 1K per chunk.
  Defaults to `DoubleDelta`, but we recommend `Bigchunk`.

- `-store.bigchunk-size-cap-bytes`

   When using bigchunks, start a new bigchunk and flush the old one if the old one reaches this size. Use this setting to limit memory growth of ingesters with a lot of timeseries that last for days.

- `-ingester-client.expected-timeseries`

   When `push` requests arrive, pre-allocate this many slots to decode them. Tune this setting to reduce memory allocations and garbage. This should match the `max_samples_per_send` in your `queue_config` for Prometheus.

- `-ingester-client.expected-samples-per-series`

   When `push` requests arrive, pre-allocate this many slots to decode them. Tune this setting to reduce memory allocations and garbage. Under normal conditions, Prometheus scrapes should arrive with one sample per series.

- `-ingester-client.expected-labels`

   When `push` requests arrive, pre-allocate this many slots to decode them. Tune this setting to reduce memory allocations and garbage. The optimum value will depend on how many labels are sent with your timeseries samples.

- `-store.chunk-cache-stubs`

   Where you don't want to cache every chunk written by ingesters, but you do want to take advantage of chunk write deduplication, this option will make ingesters write a placeholder to the cache for each chunk.
   Make sure you configure ingesters with a different cache to queriers, which need the whole value.

## Ingester, Distributor & Querier limits.

Cortex implements various limits on the requests it can process, in order to prevent a single tenant overwhelming the cluster.  There are various default global limits which apply to all tenants which can be set on the command line.  These limits can also be overridden on a per-tenant basis, using a configuration file.  Specify the filename for the override configuration file using the `-limits.per-user-override-config=<filename>` flag.  The override file will be re-read every 10 seconds by default - this can also be controlled using the `-limits.per-user-override-period=10s` flag.

The override file should be in YAML format and contain a single `overrides` field, which itself is a map of tenant ID (same values as passed in the `X-Scope-OrgID` header) to the various limits.  An example `overrides.yml` could look like:

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

When running Cortex on Kubernetes, store this file in a config map and mount it in each services' containers.  When changing the values there is no need to restart the services, unless otherwise specified.

Valid fields are (with their corresponding flags for default values):

- `ingestion_rate` / `-distributor.ingestion-rate-limit`
- `ingestion_burst_size` / `-distributor.ingestion-burst-size`

  The per-tenant rate limit (and burst size), in samples per second. Enforced on a per distributor basis, actual effective rate limit will be N times higher, where N is the number of distributor replicas.

  **NB** Limits are reset every `-distributor.limiter-reload-period`, as such if you set a very high burst limit it will never be hit.

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

- `max_series_per_query` / `-ingester.max-series-per-query`
- `max_samples_per_query` / `-ingester.max-samples-per-query`

  Limits on the number of timeseries and samples returns by a single ingester during a query.

## Storage

- `s3.force-path-style`

  Set this to `true` to force the request to use path-style addressing (`http://s3.amazonaws.com/BUCKET/KEY`). By default, the S3 client will use virtual hosted bucket addressing when possible (`http://BUCKET.s3.amazonaws.com/KEY`).
