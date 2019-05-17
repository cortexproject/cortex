# Cortex Arguments Explained

We use Viper for configuration management. All below described flags can be configured via YAML or by setting those flags. If you want to pass these configurations via YAML files you must lookup the key names in the code. Please take note that the set defaults do not represent recommended settings. Instead they have been set to whatever was first supported in Cortex and have not been changed to ensure backwards compatibility.

Viper supports parsing of durations from strings. When you are asked to configure a duration you can for instance set "5h" which equals a 5 hour duration.

## Querier

- `-querier.max-concurrent`

  The maximum number of top-level PromQL queries that will execute at the same time, per querier process. If using the query frontend, this should be set to at least (`querier.worker-parallelism` \* number of query frontend replicas). Otherwise queries may queue in the queriers and not the frontend, which will affect QoS.

- `-querier.query-parallelism`

  This refers to database queries against the store (e.g. Bigtable or DynamoDB). This is the max subqueries run in parallel per higher-level query.

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

  Number of simultaneous queries to process, per worker process. See note on `-querier.max-concurrent`

## Querier and Ruler

The ingester query API was improved over time, but defaults to the old behaviour for backwards-compatibility. For best results both of these next two flags should be set to `true`:

- `-querier.batch-iterators`

  This uses iterators to execute query, as opposed to fully materialising the series in memory, and fetches multiple results per loop.

- `-querier.ingester-streaming`

  Use streaming RPCs to query ingester, to reduce memory pressure in the ingester.

- `-querier.iterators`

  This is similar to `-querier.batch-iterators` but less efficient. If both `iterators` and `batch-iterators` are `true`, `batch-iterators` will take precedence.

- `-promql.lookback-delta`

  Time since the last sample after which a time series is considered stale and ignored by expression evaluations.

## Query Frontend

- `-querier.align-querier-with-step`

  If set to true, will cause the query frontend to mutate incoming queries and align their start and end parameters to the step parameter of the query. This improves the cacheability of the query results.

- `-querier.split-queries-by-day`

  If set to true, will case the query frontend to split multi-day queries into multiple single-day queries and execute them in parallel.

- `-querier.cache-results`

  If set to true, will cause the querier to cache query results. The cache will be used to answer future, overlapping queries. The query frontend calculates extra queries required to fill gaps in the cache.

- `-frontend.max-cache-freshness`

  When caching query results, it is desirable to prevent the caching of very recent results that might still be in flux. Use this parameter to configure the age of results that should be excluded.

- `-memcached.{hostname, service, timeout}`

  Use these flags to specify the location and timeout of the memcached cluster used to cache query results.

## Distributor

- `-distributor.shard-by-all-labels`

  In the original Cortex design, samples were sharded amongst distributors by the combination of (userid, metric name). Sharding by metric name was designed to reduce the number of ingesters you need to hit on the read path; the downside was that you could hotspot the write path.

  In hindsight, this seems like the wrong choice: we do many orders of magnitude more writes than reads, and ingester reads are in-memory and cheap. It seems the right thing to do is to use all the labels to shard, improving load balancing and support for very high cardinality metrics.

  Set this flag to `true` for the new behaviour.

  **Upgrade notes**: As this flag also makes all queries always read from all ingesters, the upgrade path is pretty trivial; just enable the flag. When you do enable it, you'll see a spike in the number of active series as the writes are "reshuffled" amongst the ingesters, but over the next stale period all the old series will be flushed, and you should end up with much better load balancing. With this flag enabled in the queriers, reads will always catch all the data from all ingesters.

- `-distributor.extra-query-delay` This is used by a component with an embedded distributor (Querier and Ruler) to control how long to wait until sending more than the minimum amount of queries needed for a successful response.

## Ingester, Distributor & Querier limits.

Cortex implements various limits on the requests it can process, in order to prevent a single tenant overwhelming the cluster. There are various default global limits which apply to all tenants which can be set on the command line. These limits can also be overridden on a per-tenant basis, using a configuration file. Specify the filename for the override configuration file using the `-limits.per-user-override-config=<filename>` flag. The override file will be re-read every 10 seconds by default - this can also be controlled using the `-limits.per-user-override-period=10s` flag.

The override file should be in YAML format and contain a single `overrides` field, which itself is a map of tenant ID (same values as passed in the `X-Scope-OrgID` header) to the various limits. An example `overrides.yml` could look like:

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

When running Cortex on Kubernetes, store this file in a config map and mount it in each services' containers. When changing the values there is no need to restart the services, unless otherwise specified.

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

  Enforced by the ingesters; limits the number of active series a user (or a given metric) can have. When running with `-distributor.shard-by-all-labels=false` (the default), this limit will enforce the maximum number of series a metric can have 'globally', as all series for a single metric will be sent to the same replication set of ingesters. This is not the case when running with `-distributor.shard-by-all-labels=true`, so the actual limit will be N/RF times higher, where N is number of ingester replicas and RF is configured replication factor.

  An active series is a series to which a sample has been written in the last `-ingester.max-chunk-idle` duration, which defaults to 5 minutes.

- `max_series_per_query` / `-ingester.max-series-per-query`
- `max_samples_per_query` / `-ingester.max-samples-per-query`

  Limits on the number of timeseries and samples returns by a single ingester during a query.
