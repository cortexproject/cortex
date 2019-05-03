# Cortex Arguments Explained

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

## Ingester

#### Ring Lifecycler Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.num-tokens` | Number of tokens for each ingester. | `128` |
| `-ingester.heartbeat-period` | Period at which to heartbeat to consul. | `5s` |
| `-ingester.join-after` | Period to wait for a claim from another ingester; will join automatically after this. | `0s` |
| `-ingester.min-ready-duration` | Minimum duration to wait before becoming ready. This is to work around race conditions with ingesters exiting and updating the ring. | `1m` |
| `-ingester.claim-on-rollout` | Send chunks to PENDING ingesters on exit. | `false` |
| `-ingester.normalise-tokens` | Store tokens in a normalised fashion to reduce allocations. | `false` |
| `-ingester.final-sleep` | Duration to sleep for before exiting, to ensure metrics are scraped. | `30s` |
| `-ingester.addr` | IP address to advertise in consul. | (empty string) |
| `-ingester.port` | Port to advertise in consul (defaults to server.grpc-listen-port). | `0` |
| `-ingester.id` | ID to register into consul. | (OS hostname) |

#### General Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.max-transfer-retries` | Number of times to try and transfer chunks before falling back to flushing. | `10` |
| `-ingester.flush-period` | Period with which to attempt to flush chunks. | `1m` |
| `-ingester.retain-period` | Period chunks will remain in memory after flushing. | `5m` |
| `-ingester.flush-op-timeout` | Timeout for individual flush operations. | `1m` |
| `-ingester.max-chunk-idle` | Maximum chunk idle time before flushing. | `5m` |
| `-ingester.max-chunk-age` | How long chunks can stay in memory before they must be flushed into the underlying database. | `12h` |
| `-ingester.chunk-age-jitter` | Range of time to subtract from MaxChunkAge to spread out flushes. | `20m` |
| `-ingester.concurrent-flushes` | Number of concurrent goroutines flushing to the underlying time series database (DynamoDB, BigTable, etc). | `50` |
| `-ingester.rate-update-period` | Period with which to update the per-user ingestion rates. | `15s` |
| `-ingester.normalise-tokens` | Write out "normalised" tokens to the ring. Normalised tokens consume less memory to encode and decode; as the ring is unmarshalled regularly, this significantly reduces memory usage of anything that watches the ring. Before enabling, rollout a version of Cortex that supports normalised token for all jobs that interact with the ring, then rollout with this flag set to `true` on the ingesters. The new ring code can still read and write the old ring format, so is backwards compatible. | `15s` |
| `-ingester.max-transfer-retries` | Number of times to try and transfer chunks before falling back to flushing. | `10` |

#### Chunk Flags

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.chunk-encoding` | Encoding version to use for chunks. | `DoubleDelta` |
| `-store.fullsize-chunks` | When saving varbit chunks, pad to 1024 bytes. | `true` |
| `-store.bigchunk-size-cap-bytes` | When using bigchunks, start a new bigchunk and flush the old one if the old one reaches this size. Use this setting to limit memory growth of ingesters with a lot of timeseries that last for days. Set 0 for unlimited size. | `0` |
| `-bigtable.project` | Google BigTable project ID. | (empty string) |
| `-bigtable.instance` | Google BigTable instance ID. | (empty string) |
| `-gcs.bucketname` | Name of GCS bucket to put chunks in. | (empty string) |
| `-gcs.chunk-buffer-size` | The size of the buffer that GCS client for each PUT request. 0 to disable buffering. | `0` |
| `-dynamodb.url` | DynamoDB endpoint URL with escaped Key and Secret encoded. | (none) |
| `-dynamodb.api-limit` | DynamoDB table management requests per second limit. | `2.0` |
| `-applicationautoscaling.url` | ApplicationAutoscaling endpoint URL with escaped Key and Secret encoded. | (none) |
| `-dynamodb.chunk.gang.size` | Number of chunks to group together to parallelise fetches (zero to disable). | `10` |
| `-dynamodb.chunk.get.max.parallelism` | Max number of chunk-get operations to start in parallel. | `32` |
| `-dynamodb.min-backoff` | Minimum backoff time. | `100ms` |
| `-dynamodb.max-backoff` | Maximum backoff time. | `50s` |
| `-dynamodb.max-retries` | Maximum number of times to retry an operation. | `20` |
| `-cassandra.addresses` | Comma-separated hostnames or ips of Cassandra instances. | (empty string) |
| `-cassandra.port` | Port that Cassandra is running on. | `9042` |
| `-cassandra.keyspace` | eyspace to use in Cassandra. | (empty string) |
| `-cassandra.consistency` | Consistency level for Cassandra. | `QUORUM` |
| `-cassandra.replication-factor` | Replication factor to use in Cassandra. | `1` |
| `-cassandra.disable-initial-host-lookup` | Instruct the cassandra driver to not attempt to get host info from the system.peers table. | `false` |
| `-cassandra.ssl` | Use SSL when connecting to cassandra instances. | `false` |
| `-cassandra.host-verification` | Require SSL certificate validation. | `true` |
| `-cassandra.ca-path` | Path to certificate file to verify the peer. | (empty string) |
| `-cassandra.auth` | Enable password authentication when connecting to cassandra. | `false` |
| `-cassandra.username` | Username to use when connecting to cassandra. | (empty string) |
| `-cassandra.password` | Password to use when connecting to cassandra. | (empty string) |
| `-cassandra.timeout` | Timeout when connecting to cassandra. | `600ms` |

#### Validation Limits

Limits describe all the limits for users (see [Ingester, Distributor & Querier limits](#ingester-distributor--querier-limits)); can be used to describe global default limits via flags, or per-user limits via yaml config.

| Flag | Description | Default |
| --- | --- | --- |
| `-ingester.max-series-per-query` | The maximum number of series that a query can return. | `100000` |
| `-ingester.max-samples-per-query` | The maximum number of samples that a query can return. | `1000000` |
| `-ingester.max-series-per-user` | The maximum number of samples that a query can return. | `5000000` |
| `-ingester.max-series-per-metric` | The maximum number of samples that a query can return. | `50000` |

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
