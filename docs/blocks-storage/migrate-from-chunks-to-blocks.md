---
title: "Migrate Cortex cluster from chunks to blocks"
linkTitle: "Migrate Cortex cluster from chunks to blocks"
weight: 5
slug: migrate-cortex-cluster-from-chunks-to-blocks
---

# Migrate Cortex cluster from chunks to blocks

This article describes how to migrate existing Cortex cluster from chunks storage to blocks storage,
and highlight possible issues we have found in the process.

This is an evolution of [Cortex proposal](https://cortexmetrics.io/docs/proposals/ingesters-migration/),
which was written before support for migration was in place.

## Discussion

In this article we will assume that existing Cortex cluster is managed by Kubernetes, Cortex is using chunks storage and ingesters are using WAL.
If your ingesters are not using WAL, many of the points will still apply, but presented migration script will not work properly without changes, as it assumes that ingesters are managed via StatefulSet.

Original Cortex proposal suggests that to migrate ingesters, we should first enable flushing chunks on shutdown (`-ingester.flush-on-shutdown-with-wal-enabled` flag), and then reconfigure statefulset with ingesters for blocks.
The idea is that as new statefulset rollout happens, old ingesters will flush all chunks, and when they restart they would start generating blocks.
This approach didn't work very well in practice, for several reasons.

It may be difficult to reconfigure existing StatefulSet to a different setup of volumes, required by blocks storage.
Kubernetes doesn't allow changes in volumes for existing StatefulSet configuration.
For this reason, document will use approach with using two different StatefulSets, existing "ingester" one, and new "ingester-blocks".

Another problem is use of `-ingester.flush-on-shutdown-with-wal-enabled` flag itself.
In theory it should allow for nice automated way of scale-down.
In practice, once this flag was enabled on ingesters, it makes each ingester shutdown much slower (in our experience ~20 minutes for single ingester with ~1.1M of chunks to flush).
Shutdown of ingester is now a race to finish before Kubernetes kills it because of termination grace period.
This period can be adjusted of course, but it has to be done before (or at the same time) as setting the flag.
If done later, ingesters will already attempt flush on shutdown during rollout of config change.
Note that if ingester doesn't finish flushing, and is killed prematurely, no data is lost – but ingester needs to be restarted with chunks and WAL, and then do the flush again (eg. using /flush or /shutdown).

Instead of using flush-on-shutdown flag, we will use `/shutdown` endpoint on ingesters during scale-down process in our migration script.
As the name suggests, this endpoint triggers shutdown of ingester while flushing of all chunks ([or blocks!](https://github.com/cortexproject/cortex/pull/2794)) to storage.
Nice thing about using this endpoint is that ingester puts itself into "Leaving" state in the ring, so it no longer receives any traffic.
At the same time Kubernetes sees the pod running and healthy, and doesn't want to stop the pod due to termination period.
After flushing is done, ingester removes itself from the ring completely, and starts reporting "unhealthy" state to Kubernetes.
At this point, scaling down the statefulset is quick operation.

## Preparation

Before starting the migration of ingesters, we need to prepare other services.

### Querier + Ruler

Everything discussed for querier applies to ruler as well, since it shares querier configuration – options prefix is "querier" even when used by ruler.

Querier (and ruler) needs to be reconfigured to query both chunks storage and blocks storage at the same time.
This is achieved by using `-querier.second-store-engine=tsdb` option, and providing querier with full blocks configuration, but keeping "primary" store set to `-store.engine=chunks`.

Querier has an option `-querier.query-store-after` to query store only if query hits data older than some period of time.
For example, if ingesters keep 12h of data in memory, there is no need to hit the store for queries that only need last 1h of data.
**During the migration, this flag needs to be modified to 0,** to make queriers always consult the store when handling queries.
The reason is that after chunks ingesters shutdown, they can no longer return chunks from memory.

Cortex querier has a [bug](https://github.com/cortexproject/cortex/issues/2935) and doesn't properly
merge streamed results from chunks and blocks-based ingesters. Instead it only returns data from blocks- instesters.
To avoid this problem, we need to temporarily disable this feature by setting `-querier.ingester-streaming=false`.
After migration is complete (i.e. all ingesters are running blocks only), this can be turned back to true, which is the default value.

### Query-frontend

Query frontend has an option `-querier.parallelise-shardable-queries` to split some incoming queries into multiple queries based on sharding factor used in v11 schema of chunk storage.
As the description implies, it only works when using chunks storage.
During and after the migration to blocks (and also after possible rollback), this option needs to be disabled otherwise query-frontend will generate queries that cannot be satisfied by blocks.

### Compactor, Store-gateway

These are new services that are only required when using blocks storage.
Compactor is needed to manage blocks generated by blocks ingesters.
Store-gateway is needed to handle requests from queriers and rulers for fetching blocks data.
It's best to start both services before migrating ingesters.

## Migration

We have developed a script available in Cortex `tools/migrate-ingester-statefulsets.sh` to migrate ingesters between two StatefulSets, shutting down ingesters one by one.

It can be used like this:

```
$ tools/migrate-ingester-statefulsets.sh namespace ingester ingester-blocks 1
```

Where parameters are:
- `namespace` = Kubernetes namespace which has ingesters running
- `ingester` = source statefulset to scale down (chunks)
- `ingester-blocks` = new statefulset to scale up (blocks)
- Last number is number of instances to scale down (in `ingester` statefulset) and scale up (in `ingester-blocks`), or "all" – which will scale down all remaining instances in `ingester` statefulset.

After starting new pod in `ingester-blocks` statefulset, script then triggers `/shutdown` endpoint by basically running something like: `kubectl exec ingester-20 wget http://localhost/shutdown`. There is `wget` (BusyBox version) command in distributed Cortex images, but no `curl`.

While request to `/shutdown` completes only after flushing has finished, it unfortunately returns 204 status code, which confuses some commonly used tools (wget, curl).
That is the reason why instead of waiting for `/shutdown` to complete, script waits for specific log messages to appear in the log file that signal start/end of data flushing.

Script expects messages that for both chunks store ("starting to flush all the chunks", "flushing of chunks complete") and blocks store ("starting to flush and ship TSDB blocks", "finished flushing and shipping TSDB blocks"), so migration is possible both ways.

When flushing is complete, scale down of statefulset continues, and process repeats.

There are few problems with the script.

- If expected messages don't appear in the log, but pod keeps on running, the script will never finish.
- Script doesn't verify that flush finished without any error.

Script works pretty well, but log files and script progress need to be monitored.

## After migration

When migration from `ingester` to `ingester-blocks` finishes, there are still two StatefulSets, with original StatefulSet (with chunks-based ingesters) having 0 instances now.

At this point, we can recreate `ingester` statefulset with final blocks configuration (eg. changing PVs), and use the script again to move pods from `ingester-blocks` to `ingester`.

Querier (and ruler) can be reconfigured to use `tsdb` as "primary" store to search, and `chunks` as secondary (`querier.second-store-engine`).
In practice querier makes no difference between primary and secondary store, except `querier.use-second-store-before-time` flag is only available for secondary store.
This flag can be set to a timestamp when migration has finished, and it avoids querying secondary store (chunks) for data when running queries that don't need data before given time.

Querier can also be configured to make use of streamed responses from ingester at this point (`-querier.ingester-streaming`).

## Rollback

If rollback to chunks is needed for any reason, it is possible to use the same migration script in reverse:
scaling down `ingester-blocks` and scaling up `ingester` statefulset, while keeping chunks configuration.
Blocks ingesters support the same `/shutdown` endpoint for flushing data, so this works fine.

During the rollback, queriers/rulers need to use the same configuration changes as during migration.
It's also a good idea to remove `querier.use-second-store-before-time`, set `querier.second-store-engine` back to `tsdb` and make `chunks` the primary engine.

After the rollback, some of the configuration changes need to stay in place, because some data has already been stored to blocks:

- query sharding must be disabled, because querying data in blocks would otherwise not work correctly
- `store-gateway` needs to keep running, to make querying blocks working
- `compactor` may be shutdown, after it has compacted all generated blocks

Resources related to `ingester-blocks` statefulset may however be fully removed after rollback.

After rollback, chunk-ingesters replayed their old Write-Ahead-Log, thus loading old chunks into memory.
WAL doesn't remember whether these old chunks were already flushed or not, so they are flushed again.
Until that flush happens, Cortex reports those chunks as unflushed, which may trigger some alerts (via `cortex_oldest_unflushed_chunk_timestamp_seconds` metric).

## Conclusion

We have successfully migrated two clusters from chunks to blocks and then back.

## Appendix: Jsonnet config

This section shows how to use [cortex-jsonnet](https://github.com/grafana/cortex-jsonnet) to configure additional services.

We will assume that `main.jsonnet` is main configuration for the cluster, that also imports `temp.jsonnet` – with our temporary configuration for migration.

In `main.jsonnet` we have something like this:

```jsonnet
local cortex = import 'cortex/cortex.libsonnet';
local wal = import 'cortex/wal.libsonnet';
local temp = import 'temp.jsonnet';

// Note that 'tsdb' is not imported here.
cortex + wal + temp {
  _images+:: (import 'images.libsonnet'),

  _config+:: {
    cluster: 'k8s-cluster',
    namespace: 'k8s-namespace',

...
```

To configure querier to use secondary store for querying, we need to add:

```
    querier_second_storage_engine: 'tsdb',
    storage_tsdb_bucket_name: 'bucket-for-storing-blocks',
```

to the `_config` object in main.jsonnet.

Let's generate blocks configuration now in `temp.jsonnet`.
There are comments inside that should give you an idea about what's happening.
Most important thing is generating resources with blocks configuration, and exposing some of them.


```jsonnet
{
  local cortex = import 'cortex/cortex.libsonnet',
  local tsdb = import 'cortex/tsdb.libsonnet',
  local rootConfig = self._config,
  local statefulSet = $.apps.v1beta1.statefulSet,

  // Prepare TSDB resources, but hide them. Cherry-picked resources will be exposed later.
  tsdb_config:: cortex + tsdb + {
    _config+:: {
      cluster: rootConfig.cluster,
      namespace: rootConfig.namespace,
      external_url: rootConfig.external_url,

      // This Cortex cluster is using the blocks storage.
      storage_tsdb_bucket_name: rootConfig.storage_tsdb_bucket_name,
      cortex_store_gateway_data_disk_size: '100Gi',
      cortex_compactor_data_disk_class: 'fast',
    },

    // We create another statefulset for ingesters here, with different name.
    ingester_blocks_statefulset: self.newIngesterStatefulSet('ingester-blocks', self.ingester_container) +
                                 statefulSet.mixin.spec.withReplicas(0),

    ingester_blocks_pdb: self.newIngesterPdb('ingester-blocks-pdb', 'ingester-blocks'),
    ingester_blocks_service: $.util.serviceFor(self.ingester_blocks_statefulset, self.ingester_service_ignored_labels),
  },

  _config+: {
    queryFrontend+: {
      // Disabled because querying blocks-data breaks if query is rewritten for sharding.
      sharded_queries_enabled: false,
    },
  },

  // Expose some services from TSDB configuration, needed for running Querier with Chunks as primary and TSDB as secondary store.
  tsdb_store_gateway_pdb: self.tsdb_config.store_gateway_pdb,
  tsdb_store_gateway_service: self.tsdb_config.store_gateway_service,
  tsdb_store_gateway_statefulset: self.tsdb_config.store_gateway_statefulset,

  tsdb_memcached_metadata: self.tsdb_config.memcached_metadata,

  tsdb_ingester_statefulset: self.tsdb_config.ingester_blocks_statefulset,
  tsdb_ingester_pdb: self.tsdb_config.ingester_blocks_pdb,
  tsdb_ingester_service: self.tsdb_config.ingester_blocks_service,

  tsdb_compactor_statefulset: self.tsdb_config.compactor_statefulset,

  // Querier and ruler configuration used during migration, and after.
  query_config_during_migration:: {
    // Disable streaming, as it is broken when querying both chunks and blocks ingesters at the same time.
    'querier.ingester-streaming': 'false',

    // query-store-after is required during migration, since new ingesters running on blocks will not load any chunks from chunks-WAL.
    // All such chunks are however flushed to the store.
    'querier.query-store-after': '0',
  },

  query_config_after_migration:: {
    'querier.ingester-streaming': 'true',
    'querier.query-ingesters-within': '13h',  // TSDB ingesters have data for up to 4d.
    'querier.query-store-after': '12h',  // Can be enabled once blocks ingesters are running for 12h.

    // Switch TSDB and chunks. TSDB is "primary" now so that we can skip querying chunks for old queries.
    // We can do this, because querier/ruler have both configurations.
    'store.engine': 'tsdb',
    'querier.second-store-engine': 'chunks',

    'querier.use-second-store-before-time': '2020-07-28T17:00:00Z',  // If migration from chunks finished around 18:10 CEST, no need to query chunk store for queries before this time.
  },

  querier_args+:: self.tsdb_config.blocks_metadata_caching_config + self.query_config_during_migration,  // + self.query_config_after_migration,
  ruler_args+:: self.tsdb_config.blocks_metadata_caching_config + self.query_config_during_migration,  // + self.query_config_after_migration,
}
```

