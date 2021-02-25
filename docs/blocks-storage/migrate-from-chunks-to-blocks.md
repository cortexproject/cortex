---
title: "Migrate Cortex cluster from chunks to blocks"
linkTitle: "Migrate Cortex cluster from chunks to blocks"
weight: 5
slug: migrate-cortex-cluster-from-chunks-to-blocks
---

This article describes how to migrate existing Cortex cluster from chunks storage to blocks storage,
and highlight possible issues you may encounter in the process.

_This document replaces the [Cortex proposal](https://cortexmetrics.io/docs/proposals/ingesters-migration/),
which was written before support for migration was in place._

## Introduction

This article **assumes** that:

- Cortex cluster is managed by Kubernetes
- Cortex is using chunks storage
- Ingesters are using WAL
- Cortex version 1.4.0 or later.

_If your ingesters are not using WAL, the documented procedure will still apply, but the presented migration script will not work properly without changes, as it assumes that ingesters are managed via StatefulSet._

The migration procedure is composed by 3 steps:

1. [Preparation](#step-1-preparation)
1. [Ingesters migration](#step-2-ingesters-migration)
1. [Cleanup](#step-3-cleanup)

_In case of any issue during or after the migration, this document also outlines a [Rollback](#rollback) strategy._

## Step 1: Preparation

Before starting the migration of ingesters, we need to prepare other services.

### Querier and Ruler

_Everything discussed for querier applies to ruler as well, since it shares querier configuration – CLI flags prefix is `-querier` even when used by ruler._

Querier and ruler need to be reconfigured as follow:

- `-querier.second-store-engine=blocks`
- `-querier.query-store-after=0`

#### `-querier.second-store-engine=blocks`

Querier (and ruler) needs to be reconfigured to query both chunks storage and blocks storage at the same time.
This is achieved by using `-querier.second-store-engine=blocks` option, and providing querier with full blocks configuration, but keeping "primary" store set to `-store.engine=chunks`.

#### `-querier.query-store-after=0`

Querier (and ruler) has an option `-querier.query-store-after` to query store only if query hits data older than some period of time.
For example, if ingesters keep 12h of data in memory, there is no need to hit the store for queries that only need last 1h of data.
During the migration, this flag needs to be set to 0, to make queriers always consult the store when handling queries.
As chunks ingesters shut down, they flush chunks to the storage. They are then replaced with new ingesters configured
to use blocks. Queriers cannot fetch recent chunks from ingesters directly (as blocks ingester don't reload chunks),
and need to use storage instead.

### Query-frontend

Query-frontend needs to be reconfigured as follow:

- `-querier.parallelise-shardable-queries=false`

#### `-querier.parallelise-shardable-queries=false`

Query frontend has an option `-querier.parallelise-shardable-queries` to split some incoming queries into multiple queries based on sharding factor used in v11 schema of chunk storage.
As the description implies, it only works when using chunks storage.
During and after the migration to blocks (and also after possible rollback), this option needs to be disabled otherwise query-frontend will generate queries that cannot be satisfied by blocks storage.

### Compactor and Store-gateway

[Compactor](./compactor.md) and [store-gateway](./store-gateway.md) services should be deployed and successfully up and running before migrating ingesters.

### Ingester – blocks

Migration script presented in Step 2 assumes that there are two StatefulSets of ingesters: existing one configured with chunks, and the new one with blocks.
New StatefulSet with blocks ingesters should have 0 replicas at the beginning of migration.

### Table-Manager - chunks

If you use a store with provisioned IO, e.g. DynamoDB, scale up the provision before starting the migration.
Each ingester will need to flush all chunks before exiting, so will write to the store at many times the normal rate.

Stop or reconfigure the table-manager to stop it adjusting the provision back to normal.
(Don't do the migration on Wednesday night when a new weekly table might be required.)

## Step 2: Ingesters migration

We have developed a script available in Cortex [`tools/migrate-ingester-statefulsets.sh`](https://github.com/cortexproject/cortex/blob/master/tools/migrate-ingester-statefulsets.sh) to migrate ingesters between two StatefulSets, shutting down ingesters one by one.

It can be used like this:

```
$ tools/migrate-ingester-statefulsets.sh <namespace> <ingester-old> <ingester-new> <num-instances>
```

Where parameters are:
- `<namespace>`: Kubernetes namespace where the Cortex cluster is running
- `<ingester-old>`: name of the ingesters StatefulSet to scale down (running chunks storage)
- `<ingester-new>`: name of the ingesters StatefulSet to scale up (running blocks storage)
- `<num-instances>`: number of instances to scale down (in `ingester-old` statefulset) and scale up (in `ingester-new`), or "all" – which will scale down all remaining instances in `ingester-old` statefulset

After starting new pod in `ingester-new` statefulset, script then triggers `/shutdown` endpoint on the old ingester. When the flushing on the old ingester is complete, scale down of statefulset continues, and process repeats.

_The script supports both migration from chunks to blocks, and viceversa (eg. rollback)._

### Known issues

There are few known issues with the script:

- If expected messages don't appear in the log, but pod keeps on running, the script will never finish.
- Script doesn't verify that flush finished without any error.

## Step 3: Cleanup

When the ingesters migration finishes, there are still two StatefulSets, with original StatefulSet (running the chunks storage) having 0 instances now.

At this point, we can delete the old StatefulSet and its persistent volumes and recreate it with final blocks storage configuration (eg. changing PVs), and use the script again to move pods from `ingester-blocks` to `ingester`.

Querier (and ruler) can be reconfigured to use `blocks` as "primary" store to search, and `chunks` as secondary:

- `-store.engine=blocks`
- `-querier.second-store-engine=chunks`
- `-querier.use-second-store-before-time=<timestamp after ingesters migration has completed>`
- `-querier.ingester-streaming=true`

#### `-querier.use-second-store-before-time`

The CLI flag `-querier.use-second-store-before-time` (or its respective YAML config option) is only available for secondary store.
This flag can be set to a timestamp when migration has finished, and it avoids querying secondary store (chunks) for data when running queries that don't need data before given time.

Both primary and secondary stores are queried before this time, so the overlap where some data is in chunks and some in blocks is covered.

## Rollback

If rollback to chunks is needed for any reason, it is possible to use the same migration script with reversed arguments:

- Scale down ingesters StatefulSet running blocks storage
- Scale up ingesters StatefulSet running chunks storage

_Blocks ingesters support the same `/shutdown` endpoint for flushing data._

During the rollback, queriers and rulers need to use the same configuration changes as during migration. You should also make sure the following settings are applied:

- `-store.engine=chunks`
- `-querier.second-store-engine=blocks`
- `-querier.use-second-store-before-time` should not be set
- `-querier.ingester-streaming=false`

Once the rollback is complete, some configuration changes need to stay in place, because some data has already been stored to blocks:

- The query sharding in the query-frontend must be kept disabled, otherwise querying blocks will not work correctly
- `store-gateway` needs to keep running, otherwise querying blocks will fail
- `compactor` may be shutdown, after it has no more compaction work to do

Kubernetes resources related to the ingesters running the blocks storage may be deleted.

### Known issues

After rollback, chunks ingesters will replay their old Write-Ahead-Log, thus loading old chunks into memory.
WAL doesn't remember whether these old chunks were already flushed or not, so they will be flushed again to the storage.
Until that flush happens, Cortex reports those chunks as unflushed, which may trigger some alerts based on `cortex_oldest_unflushed_chunk_timestamp_seconds` metric.

## Appendix

### Jsonnet config

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
    querier_second_storage_engine: 'blocks',
    blocks_storage_bucket_name: 'bucket-for-storing-blocks',
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
    'store.engine': 'blocks',
    'querier.second-store-engine': 'chunks',

    'querier.use-second-store-before-time': '2020-07-28T17:00:00Z',  // If migration from chunks finished around 18:10 CEST, no need to query chunk store for queries before this time.
  },

  querier_args+:: self.tsdb_config.blocks_metadata_caching_config + self.query_config_during_migration,  // + self.query_config_after_migration,
  ruler_args+:: self.tsdb_config.blocks_metadata_caching_config + self.query_config_during_migration,  // + self.query_config_after_migration,
}
```
