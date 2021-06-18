---
title: "Ingesters with WAL"
linkTitle: "Ingesters with WAL"
weight: 5
slug: ingesters-with-wal
---

**Warning: the chunks storage is deprecated. You're encouraged to use the [blocks storage](../blocks-storage/_index.md).**

By default, ingesters running with the chunks storage, store all their data in memory. If there is a crash, there could be loss of data. The Write-Ahead Log (WAL) helps fill this gap in reliability.

To use WAL, there are some changes that needs to be made in the deployment.

_This documentation refers to Cortex chunks storage engine. To understand Blocks storage please go [here](../blocks-storage/_index.md)._

## Changes to deployment

1. Since ingesters need to have the same persistent volume across restarts/rollout, all the ingesters should be run on [statefulset](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) with fixed volumes.

2. Following flags needs to be set
    * `--ingester.wal-enabled` to `true` which enables writing to WAL during ingestion.
    * `--ingester.wal-dir` to the directory where the WAL data should be stores and/or recovered from. Note that this should be on the mounted volume.
    * `--ingester.checkpoint-duration` to the interval at which checkpoints should be created. Default is `30m`, and depending on the number of series, it can be brought down to `15m` if there are less series per ingester (say 1M).
    * `--ingester.recover-from-wal` to `true` to recover data from an existing WAL. The data is recovered even if WAL is disabled and this is set to `true`. The WAL dir needs to be set for this.
        * If you are going to enable WAL, it is advisable to always set this to `true`.
    * `--ingester.tokens-file-path` should be set to the filepath where the tokens should be stored. Note that this should be on the mounted volume. Why this is required is described below.

## Changes in lifecycle when WAL is enabled

1. Flushing of data to chunk store during rollouts or scale down is disabled. This is because during a rollout of statefulset there are no ingesters that are simultaneously leaving and joining, rather the same ingester is shut down and brought back again with updated config. Hence flushing is skipped and the data is recovered from the WAL.

2. As there are no transfers between ingesters, the tokens are stored and recovered from disk between rollout/restarts. This is [not a new thing](https://github.com/cortexproject/cortex/pull/1750) but it is effective when using statefulsets.

## Disk space requirements

Based on tests in real world:

* Numbers from an ingester with 1.2M series, ~80k samples/s ingested and ~15s scrape interval.
* Checkpoint period was 20mins, so we need to scale up the number of WAL files to account for the default of 30mins. There were 87 WAL files (an upper estimate) in 20 mins.
* At any given point, we have 2 complete checkpoints present on the disk and a 2 sets of WAL files between checkpoints (and now).
* This peaks at 3 checkpoints and 3 lots of WAL momentarily, as we remove the old checkpoints.

| Observation | Disk utilisation |
|---|---|
| Size of 1 checkpoint for 1.2M series | 1410 MiB |
| Avg checkpoint size per series | 1.2 KiB |
| No. of WAL files between checkpoints (30m checkpoint) | 30 mins x 87 / 20mins = 130 |
| Size per WAL file | 32 MiB (reduced from Prometheus) |
| Total size of WAL | 4160 MiB |
| Steady state usage | 2 x 1410 MiB + 2 x 4160 MiB = ~11 GiB |
| Peak usage | 3 x 1410 MiB + 3  x 4160 MiB = ~16.3 GiB |

For 1M series at 15s scrape interval with checkpoint duration of 30m

| Usage | Disk utilisation |
|---|---|
| Steady state usage | 11 GiB / 1.2 = ~9.2 GiB |
| Peak usage | 17 GiB / 1.2 = ~13.6 GiB |

You should not target 100% disk utilisation; 70% is a safer margin, hence for a 1M active series ingester, a 20GiB disk should suffice.

## Migrating from stateless deployments

The ingester _deployment without WAL_ and _statefulset with WAL_ should be scaled down and up respectively in sync without transfer of data between them to ensure that any ingestion after migration is reliable immediately.

Let's take an example of 4 ingesters. The migration would look something like this:

1. Bring up one stateful ingester `ingester-0` and wait till it's ready (accepting read and write requests).
2. Scale down old ingester deployment to 3 and wait till the leaving ingester flushes all the data to chunk store.
3. Once that ingester has disappeared from `kc get pods ...`, add another stateful ingester and wait till it's ready. This assures not transfer. Now you have `ingester-0 ingester-1`.
4. Repeat step 2 to reduce remove another ingester from old deployment.
5. Repeat step 3 to add another stateful ingester. Now you have `ingester-0 ingester-1 ingester-2`.
6. Repeat step 4 and 5, and now you will finally have `ingester-0 ingester-1 ingester-2 ingester-3`.

## How to scale up/down

### Scale up

Scaling up is same as what you would do without WAL or statefulsets. Nothing to change here.

### Scale down

Since Kubernetes doesn't differentiate between rollout and scale down when sending a signal, the flushing of chunks is disabled by default. Hence the only thing to take care during scale down is flushing of chunks.

There are 2 ways to do it, with the latter being a fallback option.

**First option**
Consider you have 4 ingesters `ingester-0 ingester-1 ingester-2 ingester-3` and you want to scale down to 2 ingesters, the ingesters which will be shutdown according to statefulset rules are `ingester-3` and then `ingester-2`.

Hence before actually scaling down in Kubernetes, port forward those ingesters and hit the [`/shutdown`](https://github.com/cortexproject/cortex/pull/1746) endpoint. This will flush the chunks and shut down the ingesters (while also removing itself from the ring).

After hitting the endpoint for `ingester-2 ingester-3`, scale down the ingesters to 2.

PS: Given you have to scale down 1 ingester at a time, you can pipeline the shutdown and scaledown process instead of hitting shutdown endpoint for all to-be-scaled-down ingesters at the same time.

**Fallback option**

There is a `flusher` target that can be used to flush the data in the WAL. It's config can be found [here](../configuration/config-file-reference.md#flusher-config). As flusher depends on the chunk store and the http API components, you need to also set all the config related to them similar to ingesters (see [api,storage,chunk_store,limits,runtime_config](../configuration/config-file-reference.md#supported-contents-and-default-values-of-the-config-file) and [schema](schema-config.md)). Pro tip: Re-use the ingester config and set the `target` as `flusher` with additional flusher config, the irrelevant config will be ignored.

You can run it as a Kubernetes job which will:

1. Attach to the volume of the scaled down ingester.
2. Recover from the WAL.
3. And flush all the chunks.

This job is to be run for all the PVCs linked to the ingesters that you missed hitting the shutdown endpoint as a first option.

## Additional notes

* If you have lots of ingestion with the WAL replay taking a longer time, you can try reducing the checkpoint duration (`--ingester.checkpoint-duration`) to `15m`. This would require slightly higher disk bandwidth for writes (still less in absolute terms), but it will reduce the WAL replay time overall.

### Non-Kubernetes or baremetal deployments

* When the ingester restarts for any reason (upgrade, crash, etc), it should be able to attach to the same volume in order to recover back the WAL and tokens.
    * If it fails to attach to the same volume for any reason, use the [flusher](#scale-down) to flush that data.
* 2 ingesters should not be working with the same volume/directory for the WAL. It will cause data corruptions.
* Basing from above point, rollout should include bringing down an ingester completely and then starting the new ingester. Not the other way round, i.e. bringing another ingester live and taking the old one down.