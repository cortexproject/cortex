---
title: "Ingesters with WAL"
linkTitle: "Ingesters with WAL"
weight: 5
slug: ingesters-with-wal
---

Currently the ingesters running in the chunks storage mode, store all their data in memory. If there is a crash, there could be loss of data. WAL helps fill this gap in reliability.

To use WAL, there are some changes that needs to be made in the deployment.

## Changes to deployment

1. Since ingesters need to have the same persistent volume across restarts/rollout, all the ingesters should be run on [statefulset](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) with fixed volumes.

2. Following flags needs to be set
    * `--ingester.wal-dir` to the directory where the WAL data should be stores and/or recovered from. Note that this should be on the mounted volume.
    * `--ingester.wal-enabled` to `true` which enables writing to WAL during ingestion.
    * `--ingester.checkpoint-enabled` to `true` to enable checkpointing of in-memory chunks to disk. This is optional which helps in speeding up the replay process.
    * `--ingester.checkpoint-duration` to the interval at which checkpoints should be created. Default is `30m`, and depending on the number of series, it can be brought down to `15m` if there are less series per ingester (say 1M).
    * `--ingester.recover-from-wal` to `true` to recover data from an existing WAL. The data is recovered even if WAL is disabled and this is set to `true`. The WAL dir needs to be set for this.
        * If you are going to enable WAL, it is advisable to always set this to `true`.
    * `--ingester.tokens-file-path` should be set to the filepath where the tokens should be stored. Note that this should be on the mounted volume. Why this is required is described below.

## Changes in lifecycle when WAL is enabled

1. Flushing of data to chunk store during rollouts or scale down is disabled. This is because during a rollout of statefulset there are no ingesters that are simultaneously leaving and joining, rather the same ingester is shut down and brought back again with updated config. Hence flushing is skipped and the data is recovered from the WAL.

2. As there are no transfers between ingesters, the tokens are stored and recovered from disk between rollout/restarts. This is [not a new thing](https://github.com/cortexproject/cortex/pull/1750) but it is effective when using statefulsets.

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

There is a [flush mode ingester](https://github.com/cortexproject/cortex/pull/1747) in progress, and with recent discussions there will be a separate target called flusher in it's place.

You can run it as a kubernetes job which will 
* Attach to the volume of the scaled down ingester
* Recover from the WAL
* And flush all the chunks. 

This job is to be run for all the ingesters that you missed hitting the shutdown endpoint as a first option.

More info about the flusher target will be added once it's upstream.