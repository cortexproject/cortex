---
title: "Migrating ingesters from chunks to blocks and back."
linkTitle: "Migrating ingesters from chunks to blocks and back."
weight: 1
slug: ingesters-migration
---

- Author: @pstibrany
- Reviewers:
- Date: June 2020
- Status: Proposed

# Migrating ingesters from chunks to blocks

This short document describes the first step in full migration of the Cortex cluster from using chunks storage to using blocks storage, specifically switching ingesters to using blocks, and modification of queriers to query both chunks and blocks storage.

## Ingesters

When switching ingesters from chunks to blocks, we need to consider the following:

- Ingesting of new data, and querying should work during the switch.
- Ingesters are rolled out with new configuration over time. There is overlap: ingesters of both kinds (chunks, blocks) are running at the same time.
- Ingesters using WAL don’t flush in-memory chunks to storage on shutdown.
- Rollout should be as automated as possible.

How do we handle ingesters with WAL? There are several possibilities, but the simplest option seems to be adding a new flag to ingesters to flush chunks on shutdown. This is trivial change to ingester, and allows us to do automated migration by:

1. Enabling this flag on each ingester (first rollout).
2. Turn off chunks, enable TSDB (second rollout). During the second rollout, as the ingester shuts down, it will flush all chunks in memory, and when it restarts, it will start using TSDB.

Benefit of this approach is that it is trivial to add the flag, and then rollout in both steps can be fully automated.

Notice that querier can ask only ingesters for most recent data and not consult the store, but during the rollout (and some time after), ingesters that are already using blocks will **not** have the most recent chunks in memory. To make sure queries work correctly, `-querier.query-store-after` needs to be set to 0, in order for queriers to not rely on ingesters only for most recent data. After couple of hours after rollout, this value can be increased again, depending on how much data ingesters keep. (`-experimental.tsdb.retention-period` for blocks, `-ingester.retain-period` for chunks)

Other alternatives considered for flushing chunks / handling WAL:

* Replay chunks-WAL into TSDB head on restart. In this scenario, chunks-ingester shuts down, and block ingester starts. It can detect existing chunks WAL, and replay it into TSDB head (and then delete old WAL). Issue here is that current chunks-WAL is quite specific to ingester code, and would require some refactoring to make this happen. Deployment is trivial: just reconfigure ingesters to start using blocks, and replay chunks WAL if found. Required change seems like a couple of days of coding work, but it is essentially only used once (for each cluster). Doesn't seem like good time investment.
* Shutdown single chunks-ingester, run flusher in its place, and when done start new blocks ingester. This is similar to the procedure we did during the introduction of WAL. Flusher can be run via initContainer support in pods. This still requires two-step deployment: 1) enable flusher and reconfigure ingesters to use blocks, 2) remove flusher.

After all ingesters are converted to blocks, we can set cut-off time for querying chunks storage on queriers.

For rollback from blocks to chunks, we need to be able to flush data from ingesters to the blocks storage, and then switch ingesters back to chunks.
Ingesters are currently not able to flush blocks to storage, but adding flush-on-shutdown option, support for `/shutdown` endpoint and support in flusher component similar to chunks is doable, and should be part of this work.

With this ability, rollback would follow the same process, just in reverse: 1) redeploy with flush flag enabled, 2) redeploy with config change from blocks to chunks.
Note that this isn't a *full* rollback to chunks-only solution, as generated blocks still need to be queried after the rollback, otherwise samples pushed to blocks would be missing.
This means running store-gateways and queriers that can query both chunks and blocks store.

Alternative plan could be to use a separate Cortex cluster configured to use blocks, and redirect incoming traffic to both chunks and blocks cluster.
When one is confident about the blocks cluster running correctly, old chunks cluster can be shutdown.
In this plan, there is an overlap where both clusters are ingesting same data.
Blocks cluster needs to be configured to be able to query chunks storage as well, with cut-off time based on when clusters were configured (at latest, to minimize amount of duplicated samples that need to be processed during queries.)

## Querying

To be able to query both blocks and chunks, querier needs to be extended with this ability, and merge results from both. Queriers can always query blocks – each querier knows about existing blocks and their timeranges, so it can quickly determine whether there are any blocks with relevant data. Always querying blocks is also useful when there is some background process converting chunks to blocks. As new blocks appear on the store, they get queried even if they are old.

For querying chunks storage, we have two options:

- Always query the chunks store – useful during ingesters switch, or after rollback from blocks to chunks.
- Query chunk store only for queries that ask for data after specific cut-off time. This is useful after all ingesters have switched, and we know the timestamp since ingesters are only writing blocks.

Querier needs to support both modes of running. While we could use runtime-config for on-the-fly switch without restarts, queriers restart quickly and so switching via configuration or command line option seems enough.

## Work to do

- Ingester: Add flags for always flushing on shutdown, even when using WAL or blocks.
- Querier: Add support for querying both chunk store and blocks at the same time and test the support for querying both chunks and blocks from ingesters works correctly
- Querier: Add cut-off time support to querier to query chunk the store only if needed, based on query time.
