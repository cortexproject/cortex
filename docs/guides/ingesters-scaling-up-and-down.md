---
title: "Ingesters scaling up and down"
linkTitle: "Ingesters scaling up and down"
weight: 10
slug: ingesters-scaling-up-and-down
---

This guide explains how to scale up and down ingesters.

If you're looking for how to run ingesters rolling updates, please refer to the [dedicated guide](./ingesters-rolling-updates.md)._

## Scaling up

Adding more ingesters to a Cortex cluster is considered a safe operation. When a new ingester starts, it will register to the [hash ring](../architecture.md#the-hash-ring) and the distributors will reshard received series accordingly.
Ingesters that were previously receiving those series will see data stop arriving and will consider those series "idle".

If you run with `-distributor.shard-by-all-labels=false` (the default), before adding a second ingester, you have to wait until data has migrated from idle series to the back-end store; otherwise, you will see gaps in queries. This will happen after the next "head compaction" (typically every 2 hours).
If you have set `-querier.query-store-after`, then that is also a minimum time you have to wait before adding a second ingester.

If you run with `-distributor.shard-by-all-labels=true`,
no special care is required to take when scaling up ingesters.

## Scaling down

A running ingester holds several hours of time series data in memory before they’re flushed to the long-term storage. When an ingester shuts down because of a scale down operation, the in-memory data must not be discarded in order to avoid any data loss.

Ingesters don’t flush series to blocks at shutdown by default. However, Cortex ingesters expose an API endpoint [`/shutdown`](../api/_index.md#shutdown) that can be called to flush series to blocks and upload blocks to the long-term storage before the ingester terminates.

Even if ingester blocks are compacted and shipped to the storage at shutdown, it takes some time for queriers and store-gateways to discover the newly uploaded blocks. This is due to the fact that the blocks storage runs a periodic scanning of the storage bucket to discover blocks. If two or more ingesters are scaled down in a short period of time, queriers may miss some data at query time due to series that were stored in the terminated ingesters but their blocks haven’t been discovered yet.

### New Gradual Scaling Approach (Recommended)

Starting with Cortex 1.19.0, a new **READONLY** state for ingesters was introduced that enables gradual, safe scaling down without data loss or performance impact. This approach eliminates the need for complex configuration changes and allows for more flexible scaling operations.

#### How the READONLY State Works

The READONLY state allows ingesters to:
- **Stop accepting new writes** - Push requests will be rejected and redistributed to other ingesters
- **Continue serving queries** - Existing data remains available for queries, maintaining performance
- **Gradually age out data** - As time passes, data naturally ages out according to your retention settings
- **Be safely removed** - Once data has aged out, ingesters can be terminated without any impact

#### Step-by-Step Scaling Process

1. **Set ingesters to READONLY mode**
   ```bash
   # Transition ingester to READONLY state
   curl -X POST http://ingester-1:8080/ingester/mode -d '{"mode": "READONLY"}'
   curl -X POST http://ingester-2:8080/ingester/mode -d '{"mode": "READONLY"}'
   curl -X POST http://ingester-3:8080/ingester/mode -d '{"mode": "READONLY"}'
   ```

2. **Monitor data aging** (Optional but recommended)
   ```bash
   # Check user statistics and loaded blocks on the ingester
   curl http://ingester-1:8080/ingester/all_user_stats
   ```

3. **Wait for safe removal window**
   - **Immediate removal** (after step 1): Safe once queries no longer need the ingester's data
   - **Conservative approach**: Wait for `querier.query-ingesters-within` duration (e.g., 5 hours)
   - **Complete data aging**: Wait for full retention period to ensure all blocks are removed

4. **Remove ingesters**
   ```bash
   # Terminate the ingester processes
   kubectl delete pod ingester-1 ingester-2 ingester-3
   ```

#### Timeline Example

For a cluster with `querier.query-ingesters-within=5h`:

- **T0**: Set ingesters 5, 6, 7 to READONLY state
- **T1**: Ingesters stop receiving new data but continue serving queries
- **T2 (T0 + 5h)**: Ingesters no longer receive query requests (safe to remove)
- **T3 (T0 + retention_period)**: All blocks naturally removed from ingesters
- **T4**: Remove ingesters from cluster

**Any time after T2 is safe for removal without service impact.**

### Legacy Approach (For Older Versions)

If you're running an older version of Cortex that doesn't support the READONLY state, you'll need to follow the legacy approach.

The ingesters scale down is deemed an infrequent operation and no automation is currently provided. However, if you need to scale down ingesters, please be aware of the following:

- Configure queriers and rulers to always query the storage
  - `-querier.query-store-after=0s`
- Frequently scan the storage bucket
  - `-blocks-storage.bucket-store.sync-interval=5m`
  - `-compactor.cleanup-interval=5m`
- Lower bucket scanning cache TTLs
  - `-blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl=1m`
  - `-blocks-storage.bucket-store.metadata-cache.tenant-blocks-list-ttl=1m`
  - `-blocks-storage.bucket-store.metadata-cache.metafile-doesnt-exist-ttl=1m`
- Ingesters should be scaled down one by one:
  1. Call `/shutdown` endpoint on the ingester to shutdown
  2. Wait until the HTTP call returns successfully or "finished flushing and shipping TSDB blocks" is logged
  3. Terminate the ingester process (the `/shutdown` will not do it)
  4. Before proceeding to the next ingester, wait 2x the maximum between `-blocks-storage.bucket-store.sync-interval` and `-compactor.cleanup-interval`

