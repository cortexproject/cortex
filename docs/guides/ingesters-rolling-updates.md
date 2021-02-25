---
title: "Ingesters rolling updates"
linkTitle: "Ingesters rolling updates"
weight: 10
slug: ingesters-rolling-updates
---

Cortex [ingesters](architecture.md#ingester) are semi-stateful.
A running ingester holds several hours of time series data in memory, before they're flushed to the long-term storage.
When an ingester shutdowns, because of a rolling update or maintenance, the in-memory data must not be discarded in order to avoid any data loss.

In this document we describe the techniques employed to safely handle rolling updates, based on different setups:

- [Blocks storage](#blocks-storage)
- [Chunks storage with WAL enabled](#chunks-storage-with-wal-enabled)
- [Chunks storage with WAL disabled](#chunks-storage-with-wal-disabled-hand-over)

_If you're looking how to scale up / down ingesters, please refer to the [dedicated guide](./ingesters-scaling-up-and-down.md)._

## Blocks storage

The Cortex [blocks storage](../blocks-storage/_index.md) requires ingesters to run with a persistent disk where the TSDB WAL and blocks are stored (eg. a StatefulSet when deployed on Kubernetes).

During a rolling update, the leaving ingester closes the open TSDBs, synchronize the data to disk (`fsync`) and releases the disk resources.
The new ingester, which is expected to reuse the same disk of the leaving one, will replay the TSDB WAL on startup in order to load back in memory the time series that have not been compacted into a block yet.

_The blocks storage doesn't support the series [hand-over](#chunks-storage-with-wal-disabled-hand-over)._

## Chunks storage

The Cortex chunks storage optionally supports a write-ahead log (WAL).
The rolling update procedure for a Cortex cluster running the chunks storage depends whether the WAL is enabled or not.

### Chunks storage with WAL enabled

Similarly to the blocks storage, when Cortex is running the [chunks storage](../chunks-storage/_index.md) with WAL enabled, it requires ingesters to run with a persistent disk where the WAL is stored (eg. a StatefulSet when deployed on Kubernetes).

During a rolling update, the leaving ingester closes the WAL, synchronize the data to disk (`fsync`) and releases the disk resources.
The new ingester, which is expected to reuse the same disk of the leaving one, will replay the WAL on startup in order to load back in memory the time series data.

_For more information about the WAL, please refer to [Ingesters with WAL](../chunks-storage/ingesters-with-wal.md)._

### Chunks storage with WAL disabled (hand-over)

When Cortex is running the [chunks storage](../chunks-storage/_index.md) with WAL disabled, Cortex supports on-the-fly series hand-over between a leaving ingester and a joining one.

The hand-over is based on the ingesters state stored in the ring. Each ingester could be in one of the following **states**:

- `PENDING`
- `JOINING`
- `ACTIVE`
- `LEAVING`

On startup, an ingester goes into the **`PENDING`** state.
In this state, the ingester is waiting for a hand-over from another ingester that is `LEAVING`.
If no hand-over occurs within the configured timeout period ("auto-join timeout", configurable via `-ingester.join-after` option), the ingester will join the ring with a new set of random tokens (eg. during a scale up) and will switch its state to `ACTIVE`.

When a running ingester in the **`ACTIVE`** state is notified to shutdown via `SIGINT` or `SIGTERM` Unix signal, the ingester switches to `LEAVING` state. In this state it cannot receive write requests anymore, but it can still receive read requests for series it has in memory.

A **`LEAVING`** ingester looks for a `PENDING` ingester to start a hand-over process with.
If it finds one, that ingester goes into the `JOINING` state and the leaver transfers all its in-memory data over to the joiner.
On successful transfer the leaver removes itself from the ring and exits, while the joiner changes its state to `ACTIVE`, taking over ownership of the leaver's [ring tokens](../architecture.md#hashing). As soon as the joiner switches it state to `ACTIVE`, it will start receive both write requests from distributors and queries from queriers.

If the `LEAVING` ingester does not find a `PENDING` ingester after `-ingester.max-transfer-retries` retries, it will flush all of its chunks to the long-term storage, then removes itself from the ring and exits. The chunks flushing to the storage may take several minutes to complete.

#### Higher number of series / chunks during rolling updates

During hand-over, neither the leaving nor joining ingesters will
accept new samples. Distributors are aware of this, and "spill" the
samples to the next ingester in the ring. This creates a set of extra
"spilled" series and chunks which will idle out and flush after hand-over is
complete.

#### Observability

The following metrics can be used to observe this process:

- **`cortex_member_ring_tokens_owned`**<br />
  How many tokens each ingester thinks it owns.
- **`cortex_ring_tokens_owned`**<br />
  How many tokens each ingester is seen to own by other components.
- **`cortex_ring_member_ownership_percent`**<br />
  Same as `cortex_ring_tokens_owned` but expressed as a percentage.
- **`cortex_ring_members`**<br />
  How many ingesters can be seen in each state, by other components.
- **`cortex_ingester_sent_chunks`**<br />
  Number of chunks sent by leaving ingester.
- **`cortex_ingester_received_chunks`**<br />
  Number of chunks received by joining ingester.

You can see the current state of the ring via http browser request to
`/ring` on a distributor.
