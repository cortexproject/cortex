---
title: "Ingester WAL"
linkTitle: "Ingester WAL"
weight: 5
slug: ingester-wal
---

The ingester relies on the [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/) Write-Ahead Log (WAL) to durably record incoming samples until they are flushed as blocks to long-term storage. Cortex does not implement its own WAL — each per-tenant TSDB uses the WAL provided by the vendored TSDB package.

This page describes how the WAL is written, replayed, and truncated, and which configuration flags influence its lifecycle.

## Write path

When a push request arrives, the ingester opens a TSDB appender, appends the samples/exemplars/histograms, and commits. The fsync that persists the batch to the WAL happens inside `Appender.Commit()`. There is no additional buffering above TSDB.

Each tenant has its own TSDB and therefore its own WAL directory under the ingester's data directory.

## Replay on startup

When an ingester restarts, `tsdb.Open()` automatically replays the WAL segments to reconstruct the in-memory head. Immediately after replay, Cortex forces a compaction so any head data that has aged past the block range boundary is written out as blocks and the corresponding WAL segments become eligible for truncation. This keeps the head from staying "fat" after restart.

To speed up cold starts, set `-blocks-storage.tsdb.memory-snapshot-on-shutdown=true`. When enabled, TSDB writes a memory snapshot on graceful shutdown that is used in place of (most of) the WAL replay on the next start.

## What triggers WAL truncation

WAL truncation is **a side effect of head compaction** — the WAL only shrinks when a head block is persisted. Two systems interact:

1. **Cortex's compaction loop** decides *when* to compact each per-tenant TSDB.
2. **Prometheus TSDB's `truncateWAL`** decides *how much* WAL to remove after a compaction succeeds.

### The Cortex compaction loop

The ingester runs a compaction loop that, every `-blocks-storage.tsdb.head-compaction-interval` (default `1m`, capped at `30m`, with up to 50% jitter on the first tick), evaluates each open TSDB and calls one of:

- `db.Compact(ctx)` — regular case. Only proceeds if the TSDB head is compactable (see below).
- `db.CompactHead(...)` — forced. Used when:
  - The TSDB has been idle for longer than `-blocks-storage.tsdb.head-compaction-idle-timeout` (default `1h`, with up to 25% jitter, `0` disables it), **or**
  - An operator flush is requested, **or**
  - The ingester is shutting down gracefully.

When either call successfully persists a head block, TSDB internally invokes `truncateWAL(blockMaxT)`.

### When the head is "compactable"

The TSDB head is only considered compactable in the regular path when:

```
head.MaxTime - head.MinTime  >  chunkRange * 3 / 2
```

`chunkRange` comes from `-blocks-storage.tsdb.block-ranges-period` (default `2h`). With defaults, the head must therefore span **more than 3 hours** of *ingested sample time* before it will be compacted by the regular path. Low-volume or idle tenants rely on the **idle-timeout path** to shed their WAL — otherwise their WAL would grow unbounded.

### How much gets truncated

Given a successful head-block persist with the block's max time `mint`, TSDB does the following:

1. Compute the range of WAL segments `[first, last]` that cover data before `mint`.
2. Reduce that range: `last = first + (last-first) * 2/3`. In other words, only the **lower two-thirds** of the eligible segments are considered — the newest third is kept for safety.
3. If `last <= first`, bail out (nothing to do).
4. Write a **checkpoint** covering segments `[first .. last]`. The checkpoint condenses the still-live series/tombstones from those segments into a single new checkpoint file so they are not lost when the segments are deleted.
5. `wal.Truncate(last+1)` deletes the segments below the checkpoint.

Checkpoint and truncation are one operation, not two separate triggers.

### Summary of triggers

| Trigger | Path |
|---|---|
| Head spans more than `1.5 × block-ranges-period` and the next compaction tick fires | Regular compaction → WAL truncated up to the new block's max time |
| TSDB idle for more than `head-compaction-idle-timeout` | Forced compaction → WAL truncated |
| Operator flush (via `/ingester/flush`) | Forced compaction → WAL truncated |
| Graceful shutdown | Forced compaction → WAL truncated |
| WAL segments below the new block's max time number three or fewer | Skipped (nothing to checkpoint) |

## Configuration flags that affect WAL cadence

| Flag | Default | Effect |
|---|---|---|
| `-blocks-storage.tsdb.head-compaction-interval` | `1m` | Upper bound on how often WAL truncation can be evaluated. Max `30m`. |
| `-blocks-storage.tsdb.head-compaction-idle-timeout` | `1h` | Forces compaction (and therefore truncation) for tenants whose head is not otherwise compactable. `0` disables. |
| `-blocks-storage.tsdb.block-ranges-period` | `2h` | Sets both the block size and the `3/2 × block-range` head-compactable threshold. |
| `-blocks-storage.tsdb.wal-compression-type` | *(none)* | `snappy` or `zstd` reduces WAL size on disk; does not change truncation timing. |
| `-blocks-storage.tsdb.wal-segment-size-bytes` | TSDB default | Changes the size of each WAL segment; does not change truncation timing. |
| `-blocks-storage.tsdb.memory-snapshot-on-shutdown` | `false` | Writes an in-memory snapshot on graceful shutdown to speed up subsequent restart. |
| `-blocks-storage.tsdb.retention-period` | `6h` | **Unrelated to the WAL.** Controls how long local blocks are kept on disk after they have been shipped to object storage. |

## Persistent disk requirements

Because the WAL is the recovery mechanism for in-memory series, it must live on a disk that survives ingester restarts and pod rescheduling (for example an AWS EBS volume or GCP persistent disk). On Kubernetes, ingesters are typically deployed as a `StatefulSet` with a `PersistentVolumeClaim`. See [Ingesters rolling updates](../guides/ingesters-rolling-updates.md) for related operational guidance and [Production tips: Ingester disk space](./production-tips.md#ingester-disk-space) for sizing.

## Observability

Notable per-ingester metrics (all prefixed `cortex_ingester_tsdb_`):

- `wal_fsync_duration_seconds` — WAL fsync latency histogram.
- `wal_page_flushes_total` — count of WAL page flushes.
- `wal_truncations_total` / `wal_truncations_failed_total` — attempts and failures of the block-driven truncation described above.
- `wal_replay_unknown_refs_total` — non-fatal replay anomalies during startup.
- `wal_record_part_*` — record-part write counts and compression byte savings.
- `checkpoint_creations_total` / `checkpoint_deletions_total` (and their `_failed_total` variants) — checkpoint lifecycle.
