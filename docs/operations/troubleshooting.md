---
title: "Troubleshooting Cortex"
linkTitle: "Troubleshooting"
weight: 2
slug: troubleshooting
---

A decision tree for the most common production issues. Each section starts with
the symptom an operator sees, names the metrics and logs to inspect, and points
to the upstream fix.

The [bundled dashboards and alerts]({{< relref "./monitoring-cortex.md" >}})
surface most of the signals referenced below. Install them first if you have
not already.

## Write path

### Distributors return 5xx on `/api/v1/push`

1. **Confirm where the error originates.** Distributor logs include the cause:
   ingester unreachable, rate-limit exceeded, validation error. Filter for
   `level=warn` and `level=error` on the distributor.
2. **Check ingester health on the ring page** (`/ring` on any distributor). All
   ingesters should be in state `ACTIVE`. `UNHEALTHY` or missing ingesters
   point at a partition between distributor and ingester, or at the KV store.
3. **Check the `CortexIngesterUnhealthy` alert.** If it is firing, follow it:
   the offending ingester is in the alert's labels.
4. **Inspect `cortex_distributor_ingester_append_failures_total`.** A non-zero
   rate that matches the 5xx rate confirms ingester-side rejection.

If the cause is `per-user limit exceeded`, raise the limit in `runtime_config`
([Overrides]({{< relref "../guides/overrides.md" >}})) rather than scaling out.

### Samples are accepted but never appear in queries

1. **Verify the tenant header.** The push and the query must use the same
   `X-Scope-OrgID`. The single most common cause of "missing data" is a
   tenant-ID mismatch.
2. **Check `cortex_ingester_memory_series` on the receiving ingester.** If
   non-zero for the tenant, the data is in memory and queries should see it.
3. **Confirm time-range overlap.** Ingesters serve recent data from the TSDB
   head and from local on-disk blocks until they age out per
   `-blocks-storage.tsdb.retention-period` (default `6h`). Queriers stop
   consulting ingesters entirely for time ranges older than
   `-limits.query-ingesters-within` (per-tenant, when set). Older data must
   have been shipped and must be visible to the store-gateway via the bucket
   index — check `cortex_ingester_shipper_uploads_total`, the
   `CortexIngesterHasNotShippedBlocks` alert, and
   `CortexBucketIndexNotUpdated`.

### Distributor `inflight push requests` rejected

The `CortexDistributorReachingInflightPushRequestLimit` alert fires when
distributors near `-distributor.instance-limits.max-inflight-push-requests`.
Either scale distributors horizontally or raise the limit if CPU and memory
have headroom.

## Read path

### Queries time out at the frontend

1. **Look at `cortex-reads.json` and `cortex-slow-queries.json`.** They show
   queue depth, per-step latency, and the offending PromQL.
2. **If the frontend queue is full** (`CortexFrontendQueriesStuck` or
   `CortexSchedulerQueriesStuck`): there are not enough queriers, or queriers
   are blocked on something downstream. Check querier CPU, then ingester and
   store-gateway latency.
3. **If the queue is empty but queries are still slow:** the bottleneck is in
   the querier or below. Look at chunks fetched per query and bytes scanned —
   an expensive query may need the protections in [Protecting Cortex from
   Heavy Queries]({{< relref "../guides/protecting-cortex-from-heavy-queries.md"
   >}}).

### Queries return partial or no data for old time ranges

Old data lives in object storage and is served by the store-gateway. Check:

- `CortexStoreGatewayHasNotSyncTheBucket` — a stale store-gateway will not see
  recently uploaded blocks.
- `CortexBucketIndexNotUpdated` — the compactor maintains the bucket index;
  querier and store-gateway use it to discover blocks.
- `CortexQuerierHighRefetchRate` — symptom of store-gateways missing blocks
  the querier expected to find.

### Queries return incorrect results

`CortexQueriesIncorrect` fires when the same query, run through the query-tee
against two backends, disagrees. Cortex ships a [Query
Auditor]({{< relref "./query-auditor.md" >}}) for this case; pair it with the
[Query Tee]({{< relref "./query-tee.md" >}}) to bisect which deployment is
wrong.

## Storage path

### Ingester is not shipping blocks

The `CortexIngesterHasNotShippedBlocks` and `CortexIngesterHasUnshippedBlocks`
alerts catch this. Common causes:

- Object-store credentials misconfigured — see distributor and ingester logs
  for `403`/`AccessDenied`.
- A new block has not been cut yet. Ingesters cut blocks every
  `-blocks-storage.tsdb.block-ranges-period` (default `2h`); a recently
  started ingester has nothing to ship until the first block-range elapses.
- Disk pressure: check `cortex_ingester_tsdb_*` metrics and pod disk usage.

### TSDB head compaction or WAL errors

`CortexIngesterTSDBHeadCompactionFailed`, `CortexIngesterTSDBWALCorrupted`, and
`CortexIngesterTSDBWALWritesFailed` indicate disk-level problems. Treat the
affected ingester as a failed replica: cordon it, let traffic move to the
other replicas in the ring, then restore from a healthy ingester or replay
the WAL on a fresh volume. Do **not** restart in place if the WAL is corrupt —
you will lose the in-memory series.

### Compactor falls behind

`CortexCompactorHasNotSuccessfullyRunCompaction` means recent blocks are
piling up and queries will get slower over time. Check:

- Compactor CPU and memory headroom — compaction is CPU-bound.
- Object-store latency on the compactor (it does a lot of small reads/writes).
- The `cortex-compactor.json` dashboard for per-tenant progress.

See [Partitioning Compactor]({{< relref "../guides/partitioning-compactor.md"
>}}) for scaling out.

## Hash ring and KV store

### `CortexKVStoreFailure` is firing

The component named in the alert cannot reach the KV store backend (Consul,
etcd, or memberlist). Steps:

1. From an affected pod, hit the KV backend's health endpoint directly.
2. If the backend is up, look for network policy or DNS changes since the alert
   started.
3. With memberlist, check `cortex_memberlist_client_messages_received_total`
   and `cortex_memberlist_client_messages_sent_total` on each pod; a partition
   shows up as one-sided traffic.

### Ingesters keep joining and leaving the ring

`CortexGossipMembersMismatch` indicates members disagree on cluster membership.
This is almost always a misconfigured `join_members:` list (some pods do not
list a bootstrap peer that resolves) or a packet-loss issue between zones.
[Gossip Ring Getting Started]({{< relref "../guides/gossip-ring-getting-started.md"
>}}) walks through the canonical configuration.

## Alertmanager

`CortexAlertmanagerSyncConfigsFailing`, `CortexAlertmanagerReplicationFailing`,
and the `*Persist*` / `*InitialSync*` alerts trace to the Alertmanager's
storage backend or its peer replication. Inspect the alertmanager logs for the
specific operation that failed; the alert annotations include the storage
endpoint that returned the error.

## Ruler

A spike in `CortexRulerMissedEvaluations` typically means a ruler tenant has
too many rules for the assigned shards. Either shard more aggressively (see
[Sharded Ruler]({{< relref "../guides/sharded_ruler.md" >}})) or move
heavy-evaluation tenants to the
[query-frontend-backed rule evaluation path]({{< relref
"../guides/rule-evaluations-via-query-frontend.md" >}}) so they share the
query path's capacity rather than the ruler's local one.

## Multi-tenant noisy-neighbour

If one tenant is degrading the cluster for everyone:

1. Use `cortex-queries.json` filtered by tenant to confirm the source.
2. Apply tenant-specific limits via `runtime_config` ([Overrides]({{< relref
   "../guides/overrides.md" >}})). Limits take effect within seconds — no
   restart needed.
3. For longer-term isolation, move the tenant to its own shuffle shard
   ([Shuffle Sharding]({{< relref "../guides/shuffle-sharding.md" >}})).

## When the answer isn't here

- Search recent CHANGELOG entries for the component you suspect — many subtle
  bugs are documented there before they show up in an issue.
- Check [GitHub issues](https://github.com/cortexproject/cortex/issues) for the
  alert name or error string; production issues are frequently filed verbatim.
- Ask in the
  [#cortex Slack channel](https://cloud-native.slack.com/messages/cortex) with
  the alert name, the dashboard timeframe, and a relevant log line.
