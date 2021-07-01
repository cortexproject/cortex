---
title: "Downsampling"
linkTitle: "Downsampling"
weight: 1
slug: downsampling
---

- Author: [Michael Hausenblas](https://github.com/mhausenblas)
- Reviewers:
- Date: July 2021
- Status: Proposed

## Problem Statement

Currently, Cortex stores samples raw, that is, there is no support to
decrease the resolution of the ingested samples in the write path.

There are different reasons why one may want to perform downsampling:

1. Enhancing query performance: for queries that span multiple weeks or months,
   the current setup may result in high-latency query times since. With 
   downsampling, the number of samples to process drastically decrease and
   one can expect that with this the query times for such queries improve
   significantly.
1. Reducing storage foot print: depending on how downsampling is implemented,
   on could reduce the storage space used up for blocks and indices.

With the general motivation out of the way, let's have a closer look at what
tradeoffs we might want to make and consider factors that impact the solution
space.

### Considerations

*Cardinality*: independent of the design, we expect that downsampling does not
help with cardinality explosion, as downsampling solely works along the time
dimension.

*Accuracy*: when performing downsampling, one has two options: A) keep the raw
samples around, that is, store the downsampled data alonside the raw data, or
B) discard the raw data either on ingestion or after some tim and, in the long
term, only store the downsampled data.

Going forward we will call the approach (A) comprehensive and (B) trimmed
downsampling. In case of comprehensive downsampling we can perform
high-fidelity queries (as the raw data is still available and we can "zoom in"
if necessary) with the tradeoff that the overall storage space required
increases while trimmed downsampling allows us to optimize storage usage with
the tradeoff that we can (at least for historical data), potentially not
return fine-grained results.

*Tenants*: different [tenants][tenants] may have different requirements, for 
example, one tenant may wish to opt for comprehensive downsampling and with it
accepting the higher storage costs, while another may prefer trimmed
downsampling, trading accuracy for storage foot print. The selected design for
downsampling has to take into account dependencies concerning other 
tenant-related work streams, including but not limited to
[Deletion of Tenant Data from Blocks Storage][tenant-deletion],
[Retention of Tenant Data from Blocks Storage][tenant-retention], and
[Parallel Compaction by Time Interval][parallel-compaction].

### Prior Art

Cortex is not the first Prometheus LTS solution offering downsampling. For
example, [Thanos][thanos-ds] offers a comprehensive downsampling as part of
the compactor, [M3][m3-ds] supports trimmed downsampling via the aggregator,
and [InfluxDB][influxdb-ds] allows users to perform downsampling via a
continuous query.

## Potential Solutions

Based on the considerations discussed above as well as the review of prior art
in other Prometheus ecosystem projects, there are two principled
solution approaches for Cortex one can think of. Before we get into the details
let us step back and review the write path:

```

            │
        [1] │
            │
            │
            ▼
   ┌────────────────┐     ┌────────────────┐
   │   distributor  │     │   distributor  │
   └────┬───────────┘     └────────────────┘
        │
        │
   [1]  │
        │
        ▼
 ┌────────────┐  ┌────────────┐  ┌────────────┐
 │  ingester  │  │  ingester  │  │  ingester  │
 └─────┬──────┘  └────────────┘  └────────────┘
       │
       │
       │
  [1]  │
       │
       │         ┌────────────┐
       │         │            │     [2]   ┌─────────────┐
       └───────► │   object   │ ◄─────────┤  compactor  │
                 │   store    │           └─────────────┘
                 │            │
                 └────────────┘
```

We have two ways to manipulate the blocks and indices: [1] is during the
ingestion of the samples, and [2] represents the offline case, operating on
already ingested data.

### Offline

One solution would be implementing downsampling in Cortex as part of the
compactor [2]. This would result in a design that is, at least on a high level,
aligned with Thanos and would allow both comprehensive as well as 
trimmed downsampling.

### Streaming

If one were to implement downsampling in Cortex along the write path as
hinted at in [1], either in the distributor or ingester, only trimmed
downsampling would be supported.

## Proposed Solution

We now turn our attention to the proposed solution for downsampling in Cortex.

### Requirements

* MUST support opt-in (default behavior is as without downsampling).
* MUST support tenant-level granularity (can opt-in on an per-tenant basis).
* SHOULD not have a significant impact on the write path performance.

### Concept

It seems that the offline downsampling would be the preferred solution, allowing
us to address the requirements as well as, in the mid to long run, aligning 
efforts with the Thanos project.


[tenants]: https://cortexmetrics.io/docs/guides/glossary/#tenant
[tenant-deletion]: https://cortexmetrics.io/docs/proposals/tenant-deletion/
[tenant-retention]: https://cortexmetrics.io/docs/proposals/tenant-retention/
[parallel-compaction]: https://cortexmetrics.io/docs/proposals/parallel-compaction/
[thanos-ds]: https://thanos.io/tip/components/compact.md/#downsampling
[m3-ds]: https://github.com/m3db/m3/wiki/Downsampling-with-aggregation-instead-of-compaction
[influxdb-ds]: https://docs.influxdata.com/influxdb/v2.0/process-data/common-tasks/downsample-data/
