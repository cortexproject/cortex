---
title: "Downsampling"
linkTitle: "Downsampling"
weight: 1
slug: downsampling
---

- Author: [Michael Hausenblas](https://github.com/mhausenblas)
- Reviewers:
- Date: September 2021
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
B) discard the raw data either on ingestion or after some time and, in the long
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

Concerning the sampling tiers we want to support the following time periods
(note that the resolutions below are aligned with what Thanos [uses][thanos-ds-res]
whereas the default for the time periods are, on a per-tenant basis, proposed
as is):

1. Data up to one week raw (not downsampled).
1. Data older than one week up to a month downsampled to 5m resolution.
1. Data older than a month downsampled to 1h resolution.

### Concept

Offline downsampling is the preferred solution, allowing
us to address the requirements as well as, in the mid to long run, aligning 
efforts with the Thanos project.

There are two options to realize downsampling we can consider:

1. Making the process part of the compactor. This option has the advantage that
   it would align with Thanos, but has the potential to be operationally more
   complicated to use and troubleshoot.
1. Introducing a new component dedicated to downsampling. This option is more
   flexible and operationally simpler, however increases the overall number
   of components Cortex has.

There is general agreement to align with the Thanos-based implementation of
downsampling, with the following implications:

* Using three resolutions (raw, @5min, @1h) in an additive manner, that is,
  new blocks are generated.
* Making it part of the compactor on an opt-in basis for each tenant.

The implementation would do the following:

#### Configuration

In `github.com/cortexproject/cortex/pkg/compactor.Config` ([source][src-compactor-config])
we add the respective retention periods per resolution:

```go
type Config struct {
        ...
        // Downsampling retention periods.
        RetentionRaw    model.Duration  `yaml:"retention_raw"`
        Retention5Mins  model.Duration  `yaml:"retention_5mins"`
        Retention1Hour  model.Duration  `yaml:"retention_1h"`
        ...
}
```

Note: above uses `model.Duration` from [Prometheus common][src-prom-common-duration]
as the case in Thanos.

#### Write path

In `github.com/cortexproject/cortex/pkg/compactor.compactUsers()` ([source][src-compactor-cu])
we add the business logic from `github.com/thanos-io/thanos/pkg/compact/downsample` 
([source][src-thanos-compactor-ds]).


#### Query path

##### Research

In Thanos' query path, downsampled data is taken into account as follows:

1. In `github.com/thanos-io/thanos/pkg/api/v1/QueryAPI.parseDownsamplingParamMillis()`
   ([source][src-thanos-queryapi]) the parsing resolution is determined (`max_resolution` parameter).
1. Next, in `github.com/thanos-io/thanos/pkg/query/querier.selectFn()` ([source][src-thanos-querier]),
   in the context of the PromQL query parsing and evaluation, data from storage
   is selected with the querier taking hints about what functions are used, informing
   what downsampling aggregations will be used, if at all (cf. `aggrsFromFunc(hints.Func)`).
1. Then, still in `github.com/thanos-io/thanos/pkg/query/querier.selectFn()` ([source][src-thanos-querier]),
   it fans out to further store APIs with the aggregations request (cf
   `q.proxy.Series()`, first to proxy (fanout) component, then it passes it on to 
   the underlying components such as the Store Gateway. The aggregation is best effort,
   that is, if a component has downsampled data, it returns this, however if it
   only has raw data, that will be returned.
1. Finally, only the Store Gateway supports downsampled data: in  
   `github.com/thanos-io/thanos/pkg/store/BucketStore.Series()` ([source][src-thanos-bucket])
   the blocks are chosen (cf. `bs.getFor()`) and in case of downsampled data,
   hints/aggregations from the requests are used to fetch proper chunks.


[tenants]: https://cortexmetrics.io/docs/guides/glossary/#tenant
[tenant-deletion]: https://cortexmetrics.io/docs/proposals/tenant-deletion/
[tenant-retention]: https://cortexmetrics.io/docs/proposals/tenant-retention/
[parallel-compaction]: https://cortexmetrics.io/docs/proposals/parallel-compaction/
[thanos-ds]: https://thanos.io/tip/components/compact.md/#downsampling
[thanos-ds-res]: https://github.com/thanos-io/thanos/blob/main/docs/components/compact.md#downsampling
[m3-ds]: https://github.com/m3db/m3/wiki/Downsampling-with-aggregation-instead-of-compaction
[influxdb-ds]: https://docs.influxdata.com/influxdb/v2.0/process-data/common-tasks/downsample-data/
[src-compactor-config]: https://github.com/cortexproject/cortex/blob/df9af3a999548e15fe44d807a96f7b74a3cfd9de/pkg/compactor/compactor.go#L91
[src-prom-common-duration]: https://github.com/prometheus/common/blob/8d1c9f84e3f78cb628a20f8e7be531c508237848/model/time.go#L172
[src-compactor-cu]: https://github.com/cortexproject/cortex/blob/df9af3a999548e15fe44d807a96f7b74a3cfd9de/pkg/compactor/compactor.go#L595
[src-thanos-compactor-ds]: https://github.com/thanos-io/thanos/blob/main/pkg/compact/downsample/downsample.go
[src-thanos-queryapi]: https://github.com/thanos-io/thanos/blob/8862ad53f63b12f6d642ecd8633f369be84889e8/pkg/api/query/v1.go#L244
[src-thanos-querier]: https://github.com/thanos-io/thanos/blob/07b377f47229c853290fcb9520a05696376d870d/pkg/query/querier.go#L258
[src-thanos-bucket]: https://github.com/thanos-io/thanos/blob/4acc744582a40deb76e22d7cea775f810e789c3c/pkg/store/bucket.go#L998
