---
title: "Timeseries Partitioning in Compactor"
linkTitle: "Timeseries Partitioning in Compactor"
weight: 1
slug: timeseries-partitioning-in-compactor
---

- Author: @roystchiang
- Reviewers:
- Date: August 2022
- Status: Proposed


## Timeseries Partitioning in Compactor

## Introduction

The compactor is a crucial component in Cortex responsible for deduplication of replicated data, and merging blocks across multiple time intervals together. This proposal will not go into great depth with why the compactor is necessary, but aims to focus on how to scale the compactor as a tenant grows within a Cortex cluster.


## Problem and Requirements

Cortex introduced horizontally scaling compactor which allows multiple compactors to compact blocks for a single tenant, sharded by time interval. The compactor is capable of compacting multiple smaller blocks into a larger block, to reduce the duplicated information in index. The following is an illustration of how the shuffle sharding compactor works, where each arrow represents a single compaction that can be carried out independently.
![Current Implementation](/images/proposals/parallel-compaction-grouping.png)

However, if the tenant is sending unique timeseries, the compaction process does not help with reducing the index size. Furthermore, this scaling of parallelism by time interval is not sufficient for a tenant with hundreds of millions of timeseries, as more timeseries means longer compaction time.

Currently, the compactor is bounded by the 64GB index size, and having a compaction that takes days to complete simply is not sustainable. This time includes the time to download the blocks, merging of the timeseries, writing to disk, and finally uploading to object storage.

The compactor is able to compact up to 400M timeseries within 12 hours, and will fail with the error of index exceeding 64GB. Depending on the number of labels and the size of labels, one might reach the 64GB limit sooner. We need a solution that is capable of:

* handling the 64GB index limit
* reducing the overall compaction time
    * downloading the data in smaller batches
    * reducing the time required to compact

## Design

A reminder of what a Prometheus TSDB is composed of: an index and chunks. An index is a mapping of timeseries to the chunks, so we can do a direct lookup in the chunks. Each timeseries is effectively a set of labels, mapped to a list of <timestamp, value> pair. This proposal focuses on partitioning of the timeseries.

### Partitioning strategy

The compactor will compact a overlapping time-range into multiple sub-blocks, instead of a single block. Cortex can determine which partition a single timeseries should go into by applying a hash to the timeseries label, and taking the modulo of the hash by the number of partition. This guarantees that with same number of partition, the same timeseries will go into the same partition.

`partitionId = Hash(timeseries label) % number of partition`

The number of partition will be determined automatically, via a configured `multiplier`. This `multiplier` factor allows us to group just a subset of the blocks together to achieve the same deduplication factor as having all the blocks. Using a `multiplier` of 2 as an example, we can do grouping for partition of 2,  4 and 8. We’ll build on the actual number of partition determination in a later section.

### Determining overlapping blocks

In order to reduce the amount of time spent downloading blocks, and iterating through the index to filter out unrelated timeseries, we can do smart grouping of the blocks.

![Modulo Partitioning](/images/proposals/timeseries-partitioning-in-compactor-modulo-partition.png)

Given that we are always multiplying the number of partition by the `multiplier` factor, we can deduce from the modulo which partition could contain overlapping result

```
Given a hash N, if N % 8 == 7, then N % 4 must be 3
Given a hash N, if N % 8 == 3, then N % 4 must be 3
Given a hash N, if N % 8 == 4, then N % 4 must be 0
Given a hash N, if N % 8 == 0, then N % 4 must be 0
```

Hence it is safe to group blocks with `N % 8 == 7` with `N % 8 == 3 `together with `N % 4 == 3` together, and we are sure that other blocks won’t contain the same timeseries. We also know that if `N % 8 == 0`, then we don’t need to download blocks where `N % 4 == 1` or `N % 4 == 2
`
Given partition count and partition id, we can immediately find out which blocks are required. Using the above modulo example, we get the following partitiong mapping.

![Partition](/images/proposals/timeseries-partitioning-in-compactor-partitions.png)
### Planning the compaction

The shuffle sharding compactor introduced additional logic to group blocks by distinct time intervals. It can also sum up the sizes of all indices to determine how many shards are required in total. Using the above example again, and assuming that each block has an index of 30GB, the sum is 30GB * 14 = 420GB, which needs to be at least 7, since maximum index size is 64GB. Using the `multiplier` factor, it will be rounded up to 8.

Now the planner knows the resulting compaction will have 8 partitions, it can start planning out which groups of blocks can go into a single compaction group. Given that we need 8 partitions in total, the planner will go through the process above to find out what blocks are necessary. Using the above example again, but we have distinct time intervals, T1, T2, and T3. T1 has 2 partitions, T2 has 4 partitions, and T3 has 8 partitions, and we want to produce T1-T3 blocks
![Grouping](/images/proposals/timeseries-partitioning-in-compactor-grouping.png)
```
Compaction Group 1-8
T1 - Partition 1-2
T2 - Partition 1-4
T3 - Partition 1-8

Compaction Group 2-8
T1 - Partition 2-2
T2 - Partition 2-4
T3 - Partition 2-8

Compaction Group 3-8
T1 - Partition 1-2
T2 - Parittion 3-4
T3 - Partition 3-8

Compaction Group 4-8
T1 - Partition 2-2
T2 - Partition 4-4
T3 - Partition 4-8

Compaction Group 5-8
T1 - Partition 1-2
T2 - Partition 1-4
T3 - Partition 5-8

Compaction Group 6-8
T1 - Partition 2-2
T2 - Partition 2-4
T3 - Partition 6-8

Compaction Group 7-8
T1 - Partition 1-2
T2 - Partition 3-4
T3 - Partition 7-8

Compaction Group 8-8
T1 - Partition 2-2
T2 - Partition 4-4
T3 - Partition 8-8
```

`T1 - Partition 1-2` is used in multiple compaction groups, and the following section will describe how the compaction avoids duplicate timeseries in the resulting blocks

### Compaction

Now that the planner has produced a compaction plan for the T1-T3 compaction groups, the compactor can start downloading the necessary blocks. Using compaction group 1-8 from above as example.
![Grouping](/images/proposals/timeseries-partitioning-in-compactor-compact.png)
T1 - Partition 1-2 was created with hash % 2 == 0, and in order to avoid having duplication information in blocks produced by compaction group 3-8, compaction group 5-8, and compaction group 7-8, we need apply the filter the `%8 == 0` hash, as that’s the hash of the highest partition count.

## Performance

Currently a 400M timeseries takes 12 hours to compact, without taking block download into consideration. If we have a partition count of 2, we can reduce this down to 6 hours, and a partition count of 10 is 3 hours. The scaling is not linear, and I’m still attempting to find out why. The initial result is promising enough to continue though.

## Alternatives Considered

### Dynamic Number of Partition

We can also increase/decrease the number of partition without needing the `multiplier` factor. However, if a tenant is sending highly varying number of timeseries or label size, the index size can be very different, resulting in highly dynamic number of partitions. To perform deduplication, we’ll end up having to download all the sub-blocks, and it can be inefficient as less parallelization can be done, and we will spend more time downloading all the unnecessary blocks.

### Consistent Hashing

Jump consistent hash, rendezvous hashing, and other consistent hashing are great algorithms to avoid
reshuffling of data when introducing/removing partitions on the fly. However, it does not bring much of a benefit when determining which partition contains the same timeseries, which we need to deduplication of index.

### Partition by metric name

It is likely that when a query comes, a tenant is interested in an aggregation of a single metric, across all label names. The compactor can partition by metric name, so that all timeseries with the same name will go into the same block. However, this can result in very uneven partitioning.

## Architecture

### Planning

The shuffle partitioning compactor introduced a planner logic, which we can extend on. This planner is responsible for grouping the blocks together by time interval, in order to compact blocks in parallel. The grouper can also determine the number of partition by looking at the sum of index file sizes. In addition, it can also do the grouping of the sub-blocks together, so we can achieve even higher parallelism.

### Clean up

Cortex compactor cleans up obsolete source blocks by looking at a deletion marker. The current architecture does not have the problem of having a single source block involved in multiple compaction. However, this proposal is able to achieve higher parallelism than before, hence it is possible that a source block is involved multiple times. Changes needs to be made on the compactor regarding how many plans a particular blocked is involved in, and determining when a block is safe to be deleted.


## Changes Required in Dependencies

### Partitioning during compaction time

Prometheus exposes the possibility to pass in a custom [mergeFunc](https://github.com/prometheus/prometheus/blob/a1fcfe62dbe82c6292214f50ee91337566b0d61b/tsdb/compact.go#L148). This allows us to plug in the custom partitioning strategy. However, the mergeFunc is only called when the timeseries is successfully replicated to at least 3 replicas, meaning that we’ll produce duplicate timeseries across blocks if the data is only replicated once. To work around the issue, we can propose Prometheus to allow the configuration of the [MergeChunkSeriesSet](https://github.com/prometheus/prometheus/blob/a1fcfe62dbe82c6292214f50ee91337566b0d61b/tsdb/compact.go#L757).

### Source block checking

Cortex uses Thanos’s compactor logic, and it has a check to make sure the source blocks of the input blocks do not overlap. Meaning that if BlockA is produced from BlockY, and BlockB is also produced from BlockY, it will halt. This is not desirable for us, since partitioning by timeseries means the same source blocks will produce multiple blocks. Reason for having this check in Thanos is supposed to prevent having duplicate chunks, but the change was introduced without knowing whether it will actually help. We’ll need to introduce a change in Thanos to disable this check, or start using Thanos compactor as a library instead of a closed box.

## Work Plan

* Performance test the impact on query of having multiple blocks
* Get real data on the efficiency of modulo operator for partitioning
* Get the necessary changes in Prometheus approved and merged
* Get the necessary changes in Thanos approved and merged
* Implement the number of partition determination in group
* Implement the grouper logic in Cortex
* Implement the clean up logic in Cortex
* Implement the partitioning strategy in Cortex, passed to Prometheus
* Produce the partitioned blocks

## Appendix

### Risks

* Is taking the modulo of the hash sufficient to produce a good distribution of partitions?
* What’s the effect of having too many blocks for the same time range?

### Frequently Asked Questions

* Are we able to decrease the number of partition?
    * Using partitions of 2, and 4, and 8 as example

```
T1 partition 1 - Hash(timeseries label) % 2 == 0
T1 partition 2 - Hash(timeseries label) % 2 == 1

T2 partition 1 - Hash(timeseries label) % 4 == 0
T2 partition 2 - Hash(timeseries label) % 4 == 1
T2 partition 3 - Hash(timeseries label) % 4 == 2
T2 partition 4 - Hash(timeseries label) % 4 == 3

T3 partition 1 - Hash(timeseries label) % 8 == 0
T3 partition 2 - Hash(timeseries label) % 8 == 1
T3 partition 3 - Hash(timeseries label) % 8 == 2
T3 partition 4 - Hash(timeseries label) % 8 == 3
T3 partition 5 - Hash(timeseries label) % 8 == 4
T3 partition 6 - Hash(timeseries label) % 8 == 5
T3 partition 7 - Hash(timeseries label) % 8 == 6
T3 partition 8 - Hash(timeseries label) % 8 == 7

We are free to produce a resulting timerange T1-T3, without
having to download all 14 blocks in a single compactor

If T1-T3 can fit inside 4 partitions, we can do the following grouping

T1 partition 1 - Hash(timeseries label) % 2 == 0 && % 4 == 0
T2 partition 1 - Hash(timeseries label) % 4 == 0 &&
T3 partition 1 - Hash(timeseries label) % 8 == 0
T3 partition 5 - Hash(timeseries label) % 8 == 4

T1 partition 2 - Hash(timeseries label) % 2 == 1 && % 4 == 01
T2 partition 2 - Hash(timeseries label) % 4 == 1
T3 partition 2 - Hash(timeseries label) % 8 == 1
T3 partition 7 - Hash(timeseries label) % 8 == 5

T1 partition 1 - Hash(timeseries label) % 2 == 0 && % 4 == 2
T2 partition 3 - Hash(timeseries label) % 4 == 2
T3 partition 3 - Hash(timeseries label) % 8 == 2
T3 partition 7 - Hash(timeseries label) % 8 == 6

T1 partition 2 - Hash(timeseries label) % 2 == 1 && % 4 == 3
T2 partition 4 - Hash(timeseries label) % 4 == 3
T3 partition 4 - Hash(timeseries label) % 8 == 3
T3 partition 8 - Hash(timeseries label) % 8 == 7

If T1-T3 can fit inside 16 partitions, we can do the same grouping, and hash on top

T1 partition 1 - Hash(timeseries label) % 2 == 0 && % 8 == 0
T2 partition 1 - Hash(timeseries label) % 4 == 0 && % 8 == 0
T3 partition 1 - Hash(timeseries label) % 8 == 0

T1 partition 2 - Hash(timeseries label) % 2 == 1  && % 8 == 1
T2 partition 2 - Hash(timeseries label) % 4 == 1  && % 8 == 1
T3 partition 2 - Hash(timeseries label) % 8 == 1

T1 partition 1 - Hash(timeseries label) % 2 == 0 && % 8 == 2
T2 partition 3 - Hash(timeseries label) % 4 == 2 && % 8 == 2
T3 partition 3 - Hash(timeseries label) % 8 == 2

T1 partition 2 - Hash(timeseries label) % 2 == 1 && % 8 == 3
T2 partition 4 - Hash(timeseries label) % 4 == 3 && % 8 == 3
T3 partition 4 - Hash(timeseries label) % 8 == 3

T1 partition 1 - Hash(timeseries label) % 2 == 0 && % 8 == 4
T2 partition 1 - Hash(timeseries label) % 4 == 0 && % 8 == 4
T3 partition 5 - Hash(timeseries label) % 8 == 4

T1 partition 2 - Hash(timeseries label) % 2 == 1 && % 8 == 5
T2 partition 2 - Hash(timeseries label) % 4 == 1 && % 8 == 5
T3 partition 6 - Hash(timeseries label) % 8 == 5

T1 partition 1 - Hash(timeseries label) % 2 == 0 &&  % 8 == 6
T2 partition 3 - Hash(timeseries label) % 4 == 2 &&  % 8 == 6
T3 partition 7 - Hash(timeseries label) % 8 == 6

T1 partition 2 - Hash(timeseries label) % 2 == 1 && % 8 == 7
T2 partition 4 - Hash(timeseries label) % 4 == 3 && % 8 == 7
T3 partition 8 - Hash(timeseries label) % 8 == 7
```


