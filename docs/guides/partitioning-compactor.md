---
title: "Use Partition Compaction in Cortex"
linkTitle: "Partition Compaction"
weight: 10
slug: partition-compaction
---

## Context

Compactor is bonded by maximum 64GB of index file size. If compaction failed due to exceeding index file size limit, partition compaction can be enabled to allow compactor compacting into multiple blocks that have index file size stays within limit.

## Enable Partition Compaction

In order to enable partition compaction, the following flag needs to be set:

```
-compactor.sharding-enabled=true
-compactor.sharding-strategy=shuffle-sharding
-compactor.compaction-strategy=partitioning
```

## Configure Partition Compaction

By default, partition compaction utilizes the following configurations and their values:

```
-compactor.partition-index-size-bytes=68719476736 // 64GB
-compactor.partition-series-count=0 // no limit
```

The default value should start partitioning result blocks when sum of index files size of parent blocks exceeds 64GB. End user could also change those two configurations. Partition compaction would always calculate partition count based on both configuration and pick the one with higher partition count.

Both configurations support to be set per tenant.

Note: `compactor.partition-series-count` is using sum of series count of all parent blocks. If parent blocks were not deduped, the result block could have fewer series than the configuration value. 

## Useful Metrics

- `cortex_compactor_group_partition_count`: can be used to keep track of how many partitions being compacted for each time range.
- `cortex_compactor_group_compactions_not_planned_total`: can be used to alarm any compaction was failed to be planned due to error.
- `cortex_compact_group_compaction_duration_seconds`: can be used to monitor compaction duration or each time range compactions.
- `cortex_compactor_oldest_partition_offset`: can be used to monitor when was the oldest compaction that is still not completed.
