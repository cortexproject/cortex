---
title: "Use Partition Compaction in Cortex"
linkTitle: "Partition Compaction"
weight: 10
slug: partition-compaction
---

## Context

Compactor is bounded by maximum 64GB of index file size. If compaction failed due to exceeding index file size limit, partition compaction can be enabled to allow compactor compacting into multiple blocks that have index file size stays within limit.

## Enable Partition Compaction

In order to enable partition compaction, the following flag needs to be set:

```
-compactor.sharding-enabled=true                # Enable sharding tenants across multiple compactor instances. This is required to enable partition compaction
-compactor.sharding-strategy=shuffle-sharding   # Use Shuffle Sharding as sharding strategy. This is required to enable partition compaction
-compactor.compaction-strategy=partitioning     # Use Partition Compaction as compaction strategy. To turn if off, set it to `default`
```

### Migration

There is no special migration process needed to enable partition compaction. End user could enable it by setting the above configurations all at once.

Enabling partition compaction would group previously compacted blocks (only those have time range smaller than the largest configured compaction time ranges) with uncompacted blocks and generate new compaction plans. This would group blocks having duplicated series together and those series would be deduped after compaction.

Disabling partition compaction after enabled it does not need migration either. After disabling partition compaction, compactor would group partitioned result blocks together and compact them into one block.

## Configure Partition Compaction

By default, partition compaction utilizes the following configurations and their values:

```
-compactor.partition-index-size-bytes=68719476736   # 64GB
-compactor.partition-series-count=0                 # no limit
```

The default value should start partitioning result blocks when sum of index files size of parent blocks exceeds 64GB. End user could also change those two configurations. Partition compaction would always calculate partition count based on both configuration and pick the one with higher partition count.

Both configurations support to be set per tenant.

Note: `compactor.partition-series-count` is using sum of series count of all parent blocks. If parent blocks were not deduped, the result block could have fewer series than the configuration value.

## Useful Metrics

- `cortex_compactor_group_partition_count`: can be used to keep track of how many partitions being compacted for each time range.
- `cortex_compactor_group_compactions_not_planned_total`: can be used to alarm any compaction was failed to be planned due to error.
- `cortex_compact_group_compaction_duration_seconds`: can be used to monitor compaction duration of each time range compactions.
- `cortex_compactor_oldest_partition_offset`: can be used to monitor when was the oldest compaction that is still not completed.
