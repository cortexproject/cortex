---
title: "Zone Aware Replication"
linkTitle: "Zone Aware Replication"
weight: 5
slug: zone-aware-replication
---

Cortex supports data replication for different services. By default, data is transparently replicated across the whole pool of service instances, regardless of whether these instances are all running within the same availability zone (or data center, or rack) or in different ones.

It is completely possible that all the replicas for the given data are held within the same availability zone, even if the Cortex cluster spans multiple zones. Storing multiple replicas for a given data within the same availability zone poses a risk for data loss if there is an outage affecting various nodes within a zone or a full zone outage.

For this reason, Cortex optionally supports zone-aware replication. When zone-aware replication is **enabled**, replicas for the given data are guaranteed to span across different availability zones. This requires Cortex cluster to run at least in a number of zones equal to the configured replication factor.

The Cortex services supporting **zone-aware replication** are:

- **[Distributors and Ingesters](#distributors-and-ingesters-time-series-replication)**
- **[Store-gateways](#store-gateways-blocks-replication)** ([blocks storage](../blocks-storage/_index.md) only)

## Distributors / Ingesters: time-series replication

The Cortex time-series replication is used to hold multiple (typically 3) replicas of each time series in the **ingesters**.

**To enable** the zone-aware replication for the ingesters you should:

1. Configure the availability zone for each ingester via the `-ingester.availability-zone` CLI flag (or its respective YAML config option)
2. Rollout ingesters to apply the configured zone
3. Enable time-series zone-aware replication via the `-distributor.zone-awareness-enabled` CLI flag (or its respective YAML config option). Please be aware this configuration option should be set to distributors, queriers and rulers.

## Store-gateways: blocks replication

The Cortex [store-gateway](../blocks-storage/store-gateway.md) (used only when Cortex is running with the [blocks storage](../blocks-storage/_index.md)) supports blocks sharding, used to horizontally scale blocks in a large cluster without hitting any vertical scalability limit.

To enable the zone-aware replication for the store-gateways, please refer to the [store-gateway](../blocks-storage/store-gateway.md#zone-awareness) documentation.

## Minimum number of zones

For Cortex to function correctly, there must be at least the same number of availability zones as the replication factor. For example, if the replication factor is configured to 3 (default for time-series replication), the Cortex cluster should be spread at least over 3 availability zones.

It is safe to have more zones than the replication factor, but it cannot be less. Having fewer availability zones than replication factor causes a replica write to be missed, and in some cases, the write fails if the availability zones count is too low.

## Impact on unbalanced zones

**Cortex requires that each zone runs the same number of instances** of a given service for which the zone-aware replication is enabled. This guarantees a fair split of the workload across zones.

On the contrary, if zones are unbalanced, the zones with a lower number of instances would have an higher pressure on resources utilization (eg. CPU and memory) compared to zones with an higher number of instances.

## Impact on costs

Depending on the underlying infrastructure being used, deploying Cortex across multiple availability zones may cause an increase in running costs as most cloud providers charge for inter availability zone networking. The most significant change would be for a Cortex cluster currently running in a single zone.
