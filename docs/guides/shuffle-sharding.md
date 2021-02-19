---
title: "Shuffle Sharding"
linkTitle: "Shuffle Sharding"
weight: 10
slug: shuffle-sharding
---

Cortex leverages on sharding techniques to horizontally scale both single and multi-tenant clusters beyond the capacity of a single node.

## Background

The **default sharding strategy** employed by Cortex distributes the workload across the entire pool of instances running a given service (eg. ingesters). For example, on the write path each tenant's series are sharded across all ingesters, regardless how many active series the tenant has or how many different tenants are in the cluster.

The default strategy allows to have a fair balance on the resources consumed by each instance (ie. CPU and memory) and to maximise these resources across the cluster.

However, in a **multi-tenant** cluster this approach also introduces some **downsides**:

1. An outage affects all tenants
1. A misbehaving tenant (eg. causing out of memory) could affect all other tenants

The goal of **shuffle sharding** is to provide an alternative sharding strategy to reduce the blast radius of an outage and better isolate tenants.

## What is shuffle sharding

Shuffle sharding is a technique used to isolate different tenant's workloads and to give each tenant a single-tenant experience even if they're running in a shared cluster. This technique has been publicly shared and clearly explained by AWS in their [builders' library](https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/) and a reference implementation has been shown in the [Route53 Infima library](https://github.com/awslabs/route53-infima/blob/master/src/main/java/com/amazonaws/services/route53/infima/SimpleSignatureShuffleSharder.java).

The idea is to assign each tenant a shard composed by a subset of the Cortex service instances, aiming to minimize the overlapping instances between two different tenants. Shuffle sharding brings the following **benefits** over the default sharding strategy:

- An outage on some Cortex cluster instances/nodes will only affect a subset of tenants.
- A misbehaving tenant will affect only its shard instances. Due to the low overlap of instances between different tenants, it's statistically quite likely that any other tenant will run on different instances or only a subset of instances will match the affected ones.

Shuffle sharding requires no more resources than the default sharding strategy but instances may be less evenly balanced from time to time.

### Low overlapping instances probability

For example, given a Cortex cluster running **50 ingesters** and assigning **each tenant 4** out of 50 ingesters, shuffling instances between each tenant, we get **230K possible combinations**.

Randomly picking two different tenants we have the:

- 71% chance that they will not share any instance
- 26% chance that they will share only 1 instance
- 2.7% chance that they will share 2 instances
- 0.08% chance that they will share 3 instances
- Only a 0.0004% chance that their instances will fully overlap

![Shuffle sharding probability](/images/guides/shuffle-sharding-probability.png)
<!-- Chart source at https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit -->

## Cortex shuffle sharding

Cortex currently supports shuffle sharding in the following services:

- [Ingesters](#ingesters-shuffle-sharding)
- [Query-frontend](#query-frontend-shuffle-sharding)
- [Store-gateway](#store-gateway-shuffle-sharding)
- [Ruler](#ruler-shuffle-sharding)

Shuffle sharding is **disabled by default** and needs to be explicitly enabled in the configuration.

### Guaranteed properties

The Cortex shuffle sharding implementation guarantees the following properties:

- **Stability**<br />
  Given a consistent state of the hash ring, the shuffle sharding algorithm always selects the same instances for a given tenant, even across different machines.
- **Consistency**<br />
  Adding or removing 1 instance from the hash ring leads to only 1 instance changed at most, in each tenant's shard.
- **Shuffling**<br />
  Probabilistically and for a large enough cluster, it ensures that every tenant gets a different set of instances, with a reduced number of overlapping instances between two tenants to improve failure isolation.
- **Zone-awareness**<br />
  When [zone-aware replication](./zone-replication.md) is enabled, the subset of instances selected for each tenant contains a balanced number of instances for each availability zone.

### Ingesters shuffle sharding

By default the Cortex distributor spreads the received series across all running ingesters.

When shuffle sharding is **enabled** via `-distributor.sharding-strategy=shuffle-sharding` (or its respective YAML config option), the distributor spreads each tenant series across `-distributor.ingestion-tenant-shard-size` number of ingesters.

_The shard size can be overridden on a per-tenant basis in the limits overrides configuration._

### Query-frontend shuffle sharding

By default all Cortex queriers can execute received queries for given tenant.

When shuffle sharding is **enabled** by setting `-frontend.max-queriers-per-tenant` (or its respective YAML config option) to a value higher than 0 and lower than the number of available queriers, only specified number of queriers will execute queries for single tenant. Note that this distribution happens in query-frontend, or query-scheduler if used. When using query-scheduler, `-frontend.max-queriers-per-tenant` option must be set for query-scheduler component. When not using query-frontend (with or without scheduler), this option is not available.

_The maximum number of queriers can be overridden on a per-tenant basis in the limits overrides configuration._

### Store-gateway shuffle sharding

The Cortex store-gateway -- used by the [blocks storage](../blocks-storage/_index.md) -- by default spreads each tenant's blocks across all running store-gateways.

When shuffle sharding is **enabled** via `-store-gateway.sharding-strategy=shuffle-sharding` (or its respective YAML config option), each tenant blocks will be sharded across a subset of `-store-gateway.tenant-shard-size` store-gateway instances.

_The shard size can be overridden on a per-tenant basis setting `store_gateway_tenant_shard_size` in the limits overrides configuration._

_Please check out the [store-gateway documentation](../blocks-storage/store-gateway.md) for more information about how it works._

### Ruler shuffle sharding

Cortex ruler can run in three modes:

1. **No sharding at all.** This is the most basic mode of the ruler. It is activated by using `-ruler.enable-sharding=false` (default) and works correctly only if single ruler is running. In this mode the Ruler loads all rules for all tenants.
2. **Default sharding**, activated by using `-ruler.enable-sharding=true` and `-ruler.sharding-strategy=default` (default). In this mode rulers register themselves into the ring. Each ruler will then select and evaluate only those rules that it "owns".
3. **Shuffle sharding**, activated by using `-ruler.enable-sharding=true` and `-ruler.sharding-strategy=shuffle-sharding`. Similarly to default sharding, rulers use the ring to distribute workload, but rule groups for each tenant can only be evaluated on limited number of rulers (`-ruler.tenant-shard-size`, can also be set per tenant as `ruler_tenant_shard_size` in overrides).

Note that when using sharding strategy, each rule group is evaluated by single ruler only, there is no replication.

## FAQ

### Does shuffle sharding add additional overhead to the KV store?
No, shuffle sharding subrings are computed client-side and are not stored in the ring. KV store sizing still depends primarily on the number of replicas (of any component that uses the ring, e.g. ingesters) and tokens per replica.

However, each tenant's subring is cached in memory on the client-side which may slightly increase the memory footprint of certain components (mostly the distributor).
