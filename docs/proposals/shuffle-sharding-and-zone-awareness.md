---
title: "Shuffle sharding and zone awareness"
linkTitle: "Shuffle sharding and zone awareness"
weight: 1
slug: shuffle-sharding-and-zone-awareness
---

- Author: @pracucci, @tomwilkie, @pstibrany
- Reviewers:
- Date: August 2020
- Status: Accepted, implemented in [PR #3090](https://github.com/cortexproject/cortex/pull/3090)

## Shuffle sharding and zone awareness


### Background

Cortex shards the received series across all available ingesters. In a multi-tenant cluster, each tenant series are sharded across all ingesters. This allows to horizontally scale the series across the pool of ingesters but also suffers some issues:

1. Given every tenant writes series to all ingesters, there’s no isolation between tenants - a single misbehaving tenant can affect the whole cluster.
2. Each ingester needs an open TSDB per tenant per ingester - which has significant memory overhead. The larger the number of tenants, the higher the TSDB memory overhead, regardless of the number of series stored in each TSDB.
3. Similarly, the number of uploaded blocks to the storage every 2 hours is a function of the number of TSDBs open for each ingester. A cluster with a large number of small tenants will upload a very large number of blocks to the storage, each block being very small, increasing the number of API calls against the storage bucket.

Cortex currently supports sharding a tenant to a subset of the ingesters on the write path [PR](https://github.com/cortexproject/cortex/pull/1947), using a feature called “**subring**”. However, the current subring implementation suffers two issues:

1. **No zone awareness:** it doesn’t guarantee selected instances are balanced across availability zones
2. **No shuffling:** the implementation is based on the hash ring and it selects N consecutive instances in the ring. This means that, instead of minimizing the likelihood that two tenants share the same instances, it emphasises it. In order to provide a good isolation between tenants, we want to minimize the chances that two tenants share the same instances.

### Goal

The goal of this work is to fix “shuffling” and “zone-awareness” when building the subring for a given tenant, honoring the following properties:

- **Stability:** given the same ring, the algorithm always generates the same subring for a given tenant, even across different machines
- [**Consistency:**](https://en.wikipedia.org/wiki/Consistent_hashing) when the ring is resized, only n/m series are remapped on average (where n is the number of series and m is the number of replicas).
- **Shuffling:** probabilistically and for a large enough cluster, ensure every tenant gets a different set of instances, with a reduced number of overlapping instances between two tenants to improve failure isolation.
- **Zone-awareness (balanced):** the subring built for each tenant contains a balanced number of instances for each availability zone. Selecting the same number of instances in each zone is an important property because we want to preserve the balance of in-memory series across ingesters. Having less replicas in one zone will mean more load per node in this zone, which is something we want to avoid.

### Proposal

This proposal is based on [Amazon’s Shuffle Sharding article](https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/) and the algorithm has been inspired by shuffle sharding implementation in the [AWS Route53 infima library](https://github.com/awslabs/route53-infima/blob/master/src/main/java/com/amazonaws/services/route53/infima/SimpleSignatureShuffleSharder.java).

Given a tenant and a shard size S (number of instances to which tenant data/workload should be sharded to), we build a subring selecting N instances from each zone, where N = ceil(S / num of zones). The shard size S is required to be a multiple of the number of zones, in order to select an equal number of instances from each zone.

To do it, we **treat each zone as a separate ring** and select N unique instances from each zone. The instances selection process works as follow:

1. Generate a seed based on the tenant ID
2. Initialise a pseudo random number generator with the tenant’s seed. The random generator must guarantee predictable numbers given the same input seed.
3. Generate a sequence of N random numbers, where N is the number of instances to select from the zone. Each random number is used as a “token” to look up instances in the ring. For each random number:
  1. Lookup the instance holding that token in the ring
  2. If the instance has not been previously selected, then pick it
  3. If the instance was previously selected (we call this a “collision”), then continue walking the ring clockwise until we find an instance which has not been selected yet

### Guaranteed properties

#### Stability

The same tenant ID always generates the same seed. Given the same seed, the pseudo number random generator always generates the same sequence of numbers.

This guarantees that, given the same ring, we generate the same exact subring for a given tenant.

#### Consistency

The consistency property is honored by two aspects of the algorithm:

1. The quantity of random numbers generated is always equal to the shard size S, even in case of “collisions”. A collision is when the instance holding the random token has already been picked and we need to select a different instance which has not been picked yet.
2. In case of collisions, we select the “next” instance continuing walking the ring instead of generating another random number

##### Example adding an instance to the ring

Let’s consider an initial ring with 3 instances and 1 zone (for simplicity):

- I1 - Tokens: 1,  8, 15
- I2 - Tokens: 5, 11, 19
- I3 - Tokens: 7, 13, 21

With a replication factor = 2, the random sequence looks up:

- 3 (I2)
- 6 (I1)

Then we add a new instance and the **updated ring** is:

- I1 - Tokens: 1,  8, 15
- I2 - Tokens: 5, 11, 19
- I3 - Tokens: 7, 13, 21
- I4 - Tokens: 4,  7, 17

Now, let’s compare two different algorithms to solve collisions:

- Using the random generator:<br />
Random sequence = 3 (**I4**), 6 (I4 - collision), 12 (**I3**)<br />
**all instances are different** (I4, I3)
- Walking the ring:<br />
Random sequence = 3 (**I4**), 6 (I4 - collision, next is **I1**)<br />
**only 1 instance is different** (I4, I1)

#### Shuffling

Unless when resolving collisions, the algorithm doesn’t walk the ring to find the next instances, but uses a sequence of random numbers. This guarantees instances are shuffled, between different tenants, when building the subring.

#### Zone-awareness

We treat each zone as a separate ring and select an equal number of instances from each zone. This guarantees a fair balance of instances between zones.

### Proof of concept

We’ve built a [reference implementation](https://github.com/cortexproject/cortex/pull/3090) of the proposed algorithm, to test the properties described above.

In particular, we’ve observed that the [actual distribution](https://github.com/cortexproject/cortex/pull/3090/files#diff-121ffce90aa9932f6b87ffd138e0f36aR281) of matching instances between different tenants is very close to the [theoretical one](https://docs.google.com/spreadsheets/d/1FXbiWTXi6bdERtamH-IfmpgFq1fNL4GP_KX_yJvbRi4/edit), as well as consistency and stability properties are both honored.

