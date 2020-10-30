---
title: "Shuffle sharding on the read path"
linkTitle: "Shuffle sharding on the read path"
weight: 1
slug: shuffle-sharding-on-the-read-path
---

- Author: @pracucci, @tomwilkie, @pstibrany
- Reviewers:
- Date: August 2020
- Status: Proposed, partially implemented

## Background

Cortex currently supports sharding of tenants to a subset of the ingesters on the write path [PR](https://github.com/cortexproject/cortex/pull/1947).

This feature is called “subring”, because it computes a subset of nodes registered to the hash ring. The aim of this feature is to improve isolation between tenants and reduce the number of tenants impacted by an outage.

This approach is similar to the techniques described in [Amazon’s Shuffle Sharding article](https://aws.amazon.com/builders-library/workload-isolation-using-shuffle-sharding/), but currently suffers from a non random selection of nodes (*proposed solution below*).

Cortex can be **configured** with a default subring size, and then it can be [customized on a per-tenant basis](https://cortexmetrics.io/docs/configuration/configuration-file/#limits_config). The per-tenant configuration is live reloaded during runtime and applied without restarting the Cortex process.

The subring sharding currently supports only the write-path. The read-path is not shuffle sharding aware. For example, an outage of more than one ingester with RF=3 will affect all tenants, or a particularly noisy tenant wrt queries has the ability to affect all tenants.

## Goals

The Cortex **read path should support shuffle sharding to isolate** the impact of an outage in the cluster. The shard size must be dynamically configurable on a per-tenant basis during runtime.

This deliverable involves introducing shuffle sharding in:
- **Query-frontend → Querier** (for queries sharding) [PR #3113](https://github.com/cortexproject/cortex/pull/3113)
- **Querier → Store-gateway** (for blocks sharding) [PR #3069](https://github.com/cortexproject/cortex/pull/3069)
- **Querier→ Ingesters** (for queries on recent data)
- **Ruler** (for rule and alert evaluation)

### Prerequisite: fix subring shuffling

The solution is implemented in https://github.com/cortexproject/cortex/pull/3090.

#### The problem
The subring is a subset of nodes that should be used for a specific tenant.

The current subring implementation doesn’t shuffle tenants across nodes. Given a tenant ID, it finds the first node owning the hash(tenant ID) token and then it picks N distinct consecutive nodes walking the ring clockwise.

For example, in a cluster with 6 nodes (numbered 1-6) and a replication factor of 3, three tenants (A, B, C) could have the following shards:

Tenant ID | Node 1 | Node 2 | Node 3 | Node 4 | Node 5 | Node 6
----------|--------|--------|--------|--------|--------|-------
A | x | x | x | | |
B | | x | x | x | |
C | | | x | x | x |


#### Proposal

We propose to build the subring picking N distinct and random nodes registered in the ring, using the following algorithm:

1. SID = tenant ID
2. SID = hash(SID)
3. Look for the node owning the token range containing FNV-1a(SID)
4. Loop to (2) until we’ve found N distinct nodes (where N is the shard size)

*hash() function to be decided. The required property is to be strong enough to not generate loops across multiple subsequent hashing of the previous hash.*

### Query-frontend → Queriers shuffle sharding

Implemented in https://github.com/cortexproject/cortex/pull/3113.

### How querier runs query-frontend jobs

Today **each** querier connects to **each** query-frontend instance, and calls a single “Process” method via gRPC.

“Process” is a bi-directional streaming gRPC method – using the server-to-client stream for sending requests from query-frontend to the querier, and client-to-server stream for returning results from querier to the query-frontend.  NB this is the opposite of what might be considered normal. Query-frontend scans all its queues with pending query requests, and picks a query to execute based on a fair schedule between tenants.

The query request is then sent to an idle querier worker over the stream opened in the Process method, and the query-frontend then waits for a response from querier. This loop repeats until querier disconnects.

### Proposal

To support shuffle sharding, Query-Frontends will keep a list of connected Queriers, and randomly (but consistently between query-frontends) choose N of them to distribute requests to. When Query-Frontend looks for the next request to send to a given querier, it will only consider tenants that “belong” to the Querier.

To choose N Queriers for a tenant, we propose to use a simple algorithm:

1. Sort all Queriers by their ID
2. SID = tenant ID
3. SID = hash(SID)
4. Pick the querier from the list of sorted queries with:<br />
index = FNV-1a(SID) % number of Queriers
5. Loop to (3) until we’ve found N distinct queriers (where N is the shard size) and stop early if there aren’t enough queriers

*hash() function to be decided. The required property is to be strong enough to not generate loops across multiple subsequent hashing of the previous hash.*

### Properties

- **Stability:** this will produce the same result on all query-frontends as long as all queriers are connected to all query-frontends.
- **Simplicity:** no external dependencies.
- **No consistent hashing:** adding/removing queriers will cause “resharding” of tenants between queriers. While in general that’s not desirable property, queriers are stateless so it doesn’t seem to matter in this case.

### Implementation notes

- **Caching:** once this list of queriers to use for a tenant is computed in the query-frontend, it is cached in memory until queriers are added or removed. Per-tenant cache entries will have a TTL to discard tenants not “seen” since a while.
- **Querier ID:** Query-frontends currently don’t have any identity for queriers. We need to introduce sending of a unique  ID (eg. hostname) by querier to query-frontend when it calls “Process” method.
- **Backward-compatibility:** when querier shuffle sharding is enabled, the system expects that both query-frontend and querier will run a compatible version. Cluster version upgrade will require to rollout new query-frontends and queriers first, and then enable shuffle sharding.
- **UI:** we propose to expose the current state of the query-frontend through a new endpoint which should display:
  - Which querier are connected to the query-frontend
  - Are there any “old” queriers, that are receiving requests from all tenants?
  - Mapping of tenants to queriers. Note that this mapping may only be available for tenants with pending requests on given query-frontend, and therefore be very dynamic.

### Configuration

- **Shard size** will be configurable on a per-tenant basis via existing “runtime-configuration” mechanism (limits overrides). Changing a value for a tenant needs to invalidate cached per-tenant queriers.
- Queriers shard size will be a different setting than then one used for writes.

### Evaluated alternatives

#### Use the subring

An alternative option would be using the subring. This implies having queriers registering to the hash ring and query-frontend instances using the ring client to find the queriers subring for each tenant.

This solution looks adding more complexity without any actual benefit.

#### Change query-frontend → querier architecture

Completely different approach would be to introduce a place where starting queriers would register (eg. DNS-based service discovery), and let query-frontends discover queriers from this central registry.

Possible benefit would be that queriers don’t need to initiate connection to all query-frontends, but query-frontends would only connect to queriers for which they have actual pending requests. However this would be a significant redesign of how query-frontend / querier communication works.

## Querier → Store-gateway shuffle sharding

Implemented in https://github.com/cortexproject/cortex/pull/3069.

### Introduction

As of today, the store-gateway supports blocks sharding with customizable replication factor (defaults to 3). Blocks of a single tenant are sharded across all store-gateway instances and so to execute a query the querier may touch any store-gateway in the cluster.

The current sharding implementation is based on a **hash ring** formed by store-gateway instances.

### Proposal

The proposed solution to add shuffle sharding support to the store-gateway is to **leverage on the existing hash ring** to build a per-tenant **subring**, which is then used both by the querier and store-gateway to know to which store-gateway a block belongs to.

### Configuration

- Shuffle sharding can be enabled in the **store-gateway configuration.** It supports a **default sharding factor,** which is **overridable on a per-tenant basis** and live reloaded during runtime (using the existing limits config).
- The querier already requires the store-gateway configuration when the blocks sharding is enabled. Similarly, when shuffle sharding is enabled the querier will require the store-gateway shuffle sharding configuration as well.

### Implementation notes

When shuffle sharding is enabled:

- The **store-gateway** `syncUsersBlocks()` will build a tenant’s subring for each tenant found scanning the bucket and will skip any tenant not belonging to its shard.<br />
Likewise, ShardingMetadataFilter will first build a **tenant’s subring** and then will use the existing logic to filter out blocks not belonging to store-gateway instance itself. The tenant ID can be read from the block’s meta.json.
- The **querier** `blocksStoreReplicationSet.GetClientsFor()` will first build a **tenant’s subring** and then will use the existing logic to find out to which store-gateway instance each requested block belongs to.

### Evaluated alternatives

*Given the store-gateways already form a ring and building the shuffle sharding based on the ring (like in the write path) doesn’t introduce extra operational complexity, we haven’t discussed alternatives.*

## Querier→ Ingesters shuffle sharding

We’re currently discussing/evaluating different options.

### Problem

Cortex must guarantee query correctness; transiently incorrect results may be cached and returned forever. The main problem to solve when introducing ingesters shuffle sharding on the read path is to make sure that a querier fetch data from all ingesters having at least 1 sample for a given tenant.

The problem to solve is: how can a querier efficiently find which ingesters have data for a given tenant?  Each option must consider the changing of the set of ingesters and the changing of each tenant’s subring size.

### Proposal: use only the information contained in the ring.

*This section describes an alternative approach.  Discussion is still on-going.*

The idea is for the queries to be able to deduce what ingesters could possibly hold data for a given tenant by just consulting the ring (and the per-tenant sub ring sizes).  We posit that this is possible with only a single piece of extra information: a single timestamp per ingester saying when the ingester first joined the ring.

#### Scenario: ingester scale up

When a new ingester is added to the ring, there will be a set of user subrings that see a change: an ingester being removed, and a new one being added.  We need to guarantee that for some time period (the block flush interval), the ingester removed is also consulted for queries.

To do this, during the subring selection if we encounters an ingester added within the time period, we will add this to the subring but continue node selection as before - in effect, selecting an extra ingester:

```go
var (
    subringSize   int
    selectedNodes []Node
    deadline      = time.Now().Add(-flushWindow)
)

for len(selectedNodes) < subringSize {
    token := random.Next()
    node := getNodeByToken(token)
    for {
        if node in selectedNodes {
            node = node.Next()
            continue
        }
        if node.Added.After(deadline) {
            subringSize++
            selectedNodes.Add(node)
            node = node.Next()
            continue
        )
        selectedNodes.Add(node)
        break
    )
}
```

#### Scenario: ingester scale down

When an ingester is permanently removed from the ring it will flush its data to the object store and the subrings containing the removed ingester will gain a “new” ingester.  Queries consult the store and merge the results with those from the ingesters, so no data will be missed.

Queriers and store-gateways will discover newly flushed blocks on next sync (`-blocks-storage.bucket-store.sync-interval`, default 5 minutes).
Multiple ingesters should not be scaled-down within this interval.

To improve read-performance, queriers and rulers are usually configured with non-zero value of `-querier.query-store-after` option.
This option makes queriers and rulers to consult **only** ingesters when running queries within specified time window (eg. 12h).
During scale-down this needs to be lowered in order to let queriers and rulers use flushed blocks from the storage.

#### Scenario: increase size of a tenant’s subring

Node selection for subrings is stable - increasing the size of a subring is guaranteed to only add new nodes to it (and not remove any nodes).  Hence, if a tenant’s subring is increase in size the queriers will notice the config change and start consulting the new ingester.

#### Scenario: decreasing size of a tenant’s subring

If a tenant’s subring decreases in size, there is currently no way for the queriers to know how big the ring was previously, and hence they will potentially miss an ingester with data for that tenant.

This is deemed an infrequent operation that we considered banning, but have a proposal for how we might make it possible:

The proposal is to have separate read subring and write subring size in the config.  The read subring will not be allowed to be smaller than the write subring.  When reducing the size of a tenant’s subring, operators must first reduce the write subring, and then two hours later when the blocks have been flushed, the read subring.  In the majority of cases the read subring will not need to be specified, as it will default to the write subring size.

### Considered alternative #1: Ingesters expose list of tenants

A possible solution could be keeping in the querier an in-memory data structure to map each ingester to the list of tenants for which it has some data. This data structure would be constructed at querier startup, and then periodically updated, interpolating two information:

1. The current state of the ring
2. The list of tenants directly exposed by each ingester (via a dedicated gRPC call)

#### Scenario: new querier starts up

When a querier starts up and before getting ready:

1. It scans all ingesters (discovered via the ring) and fetches the list of tenants for which each ingester has some data
2. For each found tenant (unique list of tenant IDs across all ingesters responses), the querier looks at the current state of the ring and adds to the map the list of ingesters currently assigned to the tenant shard, even if they don’t hold any data yet (because may start receiving series shortly)

Then the querier watches the ingester ring and rebuilds the in-memory map whenever the ring topology changes.

#### Scenario: querier receives a query for an unknown tenant

A new tenant starts remote writing to the cluster. The querier doesn’t know it in its in-memory map, so it adds the tenant on the fly to the map just looking at the current state of the ring.

#### Scenario: ingester scale up / down

When a new ingester is added / removed to / from the ring, the ring topology changes and queriers will update the in-memory map.

#### Scenario: per-tenant shard size increases

Queriers periodically (every 1m) reload the limits config file. When a tenant shard size change is detected, the querier updates the in-memory map for the affected tenant.

**Issue:** some time series data may be missing in queries up to 1m.

#### Edge case: queriers notice the ring topology change before distributors

Consider the following scenario:

1. Tenant A shard is composed by ingesters 1,2,3,4,5,6
2. Tenant A is remote writing 1 single series and gets replicated to ingester 1,2,3
3. The ring topology changes and tenant A shard is ingesters 1,2,3,7,8,9
4. Querier notices the ring topology change and updates the in-memory map. Given tenant A series were only on ingester 1,2,3, the querier maps tenant A to ingester 1,2,3 (because of what received from ingesters via gRPC) and 7,8,9 (because of the current state of the ring)
5. Distributor hasn’t updated the ring state yet
6. Tenant A remote writes 1 **new** series, which get replicated to 4,5,6
7. Distributor updates the ring state
8. **Race condition:** querier will not know that ingesters 4,5,6 contains tenant A data until the next sync

### Considered alternative #2: streaming updates from ingesters to queriers

*This section describes an alternative approach.*

#### Current state

As of today, queriers discover ingesters via the ring:

- **Ingesters** register (and update their heartbeat timestamp) to the ring and queriers watch the ring, keeping an in-memory copy of the latest ingesters ring state.
- **Queriers** use the in-memory ring state to discover all ingesters that should be queried at query time.

#### Proposal

The proposal is to expose a new gRPC endpoint on ingesters, which allows queriers to receive a stream of real time updates from ingesters about the tenants for which an ingester currently has time series data.

From the querier side:

- At **startup** the querier discovers all existing ingesters. For each ingester, the querier calls the ingester’s gRPC endpoint WatchTenants() (to be created). As soon as the WatchTenants() rpc is called, the ingester sends the entire set of tenants to the querier and then will send incremental updates (tenant added or removed from ingester) while the WatchTenants() stream connection is alive.
- If the querier **loses the connection** to an ingester, it will automatically retry (with backoff) while the ingester is within the ring.
- The querier **watches the ring** to discover added/removed ingesters. When an ingester is added, the querier adds the ingester to the pool of ingesters whose state should be monitored via WatchTenants().
- At **query time,** the querier looks for all ingesters within the ring. There are two options:
  1. The querier knows the state of the ingester: the ingester will be queried only if it contains data for the query’s tenant.
  2. The querier doesn’t know the state of the ingester (eg. because it was just registered to the ring and WatchTenants() hasn’t succeeded yet): the ingester will be queried anyway (correctness first).
- The querier will fine tune [gRPC keepalive](https://godoc.org/google.golang.org/grpc/keepalive) settings to ensure a lost connection between the querier and ingester will be early detected and retried.

#### Trade-offs

Pros:

- The querier logic, used to find ingesters for a tenant’s shard, **does not require to watch the overrides** config file (containing tenant shard size override). Watching the file in the querier is problematic because of introduced delays (ConfigMap update and Cortex file polling) which could lead to distributors apply changes before queriers.
- The querier **never uses the current state of the ring** as a source of information to detect which ingesters have data for a specific tenant. This information comes directly from the ingesters themselves, which makes the implementation less likely to be subject to race conditions.

Cons:

- Each querier needs to open a gRPC connection to each ingester. Given gRPC supports multiplexing, the underlying TCP connection could be the same connection used to fetch samples from ingesters at query time, basically having 1 single TCP connection between a querier and an ingester.
- The “Edge case: queriers notice the ring topology change before distributors” described in attempt #1 can still happen in case of delays in the propagation of the state update from an ingester to queriers:
  - Short delay: a short delay (few seconds) shouldn’t be a real problem. From the final user perspective, there’s no real difference between this edge case and a delay of few seconds in the ingestion path (eg. Prometheus remote write lagging behind few seconds). In the real case of Prometheus remote writing to Cortex, there’s no easy way to know if the latest samples are missing because has not been remote written yet by Prometheus or any delay in the propagation of this information between ingesters and queriers.
  - Long delay: in case of networking issue propagating the state update from an ingester to the querier, the gRPC keepalive will trigger (because of failed ping-pong) and the querier will remove the failing ingesters in-memory data, so the ingester will be always tried by the querier for any query, until the state update will be re-established.

## Ruler sharding

### Introduction

The ruler currently supports rule groups sharding across a pool of rulers. When sharding is enabled, rulers form a hash ring and each ruler uses the ring to check if it should evaluate a specific rule group.

At a polling interval (defaults to 1 minute), the ruler:

- List all the bucket objects to find all rule groups (listing is done specifying an empty delimiter so it return objects at any depth)
- For each discovered rule group, the ruler hashes the object key and checks if it belongs to the range of tokens assigned to the ruler itself. If not, the rule group is discarded, otherwise it’s kept for evaluation.

### Proposal

We propose to introduce shuffle sharding in the ruler as well, leveraging on the already existing hash ring used by the current sharding implementation.

The **configuration** will be extended to allow to configure:

- Enable/disable shuffle sharding
- Default shard size
- Per-tenant overrides (reloaded at runtime)

When shuffle sharding is enabled:

- The ruler lists (ListBucketV2) the tenants for which rule groups are stored in the bucket
- The ruler filters out tenants not belonging to its shard
- For each tenant belonging to its shard, the ruler does a ListBucketV2 call with the “<tenant-id>/” prefix and with empty delimiter to find all the rule groups, which are then evaluated in the ruler

The ruler re-syncs the rule groups from the bucket whenever one of the following conditions happen:

1. Periodic interval (configurable)
2. Ring topology changes
3. The configured shard size of a tenant has changed

### Other notes

- The “subring” implementation is unoptimized. We will optimize it as part of this work to make sure no performance degradation is introduced when using the subring vs the normal ring.

