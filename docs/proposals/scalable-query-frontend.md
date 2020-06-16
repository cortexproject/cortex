---
title: "Scalable Query Frontend"
linkTitle: "Scalable Query Frontend"
weight: 1
slug: scalable-query frontend
---

- Author: [Joe Elliott](https://github.com/joe-elliott)
- Date: April 2020
- Status: Proposed

## Overview
This document aims to describe the [role](#query-frontend-role) that the Cortex Query Frontend plays in running multitenant Cortex at scale.  It also describes the [challenges](#challenges-and-proposals) of horizontally scaling the query frontend component and includes several recommendations and options for creating a reliably scalable query-frontend.  Finally, we conclude with a discussion of the overall philosophy of the changes and propose an [alternative](#alternative).

For the original design behind the query frontend, you should read [Cortex Query Optimisations design doc from 2018-07](https://docs.google.com/document/d/1lsvSkv0tiAMPQv-V8vI2LZ8f4i9JuTRsuPI_i-XcAqY).


## Reasoning

Query frontend scaling is becoming increasingly important for two primary reasons.

The Cortex team is working toward a scalable single binary solution.  Recently the query-frontend was [added](https://github.com/cortexproject/cortex/pull/2437) to the Cortex single binary mode and, therefore, needs to seamlessly scale.  Technically, nothing immediately breaks when scaling the query-frontend, but there are a number of concerns detailed in [Challenges And Proposals](#challenges-and-proposals).

As the query-frontend continues to [support additional features](https://github.com/cortexproject/cortex/pull/1878) it will start to become a bottleneck of the system.  Current wisdom is to run very few query-frontends in order to maximize [Tenancy Fairness](#tenancy-fairness) but as more features are added scaling horizontally will become necessary.

## Query Frontend Role

### Load Shedding

The query frontend maintains a queue per tenant of configurable length (default 100) in which it stores a series of requests from that tenant.  If this queue fills up then the frontend will return 429’s thus load shedding the rest of the system.

This is particularly effective due to the “pull” based model in which queriers pull requests from query frontends.

### Query Retries

The query frontend is capable of retrying a query on another querier if the first should fail due to OOM or network issues.

### Sharding/Parallelization

The query frontend shards requests by interval and [other factors](https://github.com/cortexproject/cortex/pull/1878) to concurrently run a single query across multiple queriers.

### Query Alignment/Caching

Queries are aligned to their own step and then stored/retrieved from cache.

### Tenancy Fairness

By maintaining one queue per tenant, a low demand tenant will have the same opportunity to have a query serviced as a high demand tenant.  See [Dilutes Tenant Fairness](#dilutes-tenant-fairness) for additional discussion.

For clarity, tenancy fairness only comes into play when queries are actually being queued in the query frontend.  Currently this rarely occurs, but as [query sharding](https://github.com/cortexproject/cortex/pull/1878) becomes more aggressive this may become the norm.

## Challenges And Proposals

### Dynamic Querier Concurrency

#### Challenge

For every query frontend the querier adds a [configurable number of goroutines](https://github.com/cortexproject/cortex/blob/50f53dba8f8bd5f62c0e85cc5d85684234cd1c1c/pkg/querier/frontend/worker.go#L146) which are each capable of executing a query.  Therefore, scaling the query frontend impacts the amount of work each individual querier is attempting to do at any given time.

Scaling up may cause a querier to attempt more work than they are configured for due to restrictions such as memory and cpu limits. Additionally, the promql engine itself is limited in the number of queries it can do as configured by the `-querier.max-concurrent` parameter.  Attempting more queries concurrently than this value causes the queries to queue up in the querier itself.

For similar reasons scaling down the query frontend may cause a querier to not use its allocated memory and cpu effectively.  This will lower effective resource utilization.  Also, because individual queriers will be doing less work, this may cause increased queueing in the query frontends.

#### Proposal

Currently queriers are configured to have a [max parallelism per query frontend](https://github.com/cortexproject/cortex/blob/50f53dba8f8bd5f62c0e85cc5d85684234cd1c1c/pkg/querier/frontend/worker.go#L146).  An additional “total max concurrency” flag should be added.

Total Max Concurrency would then be evenly divided amongst all available query frontends. This would decouple the amount of work a querier is attempting to do with the number of query frontends that happen to exist at this moment.  Consequently this would allow allocated resources (e.g. k8s cpu/memory limits) to remain balanced with the work the querier was attempting as the query frontend is scaled up or down.

A [PR](https://github.com/cortexproject/cortex/pull/2456) has already been merged to address this.

### Overwhelming PromQL Concurrency

#### Challenge

If #frontends > promql concurrency then the queriers are incapable of devoting even a single worker to each query frontend without risking queueing in the querier.  Queuing in the querier is a highly undesirable state and one of the primary reasons the query frontend was originally created.

#### Proposal

When #frontends > promql concurrency then each querier will maintain [exactly one connection](https://github.com/cortexproject/cortex/blob/8fb86155a7c7c155b8c4d31b91b267f9631b60ba/pkg/querier/frontend/worker.go#L194-L200) to every frontend.  As the query frontend is [currently coded](https://github.com/cortexproject/cortex/blob/8fb86155a7c7c155b8c4d31b91b267f9631b60ba/pkg/querier/frontend/frontend.go#L279-L332) it will attempt to use every open GRPC connection to execute a query in the attached queriers.  Therefore, in this situation where #frontends > promql concurrency, the querier is exposing itself to more work then it is actually configured to perform.

To prevent this we will add “flow control” information to the [ProcessResponse message](https://github.com/cortexproject/cortex/blob/master/pkg/querier/frontend/frontend.proto#L21) that is used to return query results from the querier to the query frontend.  In an active system this message is passed multiple times per second from the queriers to the query frontends and would be a reliable way for the frontends to track the state of queriers and balance load.

There are a lot of options for an exact implementation of this idea.  An effective solution should be determined and chosen by modeling a set of alternatives.  The details of this would be included in another design doc.  A simple implementation would look something like the following:

Add two new fields to [ProcessResponse](https://github.com/cortexproject/cortex/blob/master/pkg/querier/frontend/frontend.proto#L21):


```protobuf
message ProcessResponse {
  httpgrpc.HTTPResponse httpResponse = 1;
  currentConcurrency int = 2;
  desiredConcurrency int = 3;
}
```

**currentConcurrency** - The current number of queries being executed by the querier.

**desiredConcurrency** - The total number of queries that a querier is capable of executing.

Add a short backoff to the main frontend [processing loop](https://github.com/cortexproject/cortex/blob/8fb86155a7c7c155b8c4d31b91b267f9631b60ba/pkg/querier/frontend/frontend.go#L288-L331).  This would cause the frontend to briefly back off of any querier that was overloaded but continue to send queries to those that were capable of doing work.

```go
if current > desired {
  zzz := (current - desired) * backoffDuration
  zzz *= 1 + rand.Float64() * .1               // jitter
  time.Sleep(zzz)
}
```

Passing flow control information from the querier to the frontend would also open up additional future work for more sophisticated load balancing across queriers.  For example by simply comparing and choosing [the least congested of two](https://www.nginx.com/blog/nginx-power-of-two-choices-load-balancing-algorithm/) queriers we could dramatically improve how well work is distributed.

### Increased Time To Failure

#### Challenge

Scaling the query frontend also increases the per tenant queue length by creating more queues.  This could result in increased latencies where failing fast (429) would have been preferred.

The operator could reduce the queue length per query frontend in response to scaling out, but then they would run the risk of unnecessarily failing a request due to unbalanced distribution across query frontends.  Also, shorter queues run the risk of failing to properly service heavily sharded queries.

Another concern is that a system with more queues will take longer to recover from an production event as it will have queued up more work.

#### Proposal

Currently we are not proposing any changes to alleviate this concern.  We believe this is solvable operationally.  This can be revisited as more information is gathered.

### Querier Discovery Lag

#### Challenge

Queriers have a configurable parameter that controls how often they refresh their query frontend list.  The default value is 10 seconds.  After a new query frontend is added the average querier will take 5 seconds (after DNS is updated) to become aware of it and begin requesting queries from it.

#### Proposal

It is recommended to add a readiness/health check to the query frontend to prevent it from receiving queries while it is waiting for queriers to connect.   HTTP health checks are supported by [envoy](https://www.envoyproxy.io/learn/health-check), [k8s](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/), [nginx](https://docs.nginx.com/nginx/admin-guide/load-balancer/http-health-check/), and basically any commodity load balancer.  The query frontend would not indicate healthy on its health check until at least one querier had connected.

In a k8s environment this will require two services.  One service for discovery with `publishNotReadyAddresses` set to true and one service for load balancing which honors the healthcheck/readiness probe.  After a new query-frontend instance is created the "discovery service" would immediately have the ip of the new instance which would allow queriers to discover and attach to it.  After queriers had connected it would then raise its readiness probe and appear on the "load balancing" service and begin receiving traffic.


### Dilutes Tenant Fairness

#### Challenge

Given `f` query frontends, `n` tenants and an average of `q` queries in the frontend per tenant.  The following assumes that queries are perfectly distributed across query frontends.  The number of tenants per instance would be:

<img src="https://render.githubusercontent.com/render/math?math=m = floor(n * \frac{min(q,f)}{f})">

The chance that a query by a tenant with `Q` queries in the frontend is serviced next is:

<img src="https://render.githubusercontent.com/render/math?math=min(Q,f)* \frac{1}{min(q * n %2b Q,f)}*\frac{1}{m %2b 1}">

Note that fewer query frontends caps the impact of the number of active queries per tenant.  If there is only one query frontend then the equation reduces to:

<img src="https://render.githubusercontent.com/render/math?math=\frac{1}{n}">

and every tenant has an equal chance of being serviced regardless of the number of queued queries.

Adding more query frontends favors high volume tenants by giving them more slots to be picked up by the next available querier.  Fewer query frontends allows for an even playing field regardless of the number of active queries.

For clarity, it should be noted that tenant fairness is only impacted if queries are being queued in the frontend.  Under normal operations this is currently not occurring although this may change with increased sharding.

#### Proposal

Tenancy fairness is complex and is currently _not_ impacting our system.  Therefore we are proposing a very simple improvement to the query frontend.  If/when frontend queuing becomes more common this can be revisited as we will understand the problem better.

Currently the query frontend [picks a random tenant](https://github.com/cortexproject/cortex/blob/50f53dba8f8bd5f62c0e85cc5d85684234cd1c1c/pkg/querier/frontend/frontend.go#L362-L367) to service when a querier requests a new query.  This can increase long tail latency if a tenant gets “unlucky” and is also exacerbated for low volume tenants by scaling the query frontend.  Instead the query frontend could use a round robin approach to choose the next tenant to service.  Round robin is a commonly used algorithm to increase fairness in scheduling.

This would be a very minor improvement, but would give some guarantees to low volume tenants that their queries would be serviced.  This has been proposed in this [issue](https://github.com/cortexproject/cortex/issues/2431).

**Pros:** Requires local knowledge only.  Easier to implement than weighted round robin.

**Cons:** Improvement is minor.

**Alternatives to Round Robin**

**Do Nothing**

As is noted above tenancy fairness only comes into play when queries start queueing up in the query frontend.  Internal Metrics for multi-tenant Cortex at Grafana show that this has only happened 5 times in the past week significantly enough to have been caught by Prometheus.

Right now doing nothing is a viable option that will, almost always, fairly serve our tenants.  There is, however, some concern that as sharding becomes more commonplace queueing will become more common and QOS will suffer due to reasons outlined in [Dilutes Tenant Fairness](#dilutes-tenant-fairness).

**Pros:** Easy!

**Cons:** Nothing happens!

**Weighted Round Robin**

The query frontends could maintain a local record of throughput or work per tenant.  Tenants could then be sorted in QOS bands.  In its simplest form there would be two QOS bands.  The band of low volume tenants would be serviced twice for every one time the band of high volume tenants would be serviced.  The full details of this approach would require a separate proposal.

This solution would also open up interesting future work.  For instance, we could allow operators to manually configure tenants into QOS bands.

**Pros:** Requires local knowledge only.  Can be extended later to allow tenants to be manually sorted into QOS tiers.

**Cons:** Improvement is better than Round Robin only.  Relies on even distribution of queries across frontends.  Increased complexity and difficulty in reasoning about edge cases.

**Weighted Round Robin With Gossiped Traffic**

This approach would be equivalent to Weighted Round Robin proposed above but with tenant traffic volume gossiped between query frontends.

**Pros:** Benefits of Weighted Round Robin without the requirement of even query distribution.  Even though it requires distributed information a failure in gossip means it gracefully degrades to Weighted Round Robin.

**Cons:** Requires cross instance communication.  Increased complexity and difficulty in reasoning about edge cases.

## Alternative

The proposals in this document have preferred augmenting existing components to make decisions with local knowledge.  The unstated goal of these proposals is to build a distributed queue across a scaled query frontend that reliably and fairly serves our tenants.

Overall, these proposals will create a robust system that is resistant to network partitions and failures of individual pieces.  However, it will also create a complex system that could be difficult to reason about, contain hard to ascertain edge cases and nuanced failure modes.

The alternative is, instead of building a distributed queue, to add a new cortex queueing service that sits in between the frontends and the queriers.  This queueing service would pull from the frontends and distribute to the queriers.  It would decouple the stateful queue from the stateless elements of the query frontend and allow us to easily scale the query frontend while keeping the queue itself a singleton.  In a single binary HA mode one (or few) of the replicas would be leader elected to serve this role.

Having a singleton queue is attractive because it is simple to reason about and gives us a single place to make fair cross tenant queueing decisions.  It does, however, create a single point of failure and add another network hop to the query path.

## Conclusion

In this document we reviewed the [reasons the frontend exists](#query-frontend-role), [challenges and proposals to scaling the frontend](#challenges-and-proposals) and [an alternative architecture that avoids most problems but comes with its own challenges.](#alternative)

<table>
  <tr>
   <td><strong>Challenge</strong>
   </td>
   <td><strong>Proposal</strong>
   </td>
   <td><strong>Status</strong>
   </td>
  </tr>
  <tr>
   <td>Dynamic Querier Concurrency
   </td>
   <td>Add Max Total Concurrency in Querier
   </td>
   <td><a href="https://github.com/cortexproject/cortex/pull/2456">Pull Request</a>
   </td>
  </tr>
  <tr>
   <td>Overwhelming PromQL Concurrency
   </td>
   <td>Queriers Coordinate Concurrency with Frontends
   </td>
   <td>Proposed
   </td>
  </tr>
  <tr>
   <td>Increased Time to Failure
   </td>
   <td>Operational/Configuration Issue.  No Changes Proposed.
   </td>
   <td>
   N/A
   </td>
  </tr>
  <tr>
   <td>Querier Discovery Lag
   </td>
   <td>Query Frontend HTTP Health Checks
   </td>
   <td><a href="https://github.com/cortexproject/cortex/pull/2733">Pull Request</a>
   </td>
  </tr>
  <tr>
   <td>Dilutes Tenant Fairness
   </td>
   <td>Round Robin with additional alternatives proposed
   </td>
   <td><a href="https://github.com/cortexproject/cortex/pull/2553">Pull Request</a>
   </td>
  </tr>
</table>
