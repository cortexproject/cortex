---
title: "Scaling the Query Frontend"
linkTitle: "Scaling the Query Frontend"
weight: 5
slug: scaling-query-frontend
---

Historically scaling the Cortex query frontend has [posed some challenges](https://cortexmetrics.io/docs/proposals/scalable-query-frontend/).
This document aims to detail how to use some of the added configuration parameters to correctly scale the frontend.
Note that these instructions apply in both the HA single binary scenario or microservices mode.

## Using Query Scheduler

Query scheduler is new service that moves the in-memory queue from query frontend to a separate component.
This makes scaling query frontend easier, as it allows running multiple query frontends without increasing the number of queues,
which makes life difficult for queriers (as above-linked document explains).

It is recommended to run two query scheduler instances.

When using single-binary mode, Cortex defaults to run **without** query-scheduler.

## DNS Configuration / Readiness

When a new frontend is first created on scale up it will not immediately have queriers attached to it.
The existing endpoint `/ready` was updated to only return http 200 when the query frontend was ready to serve queries.
Make sure to configure this endpoint as a healthcheck in your load balancer.
Otherwise a query frontend scale up event might result in failed queries or high latency for a bit while queriers attach.

When using query frontend with query scheduler, `/ready` will report 200 status code only after frontend discovers some schedulers via DNS resolution.

## Querier Max Concurrency

When using query frontend (possibly in combination with query scheduler), queriers need to connect to frontends or schedulers.
To make sure that querier doesn't receive more queries that it can handle at the same time, make sure to configure the querier to match its PromQL concurrency with number of connections.
This allows the operator to freely scale the frontend or scheduler up and down without impacting the amount of work an individual querier is attempting to perform.
More details [here](https://cortexmetrics.io/docs/proposals/scalable-query-frontend/#dynamic-querier-concurrency).

### Example Configuration

**CLI**
```
-querier.worker-match-max-concurrent=true
```

**Config File**
```yaml
frontend_worker:
  match_max_concurrent: true
```