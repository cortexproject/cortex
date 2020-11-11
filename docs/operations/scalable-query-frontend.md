---
title: "Scaling the Query Frontend"
linkTitle: "Scaling the Query Frontend"
weight: 5
slug: scaling-query-frontend
---

Historically scaling the Cortex query frontend has [posed some challenges](../proposals/scalable-query-frontend.md).
This document aims to detail how to use some added configuration parameters to correctly scale the frontend.
Note that these instructions apply in both the HA single binary scenario or microservices mode.

## Scaling the Query Frontend

For every query frontend the querier adds a [configurable number of concurrent workers](https://github.com/cortexproject/cortex/blob/1797adfed2979f6096c3305b0dc9162c1ec0c046/pkg/querier/worker/worker.go#L212)
which are each capable of executing a query.
Each worker is connected to a single query frontend instance, therefore scaling up the query frontend impacts the amount of work each individual querier is attempting to do at any given time.

Scaling up may cause a querier to attempt more work than they are configured for due to restrictions such as memory and cpu limits.
Additionally, the PromQL engine itself is limited in the number of queries it can do as configured by the `-querier.max-concurrent` parameter.
Attempting more queries concurrently than this value causes the queries to queue up in the querier itself.

For similar reasons scaling down the query frontend may cause a querier to not use its allocated memory and cpu effectively.
This will lower effective resource utilization.
Also, because individual queriers will be doing less work, this may cause increased queueing in the query frontends.

### Querier Max Concurrency

To guarantee that querier doesn't receive more queries that it can handle at the same time, make sure to configure the querier to match its PromQL concurrency with number of connections.
This can be done by using `-querier.worker-match-max-concurrent=true` option, or `match_max_concurrent: true` field in `frontend_worker` section of YAML config file.
This allows the operator to freely scale the frontend or scheduler up and down without impacting the amount of work an individual querier is attempting to perform.

### Query Scheduler

Query scheduler is a service that moves the in-memory queue from query frontend to a separate component.
This makes scaling query frontend easier, as it allows running multiple query frontends without increasing the number of queues.

In order to use query scheduler, both query frontend and queriers must be configured with query scheduler address
(using `-frontend.scheduler-address` and `-querier.scheduler-address` options respectively).

Note that querier will only fetch queries from query frontend or query scheduler, but not both.
`-querier.frontend-address` and `-querier.scheduler-address` options are mutually exclusive, and at most one can be set.

When using query scheduler, it is recommended to run two query scheduler instances.
Running only one query scheduler poses a risk of increased query latency when single scheduler crashes or restarts.
Running two query-schedulers should be enough even for large Cortex clusters with an high QPS.

When using single-binary mode, Cortex defaults to run **without** query scheduler.

### DNS Configuration / Readiness

When a new frontend is first created on scale up it will not immediately have queriers attached to it.
The existing endpoint `/ready` returns HTTP 200 status code only when the query frontend is ready to serve queries.
Make sure to configure this endpoint as a healthcheck in your load balancer,
otherwise a query frontend scale up event might result in failed queries or high latency for a bit while queriers attach.

When using query frontend with query scheduler, `/ready` will report 200 status code only after frontend discovers some schedulers via DNS resolution.
