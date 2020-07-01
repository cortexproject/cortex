---
title: "Scaling the Query Frontend"
linkTitle: "Scaling the Query Frontend"
weight: 5
slug: scaling-query-frontend
---

Historically scaling the Cortex query frontend has [posed some challenges](https://cortexmetrics.io/docs/proposals/scalable-query-frontend/).  This document aims to detail how to use some of the added configuration parameters to correctly scale the frontend.  Note that these instructions apply in both the HA single binary scenario or microservices mode.

## DNS Configuration / Readiness

When a new frontend is first created on scale up it will not immediately have queriers attached to it.  The existing endpoint `/ready` was updated to only return http 200 when the query frontend was ready to serve queries.  Make sure to configure this endpoint as a healthcheck in your load balancer.  Otherwise a query frontend scale up event might result in failed queries or high latency for a bit while queriers attach.

## Querier Max Concurrency

Make sure to configure the querier frontend worker to match max concurrency.  This will allow the operator to freely scale the frontend up and down without impacting the amount of work an individual querier is attempting to perform.  More details [here](https://cortexmetrics.io/docs/proposals/scalable-query-frontend/#dynamic-querier-concurrency).

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