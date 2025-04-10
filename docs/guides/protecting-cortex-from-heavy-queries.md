---
title: "Protecting Cortex from Heavy Queries"
linkTitle: "Protecting Cortex from Heavy Queries"
weight: 11
slug: protecting-cortex-from-heavy-queries
---

PromQL is powerful, and is able to result in query requests that have very wide range of data fetched and samples processed. Heavy queries can cause:

1. CPU on any query component to be partially exhausted, increasing latency and causing incoming queries to queue up with high chance of time-out.
2. CPU on any query component to be fully exhausted, causing GC to slow down leading to the pod being out-of-memory and killed.
3. Heap memory on any query component to be exhausted, leading to the pod being out-of-memory and killed.

It's important to protect Cortex components by setting appropriate limits and throttling configurations based on your infrastructure and data ingested by the customers.

## Static limits

There are number of static limits that you could configure to block heavy queries from running.

### Max outstanding requests per tenant

See https://cortexmetrics.io/docs/configuration/configuration-file/#query_frontend_config:~:text=max_outstanding_requests_per_tenant for details.

### Max data bytes fetched per (sharded) query

See https://cortexmetrics.io/docs/configuration/configuration-file/#query_frontend_config:~:text=max_fetched_data_bytes_per_query for details.

### Max series fetched per (sharded) query

See https://cortexmetrics.io/docs/configuration/configuration-file/#query_frontend_config:~:text=max_fetched_series_per_query for details.

### Max chunks fetched per (sharded) query

See https://cortexmetrics.io/docs/configuration/configuration-file/#query_frontend_config:~:text=max_fetched_chunk_bytes_per_query for details.

### Max samples fetched per (sharded) query

See https://cortexmetrics.io/docs/configuration/configuration-file/#querier_config:~:text=max_samples for details.

## Resource-based throttling (Experimental)

Although the static limits are able to protect Cortex components from specific query patterns, they are not generic enough to cover different combinations of bad query patterns. For example, what if the query fetches relatively large postings, series and chunks that are slightly below the individual limits? For a more generic solution, you can enable resource-based throttling by setting CPU and heap utilization thresholds.

Currently, it only throttles incoming query requests with error code 429 (too many requests) when the resource usage breaches the configured thresholds.

For example, the following configuration will start throttling query requests if either CPU or heap utilization is above 80%, leaving 20% of room for inflight requests.

```
target: ingester
monitored_resources: cpu,heap
instance_limits:
  cpu_utilization: 0.8
  heap_utilization: 0.8
```

See https://cortexmetrics.io/docs/configuration/configuration-file/:~:text=instance_limits for details.