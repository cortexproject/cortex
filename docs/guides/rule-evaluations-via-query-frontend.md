---
title: "Rule evaluations via query frontend"
linkTitle: "Rule evaluations via query frontend"
weight: 10
slug: rule-evalutions-via-query-frontend
---

This guide explains how to configure the Ruler to evaluate rules via Query Frontends instead of the Ingester/Store Gateway and the pros and cons of rule evaluation via Query Frontend.

## How to enable

By default, the Ruler queries both Ingesters and Store Gateway depending on the Rule time range for evaluating rules (alerting rules or recording rules). If you have set `-ruler.frontend-address` then the Ruler queries the Query Frontend for evaluation rules.
The address should be the gRPC listen address in host:port format.

You can configure via args:
```
-ruler.frontend-address=query-frontend.svc.cluster.local:9095
```
And via yaml:
```yaml
ruler:
  frontend_address: query-frontend.svc.cluster.local:9095
```

In addition, you can configure gRPC client (Ruler -> Query Frontend) config, please refer to frontend_client section in [ruler config](../configuration/config-file-reference.md#ruler_config).

## Pros and Cons
If this feature is enabled, the query execute path is as follows:

Ruler -> Query Frontend -> Query Scheduler -> Querier -> Ingester/Store Gateway

There are pros and cons regarding query performance as more hops than before (Ruler -> Ingester/Store Gateway).
### Pros
- The rule evaluation performance could be improved in such a situation where the number of Queriers pulling queries from the Query Scheduler is good enough.
If then, the queries in Query Scheduler are fetched in a reasonable time (i.e. a lof of hops are not a defect for query performance). In this environment, query performance could be improved as we can use Query Frontend features like the vertical query sharding.
- The Ruler can use fewer resources as it doesn't need to run query engine to evaluate rules.

### Cons
- If there are not enough Queriers, adding rule queries to Query Scheduler could cause query starvation problem (queries in Query Scheduler could not be fetched in a reasonable time), so rules cannot be evaluated in time.
- The only support format of the query response of the Query Frontend is the JSON format. The JSON format only contains partial information of the native histogram.
So, if your rules use the native histogram, it is not allowed yet.