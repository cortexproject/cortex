---
title: "Cross-Tenant Query Federation"
linkTitle: "Cross-Tenant Query Federation"
weight: 1
slug: "cross-tenant-query-federation"
---

- Author: [Christian Simon](https://github.com/simonswine)
- Date: October 2020
- Status: Accepted

## Overview

This document aims to describe how to implement the ability to allow queries to cover data from more than a single Cortex tenant.

## Reasoning

Adopting a tenancy model within an organization with each tenant representing a department comes with the disadvantage that it will prevent queries from spanning multiple departments. This proposal tries to overcome those limitations.

## Alternatives considered

### Aggregation in PromQL API clients

In theory PromQL API clients could be aggregating/correlating query results from multiple tenants. For example Grafana could be used with multiple data sources and a cross tenant query could be achieved through using [Transformations][grafana_transformation].

As this approach comes with the following disadvantages, it was not considered further:

- Every PromQL API client needs to support the aggregation from various sources.

- Queries that are written in PromQL can't be used without extra work across tenants.

[grafana_transformation]: https://grafana.com/docs/grafana/latest/panels/transformations/

### Multi tenant aggregation in the query frontends

Another approach to multi tenant query federation could be achieved by aggregation of partial query results within the query frontend. For this a query needs to be split into sub queries per tenant and afterwards the partial results need reduced into the final result.

The [astmapper] package goes down a similar approach, but it cannot parallelize all query types. Ideally multi-tenant query federation should support the full PromQL language and the algorithms necessary would differ per query functions and operators used. This approach was deemed as a fairly complex way to achieve that tenant query federation.

[astmapper]: https://github.com/cortexproject/cortex/blob/f0c81bb59bf202db820403812e8dabcb64347bfd/pkg/querier/astmapper/parallel.go#L27

## Challenges

### Aggregate data without overlaps

#### Challenge

The series in different tenants might have exactly the same labels and hence potentially collide which each other.

#### Proposal

In order to be able to always identify the tenant correctly, queries using multiple tenants should inject a tenant label named `__tenant_id__` and its value containing the tenant ID into the results. A potentially existing label with the same name should be stored in a label with the prefix `original_`.

Label selectors containing the tenant label should behave like any other labels. This can be achieved by selecting the tenants used in a multi tenant query.

### Exposure of feature to the user

#### Challenge

The tenant ID is currently read from the `X-Scope-OrgID` HTTP header. The tenant ID has those [documented limitations][cortex_tenant_id] of values being allowed.

[cortex_tenant_id]: https://cortexmetrics.io/docs/guides/limitations/#tenant-id-naming

#### Proposal

For the query path a user should be able to specify a `X-Scope-OrgID` header with multiple tenant IDs.  Multiple tenant IDs should then be propagating throughout out the system until it reaches the querier. The `Queryable` instance returned by the querier component, is wrapped by a `mergeQueryable`, which will aggregate the results from a `Queryable` per tenant and hence treated by the downstream components as a single tenant query.

To allow such queries to be processed we suggest that an experimental configuration flag `-querier.tenant-federation.enabled` will be added, which is switched off by default. Once enabled the value of the `X-Scope-OrgID` header should be interpreted as `|` separated list of tenant ids. Components which are not expecting multiple tenant ids (e.g. the ingress path) must signal an error if multiple are used.


### Implementing Limits, Fairness and Observability for Cross-Tenant queries

#### Challenge

In Cortex the tenant id is used as the primary identifier for those components:

- The limits that apply to a certain query.

- The query-frontend maintains a per tenant query queue to implement fairness.

- Relevant metrics about the query are exposed under a `user` label.

Having a query spanning multiple tenants, the existing methods are no longer correct.

#### Proposal

The identifier for aforementioned features for queries involving more than a single tenant should be derived from: An ordered, distinct list of tenant IDs, which is joined by a `|`. This will produce a reproducible identifier for the same set of tenants no matter which order they have been specified.

While this feature is considered experimental, this provides some insights and ability to limit multi-tenant queries with these short comings:

- Cardinality costs to the possible amount of tenant ID combinations.

- Query limits applied to single tenants part of a multi tenant query are ignored.


## Conclusion

| Challenge                                                                | Status                       |
|--------------------------------------------------------------------------|------------------------------|
| Aggregate data without overlap                                           | Implementation in PR [#3250] |
| Exposure of feature to the user                                          | Implementation in PR [#3250] |
| Implementing Limits, Fairness and Observability for Cross-Tenant queries | Implementation in PR [#3250] |

[#3250]: https://github.com/cortexproject/cortex/pull/3250

### Future work

Those features are currently out of scope for this proposal, but we can foresee some interest implementing them after this proposal.

#### Cross Tenant support for the ruler

Ability to use multi tenant queries in the ruler.

#### Allow the identifier for limits, fairness and observability to be switched out

It would be great if the source identifier could be made more pluggable. This could allow to for example base all of those features on another dimension (e.g. users rather than tenants)

#### Allow customisation of the label used to expose tenant ids

As per this proposal the label name `__tenant_id__` is fixed, but users might want to be able to modify that through a configuration option.

#### Retain overlapping tenant id label values recursively

As per this proposal the tenant label injection retains an existing label value, but this is not implemented recursively. So if the result already contains `__tenant_id__` and `original__tenant_id__` labels, the value of the latter would be lost.
