---
title: "Ruler Tenant Federation"
linkTitle: "Ruler Tenant Federation"
weight: 1
slug: "Ruler Tenant Federation
---

- Author: [Rees Dooley](https://github.com/rdooley)
- Date: September 2021
- Status: Accepted

## Overview

This document aims to describe how to implement the ability to allow rules to cover data from more than a single Cortex tenant, here after refered to as federated rules. Since currently rules are owned by, query data from and save resulting series in the same tenant, this document aims to provide clear delineation of who owns a federated rule, what tenants the federated rule queries data from and where the federated rule saves resulting series.

## Reasoning

There are two primary use cases for allowing federated rules which query data from multiple tenants; administration of cortex and composite tenants.

In the case of the administration of cortex, when running Cortex within a large organization, there may be metrics spanning across tenants which might be desired to be monitored e.g. administrative metrics of the cortex system like `prometheus_rule_evaluation_failures_total` aggregated by `__tenant_id__`. In this case, a team e.g. `infra` may wish to be able to create a rule, owned by `infra` which queries multiple tenants `t0|t1|...|ti` and stores resulting series in `infra`. More generally a tenant `A` may wish to query data from other tenants `B|C|D` and store the data in a tenant which may be `A`, but could be `E`.

Additionally an organization may wish to treat several cortex tenants `t0|t1|...|ti` as one logical tenant due to compactor scalability. In this case a composite tenant `t0|t1|...|ti` would own a federated rule, which queries each subtenant `t0` thru `ti`. The resulting data could be sent to a specific tenant e.g. `admin` or as the composite tenant treats each subtenant uniformly, a random, but consistent subtenant can be chosen to store the resulting series in, in this case `tj` where 0 <= j <= i.
More explicitly, for a given recording rule and the produced series `foobarbaz` which is owned by the composite tenant `0|1|2|3`, a subtenant `0`, `1`, `2` or `3` is chosen to always store the series `foobarbaz` in, lets say `2`. Another rule and produced series `fizzbuzz` owned by the same composite tenant `0|1|2|3` makes this same chose of subtenant, in our case chosing `0`.

## Challenges

### Allow federated rules behind feature flag

#### Challenge

Federated tenant rules and alerts will not be a good fit for organization and should be behind a feature flag.

#### Proposal

For federated rules owned by a single tenant, creation of federated rules (those sourcing data from multiple tenants) should be blocked behind the feature flag `ruler.enable_federated_rules`

To support composite tenants, if tenant federation is enabled for ruler and alertmanager via `tenant-federation.enabled`, then ruler use a `mergeQueryable` to aggregate the results of querying multiple tenants. The ruler and alertmanager APIs should be updated to always call `tenant.GetTenantIDs` instead of `tenant.GetTenantID`, which will use the MultiTenantResolver when tenant federation is enabled.

### Allow federated rules only for select tenants

#### Challenge

For many organizations, the ability for any tenant to write a rule querying any other tenant is not acceptable and more fine grained control is required

#### Proposal

Since the current default is that a tenant should only be able to write rules against itself, we suggest a config option `ruler.allowed-federated-tenants`, a string slice of OrgIDs like `infra` or `0|1|2|3|4` which are allowed to write rules against all tenants. If a tenant `bar` attempts to create a federated rule, an error should be returned by the ruler api. Similarly an option `ruler.disallowed-federated-tenants` explicitly states a list of tenants for which federated rules are not allowed. Combining these in a `util.AllowedTenants` should allow us to quickly determine if federation is enabled or disabled for a given tenant at rule creation.

### Where to store resulting series of federated rule

#### Challenge

A single tenant rule always stores produced series in the tenant where the rule exists. This 1 -> 1 mapping becomes a many -> 1 mapping for federated rules.

#### Proposal

As some use cases will demand that a specific federated rule, querying tenants `B` and `C`, is stored in the owning team's tenant `A`, an option to allow explicit assignment of destination tenant for a federated rule is needed. Some use cases where a set of tenants `A|B|C|...|Z` are being treated as a single logical tenant when querying this explicit assignment of destination tenant is not needed.

To allow both of these, we suggest an additional field to the rules proto `dest_tenant_id` which determines which tenant to send the series produced by the rule to, used if and only if the rule is a federated rule. If this field is not given, for composite tenants a random but consistent subtenant of the multiple tenants owning the rule is chosen using a hashmod of the series label to determine the subtenant and for single tenants owning a federated rule the resulting series is saved in the tenant which owns the rule.

### Which tenants to query from for federated rules

#### Challenge

A single tenant rule always queries the tenant which owns the rule. This 1 -> 1 mapping becomes a 1 -> many mapping for federated rules.

#### Proposal

As some use cases will demand that a specific federated rule, querying tenant B and C, is stored in the owning teams tenant A, an option to allow explicit assignment of source tenants for a federated rule is needed. In the case of a composite tenant where a set of tenants `A|B|C|...|Z` are being treated as a single logical tenant when querying this explicit assignment of destination tenant isn't explicitly called for, but could prove useful.

To support both of these use cases, we suggest an additional field `source_tenant_ids` on the rule containing an OrgID string e.g. `t0|t1|...|ti` which when present determines which tenants to query for the given rule. In the case of a composite tenant this field would be optional, as ownership of a rule by a composite tenant implies source tenants e.g. for the composite tenant `t0|t1|...|ti` the natural source sub tenants would be `t0`, `t1`, `t2` etc.

## Conclusion

| Challenge                                                                | Status                                |
|--------------------------------------------------------------------------|---------------------------------------|
| Allow federated rules behind feature flag                                | Implementation planned for PR [#4470] |
| Allow federated rules only for select tenants                            | Implementation planned for PR [#4470] |
| Where to store resulting series of federated rules                       | Implementation planned for PR [#4470] |
| Which tenants to query from for federated rules                          | Implementation planned for PR [#4470] |

[#4470]: https://github.com/cortexproject/cortex/pull/4470
