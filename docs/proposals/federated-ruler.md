---
title: "Ruler Tenant Federation"
linkTitle: "Ruler Tenant Federation"
weight: 1
slug: "ruler-tenant-federation"
---

- Author: [Rees Dooley](https://github.com/rdooley)
- Date: November 2021
- Status: Accepted

## Overview

This document aims to describe how to implement the ability to allow rules to cover data from more than a single Cortex tenant, here after referred to as federated rules. Since currently rules are owned by, query data from and save resulting series in the same tenant, this document aims to provide clear delineation of who owns a federated rule, what tenants the federated rule queries data from and where the federated rule saves resulting series.

A federated rule is any rule which contains the `src_tenants` field.

## Reasoning

The primary use case for allowing federated rules which query data from multiple tenants is the administration of cortex.

In the case of the administration of cortex, when running Cortex within a large organization, there may be metrics spanning across tenants which might be desired to be monitored e.g. administrative metrics of the cortex system like `prometheus_rule_evaluation_failures_total` aggregated by `__tenant_id__`. In this case, a team e.g. `infra` may wish to be able to create a rule, owned by `infra` which queries multiple tenants `t0|t1|...|ti` and stores resulting series in `infra`.

## Challenges

### Allow federated rules behind feature flag

#### Challenge

Federated tenant rules and alerts will not be a good fit for organization and should be behind a feature flag.

#### Proposal

For federated rules, creation of federated rules (those sourcing data from multiple tenants) should be blocked behind the feature flag `ruler.enable-federated-rules`

If tenant federation is enabled, then ruler should use a `mergeQueryable` to aggregate the results of querying multiple tenants.

### Allow federated rules only for select tenants

#### Challenge

For many organizations, the ability for any tenant to write a rule querying any other tenant is not acceptable and more fine grained control is required

#### Proposal

Since the current default is that a tenant should only be able to write rules against itself, we suggest a config option `ruler.allowed-federated-tenants`, a string slice of OrgIDs like `infra` or `0|1|2|3|4` which are allowed to write rules against all tenants. If a tenant `bar` attempts to create a federated rule, an error should be returned by the ruler api. Similarly an option `ruler.disallowed-federated-tenants` explicitly states a list of tenants for which federated rules are not allowed. Combining these in a `util.AllowedTenants` should allow one to quickly determine if federation is enabled or disabled for a given tenant at rule creation.

### Where to store resulting series of federated rule

#### Challenge

A single tenant rule always stores produced series in the tenant where the rule exists. This 1 -> 1 mapping becomes a many -> 1 mapping for federated rules.

#### Proposal

Tenants owning a federated rule the resulting series is saved in the tenant which owns the rule.

### Which tenants to query from for federated rules

#### Challenge

A single tenant rule always queries the tenant which owns the rule. This 1 -> 1 mapping becomes a 1 -> many mapping for federated rules.

#### Proposal

As some use cases will demand that a specific federated rule, querying tenant B and C, is stored in the owning teams tenant A, an option to allow explicit assignment of source tenants for a federated rule is needed.

To support this we suggest an additional field `src_tenants` on the rule group containing an array of OrgIDs e.g. `[t0,t1,...,ti]` which when present determines which tenants to query for the given rule. Rule group is chosen as it reduces repetition between rules.

## Conclusion

| Challenge                                                                | Status                                |
|--------------------------------------------------------------------------|---------------------------------------|
| Allow federated rules behind feature flag                                | Planned but not yet implemented       |
| Allow federated rules only for select tenants                            | Planned but not yet implemented       |
| Where to store resulting series of federated rules                       | Planned but not yet implemented       |
| Which tenants to query from for federated rules                          | Planned but not yet implemented       |
