---
title: "Retention of Tenant Data from Blocks Storage"
linkTitle: "Retention of Tenant Data from Blocks Storage"
weight: 1
slug: tenant-retention
---

- Author: [Allenzhli](https://github.com/Allenzhli)
- Date: January 2021
- Status: Accepted, Implemented in [PR #3879](https://github.com/cortexproject/cortex/pull/3879).

## Retention of tenant data

## Problem

Metric data is growing over time per-tenant, at the same time, the value of data decreases. We want to have a retention policy like prometheus does. In Cortex, data retention is typically achieved via a bucket policy. However, this has two main issues:

1. Not every backend storage support bucket policies
2. Bucket policies don't easily allow a per-tenant custom retention

## Background

### tenants
When using blocks storage, Cortex stores tenant’s data in object store for long-term storage of blocks, tenant id as part of the object store path. We discover all tenants via scan the root dir of bucket.

### runtime config
Using the "overrides" mechanism (part of runtime config) already allows for per-tenant settings. See [runtime-configuration-file](https://cortexmetrics.io/docs/configuration/arguments/#runtime-configuration-file) for more details. Using it for tenant retention would fit nicely. Admin could set per-tenant retention here, and also have a single global value for tenants that don't have custom value set.

## Proposal

### retention period field

We propose to introduce just one new field `RetentionPeriod` in the Limits struct(defined at pkg/util/validation/limits.go).

`RetentionPeriod` setting how long historical metric data retention period per-tenant. `0` is disable.

Runtime config is reloaded periodically (defaults to 10 seconds), so we can update the retention settings on-the-fly.

For each tenant, if a tenant-specific *runtime_config* value exists, it will be used directly, otherwise, if a default *limits_config* value exists, then the default value will be used; If neither exists, do nothing.

### Implementation

A BlocksCleaner within the Compactor run periodically (which defaults to 15 minutes) and the retention logic will insert into it. The logic should compare retention value to block `maxTime` and blocks that match `maxTime < now - retention` will be marked for delete.

Blocks deletion is not immediate, but follows a two steps process. See [soft-and-hard-blocks-deletion](https://cortexmetrics.io/docs/blocks-storage/compactor/#soft-and-hard-blocks-deletion)
