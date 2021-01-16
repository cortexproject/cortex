---
title: "Retention of Tenant Data from Blocks Storage"
linkTitle: "Retention of Tenant Data from Blocks Storage"
weight: 1
slug: tenant-retention
---

- Author: [Allenzhli](https://github.com/Allenzhli)
- Date: January 2021
- Status: Proposed

## Retention of tenant data

## Problem

Metric data is growing over time per-tenant, at the same time, the value of data decreases. We want to have a retention policy like prometheus does. In Cortex, data retention is typically achieved via a bucket policy. However, this has two main issues:

1. Not every backend storage support bucket policies
2. Bucket policies don't easily allow a per-tenant custom retention

## Background

### tenant
When using blocks storage, Cortex stores tenant’s data in object store for long-term storage of blocks, tenant id as part of the object store path. We discover all tenants via scan the root dir of bucket.

### runtime config
Using the "overrides" mechanism (part of runtime config) already allows for per-tenant settings. See https://cortexmetrics.io/docs/configuration/arguments/#runtime-configuration-file for more details. Using it for tenant retention would fit nicely. Admin could set per-tenant retention here, and also have a single global value for tenants that don't have custom value set.

## Proposal

### retention component

We propose to introduce an new runtime config component `retention`.

We can store per-tenant settings and global setting in the retention component. As a runtime config, the settings is reload period (which defaults to 10 seconds), so we can update the settings onfly. 

Per-tenant retention configuration will be applyed for in follow order:

* tenant specified value
* global value

For each tenant, if a tenant-specific value exists, it will be used directly, otherwise, if a global value exists, then the global value will be used; If neither exists, do nothing.

### Implementation 

The runtime config will add a retention component which include a dictionary to store per-tenant retention value and global retention value.

Compactor will check per-tanant retention value and compare to meta info of blocks. The blocks that match `maxTime < now - retention` will be marked for delete.

## Compactor

Upon discovering the tenant, the compactor will check following markers:

- tenant delete marker
- block delete marker

If tenant delete marker exists, compactor use TenantCleaner delete tenant all data regardless of whether the block delete marker exists.
If only block delete marker exists, compactor use BlocksCleaner delete the block after `deletion_delay` duration.