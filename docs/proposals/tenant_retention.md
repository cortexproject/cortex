---
title: "Retention of Tenant Data from Blocks Storage"
linkTitle: "Retention of Tenant Data from Blocks Storage"
weight: 1
slug: tenant-retention
---

- Author: [Allenzhli](https://github.com/Allenzhli)
- Date: December 2020
- Status: Proposed

## Retention of tenant data

## Problem

In Cortex, we are missing retention of blocks storage. The metric data is growing over time per-tenant, at the same time, the value of data decreases. We want to support retention time of metric data like prometheus does.

## Background

When using blocks storage, Cortex stores tenant’s data in object store for long-term storage of blocks, tenant id as part of the object store path. We discover all tenants via scan the root dir of bucket.

## Proposal

### API Endpoints

#### POST /compactor/retention_tenant

We propose to introduce an `/compactor/retention_tenant` API endpoint to set data retention for the tenant.

While this endpoint works as an “admin” endpoint and should not be exposed directly to tenants, it still needs to know under which tenant it should operate.
For consistency with other endpoints this endpoint would therefore require an `X-Scope-OrgID` header and use the tenant from it.

It is safe to call “retention_tenant” multiple times.

#### GET /compactor/retention_tenant

To query the state of data retention, an endpoint will be available: `/compactor/retention_tenant`.
This will return the retention of current tenant.
For consistency with other endpoints this endpoint would therefore require an `X-Scope-OrgID` header and use the tenant from it.

### Implementation (asynchronous)

Compactor will implement both API endpoints.
Upon receiving the call to `/compactor/retention_tenant` endpoint, Compactor will initiate the retention by writing “retetion marker” objects for specific tenant to blocks bucket.

Retention marker for the tenant will be an object stored under tenant prefix, eg. “/<tenant>/retention”.
This object will contain the retention time when it was created, so that we can calculate and check if a block should be deleted.
For every clean user period, we can check if any blocks should be retention, and the other should be delete.
Delete a block also use deletetion marker.

We could reuse a subset of the proposed Thanos tombstones format, or use custom format.

### Blocks deletion marker

Blocks deletion marker will be used by compactor, querier and store-gateway.

#### Compactor

Upon discovering the tenant, the compactor will check following markers:

- tenant delete marker
- tenant retention marker

if only tenant retention exists, the compactor will check retention of all blocks that belong to the tenant after compact. The blocks which match `maxTime < now - retention` will be marked for delete.