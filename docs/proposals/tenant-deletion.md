---
title: "Deletion of Tenant Data from Blocks Storage"
linkTitle: "Deletion of Tenant Data from Blocks Storage"
weight: 1
slug: tenant-deletion
---

- Author: [Peter Stibrany](https://github.com/pstibrany)
- Date: November 2020
- Status: Proposed

## Deletion of tenant data

## Problem

When a tenant is deleted from the external system that controls access to Cortex, we want to clean up tenants data from Cortex as well.

## Background

When using blocks storage, Cortex stores tenant’s data in several places:
- object store for long-term storage of blocks,
- ingesters disks for short-term storage. Ingesters eventually upload data to long-term storage,
- various caches: query-frontend, chunks, index and metadata,
- object store for rules (separate from blocks),
- object store for alert manager configuration,
- state for alertmanager instances (notifications and silences).

This document expects that there is an external authorization system in place.
Disabling or deleting the tenant in this system will stop tenants data, queries and other API calls from reaching Cortex.
Note that there may be a delay before disabling or deleting the user, until data / queries fully stop, due to eg. caching in authorization proxies.
Cortex endpoint for deleting a tenant data should be only called after this blocking is in place.

## Proposal

### API Endpoints

#### POST /purger/delete_tenant

We propose to introduce an `/purger/delete_tenant` API endpoint to trigger data deletion for the tenant.

While this endpoint works as an “admin” endpoint and should not be exposed directly to tenants, it still needs to know under which tenant it should operate.
For consistency with other endpoints this endpoint would therefore require an `X-Scope-OrgID` header and use the tenant from it.

It is safe to call “delete_tenant” multiple times.

#### GET /purger/delete_tenant_status

To monitor the state of data deletion, another endpoint will be available: `/purger/delete_tenant_status`.
This will return OK if tenant’s data has been fully deleted – no more blocks exist on the long-term storage, alertmanager and rulers are fully stopped and configuration removed.
Similarly to “delete_tenant” endpoint, “delete_tenant_status” will require `X-Scope-OrgID` header.

### Implementation (asynchronous)

Purger will implement both API endpoints.
Upon receiving the call to `/purger/delete_tenant` endpoint, Purger will initiate the deletion by writing “deletion marker” objects for specific tenant to following buckets:

- Blocks bucket
- Ruler configuration bucket
- Alertmanager configuration bucket

Deletion marker for the tenant will be an object stored under tenant prefix, eg. “/<tenant>/deleted”.
This object will contain the timestamp when it was created, so that we can delete it later based on the specified time period.
We could reuse a subset of the proposed Thanos tombstones format, or use custom format.

“delete_tenant_status” endpoint will report success, if all of the following are true:
- There are no blocks for the tenant in the blocks bucket
- There is a “deletion finished” object in the bucket for the tenant in the ruler configuration bucket.
- There is a “deletion finished” object in the bucket for the tenant in the alertmanager configuration bucket.

See later sections on Ruler and Alertmanager for explanation of “deletion finished” objects.

### Blocks deletion marker

Blocks deletion marker will be used by compactor, querier and store-gateway.

#### Compactor

Upon discovering the blocks deletion marker, the compactor will start deletion of all blocks that belong to the tenant.
This can take hours to finish, depending on the number of blocks.
Even after deleting all blocks on the storage, ingesters may upload additional blocks for the tenant.
To make sure that the compactor deletes these blocks, the compactor will keep the deletion marker in the bucket.
After a configurable period of time it can delete the deletion marker too.

To implement deletion, Compactor should use new TenantsCleaner component similar to existing BlocksCleaner (which deletes blocks marked for deletion), and modify UserScanner to ignore deleted tenants (for BlocksCleaner and Compactor itself) or only return deleted tenants (for TenantsCleaner).

#### Querier, Store-gateway

These two components scan for all tenants periodically, and can use the tenant deletion marker to skip tenants and avoid loading their blocks into memory and caching to disk (store-gateways).
By implementing this, store-gateways will also unload and remove cached blocks.

Queriers and store-gateways use metadata cache and chunks cache when accessing blocks to reduce the number of API calls to object storage.
In this proposal we don’t suggest removing obsolete entries from the cache – we will instead rely on configured expiration time for cache items.

Note: assuming no queries will be sent to the system once the tenant is deleted, implementing support for tenant deletion marker in queriers and store-gateways is just an optimization.
These components will unload blocks once they are deleted from the object store even without this optimization.

#### Query Frontend

While query-frontend could use the tenant blocks deletion marker to clean up the cache, we don’t suggest to do that due to additional complexity.
Instead we will only rely on eventual eviction of cached query results from the cache.
It is possible to configure Cortex to set TTL for items in the frontend cache by using `-frontend.default-validity` option.

#### Ingester

Ingesters don’t scan object store for tenants.

To clean up the local state on ingesters, we will implement closing and deletion of local per-tenant data for idle TSDB. (See Cortex PR #3491).
This requires additional configuration for ingesters, specifically how long to wait before closing and deleting TSDB.
This feature needs to work properly in two different scenarios:
- Ingester is no longer receiving data due to ring changes (eg. scale up of ingesters)
- Data is received because user has been deleted.

Ingester doesn’t distinguish between the two at the moment.
To make sure we don’t break queries by accidentally deleting TSDB too early, ingester needs to wait at least `-querier.query-ingesters-within` duration.

Alternatively, ingester could check whether deletion marker exists on the block storage, when it detects idle TSDB.

**If more data is pushed to the ingester for a given tenant, ingester will open new TSDB, build new blocks and upload them. It is therefore essential that no more data is pushed to Cortex for the tenant after calling the “delete_tenant” endpoint.**

#### Ruler deletion marker

This deletion marker is stored in the ruler configuration bucket.
When rulers discover this marker during the periodic sync of the rule groups, they will
- stop the evaluation of the rule groups for the user,
- delete tenant rule groups (when multiple rulers do this at the same time, they will race for deletion, and need to be prepared to handle possible “object not found” errors)
- delete local state.

When the ruler is finished with this cleanup, it will ask all other rulers if they still have any data for the tenant.
If all other rulers reply with “no”, ruler can write “deletion finished” marker back to the bucket.
This allows rulers to ignore the ruler completely, and it also communicates the status of the deletion back to purger.

Note that ruler currently relies on cached local files when using Prometheus Ruler Manager component.
[This can be avoided now](https://github.com/cortexproject/cortex/issues/3134), and since it makes cleanup simpler, we suggest to modify Cortex ruler implementation to avoid this local copy.

**Similarly to ingesters, it is necessary to disable access to the ruler API for deleted tenant.
This must be done in external authorization proxy.**

#### Alertmanager deletion marker

Deletion marker for alert manager is stored in the alertmanager configuration bucket. Cleanup procedure for alertmanager data is similar to rulers – when individual alertmanager instances discover the marker, they will:
- Delete tenant configuration
- Delete local notifications and silences state
- Ask other alertmanagers if they have any tenant state yet, and if not, write “deletion finished” marker back to the bucket.

To perform the last step, Alertmanagers need to find other alertmanager instances. This will be implemented by using the ring, which will (likely) be added as per [Alertmanager scaling proposal](https://github.com/cortexproject/cortex/pull/3574).

Access to Alertmanager API must be disabled for tenant that is going to be deleted.

## Alternatives Considered

Another possibility how to deal with tenant data deletion is to make purger component actively communicate with ingester, compactor, ruler and alertmanagers to make data deletion faster. In this case purger would need to understand how to reach out to all those components (with multiple ring configurations, one for each component type), and internal API calls would need to have strict semantics around when the data deletion is complete. This alternative has been rejected due to additional complexity and only small benefit in terms of how fast data would be deleted.

