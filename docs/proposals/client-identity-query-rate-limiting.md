---
title: "Client-Identity Query Rate Limiting"
linkTitle: "Client-Identity Query Rate Limiting"
weight: 1
slug: client-identity-query-rate-limiting
---

- Author: Krunal Jain
- Date: July 2026
- Status: Proposed

## Background

This is a follow-on to
[Client-Identity Ingestion Rate Limiting](./client-identity-ingestion-rate-limiting.md) (write path), applying the same
idea to queries.

Cortex has no per-request-rate limiter on the read path today. What exists is a different kind of
control: a per-tenant limit on how many requests can be *queued at once*, and a family of per-query
cost limits that bound how *expensive* a single query can be. Neither bounds *how many queries per
second* a tenant's traffic issues, and, same as the write path, neither can distinguish one query
source from another within a shared tenant.

The query-frontend already reads and logs a couple of per-request identifying headers today, purely
for observability (Grafana's dashboard and panel identifiers, included in slow-query log lines).
These headers are not used for enforcement today, only logging, and this proposal does not use them
for identity either; it uses a new, generic header instead (see Proposed Design).

A single noisy dashboard, a runaway alert rule evaluating too frequently, or one team's ad hoc
Explore usage can all generate a query load that consumes a shared tenant's query-processing
capacity (queriers, store-gateways) at the expense of every other user/dashboard under that same
`X-Scope-OrgID`, mirroring exactly the ingestion-side problem the write-path proposal addresses,
just on reads.

## Problem

Provide a query-rate-limiting dimension *below* the tenant, at the query-frontend, so that one noisy
query source cannot degrade query performance for every other user sharing a tenant.

Requirements (mirroring the write-path proposal):

- **Opt-in and backward compatible.** Deployments that don't set the identity header, or don't
  enable this feature, see no behavior change.
- **Per-tenant configurable**, following the same default-plus-per-tenant-override pattern used
  throughout Cortex.
- **Presence-gated, not mandatory.** A request without the identity header is not rejected; it is
  simply not subject to the additional per-identity check, and counts only toward whatever
  tenant-level behavior already exists today (which, notably, currently has no *rate* limit to fall
  back to on the read path, see Interaction with Existing Limits).
- **Same generic, gateway-set identity header as the write-path proposal (`X-Scope-ClientID`), not a
  per-client-application-specific one.** Consistent with the write-path proposal's principle:
  Cortex trusts one opaque identity value, set by whatever sits in front of it, the same way it
  already trusts `X-Scope-OrgID`. It does not special-case any particular client application.

## Out of Scope

- The write (ingestion) path: covered by the companion proposal,
  [Client-Identity Ingestion Rate Limiting](./client-identity-ingestion-rate-limiting.md). The two share a design
  philosophy (trusted, gateway-forwarded identity header, opt-in, additive to existing limits) and,
  as of this revision, the *same* identity header. They are kept as separate proposals because they
  have different enforcement points (distributor vs. query-frontend), not because of any difference
  in identity source.
- Per-query cost limiting: unaffected and orthogonal; this proposal bounds *frequency*, those bound
  *cost per query*.
- Any new authentication mechanism, and any client-application-specific identity header. Exactly
  like the write-path proposal, this reuses a header trusted the same way `X-Scope-OrgID` already
  is; it does not add token validation, a new identity protocol, or special-cased handling for any
  particular upstream client.
- Enforcement in the querier itself (as opposed to the query-frontend/scheduler). The query-frontend
  is the natural analog to the distributor here: it is the first internal component every query
  passes through, exactly as the distributor is for writes.

## Proposed Design

### Identity extraction

The same generic, trusted header as the write-path proposal, `X-Scope-ClientID`, read once when a query
request enters the query-frontend, alongside the existing Grafana dashboard/panel header reads.
Using the exact same header and identity-extraction approach as the write-path proposal means a
deployment that sets `X-Scope-ClientID` once, at its gateway, for all Cortex-bound traffic gets consistent
per-client identity across both ingestion and queries with no additional configuration: one
identity mechanism, reused at both enforcement points, rather than two different ones.

### Configuration

New limits, following the same pattern as the write-path proposal:

```yaml
# Default (flags), applied to all tenants without an override
frontend:
  client_identity_query_rate_limit: 0        # 0 = disabled (default; no behavior change)
  client_identity_query_burst_size: 0

# Per-tenant override via runtime config
overrides:
  tenant-123:
    client_identity_query_rate_limit: 50
    client_identity_query_burst_size: 100
    # Per-tenant cap on how many distinct tracked client entries the per-client rate limiter
    # keeps in memory for this tenant; oldest-used entries are evicted once this is reached.
    # See "Bounding memory" in the write-path proposal for eviction semantics.
    client_identity_query_tracked_clients_limit: 1000
```

Units are queries per second (matching how Cortex's existing outstanding-requests limit is already
a count, not a byte/sample rate; this is a request-frequency limit, deliberately independent of
query cost).

### Enforcement

Same two-gate structure as the write-path proposal:

1. The tenant has a non-zero `client_identity_query_rate_limit` configured.
2. The request carries a non-empty `X-Scope-ClientID` value.

```
Query request arrives at query-frontend
     │
     v
┌──────────────────────────────┐    no (either gate)    ┌─────────────────┐
│ X-Scope-ClientID present AND │───────────────────────>│ Continue        │
│ tenant limit configured?     │                        │ (queue/forward) │
└──────────────────────────────┘                        └─────────────────┘
     │ yes
     v
┌───────────────────────────┐          fail          ┌──────────────┐
│ Per-(tenant, client) rate │───────────────────────>│ 429 Too Many │
│ limit check (new)         │                        │ Requests     │
└───────────────────────────┘                        └──────────────┘
     │ pass
     v
Continue (queued locally, or forwarded to the scheduler, depending on deployment topology)
```

A new rate limiter instance, using the same ring-based distributed enforcement as the
write-path proposal, is added at the query-frontend, keyed by tenant and client identity together. This reuses
the query-frontend's existing rejected-request counter and discard-reason pattern, so a rejection
under this feature is observable the same way existing query-frontend rejections already are.

The rate-limiter strategy backing it mirrors the write-path proposal's approach exactly: it splits
the combined tenant/client key and looks up the per-tenant configured limit, the same per-tenant
limits interface pattern already used elsewhere for query-side limits.

### Where this runs relative to the frontend/scheduler split

Cortex's query-frontend can run with or without a separate query-scheduler. The query-frontend's
request handler runs in the query-frontend process in both configurations: it is the entrypoint
before a request is either queued locally or forwarded to the scheduler, so enforcing here, rather
than inside the scheduler's own queueing logic, covers both deployment topologies with one check.

Each query-frontend replica uses the same ring-based distributed enforcement as the write-path
proposal: the `(tenant, client)` key is hashed onto the existing ring, and the owning replica
maintains the authoritative per-tenant, per-client counter. A client cannot route around the limit
by landing on a less-loaded frontend replica, and enforcement stays accurate as replicas scale up
or down without any reconfiguration of per-replica shares.

### Bounding memory: capped tracking, not an unbounded map

Same concern as the write-path proposal, and the same fix: the number of distinct tenant+client
keys here is bounded only by how many distinct `X-Scope-ClientID` values are presented to the
query-frontend, so tracking is capped per tenant with least-recently-used eviction rather than kept
in a plain unbounded map, exactly as described in the write-path proposal's equivalent section
(including its discussion of the enforcement consequence of eviction). The two caps are configured
independently per tenant (see Configuration above) since the query-frontend and distributor are
separate processes with independent memory budgets, but the mechanism and rationale are identical.

### Distributed enforcement and interaction with the results cache

This proposal uses the same ring-based distributed enforcement as the write-path proposal: each
`(tenant, client)` key is sharded onto the existing ring, and the owning query-frontend replica
maintains the authoritative per-tenant, per-client counter. A client cannot route around the
per-tenant limit by spreading requests across frontend replicas, and enforcement stays accurate as
replicas scale without any per-replica reconfiguration.

A read-path-specific question remains: the query-frontend serves some requests entirely from its
results cache, without ever reaching a querier or store-gateway. Enforcing the per-client check
before knowing whether a request will be a cache hit means a client issuing frequent but cheap,
repeated, cacheable queries (for example, an auto-refreshing dashboard hitting the same query
window) could be throttled even though those requests would have cost negligible querier/store
gateway capacity. Given this feature's stated goal is protecting shared query-processing capacity,
not query volume for its own sake, this is worth resolving before this feature is considered
complete: either check cache-eligibility first and only rate-limit the cache-miss path, or accept
the imprecision in a first version and document it clearly as a known limitation. This proposal
does not resolve that choice; it is called out explicitly as an open question below rather than
silently deferred.

### Metrics

- Reuses the query-frontend's existing rejected-query counter with a new discard reason for this
  limit; no new metric family needed, consistent with how other query-frontend rejections are
  already tracked.
- Per-client rejection visibility is obtained from query-frontend logs rather than a dedicated
  metric label, the same way dashboard/panel identifiers already are, and for the same reason as
  the write-path proposal: adding a client label to an existing metric that already carries several
  labels would multiply its cardinality by however many distinct clients are tracked.

### Interaction with existing limits

- **Orthogonal to per-query cost limits**: those bound how expensive one query is; this bounds how
  many queries per second one client issues.
- **Orthogonal to the existing per-tenant outstanding-requests limit**: that bounds queue depth (how
  many requests can be waiting at once); this bounds arrival rate. A client could stay under the
  queue depth limit while still issuing queries fast enough to starve other queue occupants of
  querier time; this closes that gap.
- **Consistent with the write-path proposal's philosophy**: purely additive, off by default,
  identical two-gate opt-in structure, same underlying rate-limiting mechanism.

## Value of the Identity Header Beyond Rate Limiting

The same `X-Scope-ClientID` header established on the write path (see the companion proposal) carries
equivalent value on the read path once it is available:

- **Query resource consumption per client.** Query execution time, querier CPU, and store-gateway
  reads can all be attributed per client in logs, enabling identification of expensive query
  sources — runaway alert rules, auto-refreshing dashboards, ad hoc exploration — without manual
  log correlation.
- **Per-client slow query attribution.** The query-frontend already logs slow queries; with
  `X-Scope-ClientID` present, slow query log lines carry the client identity directly, making it
  straightforward to identify which client or dashboard is responsible without cross-referencing
  external systems.
- **Future per-client query cost quotas.** Once client identity is a reliable, trusted dimension on
  the read path, per-client cost controls — for example, a cap on total querier time or
  store-gateway bytes scanned per client per interval — become implementable without any additional
  identity plumbing.

These are downstream opportunities enabled by this proposal's identity header, not in scope here.

## Rollout Plan

Same phased approach as the write-path proposal: introduced as an **experimental** feature with
global enforcement, disabled by default (`client_identity_query_rate_limit: 0`). Graduation out of
experimental status follows once operational experience from Phase 1, including resolution of the
cache-interaction question above, confirms the design is sound.

## Alternatives Considered

- **Enforce in the scheduler instead of the frontend.** Would require either scheduler-side rate
  limiting to be per-scheduler-replica (fine, but then it needs its own identical mechanism since
  the scheduler is a separate component from the frontend) or shared/global state across scheduler
  replicas, a much larger change mirroring the local-vs-global split the write-path proposal already
  has to consider. Enforcing once in the frontend, before a request is ever queued or forwarded, is
  simpler and sufficient for a first version; a global variant could be considered later exactly as
  it was for the write path.
- **A single combined proposal covering both ingestion and query paths.** Considered and explicitly
  rejected; see the write-path proposal's cross-reference to this one. Different enforcement
  points and independent review/merge risk argue for two focused proposals over one broad one, even
  though both now share the same identity mechanism.

## Open Questions

- Should this feature ship together with the write-path proposal, or fully independently, given they
  touch different components and teams (distributor vs. query-frontend) might review them
  separately? Leaning towards shipping independently, sharing only the small identity-extraction
  logic, given the review benefits of keeping them as separate changes discussed above.
- What's the right default for the per-tenant tracked-clients cap? The cap is configurable per
  tenant via `client_identity_query_tracked_clients_limit` in the overrides; evictions caused by
  hitting the cap are observable via a dedicated eviction counter, letting operators distinguish
  "cap is properly sized" from "we're actively being hit with identity churn" without needing to
  inspect memory profiles.
- Should the per-client query limit be expressible as a percentage of the tenant's overall query
  capacity, similar to how Cortex's existing max-queriers-per-tenant limit supports
  fractional/percentage values, rather than an absolute queries/second number? Left as a fast-follow
  refinement rather than blocking the initial absolute-number implementation.
- Should the per-client check run before or after results-cache eligibility is known (see "Distributed enforcement and interaction with the results cache" above)? Checking after would avoid
  throttling cheap, cache-served requests, at the cost of being a slightly more invasive change to
  the request handling order; checking before (as currently proposed) is simpler but risks
  throttling based on request *count* rather than actual resource consumption. This should be
  resolved before the feature is considered ready to implement, not left as a known limitation by
  default.
