---
title: "Client-Identity Rate Limiting"
linkTitle: "Client-Identity Rate Limiting"
weight: 1
slug: client-identity-rate-limiting
---

- Author: Krunal Jain
- Date: July 2026
- Status: Proposed

## Background

Cortex enforces ingestion rate limits at the tenant level only. Every request is identified solely
by the `X-Scope-OrgID` header value, and the distributor's rate limiter applies a single shared
budget, local or global, across everything sent under that tenant.

In practice, a single tenant is very often not a single writer. Multiple independent services,
teams, or clusters frequently share one `X-Scope-OrgID` (splitting tenants further has real
operational cost: more ring shards, more per-tenant limits to tune, more override entries to
maintain). When that happens, today's rate limiter cannot distinguish a well-behaved writer from a
noisy or misbehaving one sharing the same tenant. A single client scaling up unexpectedly (bad
config, retry storm, new deployment, or many parallel agent instances fanning out under one shared
tenant) can exhaust the *entire tenant's* ingestion budget and start throttling every other
legitimate writer sharing that org ID, even though none of the throttled traffic was the cause.

Cortex's trust model already has precedent for exactly this shape of problem at the tenant level:
`X-Scope-OrgID` itself is a plain, unauthenticated-by-Cortex header. Cortex trusts it entirely
because it assumes something in front of it (a reverse proxy, an auth gateway such as the one
described in the [Authentication Gateway](./auth-gateway.md) proposal) authenticates the caller and
sets the header correctly, and that untrusted clients can never reach Cortex directly. There is no
equivalent mechanism today for identifying *who, within a tenant,* sent a given write.

## Problem

Provide a second, optional rate-limiting dimension *below* the tenant: throttle by client identity,
where identity is an opaque string supplied by the same trusted gateway/proxy layer that already
sets `X-Scope-OrgID`, so that one noisy client cannot exhaust a shared tenant's ingestion budget for
everyone else.

Requirements:

- **Opt-in and backward compatible.** Deployments that don't set the identity header, or don't
  enable this feature, see no behavior change; the tenant-level limiter continues to work exactly
  as it does today.
- **Per-tenant configurable**, following the same default-plus-per-tenant-override pattern as every
  other Cortex limit.
- **Consistent trust model with the rest of Cortex.** The identity header is trusted the same way
  `X-Scope-OrgID` already is: because it arrives from a trusted network path, not because Cortex
  independently verifies it. This proposal does not attempt to add a stronger guarantee than
  Cortex's existing multi-tenancy model provides; it reuses the same assumption rather than
  introducing a new one.
- **Presence-gated, not mandatory.** A request without the identity header is not rejected or
  treated as suspicious; it simply isn't subject to the additional per-identity check, and its
  usage counts only toward the tenant's existing aggregate budget, unchanged from today.

## Out of Scope

- Any form of authentication Cortex does not already support (JWT validation, OAuth, SSO, mTLS
  client-cert identity). An earlier draft of this proposal considered deriving identity from a
  verified TLS client certificate; that was dropped because it only works when Cortex itself
  terminates client TLS, which excludes the common case of a service mesh (Istio, Linkerd) or
  gateway terminating mTLS in front of Cortex, a real gap given Cortex's own case studies document
  exactly this kind of mesh-fronted deployment. Reusing the existing `X-Scope-OrgID` trust model
  avoids that gap entirely.
- Cross-tenant limiting or identity: this is purely a sub-division of the existing per-tenant
  ingestion rate limit.
- Read-path (query) rate limiting by client identity is covered by a **separate, companion
  proposal**, [Client-Identity Query Rate Limiting](./client-identity-query-rate-limiting.md), kept
  independent of this one because it has a different enforcement point (query-frontend, not
  distributor); it reuses the same `X-User-ID` header and identity-extraction approach introduced
  here. Filed alongside this proposal rather than folded into it, so each can be reviewed and
  merged on its own timeline.
- Defining *how* an operator's gateway derives the identity value (API key ID, service account,
  mesh workload identity, job name, etc.). Exactly like Cortex never defines what a "tenant" is
  organizationally, this proposal treats the identity as an opaque string the gateway is trusted to
  set consistently, which is out of scope for Cortex itself to prescribe.

## Proposed Design

### Identity extraction

Add a new trusted header, `X-User-ID`, read once when a write request enters Cortex. This mirrors
the existing `X-Scope-OrgID` header handling: like `X-Scope-OrgID`, `X-User-ID` is trusted as-is,
because Cortex assumes it is set by a trusted gateway/proxy in front of it, not supplied directly by
an untrusted caller. If the header is absent, the request is unaffected by anything in this
proposal.

Cortex's HTTP library dependency already defines an `X-Scope-UserID` header, distinct from the
`X-User-ID` proposed here, but it is unused anywhere in Cortex today and its original intent isn't
documented. Reusing it was considered and rejected: adopting an existing-but-dormant header with no
clear record of its intended semantics risks silently changing behavior for any deployment that
happens to already send that header for an unrelated reason, whereas a new, clearly-scoped header
name carries no such risk. The similarity in naming is coincidental and worth calling out explicitly
so it doesn't read as an oversight during review.

The extracted identity is threaded through the request the same way source IPs are today (already
carried from the HTTP entrypoint through to the distributor for logging purposes), so it survives
the handoff into the write path without changing any internal request/response shapes.

### Configuration

New limits, following the same default-plus-per-tenant-override pattern as every other Cortex
limit:

```yaml
# Default (flags), applied to all tenants without an override
distributor:
  client_identity_ingestion_rate_limit: 0        # 0 = disabled (default; no behavior change)
  client_identity_ingestion_burst_size: 0

# Per-tenant override via runtime config
overrides:
  tenant-123:
    client_identity_ingestion_rate_limit: 5000
    client_identity_ingestion_burst_size: 10000
    # Per-tenant cap on how many distinct tracked client entries the per-client rate limiter
    # keeps in memory for this tenant; oldest-used entries are evicted once this is reached.
    # See "Bounding memory" below.
    client_identity_ingestion_tracked_clients_limit: 1000
```

`client_identity_ingestion_rate_limit: 0` is the default and means "disabled", the same convention
Cortex already uses elsewhere for a limit that should only take effect once explicitly configured.
This guarantees zero behavior change for any tenant that doesn't explicitly opt in.

### Enforcement

Enforcement is gated on **two independent conditions**, both of which must hold:

1. The tenant has a non-zero `client_identity_ingestion_rate_limit` configured.
2. The request carries a non-empty `X-User-ID` value.

If either is false, the request is subject only to the existing tenant-level ingestion rate check,
exactly as today: a missing header is not an error, and a tenant that hasn't opted in never pays
any cost for this feature.

```
Push request
     │
     v
┌─────────────────────┐      fail       ┌──────────────────┐
│ Tenant IngestionRate │────────────────>│ 429 Too Many      │
│ check (existing)     │                 │ Requests          │
└─────────────────────┘                 └──────────────────┘
     │ pass
     v
┌───────────────────────────┐   no (either gate)   ┌───────────────┐
│ X-User-ID present AND     │──────────────────────>│ Continue to   │
│ tenant limit configured?  │                        │ ingestion     │
└───────────────────────────┘                        └───────────────┘
     │ yes
     v
┌────────────────────────────┐      fail       ┌──────────────────┐
│ Per-(tenant, client) rate  │────────────────>│ 429 Too Many      │
│ limit check (new)          │                 │ Requests          │
└────────────────────────────┘                 └──────────────────┘
     │ pass
     v
Continue to ingestion
```

A second rate limiter instance is added to the distributor, keyed by tenant and client identity
together rather than by tenant alone. It enforces against the authoritative per-tenant, per-client
counter via ring-based sharding — see "Distributed enforcement via the distributor ring"
below. This check runs as an *additional* step alongside, not instead of, the existing tenant-level
limiter: a request must pass both. A tenant's aggregate throughput is still capped by the existing
tenant-level rate limit; this only prevents one client from consuming the entire budget.

The per-tenant limit and burst are looked up the same way every other tenant limit is: the
enforcement point resolves the tenant from the combined key and reads the configured value from
that tenant's overrides. The *limit value* is still a single per-tenant number, same as every other
Cortex limit; only the token bucket enforcing it is split per client.

### Distributed enforcement via the distributor ring

Per-client limits are enforced using the existing distributor ring rather than gossip-based counter
aggregation. Each `(tenant, client)` key is sharded onto the ring: the distributor instance that
owns the shard for that key maintains the authoritative rate-limit counter for it.

When a distributor receives a write request carrying an `X-User-ID` value, it hashes
`(tenant_id, client_id)` to determine the owning ring member, then:

- If it owns that shard, it increments and enforces the counter locally.
- If another replica owns the shard, it forwards the rate-limit check to that replica via an
  internal RPC before accepting or rejecting the request.

This approach:

- **Works on any ring backend** (Consul, Etcd, memberlist) — not memberlist-only — because the
  ring is used only for key ownership, not as a gossip transport.
- **Eliminates convergence lag**: the owning replica holds the single authoritative counter; there
  is no eventual-consistency window during which a burst can transiently exceed the configured
  per-tenant limit before all replicas converge.
- **Stays accurate under uneven load**: a client's requests landing on different distributors always
  count against the same shard owner, regardless of which replica physically received each request.

The tradeoff is a potential per-request remote call to the shard owner when the receiving
distributor does not own the relevant shard. For deployments with random load balancing in front of
distributors, a majority of checks will require this hop. Whether the added latency is acceptable
depends on the intra-cluster RPC latency for a given deployment; this is a known cost and worth
calling out explicitly in the implementation phase.

### Bounding memory: capped tracking, not an unbounded map

Unlike the existing tenant-level limiter, whose per-tenant map stays small because the number of
tenants is small and operator-controlled, the number of distinct tenant+client keys here is
bounded only by how many distinct `X-User-ID` values a gateway ever sends. Left as a plain
unbounded map, this becomes both a slow memory leak under normal churn (clients renamed, rotated,
or retired over time never get cleaned up) and, combined with the header trust boundary above, a
denial-of-service vector: a caller able to set arbitrary `X-User-ID` values could grow that map
without limit simply by rotating identities.

Cortex already solves an analogous problem elsewhere: the tenant-federation regex resolver bounds
a similar per-key cache with a fixed-size, least-recently-used eviction cache rather than a plain
map, sized by an operator-configurable limit. This proposal adopts the same approach: the per-client
tracking for each tenant is capped by a new per-tenant limit (configurable via
`client_identity_ingestion_tracked_clients_limit` in the tenant's overrides, with a sensible default
on the order of 1,000 tracked clients per tenant) using least-recently-used eviction once the cap
is reached.

Unlike the federation regex resolver, where eviction only affects latency (a cache miss means a
slower regex rebuild), eviction here has an enforcement consequence: an evicted client's counter is
discarded, so if it reappears it starts from zero and can issue a fresh burst before its token
bucket fills and enforcement kicks in again. This is a deliberate tradeoff — the alternative
(refusing new clients once the cap is full) risks denying service to legitimate new clients when a
tenant has many active writers. The LRU approach bounds memory and preserves fairness under normal
cardinality, at the cost of a brief enforcement gap when a previously-evicted client resumes. Under
sustained high-cardinality abuse (an attacker rotating identities to flood the LRU), the failure
mode is bounded churn (evictions) rather than unbounded memory growth, which is acceptable given
the existing guidance to keep `X-User-ID` behind a trusted gateway.

Operators can tune the per-tenant cap upward for tenants with many legitimate concurrent writers,
or observe the eviction counter (a dedicated counter incremented in the LRU eviction callback) to
distinguish "cap is properly sized" from "we are actively seeing identity churn."

### Header trust boundary

Because `X-User-ID` is trusted the same way `X-Scope-OrgID` is, deployments that expose Cortex's
HTTP endpoints directly to untrusted clients (rather than through a gateway/proxy that strips and
re-sets both headers) would let a malicious caller set an arbitrary identity, for example to evade
throttling by rotating identity values, or to frame another client. This is not a new risk specific
to this feature: it is the exact risk profile `X-Scope-OrgID` already carries today, and Cortex's
existing guidance (run behind a reverse proxy/gateway that authenticates callers and controls these
headers) applies unchanged. This should be stated explicitly in the docs for this feature, the same
way multi-tenancy setup docs already caution about `X-Scope-OrgID` exposure. The bounded tracking
above ensures that even under this threat model, the failure mode is bounded churn, not unbounded
memory growth.

### Metrics

- The existing discarded-samples metric gains a new discard reason for this rate limit, following
  the same reason-labeling convention already used for the tenant-level rate limiter, so it's
  observable per tenant the same way tenant-level rate limiting already is.
- Per-client rejection visibility (which specific client tripped the limit) is available from
  request logs rather than a dedicated per-client metric label. Adding a client label directly to
  the discarded-samples metric would multiply its existing cardinality (already keyed by discard
  reason and tenant) by the number of distinct tracked clients; logs are the better fit for a
  dimension that's expected to have many more distinct values than tenants do. This mirrors the
  same choice made in the companion query-path proposal, for consistency between the two.

A cardinality caveat on tracking itself, separate from the metrics question above: client identity
values are gateway-controlled, but a deployment that sets a distinct identity per end user (rather
than per service/team) could still produce many distinct tracked clients. The bounded-tracking cap
described above already limits the worst case; this is called out here because it's the same
underlying cardinality concern Cortex's existing per-labelset limits feature already warns about,
just showing up in a different place (limiter memory) rather than metrics.

### Interaction with existing limits

This is purely additive:

- **The tenant-level ingestion rate limit is unaffected** and remains the hard ceiling for the
  tenant as a whole.
- **Per-labelset limits** partition by data content (label matchers); this proposal partitions by
  data origin (who sent it). They are orthogonal and can be used together.
- If a tenant has no client-identity limit configured (the default), or the request has no
  `X-User-ID` header, the new check is a no-op and behavior is identical to today.

## Value of the Identity Header Beyond Rate Limiting

Establishing `X-User-ID` as a first-class header in the write path opens up a broader set of
per-client observability and control capabilities that are not in scope for this proposal but
become straightforward extensions once the identity is available:

- **Resource consumption attribution.** Samples ingested, bytes written, and active series can all
  be broken down per client in logs and metrics, enabling capacity planning and chargeback at the
  client level rather than only at the tenant level.
- **Out-of-order sample detection per client.** The distributor already detects and discards
  out-of-order samples; with `X-User-ID` present, those rejections can be attributed to the
  specific client sending them, making it straightforward to identify a misconfigured or misbehaving
  writer without manual log correlation across requests.
- **Per-client cardinality tracking.** Today cardinality can only be attributed to a tenant as a
  whole. With client identity available on the write path, high-cardinality contributors can be
  identified by source, not just by label patterns.
- **Audit and debugging.** A client identity on every write request makes it possible to answer
  questions like "which client started sending this metric series?" or "which client's deployment
  caused this ingestion spike?" directly from logs, without cross-referencing external systems.
- **Future per-client quota enforcement.** Once client identity is a reliable, trusted dimension,
  per-client quotas (not just rate limits) — for example, a cap on active series or total samples
  per day per client — become implementable without any additional identity plumbing.

None of these require changes to this proposal; they are downstream opportunities that justify
establishing a clean, trusted identity header now rather than bolting it on later.

## Rollout Plan

Introduced as an **experimental** feature, consistent with how Cortex introduces most new limits
(disabled by default, documented as experimental, graduated to stable after operational
experience). Rollout in two phases:

**Phase 1**: Ship with distributed enforcement (see "Distributed enforcement via the distributor ring"
above) behind the `client_identity_ingestion_rate_limit` per-tenant override, defaulting to `0`
(disabled) so existing deployments see no change. Validate with a small number of opted-in tenants.

**Phase 2**: Based on operational feedback, consider graduating the feature out of experimental
status.

## Alternatives Considered

- **mTLS client certificate identity.** Cryptographically stronger than a trusted header, and
  Cortex's server already supports client-cert authentication. Rejected as the primary mechanism
  because it only works when Cortex itself terminates client TLS: a common deployment pattern is a
  service mesh or gateway terminating mTLS *before* Cortex, in which case the certificate Cortex
  sees reflects the mesh sidecar, not the original caller. It would also reopen an unresolved
  Common-Name-vs-Subject-Alternative-Name identity question. The header-based approach sidesteps
  both problems and matches Cortex's existing `X-Scope-OrgID` trust model rather than introducing a
  second, different one. Could still be revisited later as an alternate identity *source* feeding
  the same enforcement path, for deployments that do terminate TLS at Cortex.
- **JWT claim-based identity.** Would require Cortex to parse and validate bearer tokens itself,
  which is a meaningfully larger surface (token validation, key rotation, clock skew) than reading a
  header the same way `X-Scope-OrgID` already is. Left as potential future work if operators need
  cryptographic identity guarantees stronger than the trusted-header model provides.
- **Basic Auth username.** Only applicable if a deployment terminates HTTP Basic Auth at Cortex
  itself, which is uncommon for the write path in practice (most Cortex deployments put
  authentication at a gateway/proxy in front of Cortex, per the Authentication Gateway proposal),
  the same gateway that would set `X-User-ID` under this proposal, making a separate Basic Auth
  path redundant.

## Open Questions

- Should the client-identity limiter default to enabled with a very high limit (encouraging
  visibility/metrics even for tenants that don't want enforcement), or fully opt-in (0 = off, as
  described above)? This proposal defaults to fully opt-in to guarantee zero behavior change on
  upgrade, consistent with how most new Cortex limits are introduced as disabled-by-default
  experimental features.
- Should the header name be configurable, the way Cortex already allows configuring the header used
  for source-IP logging, rather than a hardcoded `X-User-ID`? Leaning toward configurable, to avoid
  collisions with header names deployments may already use for a similar purpose.
- Should this eventually extend to the query path (read-side per-client throttling, reusing this
  same `X-User-ID` header)? **Yes: see the companion proposal,
  [Client-Identity Query Rate Limiting](./client-identity-query-rate-limiting.md),** filed alongside
  this one and deliberately kept separate rather than expanding this proposal's scope.
- What's the right default for the per-tenant tracked-clients cap? The cap is configurable per
  tenant via `client_identity_ingestion_tracked_clients_limit` in the overrides; evictions caused
  by hitting the cap are observable via a dedicated eviction counter, letting operators distinguish
  "cap is properly sized" from "we're actively being hit with identity churn" without needing to
  inspect memory profiles.
