---
title: "Client-Identity Rate Limiting"
linkTitle: "Client-Identity Rate Limiting"
weight: 10
slug: client-identity-rate-limiting
---

Cortex rate limits at the tenant level: every request is identified only by `X-Scope-OrgID`. When several independent writers share one tenant, one noisy client can exhaust the whole tenant's budget. Client-identity rate limiting adds an optional second dimension *below* the tenant, so a single client is throttled without affecting the others.

This feature is **experimental**. It is disabled by default and changes nothing for tenants that don't opt in. See the [design proposal](../proposals/client-identity-ingestion-rate-limiting.md) for the full rationale.

## How it works

A request is subject to the per-client check only when **both** conditions hold:

1. The tenant has a non-zero per-client rate limit configured.
2. The request carries a non-empty `X-Scope-ClientID` header.

If either is false, the request is handled exactly as before. A missing header is never an error.

The per-client check runs *in addition to* the existing checks, never instead of them. The tenant-level ingestion rate limit is evaluated first and remains the hard ceiling for the tenant as a whole.

### Distributed enforcement

Each distributor counts requests per `(tenant, client)` over a short fixed window (`-distributor.client-identity-rate-limit.window`, default `1s`) and gossips its own counts through a memberlist-backed KV store. Every replica merges the other replicas' counts and admits a request only if `own count + latest known count of every other replica` stays within the window's capacity, `limit * window + burst`.

Counts are never fetched synchronously on the request path, and no ring or per-request RPC is involved. As a result:

- A replica's view of its peers lags by up to one window, so a client spread evenly across many replicas can briefly exceed the limit by about one window's worth of requests.
- If the KV store is unavailable, enforcement continues on local and last-known peer state. It fails open and never blocks traffic because of a gossip failure.

### Bounded memory

The number of distinct clients is bounded only by the `X-Scope-ClientID` values a gateway sends, so tracking is capped per tenant by `client_identity_*_tracked_clients_limit` (default `1000`) with least-recently-used eviction. An evicted client that reappears starts from zero and can issue a fresh burst. Watch `cortex_distributor_client_identity_tracked_clients_evicted_total` to tell whether the cap is sized correctly or you are seeing identity churn.

## Header trust boundary

**`X-Scope-ClientID` is trusted exactly the way `X-Scope-OrgID` is.** Cortex does not authenticate it. It assumes a gateway or reverse proxy in front of Cortex authenticates the caller, strips any caller-supplied `X-Scope-OrgID` and `X-Scope-ClientID`, and sets both itself.

If Cortex's HTTP or gRPC endpoints are reachable by untrusted clients, a caller can set an arbitrary identity, for example to evade throttling by rotating values, or to frame another client. This is the same risk `X-Scope-OrgID` already carries, and the same guidance applies: never expose Cortex directly to untrusted clients. The per-tenant tracked-clients cap bounds the worst case to cache churn rather than unbounded memory growth.

The value is an opaque string. What it represents (API key ID, service account, workload identity, job name) is up to your gateway. Prefer a per-service or per-team identity over a per-end-user one, to keep the number of tracked clients small.

## Configuration

Enable gossip through memberlist first. The KV store defaults to `consul` like every other Cortex KV config, so set it to `memberlist` and configure `memberlist` as you would for the ring (see [Gossip Ring Getting Started](./gossip-ring-getting-started.md)):

```yaml
distributor:
  client_rate_limit_kvstore:
    store: memberlist
```

The KV client is created lazily, the first time a tenant actually exercises the feature, so deployments that never opt in start no KV client or watch. On the write path, `X-Scope-ClientID` is read from the HTTP headers (remote write, OTLP over HTTP) or the gRPC metadata (OTLP over gRPC).

Per-tenant limits, defaulting to `0` (disabled), set through `limits` or the runtime overrides:

```yaml
overrides:
  tenant-123:
    # Samples per second and burst in samples.
    client_identity_ingestion_rate_limit: 5000
    client_identity_ingestion_burst_size: 10000
    client_identity_ingestion_tracked_clients_limit: 1000
```

Related flags:

- `-distributor.client-identity-rate-limit.store`: set to `memberlist`.
- `-distributor.client-identity-rate-limit.window`: fixed window length, default `1s`.

## Observability

- Rejected requests return HTTP `429` and are counted in `cortex_discarded_samples_total{reason="client_rate_limited"}` (and the matching exemplar and metadata counters) per tenant.
- Per-client visibility (which client tripped the limit) is in the `429` error message and request logs, deliberately not a metric label, to avoid multiplying metric cardinality by the number of clients.

## Known limitations

- Enforcement is approximate across replicas, as described above. A strictly authoritative limit would need ring-based shard ownership.
