---
title: "Rate Limiting"
linkTitle: "Rate Limiting"
weight: 10
slug: rate-limiting
---

Cortex applies rate limits on the write path to protect itself from overload and to enforce a fair share of capacity across tenants. This page describes the limiters that gate sample ingestion, how to configure them, and how to interpret the resulting errors and metrics.

## Overview

There are two layers of rate limiting on the write path:

1. **Per-tenant ingestion limit (distributor)** — a token-bucket limiter applied per tenant in every distributor. This is the main knob most operators tune. It is configured with `ingestion_rate` and `ingestion_burst_size` (and the parallel pair for native histograms).
2. **Per-instance ingestion limit (distributor and ingester)** — a per-process safety valve on the total sample rate a single distributor or ingester will accept, regardless of tenant. Configured with `-distributor.instance-limits.max-ingestion-rate` and `-ingester.instance-limits.max-ingestion-rate`.

The per-tenant limiter rejects with HTTP `429 Too Many Requests`; the per-instance limit rejects with HTTP `503 Service Unavailable`. Prometheus `remote_write` retries both by default (it retries on 5xx, and on 429 when `retry_on_http_429` is set).

## Per-tenant ingestion limit

The per-tenant limit uses Go's [`golang.org/x/time/rate`](https://pkg.go.dev/golang.org/x/time/rate) token bucket. The bucket has two parameters:

- `ingestion_rate` — the steady-state refill rate, in samples per second.
- `ingestion_burst_size` — the bucket capacity, i.e. the largest number of "tokens" that can be consumed in a single instant when the bucket is full.

For each push request the distributor computes:

```
totalN = float samples + native histogram samples + exemplars + metadata entries
```

and consumes `totalN` tokens from the tenant's bucket. If the bucket has fewer tokens than `totalN`, the entire request is rejected with HTTP `429 Too Many Requests` and the dropped items are counted in `cortex_discarded_samples_total{reason="rate_limited"}`, `cortex_discarded_exemplars_total{reason="rate_limited"}` and `cortex_discarded_metadata_total{reason="rate_limited"}`.

The limit and burst configured for a tenant are re-read every 10 seconds, so changes to the runtime overrides file (or to a tenant override pushed via the [User Overrides API](./overrides.md)) take effect within that window without a restart.

### Configuration

| YAML | CLI flag | Default | Description |
| --- | --- | --- | --- |
| `ingestion_rate` | `-distributor.ingestion-rate-limit` | `25000` | Per-tenant ingestion rate, samples/sec. |
| `ingestion_burst_size` | `-distributor.ingestion-burst-size` | `50000` | Per-tenant bucket capacity, in samples. |
| `ingestion_rate_strategy` | `-distributor.ingestion-rate-limit-strategy` | `local` | `local` or `global`. See below. |

All three values are per-tenant and can be overridden via the runtime configuration overrides file or the [User Overrides API](./overrides.md). See the [Arguments](../configuration/arguments.md) page for the full set of per-tenant limits.

### Local vs. global strategy

The strategy controls how the `ingestion_rate` value is interpreted in a multi-distributor cluster:

- **`local` (default)** — each distributor enforces `ingestion_rate` independently. With N distributors the effective cluster-wide rate is `N * ingestion_rate`.
- **`global`** — each distributor enforces `ingestion_rate / N`, where `N` is the number of healthy distributors discovered through the distributor ring. The cluster-wide effective rate is `ingestion_rate`. `N` is recomputed automatically as distributors come and go.

`ingestion_burst_size` is **not** divided by `N` under the global strategy — every distributor uses the full configured burst. This is intentional: it keeps the burst a single, easy-to-reason-about value and means a single large push request is not rejected just because the cluster has many distributors.

The global strategy assumes that push requests are evenly distributed across distributors. A load balancer in front of the distributors satisfies this; a setup that pins clients to specific distributors does not, and will produce uneven enforcement of the global limit.

The global strategy requires the distributors to form their own hash ring, configured under `distributor.ring` / `-distributor.ring.*`.

### Sizing guidance

- Set `ingestion_rate` to the expected sustained per-tenant write rate plus headroom. Under the global strategy, that value is the cluster-wide rate; under the local strategy it is per-distributor.
- Set `ingestion_burst_size` to at least the **largest single push request you expect**. Prometheus `remote_write` can batch thousands of samples per request, and OpenTelemetry exporters can be larger still. A burst smaller than a single request will produce spurious 429s even when the steady rate is well under the limit.
- A common starting point is `ingestion_burst_size = 2 * ingestion_rate`, then increase if you see rate-limit errors during normal operation.

## Native histogram ingestion limit

Native histogram samples are gated by a **separate** token-bucket limiter that runs *after* the main ingestion limiter. If a request passes the main limiter but fails the native-histogram limiter, only the histogram samples are dropped; float samples, exemplars and metadata are still ingested.

| YAML | CLI flag | Default | Description |
| --- | --- | --- | --- |
| `native_histogram_ingestion_rate` | `-distributor.native-histogram-ingestion-rate-limit` | `Inf` (disabled) | Per-tenant native histogram ingestion rate, samples/sec. |
| `native_histogram_ingestion_burst_size` | `-distributor.native-histogram-ingestion-burst-size` | `0` | Per-tenant native histogram bucket capacity. |

The same `ingestion_rate_strategy` value is also used by this limiter, with the same local-vs-global semantics. Dropped histogram samples are counted under `cortex_discarded_samples_total{reason="native_histogram_rate_limited"}`.

If both values are left at their defaults, native histogram ingestion works as unlimited.
`native_histogram_ingestion_rate` defaults to `rate.Inf`, which always allows the limiter and causes the zero burst to be ignored.

## Per-instance ingestion limits

In addition to the per-tenant limits, both the distributor and the ingester can enforce a per-process cap on the **total** sample rate they will accept, summed across all tenants. This is intended as a safety valve to protect a single instance that has been disproportionately loaded, not as a primary admission control.

| CLI flag | Component | Default | Description |
| --- | --- | --- | --- |
| `-distributor.instance-limits.max-ingestion-rate` | distributor | `0` (unlimited) | Max samples/sec this distributor will accept across all tenants. |
| `-ingester.instance-limits.max-ingestion-rate` | ingester | `0` (unlimited) | Max samples/sec this ingester will accept across all tenants. |

The current rate is computed as an exponentially weighted moving average and updated every second. Requests that arrive while the EWMA is over the configured limit are rejected. The distributor returns HTTP `503 Service Unavailable` with the body `distributor's samples push rate limit reached`; the ingester returns the gRPC error `ingester's samples push rate limit reached`, which the distributor surfaces upstream.

## Errors and observability

A rate-limited push request returns one of the following:

- HTTP `429 Too Many Requests` — `ingestion rate limit (<rate>) exceeded while adding <samples> samples and <metadata> metadata` — per-tenant ingestion limiter.
- HTTP `429 Too Many Requests` — `native histogram ingestion rate limit (<rate>) exceeded while adding <samples> native histogram samples` — per-tenant native histogram limiter.
- HTTP `503 Service Unavailable` — `distributor's samples push rate limit reached` — per-distributor instance limit.
- Upstream `ingester's samples push rate limit reached` — per-ingester instance limit, surfaced through the distributor.

Prometheus 2.26+ supports `retry_on_http_429` in the `remote_write` configuration; clients that enable it will back off and retry 429s automatically. 5xx responses are retried by default.

Useful metrics:

- `cortex_distributor_ingestion_rate_samples_per_second` — the current per-distributor sample rate EWMA.
- `cortex_discarded_samples_total{reason="rate_limited"}` — samples dropped by the per-tenant ingestion limiter.
- `cortex_discarded_samples_total{reason="native_histogram_rate_limited"}` — native histogram samples dropped by the native histogram limiter.
- `cortex_discarded_exemplars_total{reason="rate_limited"}` and `cortex_discarded_metadata_total{reason="rate_limited"}` — exemplars and metadata dropped by the per-tenant ingestion limiter.
- `cortex_distributor_instance_limits{limit="max_ingestion_rate"}` and the ingester equivalent — the configured per-instance caps.

If you run the [Overrides Exporter](./overrides-exporter.md), the effective per-tenant `ingestion_rate` and `ingestion_burst_size` are exposed as `cortex_overrides{limit_name="ingestion_rate"}` and `cortex_overrides{limit_name="ingestion_burst_size"}`, which makes it easy to alert on tenants near their limits.
