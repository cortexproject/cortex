---
date: 2025-01-15
title: "Resource-Based Query Protection in Cortex v1.20.0"
linkTitle: Resource-Based Query Protection
tags: [ "blog", "cortex", "query", "protection", "scaling" ]
categories: [ "blog" ]
projects: [ "cortex" ]
description: >
  This article explores the new resource-based query protection feature in Cortex v1.20.0, which automatically throttles queries when CPU or memory utilization exceeds configured thresholds.
author: Shriya Tarcar ([@shri3016](https://github.com/shri3016))
---

## Introduction

PromQL is a powerful query language, but that power comes with risk. A single poorly-constructed query can fetch millions of series, consume gigabytes of memory, and exhaust CPU resources across your Cortex cluster. When this happens, the consequences cascade:

1. **CPU exhaustion** increases latency, causing queries to queue and timeout
2. **Memory pressure** triggers aggressive garbage collection, further degrading performance
3. **Complete resource exhaustion** leads to out-of-memory kills and service disruption

![Resource Exhaustion Flow](/images/blog/2025/resource-exhaustion-flow.png)


While Cortex has long offered static limits (max series, max chunks, max bytes per query), these protect against specific patterns but can't catch every combination of resource-intensive operations. A query might stay just under each individual limit while still overwhelming a component.


Cortex v1.20.0 introduces **Resource-Based Query Protection** — a generic safeguard that monitors actual CPU and heap utilization, automatically throttling incoming queries when thresholds are exceeded.

## How It Works

The feature consists of two components working together:

### ResourceMonitor

The `ResourceMonitor` continuously tracks resource utilization across Cortex components:

- **CPU Utilization**: Calculated as the average CPU rate over a configurable window (default: 1 minute)
- **Heap Utilization**: Current heap memory usage relative to the container's memory limit

The monitor runs on a configurable interval (default: 100ms), maintaining a rolling buffer of CPU samples to compute accurate averages.

### ResourceBasedLimiter

The `ResourceBasedLimiter` sits in the request path for ingesters and store-gateways. Before accepting a new query request, it checks:

```
if current_utilization >= configured_threshold:
    reject request with 429 (Resource Exhausted)
else:
    accept request
```

![Resource-Based Limiting Flow](/images/blog/2025/resource-based-limiting-flow.png)

When a request is rejected, the client receives a gRPC `ResourceExhausted` error (HTTP 429), signaling it should back off and retry.

## Protected Endpoints

Resource-based limiting protects query endpoints in two critical components:

| Component | Protected Endpoints |
|-----------|---------------------|
| **Ingester** | `QueryStream()` |
| **Store-Gateway** | `Series()`, `LabelNames()`, `LabelValues()` |

These are the primary endpoints where heavy queries consume resources. Write paths (push requests) are not affected by this feature.

## Configuration

### Step 1: Enable Resource Monitoring

First, enable the ResourceMonitor to track CPU and/or heap utilization:

```yaml
resource_monitor:
  # Which resources to monitor (comma-separated: cpu, heap, or both)
  resources: "cpu,heap"

  # How frequently to sample resource usage
  interval: 100ms

  # Time window for calculating average CPU utilization
  cpu_rate_interval: 1m
```

Or via CLI flags:

```bash
-resource-monitor.resources=cpu,heap
-resource-monitor.interval=100ms
-resource-monitor.cpu-rate-interval=1m
```

### Step 2: Configure Ingester Thresholds

Set the utilization thresholds at which the ingester starts rejecting queries:

```yaml
ingester:
  query_protection:
    rejection:
      threshold:
        # Reject queries when CPU utilization exceeds 80%
        cpu_utilization: 0.8

        # Reject queries when heap utilization exceeds 80%
        heap_utilization: 0.8
```

Or via CLI flags:

```bash
-ingester.query-protection.rejection.threshold.cpu-utilization=0.8
-ingester.query-protection.rejection.threshold.heap-utilization=0.8
```

### Step 3: Configure Store-Gateway Thresholds

Apply similar protection to store-gateways:

```yaml
store_gateway:
  query_protection:
    rejection:
      threshold:
        cpu_utilization: 0.8
        heap_utilization: 0.8
```

Or via CLI flags:

```bash
-store-gateway.query-protection.rejection.threshold.cpu-utilization=0.8
-store-gateway.query-protection.rejection.threshold.heap-utilization=0.8
```

### Complete Example

Here's a complete configuration combining all settings:

```yaml
# Enable resource monitoring
resource_monitor:
  resources: "cpu,heap"
  interval: 100ms
  cpu_rate_interval: 1m

# Protect ingesters
ingester:
  query_protection:
    rejection:
      threshold:
        cpu_utilization: 0.8
        heap_utilization: 0.8

# Protect store-gateways
store_gateway:
  query_protection:
    rejection:
      threshold:
        cpu_utilization: 0.8
        heap_utilization: 0.8
```

## Monitoring and Observability

The feature exposes Prometheus metrics for monitoring throttling behavior:

### Resource Utilization

```promql
# Current CPU utilization (0-1)
cortex_resource_utilization{resource="cpu"}

# Current heap utilization (0-1)
cortex_resource_utilization{resource="heap"}
```

### Throttling Metrics

```promql
# Total requests throttled due to CPU limits
cortex_resource_based_limiter_throttled_total{component="ingester", resource="cpu"}

# Total requests throttled due to heap limits
cortex_resource_based_limiter_throttled_total{component="store-gateway", resource="heap"}

# Configured limit thresholds
cortex_resource_based_limiter_limit{component="ingester", resource="cpu"}
```

### Example Grafana Alert

```yaml
# Alert when throttling is happening
- alert: CortexResourceThrottling
  expr: rate(cortex_resource_based_limiter_throttled_total[5m]) > 0
  for: 5m
  labels:
    severity: warning
  annotations:
    summary: "Cortex {{ $labels.component }} is throttling queries due to {{ $labels.resource }} pressure"
```

## Real-World Scenario

Consider a Cortex cluster where an ingester typically runs at 40% CPU utilization. A user runs a heavy query that causes CPU to spike:

![Throttling Timeline](/images/blog/2025/throttling-timeline.png)

**Without resource-based limiting:**
1. Heavy query arrives, CPU spikes to 95%
2. GC struggles, heap pressure builds
3. Subsequent queries queue and timeout
4. Potential OOM kill and service disruption

**With resource-based limiting (threshold at 80%):**
1. Heavy query arrives, CPU rises toward 80%
2. At 80% utilization, new queries receive 429 errors
3. Clients back off, allowing the heavy query to complete
4. CPU drops below 80%, normal queries resume
5. Service remains stable throughout

## Drawbacks and Considerations

### 1. False Positives During Spikes

Short CPU spikes (e.g., during compaction or GC) might trigger throttling even when the system could handle more queries. Mitigation:
- Use the `cpu_rate_interval` setting to smooth out short spikes (default 1 minute)
- Set thresholds with headroom (e.g., 80% rather than 90%)

### 2. No Query Prioritization

All queries are treated equally. A critical alerting query gets the same 429 as an ad-hoc exploration query. If you need differentiated treatment, combine this with [query priority](../query-priority/) settings.

### 3. Client Retry Behavior

Clients must handle 429 responses appropriately. Aggressive retry without backoff could worsen the situation. Ensure your query clients (Grafana, alerting systems) have proper retry policies.

### 4. Linux-Only CPU Monitoring

CPU utilization monitoring uses `/proc/self/stat` and is only available on Linux. On other platforms, only heap-based limiting is available.

### 5. Experimental Status

This feature is marked **experimental** in v1.20.0. Configuration options and behavior may change in future releases.

## When to Use Resource-Based Limiting

**Good fit:**
- Multi-tenant environments where one tenant's queries shouldn't impact others
- Clusters experiencing occasional query-induced outages
- Environments where static limits alone aren't sufficient protection

**May not be needed:**
- Small, single-tenant deployments with predictable query patterns
- Environments where all queries are already well-tuned
- When static limits (max series, max chunks) already provide sufficient protection

## Other Notable Features in v1.20.0

While this post focused on resource-based protection, Cortex v1.20.0 includes several other significant features:

- **Prometheus Remote Write 2.0 support** (experimental) — improved compression and transmission efficiency
- **Parquet format support** (experimental) — columnar storage for analytical query patterns
- **Regex tenant resolver** — query across tenants matching a regex pattern
- **gRPC stream push** — reduced connection overhead for distributor-ingester communication
- **Out-of-order native histogram ingestion** — enhanced histogram support

See the [full changelog](https://github.com/cortexproject/cortex/releases/tag/v1.20.0) for complete details.

## Conclusion

Resource-based query protection provides a safety net that static limits cannot. By monitoring actual CPU and heap utilization, Cortex can now automatically throttle queries before they cause cascading failures.

To get started:
1. Enable resource monitoring with `-resource-monitor.resources=cpu,heap`
2. Set conservative thresholds (start with 0.8) on ingesters and store-gateways
3. Monitor `cortex_resource_based_limiter_throttled_total` to understand throttling patterns
4. Adjust thresholds based on your workload characteristics

As this feature is experimental, we encourage you to test it in staging environments first and share feedback with the Cortex community.
