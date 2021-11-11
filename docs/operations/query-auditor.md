---
title: "Query Auditor (tool)"
linkTitle: "Query Auditor (tool)"
weight: 2
slug: query-auditor
---

The query auditor is a tool bundled in the Cortex repository, but **not** included in Docker images -- this must be built from source. It's primarily useful for those _developing_ Cortex, but can be helpful to operators as well during certain scenarios (backend migrations come to mind).

## How it works

The `query-audit` tool performs a set of queries against two backends that expose the Prometheus read API. This is generally the `query-frontend` component of two Cortex deployments. It will then compare the differences in the responses to determine the average difference for each query. It does this by:

 - Ensuring the resulting label sets match.
 - For each label set, ensuring they contain the same number of samples as their pair from the other backend.
 - For each sample, calculates their difference against it's pair from the other backend/label set.
 - Calculates the average diff per query from the above diffs.

### Limitations

It currently only supports queries with `Matrix` response types.

### Use cases

- Correctness testing when working on the read path.
- Comparing results from different backends.

### Example Configuration

```yaml
control:
  host: http://localhost:8080/prometheus
  headers:
    "X-Scope-OrgID": 1234

test:
  host: http://localhost:8081/prometheus
  headers:
    "X-Scope-OrgID": 1234

queries:
  - query: 'sum(rate(container_cpu_usage_seconds_total[5m]))'
    start: 2019-11-25T00:00:00Z
    end: 2019-11-28T00:00:00Z
    step_size: 15m
  - query: 'sum(rate(container_cpu_usage_seconds_total[5m])) by (container_name)'
    start: 2019-11-25T00:00:00Z
    end: 2019-11-28T00:00:00Z
    step_size: 15m
  - query: 'sum(rate(container_cpu_usage_seconds_total[5m])) without (container_name)'
    start: 2019-11-25T00:00:00Z
    end: 2019-11-26T00:00:00Z
    step_size: 15m
  - query: 'histogram_quantile(0.9, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le, job))'
    start: 2019-11-25T00:00:00Z
    end: 2019-11-25T06:00:00Z
    step_size: 15m
    # two shardable legs
  - query: 'sum without (instance, job) (rate(cortex_query_frontend_queue_length[5m])) or sum by (job) (rate(cortex_query_frontend_queue_length[5m]))'
    start: 2019-11-25T00:00:00Z
    end: 2019-11-25T06:00:00Z
    step_size: 15m
    # one shardable leg
  - query: 'sum without (instance, job) (rate(cortex_cache_request_duration_seconds_count[5m])) or rate(cortex_cache_request_duration_seconds_count[5m])'
    start: 2019-11-25T00:00:00Z
    end: 2019-11-25T06:00:00Z
    step_size: 15m
```

### Example Output

Under ideal circumstances, you'll see output like the following:

```
$ go run ./tools/query-audit/ -f config.yaml

0.000000% avg diff for:
        query: sum(rate(container_cpu_usage_seconds_total[5m]))
        series: 1
        samples: 289
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-28 00:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: sum(rate(container_cpu_usage_seconds_total[5m])) by (container_name)
        series: 95
        samples: 25877
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-28 00:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: sum(rate(container_cpu_usage_seconds_total[5m])) without (container_name)
        series: 4308
        samples: 374989
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-26 00:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: histogram_quantile(0.9, sum(rate(cortex_cache_value_size_bytes_bucket[5m])) by (le, job))
        series: 13
        samples: 325
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-25 06:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: sum without (instance, job) (rate(cortex_query_frontend_queue_length[5m])) or sum by (job) (rate(cortex_query_frontend_queue_length[5m]))
        series: 21
        samples: 525
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-25 06:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: sum without (instance, job) (rate(cortex_cache_request_duration_seconds_count[5m])) or rate(cortex_cache_request_duration_seconds_count[5m])
        series: 942
        samples: 23550
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-25 06:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: sum by (namespace) (predict_linear(container_cpu_usage_seconds_total[5m], 10))
        series: 16
        samples: 400
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-25 06:00:00 +0000 UTC
        step: 15m0s

0.000000% avg diff for:
        query: sum by (namespace) (avg_over_time((rate(container_cpu_usage_seconds_total[5m]))[10m:]) > 1)
        series: 4
        samples: 52
        start: 2019-11-25 00:00:00 +0000 UTC
        end: 2019-11-25 01:00:00 +0000 UTC
        step: 5m0s
```
