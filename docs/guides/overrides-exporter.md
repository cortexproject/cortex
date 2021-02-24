---
title: "Overrides Exporter"
linkTitle: "Overrides Exporter"
weight: 10
slug: overrides-exporter
---

Since Cortex is a multi-tenant system, it supports applying limits to each tenant to prevent
any single one from using too many resources. In order to help operators understand how close
to their limits tenants are, the `overrides-exporter` module can expose limits as Prometheus metrics.

## Context

To update configuration without restarting, Cortex allows operators to supply a `runtime_config`
file that will be periodically reloaded. This file can be specified under the `runtime_config` section
of the main [configuration file](../configuration/arguments.md#runtime-configuration-file) or using the `-runtime-config.file`
command line flag. This file is used to apply tenant-specific limits.

## Example

The `overrides-exporter` is not enabled by default, it must be explicitly enabled. We recommend
only running a single instance of it in your cluster due to the cardinality of the metrics
emitted.

With a `runtime.yaml` file given below

[embedmd]:# (./overrides-exporter-runtime.yaml)
```yaml
# file: runtime.yaml
# In this example, we're overriding ingestion limits for a single tenant.
overrides:
  "user1":
    ingestion_burst_size: 350000
    ingestion_rate: 350000
    max_global_series_per_metric: 300000
    max_global_series_per_user: 300000
    max_series_per_metric: 0
    max_series_per_user: 0
    max_samples_per_query: 100000
    max_series_per_query: 100000
```

The `overrides-exporter` is configured to run as follows

```
cortex -target overrides-exporter -runtime-config.file runtime.yaml -server.http-listen-port=8080
```

After the `overrides-exporter` starts, you can to use `curl` to inspect the tenant overrides.

```text
curl -s http://localhost:8080/metrics | grep cortex_overrides
# HELP cortex_overrides Resource limit overrides applied to tenants
# TYPE cortex_overrides gauge
cortex_overrides{limit_name="ingestion_burst_size",user="user1"} 350000
cortex_overrides{limit_name="ingestion_rate",user="user1"} 350000
cortex_overrides{limit_name="max_global_series_per_metric",user="user1"} 300000
cortex_overrides{limit_name="max_global_series_per_user",user="user1"} 300000
cortex_overrides{limit_name="max_local_series_per_metric",user="user1"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="user1"} 0
cortex_overrides{limit_name="max_samples_per_query",user="user1"} 100000
cortex_overrides{limit_name="max_series_per_query",user="user1"} 100000
```

With these metrics, you can set up alerts to know when tenants are close to hitting their limits
before they exceed them.
