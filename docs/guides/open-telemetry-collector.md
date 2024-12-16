---
title: "OpenTelemetry Collector"
linkTitle: "OpenTelemetry Collector"
weight: 10
slug: opentelemetry-collector
---

This guide explains how to configure open-telemetry collector and OTLP(OpenTelemetry Protocol) configurations in the
Cortex.

## Context

The [open-telemetry collector](https://opentelemetry.io/docs/collector/) can write collected metrics to the Cortex with
the Prometheus and OTLP formats.

## Push with Prometheus format

To push metrics via the `Prometheus` format, we can
use [prometheusremotewrite](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/prometheusremotewriteexporter)
exporter in the open-telemetry collector.
In the `exporters` and `service` sections in the open-telemetry collector yaml file, we can add as follows:

```
exporters:
  prometheusremotewrite:
    endpoint: http://<cortex-endpoint>/api/v1/push
    headers:
      X-Scope-OrgId: <orgId>

...

service:
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [prometheusremotewrite]
```

Please refer to [Authentication and Authorisation](./authentication-and-authorisation.md) section for the
`X-Scope-OrgId` explanation.

## Push with OTLP format

To push metrics via the `OTLP` format, we can
use [otlphttp](https://github.com/open-telemetry/opentelemetry-collector/tree/main/exporter/otlphttpexporter) exporter
in the open-telemetry collector.
In the `exporters` and `service` sections in the open-telemetry collector yaml file, we can add as follows:

```
exporters:
  otlphttp:
    endpoint: http://<cortex-endpoint>/api/v1/otlp
    headers:
      X-Scope-OrgId: <orgId>

...

service:
  pipelines:
    metrics:
      receivers: [...]
      processors: [...]
      exporters: [otlphttp]
```

## Configure OTLP

### target_info metric

By default,
the [target_info](https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md#supporting-target-metadata-in-both-push-based-and-pull-based-systems)
is enabled to write and can be disabled via `-distributor.otlp.disable-target-info=true`.

### Resource attributes conversion

The conversion of
all [resource attributes](https://opentelemetry.io/docs/specs/semconv/resource/) to labels is
disabled by default and can be enabled via
`-distributor.otlp.convert-all-attributes=true`.

You can specify the attributes converted to labels via `-distributor.promote-resource-attributes` flag. It is supported
only if `-distributor.otlp.convert-all-attributes=false`.

These flags can be configured via yaml:

```
limits:
 promote_resource_attributes: <list of string>
...
distributor:
  otlp:
    convert_all_attributes: <boolean>
    disable_target_info: <boolean>
```

These are the yaml examples:

- Example 1: All of the resource attributes are converted, and the `target_info` metric is disabled to push.

```
distributor:
  otlp:
    convert_all_attributes: true
    disable_target_info: true
```

- Example 2: Only `service.name` and `service.instance.id` resource attributes are converted to labels and the
  `target_info` metric is enabled to push.

```
limits:
 promote_resource_attributes: ["service.name", "service.instance.id"]
distributor:
  otlp:
    convert_all_attributes: false
    disable_target_info: false
```

### Configure promote resource attributes per tenants

The `promote_resource_attributes` is a [runtime config](./overrides-exporter.md) so you can configure it per tenant.

For example, this yaml file specifies `attr1` being converted to label in both `user-1` and `user-2`. But, the `attr2`
is converted only for `user-2`.

```
overrides:
  user-1:
    promote_resource_attributes: ["attr1"]
  user-2:
    promote_resource_attributes: ["attr1", "attr2"]
`
```
