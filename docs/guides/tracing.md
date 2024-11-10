---
title: "Tracing"
linkTitle: "Tracing"
weight: 10
slug: tracing
---

Cortex uses [Jaeger](https://www.jaegertracing.io/) or [OpenTelemetry](https://opentelemetry.io/) to implement distributed
tracing. We have found tracing invaluable for troubleshooting the behavior of
Cortex in production.

## Jaeger

### Dependencies

In order to send traces, you will need to set up a Jaeger deployment. A
deployment includes either the Jaeger all-in-one binary or else a distributed
system of agents, collectors, and queriers.  If running on Kubernetes, [Jaeger
Kubernetes](https://github.com/jaegertracing/jaeger-kubernetes) is an excellent
resource.

### Configuration

In order to configure Cortex to send traces, you must do two things:
1. Set the `JAEGER_AGENT_HOST` environment variable in all components to point
   to your Jaeger agent. This defaults to `localhost`.
1. Enable sampling in the appropriate components:
    * The Ingester and Ruler self-initiate traces and should have sampling
      explicitly enabled.
    * Sampling for the Distributor and Query Frontend can be enabled in Cortex
      or in an upstream service such as your front door.

To enable sampling in Cortex components, you can specify either
`JAEGER_SAMPLER_MANAGER_HOST_PORT` for remote sampling or
`JAEGER_SAMPLER_TYPE` and `JAEGER_SAMPLER_PARAM` to manually set sampling
configuration. See the [Jaeger Client Go
documentation](https://github.com/jaegertracing/jaeger-client-go#environment-variables)
for the full list of environment variables you can configure.

Note that you must specify one of `JAEGER_AGENT_HOST` or
`JAEGER_SAMPLER_MANAGER_HOST_PORT` in each component for Jaeger to be enabled,
even if you plan to use the default values.


## OpenTelemetry

### Dependencies

In order to send traces, you will need to set up an OpenTelemetry Collector. The collector will be able to send traces to
multiple destinations such as [AWS X-Ray](https://aws-otel.github.io/docs/getting-started/x-ray),
[Google Cloud](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/googlecloudexporter),
[DataDog](https://docs.datadoghq.com/tracing/trace_collection/open_standards/otel_collector_datadog_exporter/) and
[others(https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter). OpenTelemetry Collector
provides a [helm chart](https://github.com/open-telemetry/opentelemetry-helm-charts/tree/main/charts/opentelemetry-collector/examples/deployment-otlp-traces)
to set up the environment.

### Configuration

See the document on the tracing section in the [Configuration file](https://cortexmetrics.io/docs/configuration/configuration-file/).

### Current State

Cortex is maintaining backward compatibility with Jaeger support. Cortex has not fully migrated from OpenTracing to OpenTelemetry and is currently using the
[OpenTracing bridge](https://opentelemetry.io/docs/migration/opentracing/).
