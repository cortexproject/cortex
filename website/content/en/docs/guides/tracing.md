---
title: "Tracing"
linkTitle: "Tracing"
weight: 6
slug: tracing
---

Cortex uses [Jaeger](https://www.jaegertracing.io/) to implement distributed
tracing. We have found Jaeger invaluable for troubleshooting the behavior of
Cortex in production.

## Dependencies

In order to send traces you will need to set up a Jaeger deployment. A
deployment includes either the jaeger all-in-one binary, or else a distributed
system of agents, collectors, and queriers.  If running on Kubernetes, [Jaeger
Kubernetes](https://github.com/jaegertracing/jaeger-kubernetes) is an excellent
resource.

## Configuration

In order to configure Cortex to send traces you must do two things:
1. Set the `JAEGER_AGENT_HOST` environment variable in all components to point
   to your Jaeger agent. This defaults to `localhost`.
1. Enable sampling in the appropriate components:
   * The Ingester and Ruler self-initiate traces and should have sampling
     explicitly enabled.
   * Sampling for the Distributor and Query Frontend can be enabled in Cortex
     or in an upstream service such as your frontdoor.

To enable sampling in Cortex components you can specify either
`JAEGER_SAMPLER_MANAGER_HOST_PORT` for remote sampling, or
`JAEGER_SAMPLER_TYPE` and `JAEGER_SAMPLER_PARAM` to manually set sampling
configuration. See the [Jaeger Client Go
documentation](https://github.com/jaegertracing/jaeger-client-go#environment-variables)
for the full list of environment variables you can configure.

Note that you must specify one of `JAEGER_AGENT_HOST` or
`JAEGER_SAMPLER_MANAGER_HOST_PORT` in each component for Jaeger to be enabled,
even if you plan to use the default values.
