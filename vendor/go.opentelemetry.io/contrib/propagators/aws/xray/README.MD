# AWS X-Ray Propagator/IDGenerator

This package contains an AWS X-Ray compatible `TextMapPropagator` and `IDGenerator`.

## `traceIdRatioSampler` and `x-ray IDGenerator` compatibility

It is a general suggestion to **not** use the `traceIDRatioSampler` while also
using the X-Ray `IDGenerator`. The non-random nature of building an X-Ray `traceId`
may lead to unexpected sampling results.
