---
title: "Roadmap"
linkTitle: "Roadmap"
weight: 10
slug: roadmap
---

This document highlights some ideas for major features we'd like to implement in the near future.
To get a more complete overview of planned features and current work, see the [issue tracker](https://github.com/cortexproject/cortex/issues).
Note that these are not ordered by priority.

Last updated: January 4, 2025

## Short-term (< 6 months)

### Support for Prometheus Remote Write 2.0

[Prometheus Remote Write 2.0](https://prometheus.io/docs/specs/remote_write_spec_2_0/)

* adds a new Protobuf Message with new features enabling more use cases and wider adoption on top of performance and cost savings
* deprecates the previous Protobuf Message from a 1.0 Remote-Write specification
* adds mandatory X-Prometheus-Remote-Write-*-Written HTTP response headers for reliability purposes

For more information tracking this, please see [issue #6116](https://github.com/cortexproject/cortex/issues/6116).

## Long-term (> 6 months)

### CNCF Graduation Status

Cortex was accepted to the CNCF on September 20, 2018 and moved to the Incubating maturity level on August 20, 2020. The Cortex maintainers are working towards promoting the project to the graduation status.

For more information tracking this, please see [issue #6075](https://github.com/cortexproject/cortex/issues/6075).

### Downsampling

[Downsampling](https://thanos.io/tip/components/compact.md/#downsampling) means storing fewer samples, e.g. one per minute instead of one every 15 seconds.
This makes queries over long periods more efficient. It can reduce storage space slightly if the full-detail data is discarded.

For more information tracking this, please see [issue #4322](https://github.com/cortexproject/cortex/issues/4322).
