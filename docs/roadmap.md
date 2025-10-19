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

## Changes to this Roadmap

Changes to this roadmap will take the form of pull requests containing the suggested change. All such PRs must be posted to the [#cortex](https://cloud-native.slack.com/archives/CCYDASBLP) Slack channel in
the [CNCF slack](https://communityinviter.com/apps/cloud-native/cncf) so that they're made visible to all other developers and maintainers.

Significant changes to this document should be discussed in the [monthly meeting](https://github.com/cortexproject/cortex?tab=readme-ov-file#engage-with-our-community)
before merging, to raise awareness of the change and to provide an opportunity for discussion. A significant change is one which meaningfully alters
one of the roadmap items, adds a new item, or removes an item.

Insignificant changes include updating links to issues, spelling fixes or minor rewordings which don't significantly change meanings. These insignificant changes
don't need to be discussed in a meeting but should still be shared in Slack.
