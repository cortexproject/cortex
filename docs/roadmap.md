---
title: "Roadmap"
linkTitle: "Roadmap"
weight: 10
slug: roadmap
---

This document highlights some ideas for major features we'd like to implement in the near future.
To get a more complete overview of planned features and current work, see the [issue tracker](https://github.com/cortexproject/cortex/issues).
Note that these are not ordered by priority.

## Helm charts and other packaging

We have a [helm chart](https://github.com/cortexproject/cortex-helm-chart) but it needs work before it can be effectively utilised by different backends. We also don't provide an official set of dashboards and alerts to our users yet. This is one of the most requested features and something we will tackle in the immediate future. We also plan on publishing debs, rpms along with guides on how to run Cortex on bare-metal.

## Auth Gateway

Cortex server has a simple authentication mechanism (X-Scope-OrgId) but users can't use the multitenancy features out of the box without complicated proxy configuration. It's hard to support all the different authentication mechanisms used by different companies but plan to have a simple but opinionated auth-gateway that provides value out of the box. The configuration could be as simple as:

```
tenants:
- name: infra-team
  password: basic-auth-password
- name: api-team
  password: basic-auth-password2
```

## Billing and Usage analytics

We have all the metrics to track how many series, samples and queries each tenant is sending but don't have dashboards that help with this. We plan to have dashboards and UIs that will help operators monitor and control each tenants usage out of the box.

## Downsampling
Downsampling means storing fewer samples, e.g. one per minute instead of one every 15 seconds.
This makes queries over long periods more efficient. It can reduce storage space slightly if the full-detail data is discarded.

## Per-metric retention

Cortex blocks storage supports deleting all data for a tenant after a time period (e.g. 3 months, 1 year), but we would also like to have custom retention for subsets of metrics (e.g. delete server metrics but retain business metrics).

## Exemplar support
[Exemplars](https://docs.google.com/document/d/1ymZlc9yuTj8GvZyKz1r3KDRrhaOjZ1W1qZVW_5Gj7gA/edit)
let you link metric samples to other data, such as distributed tracing.
As of early 2021 Prometheus will collect exemplars and send them via remote write, but Cortex needs to be extended to handle them.

## Scalability

Scalability has always been a focus for the project, but there is a lot more work to be done. We can now scale to 100s of Millions of active series but 1 Billion active series is still an unknown.
