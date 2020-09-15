---
title: "Roadmap"
linkTitle: "Roadmap"
weight: 50
slug: roadmap
---

The following is only a selection of some of the major features we plan to implement in the near future. To get a more complete overview of planned features and current work, see the issue trackers for the various repositories, for example, the [Cortex repo](https://github.com/cortexproject/cortex/issues). Note that these are not ordered by priority.

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

## Downsampling and Per tenant/metric retention

Currently, we only support a single retention period for all metrics and tenants. For most operators, the ability to set per tenant retention and also custom retention for subsets of metrics is important. We will add support per tenant and metric retention policies. Also, we currently store all the samples we ingested, and there is no way to reduce the resolution for the metrics. We plan to add downsampling to allow users to store less data when needed.

## Soft Multitenancy

Currently our multitenancy allows a tenant to view _all_ their metrics and only their metrics. There is no way for an "admin" tenant to view all the metrics in the system but for particular teams to only view theirs. This is another feature we plan to add into Cortex.

## Exemplar and Prometheus metadata support

There is currently an ongoing effort in Prometheus to add [exemplar support](https://docs.google.com/document/d/1ymZlc9yuTj8GvZyKz1r3KDRrhaOjZ1W1qZVW_5Gj7gA/edit) and we should be an active stakeholder in the discussion. The plan is to propagate the exemplars through remote write and make them available for querying in Cortex. We currently have experimental metadata support for Prometheus but this is using the Grafana Cloud Agent. We should help move this [PR forward](https://github.com/prometheus/prometheus/pull/6815) and also add persistence of the metadata (right now it's only in-mem).

## Bulk loading historical data

This is another highly requested features. There is currently no way to backfill the existing data in local Prometheus to Cortex. The plan is to add an API for users to ship the TSDB blocks to Cortex and a side-car / command to do this.

## Scalability

Scalability has always been a focus for the project, but there is a lot more work to be done. We can now scale to 100s of Millions of active series but 1 Billion active series is still an unknown. We also need to make the Alertmanager horizontally scalable with the number of users.
