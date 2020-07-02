---
title: "Roadmap"
linkTitle: "Roadmap"
weight: 50
slug: roadmap 
---

The following is only a selection of some of the major features we plan to implement in the near future. To get a more complete overview of planned features and current work, see the issue trackers for the various repositories, for example, the [Cortex repo](https://github.com/cortexproject/cortex/issues).

## Making a NoSQL store optional

Currently Cortex depends on a NoSQL store (like Bigtable, DynamoDB, Cassandra) for its index storage. This adds a lot of complexity and cost to manage and we are currently working on making the NoSQL store optional by using an ObjectStore for index storage as well.

## helm charts and other packaging

We don't provide a set of helm charts and dashboards / alerts to our users yet. This is one of the most requested features and something we will tackle in the immediate future. We also plan on publishing debs, rpms along with guides on how to run Cortex on bare-metal.

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
