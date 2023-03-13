---
title: "Authentication Gateway"
linkTitle: "Authentication Gateway"
weight: 1
slug: "auth-gateway"
---

- Author: [Doğukan Teber](https://github.com/dogukanteber)
- Date: March 2023
- Status: Proposedj

## Overview

If you run Cortex for multiple tenants you need to identify your tenants every time they send metrics or query them. This is needed to ensure that metrics can be ingested and queried separately from each other. For this purpose, the Cortex microservices require you to pass a Header called `X-Scope-OrgID`. Unfortunately, the Prometheus Remote write API has no config option to send headers and for Grafana you must provide a data source to do so. Therefore the Cortex k8s manifests suggest deploying an NGINX server inside of each tenant which acts as a reverse proxy. Its sole purpose is proxying the traffic and setting the `X-Scope-OrgID` header for your tenant.

## Proposal

We propose to solve this problem by adding a gateway that can be considered the entry point for all requests towards Cortex. This gateway will be responsible for handling multi-tenancy features without complicated proxy configuration.


Here is an example configuration:

```yaml

basic:
  username:
    password: "password"
    tenant: "orgid"
  username2:
    password: "password2"
    tenant: "orgid"
routes:
  - path: "/api/v1/push"
    target: "distributor"
  - path: "/prometheus"
    prefix: true
    target: "query-frontend"
  - path: "/api/v1/alerts"
    target: "ruler"
  - path: "/api/v1/rules"
    prefix: true
    target: "ruler"
  - path: "/alertmanager"
    prefix: true
    target: "alertmanager"
targets:
  distributor: "http://127.0.0.1:8004"
  query-frontend: "http://127.0.0.1:8004"
  alertmanager: "http://127.0.0.1:8004"
  ruler: "http://127.0.0.1:8004"
  notify: "http://127.0.0.1:8100"

```

The above configuration is an example given by [one of the developers](https://github.com/cortexproject/cortex/issues/5106#issuecomment-1414759680) in the community. The configuration file might change during the implementation phase according to the needs. However, it is aimed to be easy to use.

We will also ensure the load is balanced between the targets and proper timeouts are configured for each service as nginx [supports](https://github.com/cortexproject/cortex-helm-chart/blob/master/templates/nginx/nginx-config.yaml ) in the current helm chart.
