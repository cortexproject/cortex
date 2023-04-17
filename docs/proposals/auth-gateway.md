---
title: "Authentication Gateway"
linkTitle: "Authentication Gateway"
weight: 1
slug: "auth-gateway"
---

- Author: [Doğukan Teber](https://github.com/dogukanteber)
- Date: March 2023
- Status: Proposed

## Overview

If you run Cortex for multiple tenants you need to identify your tenants every time they send metrics or query them. This is needed to ensure that metrics can be ingested and queried separately from each other. For this purpose, the Cortex microservices require you to pass a Header called `X-Scope-OrgID`. Unfortunately, the Prometheus Remote write API has no config option to send headers and for Grafana you must provide a data source to do so. Therefore the Cortex k8s manifests suggest deploying an NGINX server inside of each tenant which acts as a reverse proxy. Its sole purpose is proxying the traffic and setting the `X-Scope-OrgID` header for your tenant.

## Proposal

We propose to solve this problem by adding a gateway that can be considered the entry point for all requests towards Cortex. This gateway will be responsible for handling multi-tenancy features without complicated proxy configuration.

This gateway will be located in another repository, [cortexproject/auth-gateway](https://github.com/cortexproject/auth-gateway). For the first version, basic authentication will be supported. However, as the project grows, it is planned to support different authentication mechanisms according to the needs of the users.

We would like to have a simple configuration format. The following is the proposed configuration:

```yaml
server:
  address: localhost
  port: 8080
admin:
  address: localhost
  port: 8090
tenants:
  - authentication: basic
    username: username1
    password: password1
    id: "orgid"

  - authentication: basic
    username: username2
    password: password2
    id: "orgid"
distributor:
  url: http://127.0.0.1:9009
  paths: 
  - /api/v1/push
  - /api/prom/push
  read_timeout: 5
  write_timeout: 5
  idle_timeout: 7
frontend:
  url: http://127.0.0.1:9009
  read_timeout: 5
  write_timeout: 5
  idle_timeout: 7

...

```

`admin` will be used for endpoints that do not require authentication, ie. /metrics, /ready, /pprof. If no paths are given in the component, such as `frontend`, all of the endpoints of that component will be registered. Additionally, the users will be able to specify custom timeouts for each component as opposed to [NGINX](https://github.com/cortexproject/cortex-helm-chart/blob/571fc2a5f184b6b7c243bac3727503264249bfd1/templates/nginx/nginx-config.yaml#L50-L55).

Note that only the distributor and query-frontend are mentioned in the example configuration. However, in addition to the distributor and query-frontend, alertmanager and ruler will be supported.

Finally, we will ensure the load is balanced between the targets, though the implementation details it is not decided yet.