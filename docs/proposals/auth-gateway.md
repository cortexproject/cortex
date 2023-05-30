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

If you run Cortex for multiple tenants you need to identify your tenants every time they send metrics or query them.
This is needed to ensure that metrics can be ingested and queried separately from each other. For this purpose, the Cortex microservices require you to pass a Header called `X-Scope-OrgID`.
The Cortex k8s manifests suggest deploying an NGINX server inside of each tenant which acts as a reverse proxy.
Its sole purpose is proxying the traffic and setting the `X-Scope-OrgID` header for your tenant.

## Proposal

We propose to solve this problem by adding a gateway that can be considered the entry point for all requests towards Cortex.
This gateway will be responsible for handling multi-tenancy features without complicated proxy configuration.

This gateway will be located in another repository, [cortexproject/auth-gateway](https://github.com/cortexproject/auth-gateway).
For the first version, basic authentication will be supported.
However, as the project grows, it is planned to support different authentication mechanisms according to the needs of the users.

We would like to have a simple configuration format.
The following is the proposed configuration:

```yaml
server:
  address: localhost
  port: 8080
debug:
  address: localhost
  port: 8090
tenants:
  - authentication: basic
    username: username1
    password: password1
    id: orgid

  - authentication: basic
    username: username2
    password: password2
    id: orgid
distributor:
  dns_refresh_interval: 3s
  url: http://distributor:9009
  paths:
  - /api/v1/push
  - /api/prom/push
  http_client_timeout: 20
  read_timeout: 5
  write_timeout: 5
  idle_timeout: 7
frontend:
  url: http://frontend:9010
  read_timeout: 5
  write_timeout: 5
  idle_timeout: 7

...

```

`debug` will be used for endpoints that do not require authentication, ie. /metrics, /ready, /pprof.
If no paths are given in the component, such as `frontend`, the default API paths of that component will be registered.
If paths are provided, then the provided API paths will override the default API paths.
Additionally, the users will be able to specify custom timeouts for each component similar to [NGINX](https://github.com/cortexproject/cortex-helm-chart/blob/571fc2a5f184b6b7c243bac3727503264249bfd1/templates/nginx/nginx-config.yaml#L50-L55).

Note that only the distributor and query-frontend are mentioned in the example configuration.
However, in addition to the distributor and query-frontend, alertmanager and ruler will be supported.
The same configurations can be done for these components as well.

The time values that are specified in the above configuration are not tested and are only given as an example.
The default values for these fields will be decided later when comprehensive tests are conducted.

Finally, we will ensure the load is balanced between the targets similar to NGINX in which there is a configurable dns refresh parameter and a round-robin between all the hosts.
