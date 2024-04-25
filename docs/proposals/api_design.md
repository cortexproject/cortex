---
title: "HTTP API Design"
linkTitle: "HTTP API Design"
weight: 1
slug: http-api-design
---

- Author: @jtlisi
- Reviewers: @pracucci, @pstibrany, @khaines, @gouthamve
- Date: March 2020
- Status: Accepted

## Overview

The purpose of this design document is to propose a set of standards that should be the basis of the Cortex HTTP API. This document will outline the current state of the Cortex http api and describe limitations that result from the current approach. It will also outline a set of paradigms on how http routes should be created within Cortex.

## Current Design

As things currently stand, the majority of HTTP API calls exist under the `/api/prom` path prefix. This prefix is configurable. However, since this prefix is shared between all the modules which leads to conflicts if the Alertmanager is attempted to be run as as part of the single binary (#1722).

## Proposed Design

### Module-Based Routing

Cortex incorporates three separate APIs: Alertmanager, Prometheus, and Cortex. Each of these APIs should use a separate route prefix that accurately describes the API. Currently, all of the api calls in Cortex reside under the configured http prefix. Instead the following routing tree is proposed:

#### `/prometheus/*`

Under this path prefix, Cortex will act as a Prometheus web server. It will host all of the required Prometheus api endpoints. For example to query cortex the endpoint `/prometheus/api/v1/query_range` will be used.

#### `/alertmanager/*`

Under this path prefix, Cortex will act as a Alertmanager web server. In this case, it will forward requests to the alertmanager and support the alertmanager API. This means for a user to access their Alertmanager UI, they will use the `/alertmanager` path of cortex.

#### `/api/v1/*` -- The cortex API will exist under this path prefix.

- `/push`
- `/chunks`
- `/rules/*`

| Current             | Proposed          |
| ------------------- | ----------------- |
| `/api/prom/push`    | `/api/v1/push`    |
| `/api/prom/chunks`  | `/api/v1/chunks`  |
| `/api/prom/rules/*` | `/api/v1/rules/*` |


#### Service Endpoints

A number of endpoints currently exist that are not under the `/api/prom` prefix that provide basic web interfaces and trigger operations for cortex services. These endpoints will all be placed under a url with their service name as a prefix if it is applicable.

| Current               | Proposed                           |
| --------------------- | ---------------------------------- |
| `/status`             | `/multitenant-alertmanager/status` |
| `/config`             | `/config`                          |
| `/ring`               | `/ingester/ring`                   |
| `/ruler_ring`         | `/ruler/ring`                      |
| `/compactor/ring`     | `/compactor/ring`                  |
| `/store-gateway/ring` | `/store-gateway/ring`              |
| `/ha-tracker`         | `/distributor/ha_tracker`          |
| `/all_user_stats`     | `/distributor/all_user_stats`      |
| `/user_stats`         | `/distributor/user_stats`          |
| `/flush`              | `/ingester/flush`                  |
| `/shutdown`           | `/ingester/shutdown`               |

### Path Versioning

Cortex will utilize path based versioning similar to both Prometheus and Alertmanager. This will allow future versions of the API to be released with changes over time.

### Backwards-Compatibility

The new API endpoints and the current http prefix endpoints can be maintained concurrently. The flag to configure these endpoints will be maintained as `http.prefix`. This will allow us to roll out the new API without disrupting the current routing schema. The original http prefix endpoints can maintained indefinitely or be phased out over time. Deprecation warnings can be added to the current API either when initialized or utilized. This can be accomplished by injecting a middleware that logs a warning whenever a legacy API endpoint is used.

In cases where Cortex is run as a single binary, the Alertmanager module will only be accessible using the new API.

### Implementation

This will be implemented by adding an API module to the Cortex service. This module will handle setting up all the required HTTP routes with Cortex. It will be designed around a set of interfaces required to fulfill the API. This is similar to how the `v1` Prometheus API is implemented.

### Style

* All new paths will utilize `_` instead of `-` for their url to conform with Prometheus and its use of the underscore in the `query_range` endpoint. This applies to all operations endpoints. Component names in the path can still contain dashes. For example: `/store-gateway/ring`.
