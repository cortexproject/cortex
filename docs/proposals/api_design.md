---
title: "HTTP API Design"
linkTitle: "HTTP API Design"
weight: 1
slug: http-api-design
---

# HTTP API Design

## Overview

The purpose of this design document is to propose a set of standards that should be the basis of the Cortex HTTP API. This document will outline the current state of the Cortex http api and describe limitations that result from the current approach. It will also outline a set of paradigms on how http routes should be created within Cortex.

## Current Design

As things currently stand, the majority of HTTP API calls exist under the `/api/prom` path prefix. This prefix is configurable. However, since this prefix is shared between all the modules which leads to conflicts if the Alertmanager is attempted to be run as as part of the single binary.

## Proposed Design

### Module-Based Routing

Cortex incorporates three separate APIs: Alertmanager, Prometheus, and Cortex. Each of these APIs should use a separate route prefix that accurately describes the API. Currently, all of the api calls in Cortex reside under the configured http prefix. Instead the following routing tree is proposed:

- `/prometheus/*` -- Under this path prefix, Cortex will act as a Prometheus web server. It will host all of the required Prometheus api endpoints. For example to query cortex the endpoint `/prometheus/api/v1/query_range` will be used.
- `/alertmanager/*` -- Under this path prefix, Cortex will act as a Alertmanager web server. In this case, it will forward requests to the alertmanager and support the alertmanager API
- `/api/v1/*` -- The cortex API will exist under this path prefix.
  - `/push` & `/read` -- The Prometheus remote write endpoints will be fulfilled directly under the `/api/v1/*` path
  - `/user_stats` -- The per user stats endpoint will exist at this endpoint
  - `/chunks` 
  - `/rules/*`
- `/` -- Endpoints directly exposed under the root `/` path will be maintained as is.
  - `/config`
  - `/ring`
  - `/ruler_ring`
  - `/compactor_ring`
  - `/ha-tracker`
  - `/all_user_stats`
  - `/flush`
  - `/shutdown`

### Path Versioning

Cortex will utilize path based versioning similar to both Prometheus and Alertmanager. This will allow future versions of the API to be released with changes over time. 

### Backwards-Compatibility

The new API endpoints and the current http prefix endpoints can be maintained concurrently. The original http prefix endpoints can maintained indefinitely or be phased out over time. Deprecation warnings can be added to the current API either when initialized or utilized.

In cases where Cortex is run as a single binary, the Alertmanager module will only be accesible using the new API.

### Implementation

This will be implemented by adding an API module to the Cortex service. This module will handle setting up all the required HTTP routes with Cortex. It will be designed around a set of interfaces required to fulfill the API. This is similar to how the `v1` Prometheus API is implemented.