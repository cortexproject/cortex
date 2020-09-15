---
title: "HTTP API"
linkTitle: "HTTP API"
weight: 5
slug: api
menu:
no_section_index_title: true
---

Cortex exposes an HTTP API for pushing and querying time series data, and operating the cluster itself.

For the sake of clarity, in this document we have grouped API endpoints by service, but keep in mind that they're exposed both when running Cortex in microservices and singly-binary mode:
- **Microservices**: each service exposes its own endpoints
- **Single-binary**: the Cortex process exposes all API endpoints for the services running internally

## Endpoints

| API | Service | Endpoint |
| --- | ------- | -------- |
| [Index page](#index-page) | _All services_ | `GET /` |
| [Configuration](#configuration) | _All services_ | `GET /config` |
| [Services status](#services-status) | _All services_ | `GET /services` |
| [Readiness probe](#readiness-probe) | _All services_ | `GET /ready` |
| [Metrics](#metrics) | _All services_ | `GET /metrics` |
| [Remote write](#remote-write) | Distributor | `POST /api/v1/push` |
| [Tenants stats](#tenants-stats) | Distributor | `GET /distributor/all_user_stats` |
| [HA tracker status](#ha-tracker-status) | Distributor | `GET /distributor/ha_tracker` |
| [Flush chunks / blocks](#flush-chunks--blocks) | Ingester | `GET,POST /ingester/flush` |
| [Shutdown](#shutdown) | Ingester | `GET,POST /ingester/shutdown` |
| [Ingesters ring status](#ingesters-ring-status) | Ingester | `GET /ingester/ring` |
| [Instant query](#instant-query) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query` |
| [Range query](#range-query) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/query_range` |
| [Get series by label matchers](#get-series-by-label-matchers) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/series` |
| [Get label names](#get-label-names) | Querier, Query-frontend | `GET,POST <prometheus-http-prefix>/api/v1/labels` |
| [Get label values](#get-label-values) | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/label/{name}/values` |
| [Get metric metadata](#get-metric-metadata) | Querier, Query-frontend | `GET <prometheus-http-prefix>/api/v1/metadata` |
| [Remote read](#remote-read) | Querier, Query-frontend | `POST <prometheus-http-prefix>/api/v1/read` |
| [Get tenant ingestion stats](#get-tenant-ingestion-stats) | Querier | `GET /api/v1/user_stats` |
| [Get tenant chunks](#get-tenant-chunks) | Querier | `GET /api/v1/chunks` |
| [Ruler ring status](#ruler-ring-status) | Ruler | `GET /ruler/ring` |
| [List rules](#list-rules) | Ruler | `GET <prometheus-http-prefix>/api/v1/rules` |
| [List alerts](#list-alerts) | Ruler | `GET <prometheus-http-prefix>/api/v1/alerts` |
| [List rule groups](#list-rule-groups) | Ruler | `GET /api/v1/rules` |
| [Get rule groups by namespace](#get-rule-groups-by-namespace) | Ruler | `GET /api/v1/rules/{namespace}` |
| [Get rule group](#get-rule-group) | Ruler | `GET /api/v1/rules/{namespace}/{groupName}` |
| [Set rule group](#set-rule-group) | Ruler | `POST /api/v1/rules/{namespace}` |
| [Delete rule group](#delete-rule-group) | Ruler | `DELETE /api/v1/rules/{namespace}/{groupName}` |
| [Delete namespace](#delete-namespace) | Ruler | `DELETE /api/v1/rules/{namespace}` |
| [Alertmanager status](#alertmanager-status) | Alertmanager | `GET /multitenant_alertmanager/status` |
| [Alertmanager UI](#alertmanager-ui) | Alertmanager | `GET /<alertmanager-http-prefix>` |
| [Get Alertmanager configuration](#get-alertmanager-configuration) | Alertmanager | `GET /api/v1/alerts` |
| [Set Alertmanager configuration](#set-alertmanager-configuration) | Alertmanager | `POST /api/v1/alerts` |
| [Delete Alertmanager configuration](#delete-alertmanager-configuration) | Alertmanager | `DELETE /api/v1/alerts` |
| [Delete series](#delete-series) | Purger | `PUT,POST <prometheus-http-prefix>/api/v1/admin/tsdb/delete_series` |
| [List delete requests](#list-delete-requests) | Purger | `GET <prometheus-http-prefix>/api/v1/admin/tsdb/delete_series` |
| [Cancel delete request](#cancel-delete-request) | Purger | `PUT,POST <prometheus-http-prefix>/api/v1/admin/tsdb/cancel_delete_request` |
| [Store-gateway ring status](#store-gateway-ring-status) | Store-gateway | `GET /store-gateway/ring` |
| [Compactor ring status](#compactor-ring-status) | Compactor | `GET /compactor/ring` |
| [Get rule files](#get-rule-files) | Configs API (deprecated) | `GET /api/prom/configs/rules` |
| [Set rule files](#set-rule-files) | Configs API (deprecated) | `POST /api/prom/configs/rules` |
| [Get template files](#get-template-files) | Configs API (deprecated) | `GET /api/prom/configs/templates` |
| [Set template files](#set-template-files) | Configs API (deprecated) | `POST /api/prom/configs/templates` |
| [Get Alertmanager config file](#get-alertmanager-config-file) | Configs API (deprecated) | `GET /api/prom/configs/alertmanager` |
| [Set Alertmanager config file](#set-alertmanager-config-file) | Configs API (deprecated) | `POST /api/prom/configs/alertmanager` |
| [Validate Alertmanager config](#validate-alertmanager-config-file) | Configs API (deprecated) | `POST /api/prom/configs/alertmanager/validate` |
| [Deactivate configs](#deactivate-configs) | Configs API (deprecated) | `DELETE /api/prom/configs/deactivate` |
| [Restore configs](#restore-configs) | Configs API (deprecated) | `POST /api/prom/configs/restore` |


### Path prefixes

In this documentation you will find the usage of some placeholders for the path prefixes, whenever the prefix is configurable. The following table shows the supported prefixes.

| Prefix                       | Default         | CLI Flag                         | YAML Config |
| ---------------------------- | --------------- | -------------------------------- | ----------- |
| `<legacy-http-prefix>`       | `/api/prom`     | `-http.prefix`                   | `http_prefix` |
| `<prometheus-http-prefix>`   | `/prometheus`   | `-http.prometheus-http-prefix`   | `api > prometheus_http_prefix` |
| `<alertmanager-http-prefix>` | `/alertmanager` | `-http.alertmanager-http-prefix` | `api > alertmanager_http_prefix` |

### Authentication

When multi-tenancy is enabled, endpoints requiring authentication are expected to be called with the `X-Scope-OrgID` HTTP request header set to the tenant ID. Otherwise, when multi-tenancy is disabled, Cortex doesn't require any request to have the `X-Scope-OrgID` header.

Multi-tenancy can be enabled/disabled via the CLI flag `-auth.enabled` or its respective YAML config option.

_For more information, please refer to the dedicated [Authentication and Authorisation](../production/auth.md) guide._

## All services

The following API endpoints are exposed by all services.

### Index page

```
GET /
```

Displays an index page with links to other web pages exposed by Cortex.

### Configuration

```
GET /config
```

Displays the configuration currently applied to Cortex (in YAML format), including default values and settings via CLI flags. Sensitive data is masked. Please be aware that the exported configuration **doesn't include the per-tenant overrides**.

### Services status

```
GET /services
```

Displays a web page with the status of internal Cortex services.

### Readiness probe

```
GET /ready
```

Returns 200 when Cortex is ready to serve traffic.

### Metrics

```
GET /metrics
```

Returns the metrics for the running Cortex service in the Prometheus exposition format.

## Distributor

### Remote write

```
POST /api/v1/push

# Legacy
POST <legacy-http-prefix>/push
```

Entrypoint for the [Prometheus remote write](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write).

This API endpoint accepts an HTTP POST request with a body containing a request encoded with [Protocol Buffers](https://developers.google.com/protocol-buffers) and compressed with [Snappy](https://github.com/google/snappy). The definition of the protobuf message can be found in [`cortex.proto`](https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex.proto#28). The HTTP request should contain the header `X-Prometheus-Remote-Write-Version` set to `0.1.0`.

_For more information, please check out Prometheus [Remote storage integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)._

_Requires [authentication](#authentication)._

### Tenants stats

```
GET /distributor/all_user_stats

# Legacy
GET /all_user_stats
```

Displays a web page with per-tenant statistics updated in realtime, including the total number of active series across all ingesters and the current ingestion rate (samples / sec).

### HA tracker status

```
GET /distributor/ha_tracker

# Legacy
GET /ha-tracker
```

Displays a web page with the current status of the HA tracker, including the elected replica for each Prometheus HA cluster.

## Ingester

### Flush chunks / blocks

```
GET,POST /ingester/flush

# Legacy
GET,POST /flush
```

Triggers a flush of the in-memory time series data (chunks or blocks) to the long-term storage. This endpoint triggers the flush also when `-ingester.flush-on-shutdown-with-wal-enabled` or `-blocks-storage.tsdb.flush-blocks-on-shutdown` are disabled.

### Shutdown

```
GET,POST /ingester/shutdown

# Legacy
GET,POST /shutdown
```

Flushes in-memory time series data from ingester to the long-term storage, and shuts down the ingester service. Notice that the other Cortex services are still running, and the operator (or any automation) is expected to terminate the process with a `SIGINT` / `SIGTERM` signal after the shutdown endpoint returns. In the meantime, `/ready` will not return 200.

_This API endpoint is usually used by scale down automations._

### Ingesters ring status

```
GET /ingester/ring

# Legacy
GET /ring
```

Displays a web page with the ingesters hash ring status, including the state, healthy and last heartbeat time of each ingester.


## Querier / Query-frontend

The following endpoints are exposed both by the querier and query-frontend.

### Instant query

```
GET,POST <prometheus-http-prefix>/api/v1/query

# Legacy
GET,POST <legacy-http-prefix>/api/v1/query
```

Prometheus-compatible instant query endpoint.

_For more information, please check out the Prometheus [instant query](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries) documentation._

_Requires [authentication](#authentication)._

### Range query

```
GET,POST <prometheus-http-prefix>/api/v1/query_range

# Legacy
GET,POST <legacy-http-prefix>/api/v1/query_range
```

Prometheus-compatible range query endpoint. When the request is sent through the query-frontend, the query will be accelerated by query-frontend (results caching and execution parallelisation).

_For more information, please check out the Prometheus [range query](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries) documentation._

_Requires [authentication](#authentication)._

### Get series by label matchers

```
GET,POST <prometheus-http-prefix>/api/v1/series

# Legacy
GET,POST <legacy-http-prefix>/api/v1/series
```

Find series by label matchers. Differently than Prometheus and due to scalability and performances reasons, Cortex currently ignores the `start` and `end` request parameters and always fetches the series from in-memory data stored in the ingesters.

_For more information, please check out the Prometheus [series endpoint](https://prometheus.io/docs/prometheus/latest/querying/api/#finding-series-by-label-matchers) documentation._

_Requires [authentication](#authentication)._

### Get label names

```
GET,POST <prometheus-http-prefix>/api/v1/labels

# Legacy
GET,POST <legacy-http-prefix>/api/v1/labels
```

Get label names of ingested series. Differently than Prometheus and due to scalability and performances reasons, Cortex currently ignores the `start` and `end` request parameters and always fetches the label names from in-memory data stored in the ingesters.

_For more information, please check out the Prometheus [get label names](https://prometheus.io/docs/prometheus/latest/querying/api/#getting-label-names) documentation._

_Requires [authentication](#authentication)._

### Get label values

```
GET <prometheus-http-prefix>/api/v1/label/{name}/values

# Legacy
GET <legacy-http-prefix>/api/v1/label/{name}/values
```

Get label values for a given label name. Differently than Prometheus and due to scalability and performances reasons, Cortex currently ignores the `start` and `end` request parameters and always fetches the label values from in-memory data stored in the ingesters.

_For more information, please check out the Prometheus [get label values](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-label-values) documentation._

_Requires [authentication](#authentication)._

### Get metric metadata

```
GET <prometheus-http-prefix>/api/v1/metadata

# Legacy
GET <legacy-http-prefix>/api/v1/metadata
```

Prometheus-compatible metric metadata endpoint.

_For more information, please check out the Prometheus [metric metadata](https://prometheus.io/docs/prometheus/latest/querying/api/#querying-metric-metadata) documentation._

_Requires [authentication](#authentication)._

### Remote read

```
POST <prometheus-http-prefix>/api/v1/read

# Legacy
POST <legacy-http-prefix>/api/v1/read
```

Prometheus-compatible [remote read](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read) endpoint.

_For more information, please check out Prometheus [Remote storage integrations](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)._

_Requires [authentication](#authentication)._


## Querier

### Get tenant ingestion stats

```
GET /api/v1/user_stats

# Legacy
GET <legacy-http-prefix>/user_stats
```

Returns realtime ingestion rate, for the authenticated tenant, in `JSON` format.

_Requires [authentication](#authentication)._

### Get tenant chunks

```
GET /api/v1/chunks

# Legacy
GET <legacy-http-prefix>/chunks
```

Fetch a compressed tar of all the chunks containing samples for the given time range and label matchers. This endpoint is supported only by the **chunks storage**, requires `-querier.ingester-streaming=true` and should **not be exposed to users** but just used for debugging purposes.

| URL query parameter | Description |
| ------------------- | ----------- |
| `start` | Start timestamp, in RFC3339 format or unix epoch. |
| `end` | End timestamp, in RFC3339 format or unix epoch. |
| `matcher` | Label matcher that selects the series for which chunks should be fetched. |

_Requires [authentication](#authentication)._

## Ruler

The ruler API endpoints require to configure a backend object storage to store the recording rules and alerts. The ruler API uses the concept of a "namespace" when creating rule groups. This is a stand in for the name of the rule file in Prometheus and rule groups must be named uniquely within a namespace.

### Ruler ring status

```
GET /ruler/ring

# Legacy
GET /ruler_ring
```

Displays a web page with the ruler hash ring status, including the state, healthy and last heartbeat time of each ruler.

### List rules

```
GET <prometheus-http-prefix>/api/v1/rules

# Legacy
GET <legacy-http-prefix>/api/v1/rules
```

Prometheus-compatible rules endpoint to list alerting and recording rules that are currently loaded.

_For more information, please check out the Prometheus [rules](https://prometheus.io/docs/prometheus/latest/querying/api/#rules) documentation._

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### List alerts

```
GET <prometheus-http-prefix>/api/v1/alerts

# Legacy
GET <legacy-http-prefix>/api/v1/alerts
```

Prometheus-compatible rules endpoint to list of all active alerts.

_For more information, please check out the Prometheus [alerts](https://prometheus.io/docs/prometheus/latest/querying/api/#alerts) documentation._

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### List rule groups

```
GET /api/v1/rules

# Legacy
GET <legacy-http-prefix>/rules
```

List all rules configured for the authenticated tenant. This endpoint returns a YAML dictionary with all the rule groups for each namespace and `200` status code on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Example response

```yaml
---
<namespace1>:
- name: <string>
  interval: <duration;optional>
  rules:
  - record: <string>
      expr: <string>
  - alert: <string>
      expr: <string>
      for: <duration>
      annotations:
      <annotation_name>: <string>
      labels:
      <label_name>: <string>
- name: <string>
  interval: <duration;optional>
  rules:
  - record: <string>
      expr: <string>
  - alert: <string>
      expr: <string>
      for: <duration>
      annotations:
      <annotation_name>: <string>
      labels:
      <label_name>: <string>
<namespace2>:
- name: <string>
  interval: <duration;optional>
  rules:
  - record: <string>
      expr: <string>
  - alert: <string>
      expr: <string>
      for: <duration>
      annotations:
      <annotation_name>: <string>
      labels:
      <label_name>: <string>
```

### Get rule groups by namespace

```
GET /api/v1/rules/{namespace}

# Legacy
GET <legacy-http-prefix>/rules/{namespace}
```

Returns the rule groups defined for a given namespace.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Example response

```yaml
name: <string>
interval: <duration;optional>
rules:
  - record: <string>
    expr: <string>
  - alert: <string>
    expr: <string>
    for: <duration>
    annotations:
      <annotation_name>: <string>
    labels:
      <label_name>: <string>
```

### Get rule group

```
GET /api/v1/rules/{namespace}/{groupName}

# Legacy
GET <legacy-http-prefix>/rules/{namespace}/{groupName}
```

Returns the rule group matching the request namespace and group name.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Set rule group

```
POST /api/v1/rules/{namespace}

# Legacy
POST <legacy-http-prefix>/rules/{namespace}
```

Creates or updates a rule group. This endpoint expects a request with `Content-Type: application/yaml` header and the rules **YAML** definition in the request body, and returns `202` on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Example request

Request headers:
- `Content-Type: application/yaml`

Request body:

```yaml
name: <string>
interval: <duration;optional>
rules:
  - record: <string>
    expr: <string>
  - alert: <string>
    expr: <string>
    for: <duration>
    annotations:
      <annotation_name>: <string>
    labels:
      <label_name>: <string>
```

### Delete rule group

```
DELETE /api/v1/rules/{namespace}/{groupName}

# Legacy
DELETE <legacy-http-prefix>/rules/{namespace}/{groupName}
```

Deletes a rule group by namespace and group name. This endpoints returns `202` on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Delete namespace

```
DELETE /api/v1/rules/{namespace}

# Legacy
DELETE <legacy-http-prefix>/rules/{namespace}
```

Deletes all the rule groups in a namespace (including the namespace itself). This endpoint returns `202` on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.ruler.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

## Alertmanager

### Alertmanager status

```
GET /multitenant_alertmanager/status

# Legacy (microservices mode only)
GET /status
```

Displays a web page with the current status of the Alertmanager, including the Alertmanager cluster members.

### Alertmanager UI

```
GET /<alertmanager-http-prefix>

# Legacy (microservices mode only)
GET /<legacy-http-prefix>
```

Displays the Alertmanager UI.

_Requires [authentication](#authentication)._

### Get Alertmanager configuration

```
GET /api/v1/alerts
```

Get the current Alertmanager configuration for the authenticated tenant, reading it from the configured object storage.

This endpoint doesn't accept any URL query parameter and returns `200` on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.alertmanager.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

### Set Alertmanager configuration

```
POST /api/v1/alerts
```

Stores or updates the Alertmanager configuration for the authenticated tenant. The Alertmanager configuration is stored in the configured backend object storage.

This endpoint expects the Alertmanager **YAML** configuration in the request body and returns `201` on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.alertmanager.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

#### Example request body

```yaml
template_files:
  default_template: |
    {{ define "__alertmanager" }}AlertManager{{ end }}
    {{ define "__alertmanagerURL" }}{{ .ExternalURL }}/#/alerts?receiver={{ .Receiver | urlquery }}{{ end }}
alertmanager_config: |
  global:
    smtp_smarthost: 'localhost:25'
    smtp_from: 'youraddress@example.org'
  route:
    receiver: example-email
  receivers:
    - name: example-email
      email_configs:
      - to: 'youraddress@example.org'
```

### Delete Alertmanager configuration

```
DELETE /api/v1/alerts
```

Deletes the Alertmanager configuration for the authenticated tenant.

This endpoint doesn't accept any URL query parameter and returns `200` on success.

_This experimental endpoint is disabled by default and can be enabled via the `-experimental.alertmanager.enable-api` CLI flag (or its respective YAML config option)._

_Requires [authentication](#authentication)._

## Purger

The Purger service provides APIs for requesting deletion of series in chunks storage and managing delete requests. For more information about it, please read the [Delete series Guide](../guides/deleting-series.md).

### Delete series

```
PUT,POST <prometheus-http-prefix>/api/v1/admin/tsdb/delete_series

# Legacy
PUT,POST <legacy-http-prefix>/api/v1/admin/tsdb/delete_series
```

Prometheus-compatible delete series endpoint.

_For more information, please check out the Prometheus [delete series](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series) documentation._

_Requires [authentication](#authentication)._

### List delete requests

```
GET <prometheus-http-prefix>/api/v1/admin/tsdb/delete_series

# Legacy
GET <legacy-http-prefix>/api/v1/admin/tsdb/delete_series
```

List all the delete requests.

_Requires [authentication](#authentication)._

### Cancel delete request

```
PUT,POST <prometheus-http-prefix>/api/v1/admin/tsdb/cancel_delete_request

# Legacy
PUT,POST <legacy-http-prefix>/api/v1/admin/tsdb/cancel_delete_request
```

Cancel a delete request while the request is still in the grace period (before the request is effectively processed by the purger and time series data is hard-deleted from the storage).

| URL query parameter | Description |
| ------------------- | ----------- |
| `request_id` | Deletion request ID to cancel. Can be obtained by the [List delete requests](#list-delete-requests) endpoint. |

_Requires [authentication](#authentication)._

## Store-gateway

### Store-gateway ring status
```
GET /store-gateway/ring
```

Displays a web page with the store-gateway hash ring status, including the state, healthy and last heartbeat time of each store-gateway.

## Compactor

### Compactor ring status

```
GET /compactor/ring
```

Displays a web page with the compactor hash ring status, including the state, healthy and last heartbeat time of each compactor.

## Configs API

_This service has been **deprecated** in favour of [Ruler](#ruler) and [Alertmanager](#alertmanager) API._

The configs API service provides an API-driven multi-tenant approach to handling various configuration files for Prometheus. The service hosts an API where users can read and write Prometheus rule files, Alertmanager configuration files, and Alertmanager templates to a database. Each tenant will have its own set of rule files, Alertmanager config, and templates.

#### Request / response schema

The following schema is used both when retrieving the current configs from the API and when setting new configs via the API:

```json
{
    "id": 99,
    "rule_format_version": "2",
    "alertmanager_config": "<standard alertmanager.yaml config>",
    "rules_files": {
        "rules.yaml": "<standard rules.yaml config>",
        "rules2.yaml": "<standard rules.yaml config>"
     },
    "template_files": {
        "templates.tmpl": "<standard template file>",
        "templates2.tmpl": "<standard template file>"
    }
}
```

- **`id`**<br />
  Should be incremented every time data is updated; Cortex will use the config with the highest number.
- **`rule_format_version`**<br />
  Allows compatibility for tenants with config in Prometheus V1 format. Pass "1" or "2" according to which Prometheus version you want to match.
- **`alertmanager_config`**<br />
  The contents of the alertmanager config file should be as described [here](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/), encoded as a single string to fit within the overall JSON payload.
- **`config.rules_files`**<br />
  The contents of a rules file should be as described [here](http://prometheus.io/docs/prometheus/latest/configuration/recording_rules/), encoded as a single string to fit within the overall JSON payload.
- **`config.template_files`**<br />
  The contents of a template file should be as described [here](https://prometheus.io/docs/alerting/notification_examples/#defining-reusable-templates), encoded as a single string to fit within the overall JSON payload.

### Get rule files

```
GET /api/prom/configs/rules
```

Get the current rule files for the authenticated tenant.

_Requires [authentication](#authentication)._

### Set rule files

```
POST /api/prom/configs/rules
```

Replace the current rule files for the authenticated tenant.

_Requires [authentication](#authentication)._

### Get template files

```
GET /api/prom/configs/templates
```

Get the current template files for the authenticated tenant.

_Requires [authentication](#authentication)._

### Set template files

```
POST /api/prom/configs/templates
```

Replace the current template files for the authenticated tenant.

_Requires [authentication](#authentication)._

#### Get Alertmanager config file

```
GET /api/prom/configs/alertmanager
```

Get the current Alertmanager config for the authenticated tenant.

_Requires [authentication](#authentication)._

### Set Alertmanager config file

```
POST /api/prom/configs/alertmanager
```

Replace the current Alertmanager config for the authenticated tenant.

_Requires [authentication](#authentication)._

### Validate Alertmanager config file

```
POST /api/prom/configs/alertmanager/validate
```

Validate the Alertmanager config in the request body. The request body is expected to contain only the Alertmanager YAML config.

### Deactivate configs

```
DELETE /api/prom/configs/deactivate
```

Disable configs for the authenticated tenant. Please be aware that setting a new config will effectively "re-enable" the Rules and Alertmanager configuration for the tenant.

_Requires [authentication](#authentication)._

### Restore configs

```
POST /api/prom/configs/restore
```

Re-enable configs for the authenticated tenant, after being previously deactivated.

_Requires [authentication](#authentication)._
