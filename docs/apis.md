---
title: "Cortex APIs"
linkTitle: "Cortex APIs"
weight: 5
slug: apis
---

[this is a work in progress]

## Remote API

Cortex supports Prometheus'
[`remote_read`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read)
and
[`remote_write`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)
APIs.

The API for writes accepts a HTTP POST request with a body containing a request encoded with [Protocol Buffers](https://developers.google.com/protocol-buffers) and compressed with [Snappy](https://github.com/google/snappy).
The HTTP path for writes is `/api/prom/push`.
The definition of the protobuf message can be found in the [Cortex codebase](https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex.proto#L30) or in the [Prometheus codebase](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto#L22).
The HTTP request should contain the header `X-Prometheus-Remote-Write-Version` set to `0.1.0`.

The API for reads also accepts HTTP/protobuf/snappy, and the path is `/api/prom/read`.

See the Prometheus documentation for [more information on the Prometheus remote write format](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations).

## Ruler

### Prometheus Endpoints

Cortex supports the Prometheus' [alerts](https://prometheus.io/docs/prometheus/latest/querying/api/#alerts) and [rules](https://prometheus.io/docs/prometheus/latest/querying/api/#rules) api endpoints. This is supported in the Ruler service and can be enabled using the `experimental.ruler.enable-api` flag.

```
GET /prometheus/api/v1/rules
```

List of alerting and recording rules that are currently loaded in the Ruler. This endpoint retrieves the running rule groups in each ruler for a user, merges them and returns the response to the caller.

```json
$ curl -H 'X-Scope-OrgID:1' http://localhost:9009/prometheus/api/v1/rules

{
    "data": {
        "groups": [
            {
                "rules": [
                    {
                        "alerts": [
                            {
                                "activeAt": "2018-07-04T20:27:12.60602144+02:00",
                                "annotations": {
                                    "summary": "High request latency"
                                },
                                "labels": {
                                    "alertname": "HighRequestLatency",
                                    "severity": "page"
                                },
                                "state": "firing",
                                "value": "1e+00"
                            }
                        ],
                        "annotations": {
                            "summary": "High request latency"
                        },
                        "duration": 600,
                        "health": "ok",
                        "labels": {
                            "severity": "page"
                        },
                        "name": "HighRequestLatency",
                        "query": "job:request_latency_seconds:mean5m{job=\"myjob\"} > 0.5",
                        "type": "alerting"
                    },
                    {
                        "health": "ok",
                        "name": "job:http_inprogress_requests:sum",
                        "query": "sum by (job) (http_inprogress_requests)",
                        "type": "recording"
                    }
                ],
                "file": "rules.yaml",
                "interval": 60,
                "name": "example"
            }
        ]
    },
    "status": "success"
}
```

```
GET /prometheus/api/v1/alerts
```

Returns a list of all active alerts.

```json
$ curl -H 'X-Scope-OrgID:1' http://localhost:9009/prometheus/api/v1/alerts

{
    "data": {
        "alerts": [
            {
                "activeAt": "2018-07-04T20:27:12.60602144+02:00",
                "annotations": {},
                "labels": {
                    "alertname": "my-alert"
                },
                "state": "firing",
                "value": "1e+00"
            }
        ]
    },
    "status": "success"
}
```

### Experimental API

The ruler supports operations using a configured object storage client as a backend for the storage and management of user rule groups. In order to use this API the `experimental.ruler.enable-api` must be set and a valid object storage backend must be configured for the ruler. The ruler API uses the concept of a namespace when creating rule groups. This is a stand in for the name of the rule file in Prometheus and rule groups must be named uniquely within a namespace.

#### List Rule Groups

```
GET /api/v1/rules
```

##### Success Response

**Code**: `200 OK`

**Data**:
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

The success response for the list operation returns a yaml dictionary with all of the rule groups for each rule namespace.

#### Get Rule Group

```
GET /api/v1/rules/{namespace}/{group_name}
```

##### Success Response

**Code**: `200 OK`

**Data**:
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

#### Set Rule Group

```
POST /api/v1/rules/{namespace}
```

**Content-Type**: `application/yaml`

**Body**:

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

##### Success Response

**Code**: `202 ACCEPTED`

**Body**: None

#### Delete Rule Group

```
DELETE /api/v1/rules/{namespace}/{group_name}
```

##### Success Response

**Code**: `202 ACCEPTED`

**Body**: None

## Alertmanager

### Experimental API

Similarly to the Cortex Ruler, the Cortex Alertmanager supports operations using a configured object storage client as a backend for the storage and management of user's Alertmanager configuration. These API endpoints are opt-in and must be enabled via the `experimental.alertmanger.enable-api` CLI flag.

### Get Alertmanager configuration

```
GET /api/v1/alerts
```

##### Success Response

**Code**: `200 OK`

**Body**: None

### Set Alertmanager configuration

```
POST /api/v1/alerts
```

##### Success Response

**Code**: `201 CREATED`

**Body**:

```yaml
template_files:
  default_template: |
    {{ define "__alertmanager" }}AlertManager{{ end }}
    {{ define "__alertmanagerURL" }}{{ .ExternalURL }}/#/alerts?receiver={{ .Receiver | urlquery }}{{ end }}
alertmanager_config: "global: \n  smtp_smarthost: 'localhost:25' \n  smtp_from: 'youraddress@example.org' \nroute: \n  receiver: example-email \nreceivers: \n  - name: example-email \n    email_configs: \n    - to: 'youraddress@example.org' \n"
```

### Delete Alertmanager configuration

```
DELETE /api/v1/alerts
```

##### Success Response

**Code**: `200 OK`

**Body**: None


## Configs API

The configs service provides an API-driven multi-tenant approach to handling various configuration files for prometheus. The service hosts an API where users can read and write Prometheus rule files, Alertmanager configuration files, and Alertmanager templates to a database.

Each tenant will have it's own set of rule files, Alertmanager config, and templates. A POST operation will effectively replace the existing copy with the configs provided in the request body.

### Configs Format

At the current time of writing, the API is part-way through a migration from a single Configs service that handled all three sets of data to a split API ([Tracking issue](https://github.com/cortexproject/cortex/issues/619)). All APIs take and return all sets of data.

The following schema is used both when retrieving the current configs from the API and when setting new configs via the API.

#### Schema:

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

#### Formatting

`id` - should be incremented every time data is updated; Cortex will use the config with the highest number.

`rule_format_version` - allows compatibility for tenants with config in Prometheus V1 format. Pass "1" or "2" according to which Prometheus version you want to match.

`config.alertmanager_config` - The contents of the alertmanager config file should be as described [here](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/), encoded as a single string to fit within the overall JSON payload.

`config.rules_files` - The contents of a rules file should be as described [here](http://prometheus.io/docs/prometheus/latest/configuration/recording_rules/), encoded as a single string to fit within the overall JSON payload.

`config.template_files` - The contents of a template file should be as described [here](https://prometheus.io/docs/alerting/notification_examples/#defining-reusable-templates), encoded as a single string to fit within the overall JSON payload.

### Endpoints

#### Manage Alertmanager

`GET /api/prom/configs/alertmanager` - Get current Alertmanager config

- Normal Response Codes: OK(200)
- Error Response Codes: Unauthorized(401), NotFound(404)

`POST /api/prom/configs/alertmanager` - Replace current Alertmanager config

- Normal Response Codes: NoContent(204)
- Error Response Codes: Unauthorized(401), BadRequest(400)

The POST request body is expected to be like the following example:
```json
{
    "rule_format_version": "2",
    "alertmanager_config": "global:\n  resolve_timeout: 10s\nroute: \n  receiver: webhook\nreceivers:\n  - name: webhook\n    webhook_configs: \n    - url: http://example.com",
    "rules_files": {
        "rules.yaml": "groups:\n- name: demo-service-alerts\n  interval: 1s\n  rules:\n  - alert: SomethingIsUp\n    expr: up == 1\n"
    }
}
```

`POST /api/prom/configs/alertmanager/validate` - Validate Alertmanager config

Normal Response: OK(200)
```json
{
    "status": "success"
}
```

Error Response: BadRequest(400)
```json
{
    "status": "error",
    "error": "error message"
}
```

#### Manage Rules

`GET /api/prom/configs/rules` - Get current rule files

- Normal Response Codes: OK(200)
- Error Response Codes: Unauthorized(400), NotFound(404)

`POST /api/prom/configs/rules` - Replace current rule files

- Normal Response Codes: NoContent(204)
- Error Response Codes: Unauthorized(401), BadRequest(400)

#### Manage Templates

`GET /api/prom/configs/templates` - Get current templates

- Normal Response Codes: OK(200)
- Error Response Codes: Unauthorized(401), NotFound(404)

`POST /api/prom/configs/templates` - Replace current templates

- Normal Response Codes: NoContent(204)
- Error Response Codes: Unauthorized(401), BadRequest(400)

#### Deactivate/Restore Configs

`DELETE /api/prom/configs/deactivate` - Disable configs for a tenant

- Normal Response Codes: OK(200)
- Error Response Codes: Unauthorized(401), NotFound(404)

`POST /api/prom/configs/restore` - Re-enable configs for a tenant

- Normal Response Codes OK(200)
- Error Response Codes: Unauthorized(401), NotFound(404)

These API endpoints will disable/enable the current Rule and Alertmanager configuration for a tenant.

Note that setting a new config will effectively "re-enable" the Rules and Alertmanager configuration for a tenant.

#### Ingester Shutdown

`POST /shutdown` - Shutdown all operations of an ingester. Shutdown operations performed are similar to when an ingester is gracefully shutting down, including flushing of chunks if no other ingester is in `PENDING` state. Ingester does not terminate after calling this endpoint.

- Normal Response Codes: NoContent(204)
- Error Response Codes: Unauthorized(401)

#### Testing APIs

`POST /push` - Push samples directly to ingesters.  Accepts requests in Prometheus remote write format.  Indended for performance testing and debugging.

## Purger APIs

The Purger service provides APIs for requesting Deletion of series in Chunks storage and managing Delete Requests.
Delete Series support is still experimental. Read more about it in [Delete Series Guide](./guides/deleting-series.md)

### Endpoints

#### Delete Series

`POST|PUT /api/v1/admin/tsdb/delete_series?match%5B%5D=<matcher>&start=<start-time>&end=<end-time>` - [Prometheus compatible API](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series) for requesting deletion of series.

- Normal Response Codes Created(204)
- Error Response Codes: Unauthorized(401), NotFound(404)

#### Cancel Delete Request

`POST|PUT /api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>` - Cancel a Delete Requests before they are picked up for processing.

- Normal Response Codes OK(204)
- Error Response Codes: Unauthorized(401), NotFound(404)

#### Listing Delete Requests

`GET /api/v1/admin/tsdb/delete_series` - List all the delete requests.

- Normal Response Codes OK(200)
- Error Response Codes: Unauthorized(401), NotFound(404)