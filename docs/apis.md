# Cortex APIs

[this is a work in progress]

## Remote API

Cortex supports Prometheus'
[`remote_read`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_read)
and
[`remote_write`](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#remote_write)
APIs.  The encoding is Protobuf over http.

Read is on `/api/prom/read` and write is on `/api/prom/push`.


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
    "config": {
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
- Error Response Codes: Unauthorized(401), NotFound(404)
