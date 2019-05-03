# Configs API

The configs service provides an API-driven multi-tenant approach to handling various configuration files for prometheus. The service hosts an API where users can read and write Prometheus rule files, Alertmanager configuration files, and Alertmanager templates to a database.

Each tenant will have it's own set of rule files, Alertmanager config, and templates. A POST operation will effectively replace the existing copy with the configs provided in the request body.

At the current time of writing, the API is part-way through a migration from a single Configs service that handled all three sets of data to a split API. [Tracking issue](https://github.com/cortexproject/cortex/issues/619). So, unfortunately, all APIs take and return all sets of data.


### Manage Alertmanager

`GET /api/prom/configs/alertmanager` - Get current Alertmanager config

`POST /api/prom/configs/alertmanager` - Replace current Alertmanager config

`POST /api/prom/configs/alertmanager/validate` - Validate Alertmanager config

Example Payload:

```json
{
    "id": 99,
    "rule_format_version": "2",
    "config": {
        "rules_files": { <see below> }
        "alertmanager_config":"<standard alertmanager.yaml config>"
    }
}
```

The ID should be incremented every time you update the data; Cortex
will use the config with the highest number.

The contents of the config file should be as described [here](https://prometheus.io/docs/prometheus/latest/configuration/alerting_rules/), encoded as a single string to fit within the overall JSON payload.

### Manage Rules

`GET /api/prom/configs/rules` - Get current rule file

`POST /api/prom/configs/rules` - Replace current rule file

Example:

```json
{
    "id": 99,
    "config": {
        "alertmanager_config":"<see above>"
        "rules_files": {
            "rules.yaml": "<standard rules.yaml config>",
            "rules2.yaml": "<standard rules.yaml config>",
        },
    }
    "rule_format_version": "2",
}
```

The contents of the rules file should be as described [here](http://prometheus.io/docs/prometheus/latest/configuration/recording_rules/), encoded as a single string to fit within the overall JSON payload.

The ID should be incremented every time you update the rules; Cortex
will use the config with the highest number.

The `rule_format_version` allows compatibility for tenants with config
in Prometheus V1 format.  Pass "1" or "2" according to which
Prometheus version you want to match.

### Manage Templates

`GET /api/prom/configs/templates` - Get current templates

`POST /api/prom/configs/templates` - Replace current templates

```json
{
    "id": 99,
    "config": {
        "alertmanager_config":"<see above>"
        "rules_files": { <see above> }
        "template_files": {
            "templates.tmpl": "<standard template file>",
            "templates2.tmpl": "<standard template file>"
        }
    }
}
```

The contents of the template file should be as described [here](https://prometheus.io/docs/alerting/notification_examples/#defining-reusable-templates), encoded as a single string to fit within the overall JSON payload.

The ID should be incremented every time you update the data; Cortex
will use the config with the highest number.

### Deactivate/Restore Configs

`DELETE /api/prom/configs/deactivate` - Disable configs for a tenant

`POST /api/prom/configs/restore` - Re-enable configs for a tenant

These API endpoints will disable/enable the current Rule and Alertmanager configuration for a tenant.

Note that setting a new config will effectively "re-enable" the Rules and Alertmanager configuration for a tenant.

### Internal APIs

`GET /private/api/prom/configs/rules` - Get current rule file

`GET /private/api/prom/configs/alertmanager` - Get current Alertmanager config