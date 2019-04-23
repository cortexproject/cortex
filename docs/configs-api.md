# Configs API

The configs service provides an API-driven multi-tenant approach to handling various configuration files for prometheus. The service hosts an API where users can read and write Prometheus rule files, Alertmanager configuration files, and Alertmanager templates to a database.

Each tenant will have it's own set of rule files, Alertmanager config, and templates. A POST operation will effectively replace the existing copy with the configs provided in the request body.

### Manage Alertmanager

`GET /api/prom/configs/alertmanager` - Get current Alertmanager config

`POST /api/prom/configs/alertmanager` - Replace current Alertmanager config

`POST /api/prom/configs/alertmanager/validate` - Validate Alertmanager config

Example Payload:

```json
{ 
    "alertmanager_config": "<standard alertmanager.yaml config>"
}
```

### Manage Rules

`GET /api/prom/configs/rules` - Get current rule file

`POST /api/prom/configs/rules` - Replace current rule file

Example:

```json
{
    "rules_files": {
        "rules.yaml": "<standard rules.yaml config>",
        "rules2.yaml": "<standard rules.yaml config>",
    },
    "rule_format_version": "2",
}
```

### Manage Templates

`GET /api/prom/configs/templates` - Get current templates

`POST /api/prom/configs/templates` - Replace current templates

```json
{
    "template_files": {
        "templates.tmpl": "<standard template file>",
        "templates2.tmpl": "<standard template file>"
    }
}
```

### Update All Files

One can update all types of files together by combining the POST body. Any of the `POST /api/prom/configs/*` endpoints will update any files provided.

The following content would update both the Alertmanager configuration and the Alertmanager templates:

```json
{
    "alertmanager_config": "<new alertmanager config>",
    "template_files": {
        "templates.tmpl": "<new template file>",
        "templates2.tmpl": "<standard template file>"
    }
}
```

### Deactivate/Restore Configs

`DELETE /api/prom/configs/deactivate` - Disable configs for a tenant

`POST /api/prom/configs/restore` - Re-enable configs for a tenant

These API endpoints will disable/enable the current Rule and Alertmanager configuration for a tenant.

Note that setting a new config will effectively "re-enable" the Rules and Alertmanager configuration for a tenant.

### Internal APIs

`GET /private/api/prom/configs/rules` - Get current rule file

`GET /private/api/prom/configs/alertmanager` - Get current Alertmanager config