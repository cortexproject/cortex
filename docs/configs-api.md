# Configs API

The configs api provides API-driven multi-tenant approach to prometheus' Rule files and Alertmanager configs.

Each tenant has a single Rule file and Alertmanager Configuration. A POST operation will effectively replace the existing copy with the configs provided in the request body.

Although Prometheus works with YAML files by default, these HTTP API's only accept JSON. You may convert your YAML to JSON using a tool such as [yaml2json](https://github.com/bronze1man/yaml2json).

### Manage Rules

`GET /api/prom/configs/rules` - Get current rule file

`POST /api/prom/configs/rules` - Replace current rule file

Returns and Accepts a JSON object representing a [Prometheus rule file](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#recording-rules). 

Example:

```json
{
    "groups": [
        {
            "name": "example",
            "rules": [
                {
                    "record": "job:http_inprogress_requests:sum",
                    "expr": "sum(http_inprogress_requests) by (job)"
                }
            ]
        }
    ]
}
```

### Manage Alertmanager

`GET /api/prom/configs/alertmanager` - Get current Alertmanager config

`POST /api/prom/configs/alertmanager` - Replace current Alertmanager config

`POST /api/prom/configs/alertmanager/validate` - Validate Alertmanager config

Returns and Accepts a JSON object representing an [Alertmanager configuration file](https://prometheus.io/docs/alerting/configuration/#configuration-file). 

Example:

```json
{
    "global": {
        "slack_api_url": "http://my.slack.com/webhook/url"
    },
    "route": {
        "receiver": "default-receiver",
        "group_wait": "30s",
        "group_interval": "5m",
        "repeat_interval": "4h",
        "group_by": [ "cluster", "alertname" ],
        "routes": [
            {
                "receiver": "database-pager",
                "group_wait": "10s",
                "match_re": {
                    "service": "mysql|cassandra"
                }
            },
            {
                "receiver": "frontend-pager",
                "group_by": [ "product", "environment" ],
                "match": {
                    "team": "frontend"
                }
            }
        ]
    }
}
```

**Note:** Template files and Email Notifications are not supported in Cortex yet.


### Deactivate/Restore Configs

`DELETE /api/prom/configs/deactivate` - Disable Alertmanager config & rule file

`POST /api/prom/configs/restore` - Re-enable Alertmanager config & rule file

These API endpoints will disable/enable the current Rule and Alertmanager configuration for a tenant.

Note that setting a new config will effectively "re-enable" the Rules and Alertmanager configuration for a tenant.

### Internal APIs

`GET /private/api/prom/configs/rules` - Get current rule file

`GET /private/api/prom/configs/alertmanager` - Get current Alertmanager config