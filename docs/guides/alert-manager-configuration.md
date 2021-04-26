---
title: "Configuring Notification using Cortex AlertManager"
linkTitle: "AlertManager configuration"
weight: 10
slug: AlertManager-configuration
---

### Context

Cortex AlertManager notification setup follow mostly the syntax of Prometheus AlertManager since it is based on the same codebase.  The following is a description on how to load the configuration setup so that AlertManager can use for notification when an alert event happened.

#### Cortex AlertManager configuration

Cortex AlertManager can be uploaded via [Cortex Set Alertmanager API](https://cortexmetrics.io/docs/api/#set-alertmanager-configuration) or using Grafana Labs [Cortex Tools](https://github.com/grafana/cortex-tools).

Follow the instruction at the `cortextool` link above to download or update to the latest version of the tool.

To obtain the full help of how to use `cortextool` for all commands and flags, use
`cortextool --help-long`.

The following example shows the steps to upload the configuration to Cortex `alertmanager` using `cortextool`. 

1. Create `.yaml` file to specify the configuration for AlertManager notification characteristic.  

The following is `amconfig.yml`, an example of a configuration for Cortex `alertmanager` to send notification to a Slack channel:

```
global:
  resolve_timeout: 10m
  slack_api_url: '<slack incoming web hook defined for your slack workspace>'
route:
  group_wait: 10s

# When the first notification was sent, wait 'group_interval' to send a batch
# of new alerts that started firing for that group
  group_interval: 1m 

# If an alert has successfully been sent, wait 'repeat_interval' to
# resend them.
  repeat_interval: 10m

  receiver: slack_receiver
# All the above attributes are inherited by all child routes and can
# overwritten on each.

  routes:
    - receiver: slack_receiver
      match_re:
        severity: critical|warning
      continue: true

receivers:
- name: slack_receiver
  slack_configs:
  - send_resolved: true
    channel: '#<slack channel where to send the alert>'

# The following just describe what to put in the alert
    title: |-
      [{{ .Status | toUpper }}{{ if eq .Status "firing" }}:{{ .Alerts.Firing | len }}{{ end }}] {{ if .CommonLabels.alertname }}{{ .CommonLabels.alertname }}{{ else }}Multiple Alerts Open{{ end }}{{ if .CommonLabels.node }} for {{ .CommonLabels.node }}{{ else if .CommonLabels.nodename }} for {{ .CommonLabels.nodename }}{{ end }}
            text: >-
              {{ range .Alerts -}}
              *Alert:* {{ .Annotations.title }}
              *Description:* {{ .Annotations.description }}
                 • *Environment:* {{ if .Labels.environment }}`{{ .Labels.environment }}`{{ else }}`N/A`{{ end }}
                 • *Hostname:* {{ if .Labels.node }}`{{ .Labels.node }}`{{ else if .Labels.nodename }}`{{ .Labels.nodename }}`{{ else }}`N/A`{{ end }}
                 • *Condition:* {{ if .Labels.condition }}`{{ .Labels.condition }}`{{ else }}`N/A`{{ end }}
                 • *Current value:* {{ if .Annotations.current_value }}`{{ .Annotations.current_value }}`{{ else }}`N/A`{{ end }}
                 • *Opened at:* {{ if .StartsAt }}`{{ .StartsAt }}`{{ else }}`N/A`{{ end }}
                 • *Closed at:* {{ if eq .Status "resolved" }}`{{ .EndsAt }}`{{ else }}`Unresolved, alert still active.`{{ end }}
              *Details:*
                {{ range .Labels.SortedPairs }} • *{{ .Name }}:* `{{ .Value }}`
                {{ end }}
              {{ end }}
```

[Example on how to setup Slack](https://grafana.com/blog/2020/02/25/step-by-step-guide-to-setting-up-prometheus-alertmanager-with-slack-pagerduty-and-gmail/#:~:text=To%20set%20up%20alerting%20in,to%20receive%20notifications%20from%20Alertmanager.) to support receiving AlertManager notification.

2. With your Cortex ID,  URL and API key,  you are ready to upload the configuration Alertmanager.

In this example,  Cortex `alertmanager` is set to be available via localhost on port 8095 with only one user/org = 0.

To upload the above configuration `.yml` file with `--key` to be your Basic Authentication or API key:

```
cortextool alertmanager load ./amconfig.yml \
--address=http://localhost:8095 \
--id=0
--key=<yourKey>
```
If there is no error reported,  the upload is successful.

3. To confirm that the configuration is uploaded correctly:

```
cortextool alertmanager get \
--adress=http://localhost:8095 \
--id=0
--key=<yourKey>
```

To upload the configuration for Cortex `alertmanager` using Cortex API and curl - see [Cortex Set Alertmanager API](https://cortexmetrics.io/docs/api/#set-alertmanager-configuration).
