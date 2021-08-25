---
title: "Configuring Notification using Cortex Alertmanager"
linkTitle: "Alertmanager configuration"
weight: 10
slug: alertmanager-configuration
---

## Context

Cortex Alertmanager notification setup follow mostly the syntax of Prometheus Alertmanager since it is based on the same codebase.  The following is a description on how to load the configuration setup so that Alertmanager can use for notification when an alert event happened.

### Configuring the Cortex Alertmanager storage backend

With the introduction of Cortex 1.8 the storage backend config option shifted to the new pattern [#3888](https://github.com/cortexproject/cortex/pull/3888). You can find the new configuration [here](../configuration/config-file-reference.md#alertmanager_storage_config)

Note that when using `-alertmanager.sharding-enabled=true`, the following storage backends are not supported: `local`, `configdb`.

When using the new configuration pattern it is important that any of the old configuration pattern flags are unset (`-alertmanager.storage`), as well as `-<prefix>.configs.url`. This is because the old pattern still takes precedence over the new one. The old configuration pattern (`-alertmanager.storage`) is marked as deprecated and will be removed by Cortex version 1.11. However this change doesn't apply to `-alertmanager.storage.path` and `-alertmanager.storage.retention`.

### Cortex Alertmanager configuration

Cortex Alertmanager can be uploaded via Cortex [Set Alertmanager  configuration API](../api/_index.md#set-alertmanager-configuration) or using Grafana Labs [Cortex Tools](https://github.com/grafana/cortex-tools).

Follow the instruction at the `cortextool` link above to download or update to the latest version of the tool.

To obtain the full help of how to use `cortextool` for all commands and flags, use
`cortextool --help-long`.

The following example shows the steps to upload the configuration to Cortex `Alertmanager` using `cortextool`.

#### 1. Create the Alertmanager configuration `yml` file.

The following is `amconfig.yml`, an example of a configuration for Cortex `Alertmanager` to send notification via email:

```
global:
  # The smarthost and SMTP sender used for mail notifications.
  smtp_smarthost: 'localhost:25'
  smtp_from: 'alertmanager@example.org'
  smtp_auth_username: 'alertmanager'
  smtp_auth_password: 'password'

route:
  # A default receiver.
  receiver: send-email

receivers:
  - name: send-email
    email_configs:
      - to: 'someone@localhost'
```

[Example on how to setup Slack](https://grafana.com/blog/2020/02/25/step-by-step-guide-to-setting-up-prometheus-alertmanager-with-slack-pagerduty-and-gmail/#:~:text=To%20set%20up%20alerting%20in,to%20receive%20notifications%20from%20Alertmanager.) to support receiving Alertmanager notification.

#### 2. Upload the Alertmanager configuration

In this example,  Cortex `Alertmanager` is set to be available via localhost on port 8095 with user/org = 100.

To upload the above configuration `.yml` file with `--key` to be your Basic Authentication or API key:

```
cortextool alertmanager load ./amconfig.yml \
--address=http://localhost:8095 \
--id=100 \
--key=<yourKey>
```
If there is no error reported, the upload is successful.

To upload the configuration for Cortex `Alertmanager` using Cortex API and curl - see Cortex [Set Alertmanager configuration API](https://cortexmetrics.io/docs/api/#set-alertmanager-configuration).

#### 3. Ensure the configuration has been uploaded successfully

```
cortextool alertmanager get \
--address=http://localhost:8095 \
--id=100 \
--key=<yourKey>
```
