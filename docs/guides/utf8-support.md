---
title: "UTF-8 support"
linkTitle: "UTF-8 support"
weight: 70
slug: utf8-support
---

# UTF-8 support

## Why enable UTF-8 support?

There are a number of reasons why UTF-8 support in Cortex is useful:

1. With Prometheus version [3.0](https://prometheus.io/docs/guides/utf8/) UTF-8 support has been released for metric and label names. Thus, UTF-8 support improves the interoperability between Cortex and modern Prometheus ecosystems.
2. It makes Cortex more flexible for internationalized or externally sourced metadata which is useful when metrics or labels originate from systems, teams, or domains that do not fit neatly into ASCII-only naming.
3. It keeps behavior consistent across ingestion, rules and alerting.

## Before you enable UTF-8 support

Although UTF-8 support increases flexibility, the traditional naming style remains the safest choice for broad ecosystem compatibility, because downstream tools might assume legacy-compatible names. Check the compatibility of these tools before enabling UTF-8 support.

Before enabling UTF-8 support, review the systems that write metrics to Cortex, the rules evaluated by the Ruler, and the alerting configuration used by Alertmanager.

Enabling UTF-8 support changes validation behavior. Roll it out in a staging environment first and verify that ingestion, rule evaluation, and alert routing continue to work as expected.

## Configure UTF-8 support

Cortex supports configuring how metric names and label names are validated through the `-name-validation-scheme` flag or `name_validation_scheme` in the YAML configuration file.

Supported values are:

- `legacy` (default)
- `utf8`

UTF-8 name validation was introduced in Cortex [v1.20.0](https://github.com/cortexproject/cortex/releases/tag/v1.20.0).

This guide explains how to enable UTF-8 support and how the selected validation scheme affects the Cortex components.

To enable UTF-8 validation, set the validation scheme to `utf8`.

CLI:

```text
-name-validation-scheme=utf8
```

YAML:

```yaml
name_validation_scheme: utf8
```

## Using UTF-8

After enabling UTF-8 name validation, UTF-8 metric names and label names can be used. The following examples illustrate how to work with those names.

### Querying

PromQL queries support UTF-8 metric names and label names. For example, a UTF-8 metric name can be queried with the following expression:

```promql
{"series.1", "test.utf8.metric"="😄"}
```

Note that the metric name `series.1` is not valid under the `legacy` validation scheme. It is accepted only when UTF-8 validation is enabled. This is useful when metric or label names contain characters, such as `.`, that are supported by UTF-8 validation but not by the legacy validation scheme.

### Alertmanager

Alertmanager configuration can use UTF-8 names in matchers, `group_by` fields, and silence matchers. For example, a route can group and match on UTF-8 label names:

```yaml
route:
  receiver: default
  group_by:
    - "group.test.🙂"
  routes:
    - receiver: utf8-receiver
      matchers:
        - '"🙂.utf8.label"="😄"'

receivers:
  - name: default
  - name: utf8-receiver
```

Silences can also match alerts using UTF-8 label names and values. For example, a silence can contain the following matcher:

```json
{
  "matchers": [
    {
      "name": "🙂.utf8.label",
      "value": "😄",
      "isRegex": false,
      "isEqual": true
    }
  ]
}
```

This is a partial silence object. Creating a silence through the API additionally requires `startsAt`, `endsAt`, `createdBy`, and `comment`.

Together, these capabilities support routing, grouping, and silencing alerts that contain UTF-8 label names and values.

### Ruler

Ruler rule definitions can use UTF-8 names in rule names, rule labels and rule group labels. For example:

```yaml
groups:
  - name: "test.utf8.group"
    labels:
      "test.utf8.group.label": "😄"
    rules:
      - record: "test.utf8.recording.rule"
        expr: up
        labels:
          "test.utf8.rule.label": "😄"
      - alert: "test.utf8.alert"
        expr: up == 0
        labels:
          "test.utf8.alert.label": "😄"
```

## Impact on Cortex components

### Distributor

The [Distributor](https://cortexmetrics.io/docs/architecture/#distributor) validates incoming metric names and label names during ingestion.

When `name_validation_scheme` is set to `legacy`, Cortex applies the legacy validation behavior. When it is set to `utf8`, the Distributor accepts names that are valid under the UTF-8 scheme.

In practice, this means that enabling UTF-8 support can change whether samples or series are accepted during ingestion by the Distributor. With legacy validation, samples containing invalid metric or label names are rejected during validation. With UTF-8 validation enabled, names that are valid under the UTF-8 scheme can pass validation. Writers that send UTF-8 metric or label names require the Distributor to run with `utf8` validation enabled.

### Ruler

The [Ruler](https://cortexmetrics.io/docs/architecture/#ruler) uses the selected scheme when validating rule definitions and external label names. With `utf8`, rule and external-label names that are valid under the UTF-8 scheme can be used.

### Alertmanager

The [Alertmanager](https://cortexmetrics.io/docs/architecture/#alertmanager) uses the selected scheme directly when choosing how to parse matchers. Cortex maps `legacy` to Alertmanager's `classic-mode` and `utf8` to `utf8-strict-mode` during Alertmanager initialization. This affects parsing of matchers in Alertmanager configurations and validation of label names in fields such as `group_by`.

In UTF-8 strict mode, there is **no fallback to classic matcher parsing**. Existing Alertmanager configurations should be checked for strict-mode compatibility before enabling `utf8`, even if they do not use non-legacy label names.

## Recommended rollout strategy

1. Inventory metric producers and consumers, Ruler rules, external labels, and Alertmanager configurations. Check whether any tools assume legacy metric and label names.
2. Verify that existing Alertmanager route matchers parse in UTF-8 strict mode. Update ambiguous or incompatible matchers before switching modes.
3. Enable `utf8` in a staging environment. Ingest and query representative metrics with non-legacy names, and exercise label name, label value, and series API endpoints.
4. Load representative recording and alerting rules, including group and rule labels, and verify evaluation and notifications.
5. Test Alertmanager configuration loading, grouping, routing, and silence creation. Roll out consistently across the Cortex components that need the selected mode.

## Compatibility notes

- Use `legacy` if you need to preserve the previous validation behavior.
- Use `utf8` if you want Cortex to accept UTF-8 metric names and label names.
- Plan migration carefully when different systems in your environment assume different naming rules.
- Refer to the [configuration reference](https://cortexmetrics.io/docs/configuration/configuration-file/#supported-contents-and-default-values-of-the-config-file) for the current flag and supported values.
