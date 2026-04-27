---
title: "UTF-8 support"
linkTitle: "UTF-8 support"
weight: 70
slug: utf8-support
---

# UTF-8 support

## Why UTF-8 support?

There are a number of reasons why UTF-8 support in Cortex is useful:

1. With Prometheus version [3.0](https://prometheus.io/docs/guides/utf8/) UTF-8 support has been released for metric and label names. Thus, UTF-8 support improves the interoperability between Cortex and modern Prometheus ecosystems.
2. It makes Cortex more flexible for internationalized or externally sourced metadata which is useful when metrics or labels originate from systems, teams, or domains that do not fit neatly into ASCII-only naming.
3. It keeps behavior consistent across ingestion, rules and alerting.

## Before you enable UTF-8 support

Although UTF-8 support increases flexibility, the traditional naming style remains the safest choice for broad ecosystem compatibility, because downstream tools might assume legacy-compatible names. Definitely, check this before enabling UTF-8 support.

Before enabling UTF-8 support, review the systems that write metrics to Cortex, the rules evaluated by the Ruler, and the alerting configuration used by Alertmanager.

Enabling UTF-8 support changes validation behavior. Roll it out in a staging environment first and verify that ingestion, rule evaluation, and alert routing continue to work as expected.

## Configure UTF-8 support

Cortex supports configuring how metric names and label names are validated through the `-name-validation-scheme` flag or `name_validation_scheme` in the YAML configuration file.

Supported values are:

- `legacy` (default)
- `utf8`

The UTF-8 support has been released since Cortex [v1.20.0](https://github.com/cortexproject/cortex/releases/tag/v1.20.0).

This guide explains how to enable UTF-8 support and how the selected validation scheme affects the Cortex components.

To enable UTF-8 validation, set the validation scheme to `utf8`.

CLI:

```bash
-name-validation-scheme=utf8
```

YAML:

```yaml
name_validation_scheme: utf8
```

### Impact on Cortex components

### Distributor

The [Distributor](https://cortexmetrics.io/docs/architecture/#distributor) validates incoming metric names and label names during ingestion.

When `name_validation_scheme` is set to `legacy`, Cortex applies the legacy validation behavior. When it is set to `utf8`, the Distributor accepts names that are valid under the UTF-8 scheme.

In practice, this means that enabling UTF-8 support can change whether samples or series are accepted during ingestion by the Distributor. With legacy validation, samples containing invalid metric or label names are dropped during validation. With UTF-8 validation enabled, names that are valid under the UTF-8 scheme can pass validation. Writers that send UTF-8 metric or label names require the Distributor to run with `utf8` validation enabled.

## Recommended rollout strategy

1. Enable `utf8` in a staging environment first.
2. Test metric ingestion through the Distributor with representative UTF-8 metric names and label names.
3. If your environment also relies on UTF-8 names in rule evaluation or alerting configuration, test the affected Ruler or Alertmanager workflows before production rollout.
4. Roll out the selected validation scheme consistently wherever it is required.

## Compatibility notes

- Use `legacy` if you need to preserve the previous validation behavior.
- Use `utf8` if you want Cortex to accept UTF-8 metric names and label names.
- Plan migration carefully when different systems in your environment assume different naming rules.
- Refer to the [configuration reference](https://cortexmetrics.io/docs/configuration/configuration-file/#supported-contents-and-default-values-of-the-config-file) for the current flag and supported values.