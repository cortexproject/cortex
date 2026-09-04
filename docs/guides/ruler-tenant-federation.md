---
title: "Ruler tenant federation"
linkTitle: "Ruler tenant federation"
weight: 10
slug: ruler-tenant-federation
---

This guide explains how to configure the Ruler to evaluate federated rule groups, which query data from several tenants while the resulting series and alerts belong to a single tenant. The feature is experimental and implements the [federated ruler proposal](../proposals/federated-ruler.md).

## How it works

A federated rule group is a regular rule group with an additional `src_tenants` field listing the tenants to query:

```yaml
name: cortex-admin
interval: 1m
src_tenants: [team-a, team-b, team-c]
rules:
  - record: tenant:prometheus_rule_evaluation_failures:rate5m
    expr: sum by (__tenant_id__) (rate(prometheus_rule_evaluation_failures_total[5m]))
  - alert: TenantRuleEvaluationFailures
    expr: sum by (__tenant_id__) (rate(prometheus_rule_evaluation_failures_total[5m])) > 0
    for: 10m
    labels:
      severity: warning
    annotations:
      summary: "Rule evaluations are failing in tenant {{ $labels.__tenant_id__ }}"
```

- The rule group is owned by the tenant that created it through the Ruler API, `infra` for example.
- Every rule in the group is evaluated with the `X-Scope-OrgID` set to `team-a|team-b|team-c`, so the query runs across the source tenants exactly like a federated query, and each series carries the `__tenant_id__` label. As for any federated query, the label is not added when `src_tenants` lists a single tenant.
- The resulting series, the `ALERTS` and `ALERTS_FOR_STATE` series and the notifications are written to `infra` only. The source tenants are never modified and cannot see the rule group.
- Rule groups without `src_tenants` are not affected.

Alerting rules work the same way: the `TenantRuleEvaluationFailures` alert above fires once per source tenant with failing rule evaluations, and every alert is sent to the Alertmanager configuration of `infra`. The `__tenant_id__` label is kept on the alert as long as the expression does not aggregate it away, so the Alertmanager configuration of the owning tenant can route the alerts per source tenant:

```yaml
route:
  receiver: infra-default
  routes:
    - matchers: ['__tenant_id__="team-a"']
      receiver: team-a-slack
    - matchers: ['__tenant_id__="team-b"']
      receiver: team-b-slack
```

### Chaining rules within a federated rule group

In Prometheus, the rules of a group are evaluated in order, so a rule can use the series recorded by a previous rule of the same group. In a federated rule group this does not work out of the box, because the results of a recording rule are stored in the owning tenant while the following rules still query the source tenants. In the following group owned by `infra`, the alert never fires: `job:requests:rate5m` is written to `infra`, but the alert looks for it in `team-a` and `team-b`.

```yaml
name: traffic
src_tenants: [team-a, team-b]
rules:
  - record: job:requests:rate5m
    expr: sum by (job) (rate(http_requests_total[5m]))
  - alert: HighTraffic
    expr: job:requests:rate5m > 1000
```

To reuse the output of a previous rule, add the owning tenant to `src_tenants`. The recorded series is then found in `infra`, carrying the `__tenant_id__="infra"` label:

```yaml
name: traffic
src_tenants: [infra, team-a, team-b]
rules:
  - record: job:requests:rate5m
    expr: sum by (job) (rate(http_requests_total[5m]))
  - alert: HighTraffic
    expr: job:requests:rate5m > 1000
```

## Configuration

Federated rule groups require multi-tenant query federation and the ruler flag:

```
-tenant-federation.enabled=true
-ruler.enable-federated-rules=true
```

`-tenant-federation.enabled` must be set on all Cortex services. When the ruler evaluates rules through the query frontend (`-ruler.frontend-address`), the query frontend and the queriers perform the federated query; otherwise the ruler merges the results of the source tenants itself.

When the feature is disabled, the Ruler API rejects rule groups with `src_tenants` and any stored federated rule group is skipped with a warning log.

### Restricting the tenants allowed to create federated rule groups

By default, every tenant can create federated rule groups querying any tenant. The following flags restrict which tenants may own federated rule groups:

```
-ruler.allowed-federated-tenants=infra,platform
-ruler.disallowed-federated-tenants=untrusted
```

- If `-ruler.allowed-federated-tenants` is set, only the listed tenants can create federated rule groups.
- If `-ruler.disallowed-federated-tenants` is set, the listed tenants cannot create federated rule groups even if they are allowed otherwise.

The checks apply when a rule group is created and again when the ruler loads the rule groups, so changing the flags (and restarting the ruler) also disables the federated rule groups already stored for a tenant. Note that these flags do not restrict which tenants can be listed in `src_tenants`.

### Limits

- `-tenant-federation.max-tenant` also limits the number of tenants listed in `src_tenants`.
- When `-tenant-federation.regex-matcher-enabled` is set, the joined tenant IDs are resolved as a regular expression against the tenants discovered in the blocks storage. Tenant IDs containing regex metacharacters (`.`, `*`, `(`, `)`) are therefore rejected in `src_tenants`, and a source tenant that has not uploaded any block yet is silently ignored.

## Deployment notes

- Rulers running a version without this feature ignore the `src_tenants` field and evaluate such rule groups against the owning tenant only. Enable the feature and create federated rule groups only once every ruler has been upgraded, and delete them before downgrading.
- The `local` and `configdb` rule stores load Prometheus rule files, which cannot contain `src_tenants`. Federated rule groups require a rule store backed by an object store.
