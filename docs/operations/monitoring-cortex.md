---
title: "Monitoring Cortex"
linkTitle: "Monitoring Cortex"
weight: 1
slug: monitoring-cortex
---

This page describes the bundled assets Cortex ships for monitoring a production
deployment — Grafana dashboards, Prometheus alerting rules, and recording rules
— and how to install them. The assets live in the repository and are kept in
sync with the code; they are the same artifacts the Cortex maintainers use to
operate their own clusters.

## What ships with Cortex

| Asset | Source | Purpose |
|-------|--------|---------|
| Dashboards (JSON) | `docs/getting-started/dashboards/` | Drop-in Grafana dashboards covering every Cortex component |
| Alert rules | `docs/getting-started/alerts.yaml` | 50+ PrometheusRule alerts grouped by component |
| Recording rules | `docs/getting-started/cortex-jsonnet/cortex-mixin/recording_rules.libsonnet` | Pre-aggregated series used by the dashboards and alerts |
| Jsonnet mixin | `docs/getting-started/cortex-jsonnet/cortex-mixin/` | The source of truth — generates the JSON/YAML above |

## Dashboards

Each dashboard JSON in `docs/getting-started/dashboards/` is ready to import into
Grafana via **Dashboards → Import → Upload JSON file**.

| Dashboard | What to watch |
|-----------|---------------|
| `cortex-writes.json` | End-to-end write path: distributor QPS, ingestion rate, ingester push errors and latency, samples appended, WAL writes. The first dashboard to open during a write incident. |
| `cortex-reads.json` | End-to-end read path: query QPS at the frontend, scheduler queue length, querier execution latency, store-gateway and ingester sub-queries. |
| `cortex-queries.json` | Per-query breakdowns: chunks/series fetched, bytes processed, queries by tenant. Useful for hunting expensive queries. |
| `cortex-slow-queries.json` | The slowest queries in the last interval, including the PromQL and the tenant. Pair with the query-frontend logs. |
| `cortex-compactor.json` | Compactor run progress, blocks compacted vs. failed, sync errors. |
| `cortex-compactor-resources.json` | CPU, memory, disk, and goroutines for the compactor pods. |
| `cortex-object-store.json` | Object-store request rate, latency, and error rate broken down by operation (Get, Iter, Upload). |
| `cortex-rollout-progress.json` | Rolling-deployment progress for stateful sets (ingester, store-gateway, compactor). |
| `cortex-scaling.json` | Suggested replica counts derived from current load — pair with [Capacity Planning]({{< relref "../guides/capacity-planning.md" >}}). |
| `cortex-config.json` | The runtime configuration currently in effect, by tenant. |
| `alertmanager.json` | Alertmanager-specific: notification rate, replication, ring health. |
| `ruler.json` | Ruler-specific: evaluation rate, missed evaluations, push and query errors. |

Dashboards assume a Prometheus datasource named `Cortex`; either name your
datasource that way or edit the dashboard variables on import. Several
dashboards rely on the recording rules described below — install those first or
some panels will be empty.

## Alerts

The bundled alerts in `docs/getting-started/alerts.yaml` are grouped by concern:

| Group | Examples |
|-------|----------|
| `cortex_alerts` | `CortexIngesterUnhealthy`, `CortexRequestErrors`, `CortexRequestLatency`, `CortexQueriesIncorrect`, `CortexInconsistentRuntimeConfig`, `CortexKVStoreFailure`, `CortexMemoryMapAreasTooHigh` |
| `cortex_ingester_instance_alerts` | `CortexIngesterReachingSeriesLimit`, `CortexIngesterReachingTenantsLimit`, `CortexDistributorReachingInflightPushRequestLimit` |
| `cortex-rollout-alerts` | `CortexRolloutStuck` |
| `cortex-provisioning` | `CortexProvisioningTooManyActiveSeries`, `CortexProvisioningTooManyWrites`, `CortexAllocatingTooMuchMemory` |
| `ruler_alerts` | `CortexRulerTooManyFailedPushes`, `CortexRulerTooManyFailedQueries`, `CortexRulerMissedEvaluations`, `CortexRulerFailedRingCheck` |
| `gossip_alerts` | `CortexGossipMembersMismatch` |
| `etcd_alerts` | `EtcdAllocatingTooMuchMemory` |
| `alertmanager_alerts` | `CortexAlertmanagerSyncConfigsFailing`, `CortexAlertmanagerRingCheckFailing`, `CortexAlertmanagerPartialStateMergeFailing`, `CortexAlertmanagerReplicationFailing`, `CortexAlertmanagerPersistStateFailing`, `CortexAlertmanagerInitialSyncFailed` |
| `cortex_blocks_alerts` | `CortexIngesterHasNotShippedBlocks`, `CortexIngesterHasUnshippedBlocks`, `CortexIngesterTSDBHeadCompactionFailed`, `CortexIngesterTSDBWALCorrupted`, `CortexQuerierHasNotScanTheBucket`, `CortexQuerierHighRefetchRate`, `CortexStoreGatewayHasNotSyncTheBucket`, `CortexBucketIndexNotUpdated`, `CortexTenantHasPartialBlocks` |
| `cortex_compactor_alerts` | `CortexCompactorHasNotSuccessfullyCleanedUpBlocks`, `CortexCompactorHasNotSuccessfullyRunCompaction`, `CortexCompactorHasNotUploadedBlocks` |

For every alert, the file ships with `for`, `severity`, and a short summary in
annotations. Treat these as a starting point — tune the thresholds (and which
alerts page vs. ticket) to your SLOs.

### Installing the alerts

The alerts file is a standard Prometheus rule file. In Kubernetes with the
Prometheus Operator, wrap it in a `PrometheusRule` resource; an example lives in
`docs/getting-started/prometheusrule.yaml`. With a self-hosted Prometheus, add
the file to `rule_files:` in `prometheus.yml`.

If you also run a Cortex ruler, the same file can be loaded into Cortex itself
via `cortextool rules load` (see [Sharded Ruler]({{< relref
"../guides/sharded_ruler.md" >}})).

## Recording rules

The dashboards depend on a set of pre-aggregated metrics defined in
`docs/getting-started/cortex-jsonnet/cortex-mixin/recording_rules.libsonnet`.
These collapse per-instance counters into per-cluster/per-tenant rates so the
dashboards stay fast on large deployments. Install them the same way you
install the alerts — alongside, in the same Prometheus.

Skipping the recording rules will leave several dashboard panels blank or
extremely slow.

## The Jsonnet mixin

If you already manage Prometheus rules and dashboards via Jsonnet/Tanka, import
`docs/getting-started/cortex-jsonnet/cortex-mixin/` directly:

```jsonnet
local cortexMixin = import 'cortex-mixin/mixin.libsonnet';

{
  prometheusAlerts+:: cortexMixin.prometheusAlerts,
  prometheusRules+:: cortexMixin.prometheusRules,
  grafanaDashboards+:: cortexMixin.grafanaDashboards,
}
```

The mixin honours the standard [monitoring-mixin
contract](https://github.com/monitoring-mixins/docs), so it composes with mixins
for Kubernetes, etcd, Memcached, and the other dependencies a Cortex cluster
typically runs alongside.

The mixin's `_config` block exposes knobs for the datasource name, single-binary
vs. microservices mode, namespace/cluster labels, and per-component selectors.
See `cortex-mixin/config.libsonnet` for the full list.

## Tracing

Dashboards and alerts cover RED metrics — latency, traffic, errors. For
end-to-end request tracing, configure Cortex's OpenTelemetry/Jaeger exporter as
described in [Tracing]({{< relref "../guides/tracing.md" >}}). The
`cortex-slow-queries.json` dashboard surfaces a query ID that maps directly to a
trace when tracing is enabled, making it easy to pivot from "this query was
slow" to "here is where it spent its time."

## Related

- [Capacity Planning]({{< relref "../guides/capacity-planning.md" >}}) — sizing
  inputs to feed the scaling dashboard.
- [Tracing]({{< relref "../guides/tracing.md" >}}) — span exporter setup.
- [Query Auditor]({{< relref "./query-auditor.md" >}}) — detecting query
  correctness regressions.
- [Query Tee]({{< relref "./query-tee.md" >}}) — comparing two Cortex
  deployments side-by-side.
