---
title: "Config for sending HA Pairs data to Cortex"
linkTitle: "Config for sending HA Pairs data to Cortex"
weight: 10
slug: ha-pair-handling
---

## Context

You can have more than a single Prometheus instance monitoring and ingesting the same metrics for redundancy. Cortex already does replication for redundancy, and it doesn't make sense to ingest the same data twice. So in Cortex, we made sure we can dedupe the data we receive from HA Pairs of Prometheus. We do this via the following:

Assume that there are two teams, each running their own Prometheus, monitoring different services. Let's call the Prometheus instances T1 and T2. Now, if the teams are running HA pairs, let's call the individual Prometheus instances T1.a, T1.b, and T2.a and T2.b.

In Cortex, we make sure we only ingest from one of T1.a and T1.b, and only from one of T2.a and T2.b. We do this by electing a leader replica for each cluster of Prometheus. For example, in the case of T1, let it be T1.a. As long as T1.a is the leader, we drop the samples sent by T1.b. And if Cortex sees no new samples from T1.a for a short period (30s by default), it'll switch the leader to be T1.b.

This means if T1.a goes down for a few minutes, Cortex's HA sample handling will have switched and elected T1.b as the leader. This failover timeout is what enables us to only accept samples from a single replica at a time, but ensure we don't drop too much data in case of issues. Note that with the default scrape period of 15s, and the default timeouts in Cortex, in most cases, you'll only lose a single scrape of data in the case of a leader election failover. For any rate queries, the rate window should be at least 4x the scrape period to account for any of these failover scenarios. For example, with the default scrape period of 15s, then you should calculate rates over at least 1m periods.

Now we do the same leader election process for T2.

## Config

### Client Side

So for Cortex to achieve this, we need 2 identifiers for each process, one identifier for the cluster (T1 or T2, etc.) and one identifier to identify the replica in the cluster (a or b). The easiest way to do this is by setting external labels; the default labels are `cluster` and `__replica__`.

Deploy **two Prometheus servers** (or Prometheus pods) that scrape the same targets. Give them the **same** `cluster` value and a **different** `__replica__` value:

Prometheus / replica A (`prometheus-a`):

```yaml
global:
  external_labels:
    cluster: prom-team1
    __replica__: replica-a
```

Prometheus / replica B (`prometheus-b`):

```yaml
global:
  external_labels:
    cluster: prom-team1
    __replica__: replica-b
```

Both should `remote_write` to the same Cortex distributor endpoint.

**Important:** Prometheus does **not** expand environment variables inside `external_labels`. A value like `$POD_NAME` or `${POD_NAME}` is taken literally unless your deployment injects the concrete value when rendering the config (for example via a Kubernetes Downward API + config template, or a config reloader). Setting `__replica__: $POD_NAME` in a static file will **not** give each pod a unique replica id.

Practical ways to set a unique replica label:

* Hard-code a distinct value per Prometheus instance (`replica-a` / `replica-b`).
* Template the config so the pod name or StatefulSet ordinal is written into `external_labels`.
* Prefer attaching `__replica__` only on the remote_write path with `write_relabel_configs` (see [Remote Write replica label](#remote-write-replica-label) below). That avoids putting the replica label on local alerts and on `remote_read` queries.

Note: These HA labels are Prometheus external labels (or write-relabel labels). They are separate from other `remote_write` settings such as URL, auth, and queue config.

These two label names are configurable per-tenant within Cortex (`-distributor.ha-tracker.cluster` and `-distributor.ha-tracker.replica`, see [HA Tracker flags](../configuration/arguments.md#ha-tracker)) and should be set to something sensible. For example, the `cluster` label is already used by some workloads, and you should set the label name to something else that uniquely identifies the Prometheus HA pair. Good examples for this label name would be `team`, `cluster`, `prometheus`, etc.

The replica label value must be unique among Prometheus servers in that HA pair. Cortex **drops** the replica label when ingesting samples but **keeps** the cluster label. This way, your time series identity does not change when the elected replica fails over.

### Server Side

The minimal configuration requires:

* Enabling the HA tracker via `-distributor.ha-tracker.enable=true` CLI flag (or its YAML config option)
* Configuring the KV store for the ring (See: [Ring/HA Tracker Store](../configuration/arguments.md#ringha-tracker-store)). Only Consul and etcd are currently supported. Multi should be used for migration purposes only.
* Setting the limits configuration to accept samples via `-distributor.ha-tracker.enable-for-all-users` (or its YAML config option).


The following configuration snippet shows an example of the HA tracker config via YAML config file:

```yaml
limits:
  ...
  accept_ha_samples: true
  ...
distributor:
  ...
  ha_tracker:
    enable_ha_tracker: true
    ...
    kvstore:
      [store: <string> | default = "consul"]
      [consul | etcd: <config>]
      ...
  ...
```

For further configuration file documentation, see the [distributor section](../configuration/config-file-reference.md#distributor_config) and [Ring/HA Tracker Store](../configuration/arguments.md#ringha-tracker-store).

For flag configuration, see the [distributor flags](../configuration/arguments.md#ha-tracker) having `ha-tracker` in them.

## Remote Write replica label

If you plan to use `remote_read`, or you want HA Prometheus pairs **without** duplicating Alertmanager notifications, do **not** put the `__replica__` label in `global.external_labels`. Add it only on the `remote_write` path via `write_relabel_configs`:

```yaml
global:
  external_labels:
    cluster: prom-team1
remote_write:
  - url: https://cortex/api/v1/push
    write_relabel_configs:
      - target_label: __replica__
        replacement: replica-a
```

and on the second Prometheus:

```yaml
global:
  external_labels:
    cluster: prom-team1
remote_write:
  - url: https://cortex/api/v1/push
    write_relabel_configs:
      - target_label: __replica__
        replacement: replica-b
```

When Prometheus runs `remote_read` queries, it attaches `global.external_labels` to the selectors. If `__replica__` is a global external label, the query includes that label, and Cortex will not return the deduplicated series (the replica label was dropped at ingest). Therefore `__replica__` should only be added for remote write.

## Avoiding duplicate Alertmanager notifications

Cortex HA deduplication applies to **samples ingested via remote_write**. It does **not** stop each Prometheus replica from evaluating rules and sending its own alerts.

If both replicas send alerts to the same Alertmanager and their `external_labels` differ (for example different `__replica__` values in `global.external_labels`), Alertmanager treats them as distinct label sets and you get **duplicate notifications**.

Mitigations:

1. **Recommended:** keep a stable `cluster` (and any other shared labels) in `global.external_labels`, and attach `__replica__` only with `remote_write.write_relabel_configs` as shown above. Both replicas then send alerts with the same label set, so Alertmanager can deduplicate them.
2. Send alerts from only one replica (or from the Cortex ruler) instead of from every Prometheus HA member.
3. If replica labels must remain on alerts, configure Alertmanager inhibition / grouping so parallel notifications are suppressed — this is harder to get right than (1).

## Verifying the HA tracker

After enabling the tracker on distributors:

1. Open the distributor HA status page: [`GET /distributor/ha_tracker`](../api/_index.md#ha-tracker-status) (also served at `/ha-tracker`). You should see one elected replica per user/cluster pair.
2. Scrape distributor metrics (names may be prefixed depending on your registry configuration, commonly with `cortex_`):
   * `ha_tracker_elected_replica_changes_total` — increases when leadership fails over.
   * `ha_tracker_elected_replica_timestamp_seconds` — last update time for the elected replica.
   * `ha_tracker_user_replica_group_count` — number of HA clusters tracked per tenant.
   * `ha_tracker_kv_store_cas_total` — KV compare-and-swap traffic for elections.
3. From a test query in Grafana/Cortex, confirm series do **not** include the replica label and that values are not roughly 2× what a single Prometheus would produce.

If every sample is rejected or nothing is elected, check that both HA labels are present on written samples, the KV backend is shared by all distributors, and `accept_ha_samples` / `enable_ha_tracker` are enabled (see [Server Side](#server-side)).

## Accept multiple HA pairs in single request
Let's assume there are two teams (T1 and T2), and each team operates two Prometheus for the HA (T1.a, T1.b for T1 and
T2.a, T2.b for T2).
They want to operate another Prometheus, receiving whole Prometheus requests and sending write request to the
Distributor.

The write request flow is as follows: T1.a, T1.b, T2.a, T2.b -> Prometheus -> Distributor which means the Distributor's
incoming write request contains time series of T1.a, T1.b, T2.a, and T2.b.
In other words, there are two HA pairs in a single write request, and the expected push result is to accept each
Prometheus leader replicas (example: T1.a, T2.b for each team).

## Config
### Client side
The client setting is the same as a single HA pair.
For example:

For T1.a
```
cluster: prom-team1
__replica__: replica1 (or pod-name)
```

For T1.b

```
cluster: prom-team1
__replica__: replica2 (or pod-name)
```

For T2.a

```
cluster: prom-team2
__replica__: replica1 (or pod-name)
```

For T2.b

```
cluster: prom-team2
__replica__: replica2 (or pod-name)
```

### Server side

One additional setting is needed to accept multiple HA pairs; it is enabled via
`--experimental.distributor.ha-tracker.mixed-ha-samples=true` (or its YAML config option).

The following configuration snippet shows an example of accepting multiple HA pairs config via the YAML config file:

```yaml
limits:
  ...
  accept_ha_samples: true
  accept_mixed_ha_samples: true
  ...
distributor:
  ...
  ha_tracker:
    enable_ha_tracker: true
    ...
    kvstore:
      [ store: <string> | default = "consul" ]
        [ consul | etcd: <config> ]
        ...
  ...
```

For further configuration file documentation, see
the [limits section](../configuration/config-file-reference.md#limits_config).
