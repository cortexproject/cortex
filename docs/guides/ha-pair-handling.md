---
title: "Config for sending HA Pairs data to Cortex"
linkTitle: "Config for sending HA Pairs data to Cortex"
weight: 10
slug: ha-pair-handling
---

## Context

You can have more than a single Prometheus monitoring and ingesting the same metrics for redundancy. Cortex already does replication for redundancy, and it doesn't make sense to ingest the same data twice. So in Cortex, we made sure we can dedupe the data we receive from HA Pairs of Prometheus. We do this via the following:

Assume that there are two teams, each running their own Prometheus, monitoring different services. Let's call the Prometheus T1 and T2. Now, if the teams are running HA pairs, let's call the individual Prometheus, T1.a, T1.b, and T2.a and T2.b.

In Cortex, we make sure we only ingest from one of T1.a and T1.b, and only from one of T2.a and T2.b. We do this by electing a leader replica for each cluster of Prometheus. For example, in the case of T1, let it be T1.a. As long as T1.a is the leader, we drop the samples sent by T1.b. And if Cortex sees no new samples from T1.a for a short period (30s by default), it'll switch the leader to be T1.b.

This means if T1.a goes down for a few minutes, Cortex's HA sample handling will have switched and elected T1.b as the leader. This failover timeout is what enables us to only accept samples from a single replica at a time, but ensure we don't drop too much data in case of issues. Note that with the default scrape period of 15s, and the default timeouts in Cortex, in most cases, you'll only lose a single scrape of data in the case of a leader election failover. For any rate queries, the rate window should be at least 4x the scrape period to account for any of these failover scenarios, for example, with the default scrape period of 15s, then you should calculate rates over at least 1m periods.

Now we do the same leader election process for T2.

## Config

### Client Side

So for Cortex to achieve this, we need 2 identifiers for each process, one identifier for the cluster (T1 or T2, etc.) and one identifier to identify the replica in the cluster (a or b). The easiest way to do this is by setting external labels; the default labels are `cluster` and `__replica__`. For example:

```
cluster: prom-team1
__replica__: replica1 (or pod-name)
```

and

```
cluster: prom-team1
__replica__: replica2
```

Note: These are external labels and have nothing to do with remote_write config.

These two label names are configurable per-tenant within Cortex and should be set to something sensible. For example, the cluster label is already used by some workloads, and you should set the label to be something else that uniquely identifies the cluster. Good examples for this label-name would be `team`, `cluster`, `prometheus`, etc.

The replica label should be set so that the value for each prometheus is unique in that cluster. Note: Cortex drops this label when ingesting data but preserves the cluster label. This way, your timeseries won't change when replicas change.

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

## Remote Read

If you plan to use remote_read, you can't have the `__replica__` label in the
external section. Instead, you will need to add it only on the remote_write
section of your prometheus.yml.

```
global:
  external_labels:
    cluster: prom-team1
remote_write:
- url: https://cortex/api/v1/push
  write_relabel_configs:
    - target_label: __replica__
      replacement: 1
```

and

```
global:
  external_labels:
    cluster: prom-team1
remote_write:
- url: https://cortex/api/v1/push
  write_relabel_configs:
    - target_label: __replica__
      replacement: replica2
```

When Prometheus is executing remote read queries, it will add the external
labels to the query. In this case, if it asks for the `__replica__` label,
Cortex will not return any data.

Therefore, the `__replica__` label should only be added for remote write.

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
