---
title: "Config for sending HA Pairs data to Cortex"
linkTitle: "Config for sending HA Pairs data to Cortex"
weight: 4
slug: ha-pair-handling
---

## Context

You can have more than a single Prometheus monitoring and ingesting the same metrics for redundancy. Cortex already does replication for redundancy and it doesn't make sense to ingest the same data twice. So in Cortex, we made sure we can dedupe the data we receive from HA Pairs of Prometheus. We do this via the following:

Assume that there are two teams, each running their own Prometheus, monitoring different services. Let's call the Prometheis T1 and T2. Now, if the teams are running HA pairs, let's call the individual Prometheis, T1.a, T1.b and T2.a and T2.b.

In Cortex we make sure we only ingest from one of T1.a and T1.b, and only from one of T2.a and T2.b. We do this by electing a leader replica for each cluster of Prometheus. For example, in the case of T1, let it be T1.a. As long as T1.a is the leader, we drop the samples sent by T1.b. And if Cortex sees no new samples from T1.a for a short period (30s by default), it'll switch the leader to be T1.b.

This means if T1.a goes down for a few minutes Cortex's HA sample handling will have switched and elected T1.b as the leader. This failover timeout is what enables us to only accept samples from a single replica at a time, but ensure we don't drop too much data in case of issues. Note that with the default scrape period of 15s, and the default timeouts in Cortex, in most cases you'll only lose a single scrape of data in the case of a leader election failover. For any rate queries the rate window should be at least 4x the scrape period to account for any of these failover scenarios, for example with the default scrape period of 15s then you should calculate rates over at least 1m periods.

Now we do the same leader election process T2.

## Config

### Client Side

So for Cortex to achieve this, we need 2 identifiers for each process, one identifier for the cluster (T1 or T2, etc) and one identifier to identify the replica in the cluster (a or b). The easiest way to do with is by setting external labels, ideally `cluster` and `replica` (note the default is `__replica__`). For example:

```
cluster: prom-team1
replica: replica1 (or pod-name)
```

and

```
cluster: prom-team1
replica: replica2
```

Note: These are external labels and have nothing to do with remote_write config.

These two label names are configurable per-tenant within Cortex, and should be set to something sensible. For example, cluster label is already used by some workloads, and you should set the label to be something else but uniquely identifies the cluster. Good examples for this label-name would be `team`, `cluster`, `prometheus`, etc.

The replica label should be set so that the value for each prometheus is unique in that cluster. Note: Cortex drops this label when ingesting data, but preserves the cluster label. This way, your timeseries won't change when replicas change.

### Server Side

To enable handling of samples, see the [distributor flags]({{< relref "../configuration/arguments.md#ha-tracker" >}}) having `ha-tracker` in them.
