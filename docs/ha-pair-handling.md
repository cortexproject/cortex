# Config for sending HA Pairs data to cortex

## Context

With Prometheus, you can have more than a single prometheus monitoring and ingesting the same metrics for redundancy. Cortex already does replication for redundancy and it doesn't make sense to ingest the same data twice in cortex. So in cortex, we made sure we can dedupe the data we receive from HA Pairs of Prometheus. We do this via the following:

Assume that there are two teams, each running their own prometheus, monitoring different services. Let's call the Prometheis T1 and T2. Now, if the teams are running HA pairs, let's call the individual Prometheis, T1.a, T1.b and T2.a and T2.b.

In cortex we make sure we only ingest from one of T1.a and T1.b, and only from one of T2.a and T2.b. We do this by electing a leader replica for each cluster of Prometheus. For example, in the case of T1, let it be T1.a. As long as T1.a is the leader, we drop the samples sent by T1.b. And if cortex sees no new samples from T1.a for a short period (30s by default), it'll switch the leader to be T1.b.

This means if T1.a goes down for 10 mins and comes back, we will no longer be accepting samples from T1.a, we will be accepting from T1.b and dropping the samples from T1.a. This way we can preserve the HA redundancy behaviour and make sure we're only accepting samples from a single replica and also we don't drop too much data in case of issues. Please note that with the default scrape period of 15s, you'd ideally be losing the metrics from only a single scrape in case we need to switch leaders. Your rate windows should be atleast 4x the scrape period to make sure you can tolerate this potentially rare occurrence.

Now we do the same leader election process T2.

## Config

### Client Side

So for cortex to achieve this, we need 2 identifiers for each process, one identifier for the cluster (T1 or T2, etc) and one identifier to identify the replica in the cluster (a or b). We do this by setting the external labels, ideally `cluster` and `replica`. For example:

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

Now these two label-names are totally configurable on Cortex's end, and should be set to something sensible. For example, cluster label is already used by some workloads, and you should set the label to be something else but uniquely identifies the cluster. Good examples for this label-name would be `team`, `cluster`, `prometheus`, etc.

And coming to the replica label, the name is totally configurable again and should be set so that the value for each prometheus to be unique in that cluster. Note: Cortex drops this label when ingesting data, but preserves the cluster label. This way, your timeseries won't change when replicas change.

### Server Side

To enable handling of samples, see the [distibutor flags](./arguments.md#ha-tracker) having `ha-tracker` in them.
