---
title: "Migration KV Store to Memberlist"
linkTitle: "Migration KV Store to Memberlist"
weight: 10
slug: migration-kv-store-to-memberlist
---

This guide explains how to perform a live migration of the Cortex Key-Value (KV) Store from an external backend (such as Consul or Etcd) to Memberlist.

## Context

When deployed as microservices, Cortex relies on a KV Store to discover other Cortex components and manage ring topologies.

To reduce external dependencies and simplify operations, you may want to migrate your existing KV store to Memberlist, a built-in gossip protocol implementation.

## Step-by-step Migration Process

We can leverage the [multi KV Store](https://cortexmetrics.io/docs/configuration/arguments/#multi-kv) feature to achieve a live migration.

### Set Memberlist as the Secondary KV Store

In this example, we will walk through migrating from Consul to Memberlist.

Using the Multi KV Store feature, you will designate your existing KV store (Consul) as the `primary` and Memberlist as the `secondary`. By setting `mirror_enabled: true`, Cortex will automatically replicate all data written to the primary store into the secondary store.

Update your configuration file and deploy the changes:

```yaml
ring:
  store: memberlist
memberlist:
  abort_if_join_fails: false
  bind_port: <gossip-ring-port>
  join_members:
    - gossip-ring.<namespace>.svc.cluster.local:<gossip-ring-port>
...
ingester:
  lifecycler:
    join_after: 60s
    heartbeat_period: 5s
    ring:
      kvstore:
        store: multi
        multi:
          mirror_enabled: true
          primary: consul
          secondary: memberlist
        consul:
          host: <consul-host>:<consul-port>
...
```
> **Why `join_after: 60s`?**
>
> The Memberlist gossip protocol requires a bit of time to propagate the state across the cluster. Setting a 60-second delay ensures that the ingester has enough time to fully sync the existing ring topology from other peers before actively joining and receiving traffic.
>
> **Note:** Make sure to apply this multi KV store configuration to all other components that interact with the ring (e.g. distributors, store-gateways), not just the ingesters.

Once deployed, Cortex will begin mirroring primary (Consul) data to Memberlist.

### Verify Data Mirroring

You can verify that data is safely replicating to the secondary store by monitoring the following metrics:

- `cortex_multikv_mirror_enabled`: Indicates whether data mirroring is currently active (a value of `1` means active).
- `cortex_multikv_mirror_writes_total`: The total number of successful write operations replicated to the secondary store.
- `cortex_multikv_mirror_write_errors_total`: The total number of failed write operations to the secondary store.

**Validation Check:**
1. Check that the `cortex_multikv_mirror_enabled` metric is exposing a value of `1`.
2. Monitor the write rate using a query like `rate(cortex_multikv_mirror_writes_total[5m])`. This rate should remain stable.
3. Ensure that `cortex_multikv_mirror_write_errors_total` is not increasing.

If these conditions are met, you can safely assume that the primary data is being successfully mirrored to Memberlist.

### Set Memberlist as the Sole KV Store

Once the data migration is verified and stable, you can finalize the migration by configuring Memberlist as the primary and only KV store.

```yaml
ingester:
  lifecycler:
    join_after: 60s
    heartbeat_period: 5s
    ring:
      kvstore:
        store: memberlist
```
> **Note:** Again, ensure this update is applied across all components.

After the updated configuration is fully deployed across your cluster and everything is running stably, you can remove your Consul cluster.