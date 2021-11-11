---
title: "Glossary"
linkTitle: "Glossary"
weight: 999
slug: glossary
---

### Blocks storage

The blocks storage is a Cortex storage engine based on Prometheus TSDB, which only requires an object store (eg. AWS S3, Google GCS, ...) as backend storage.

For more information, please refer to the [Cortex blocks storage](../blocks-storage/_index.md) documentation.

### Chunks storage (deprecated)

The chunks storage is a Cortex storage engine which requires both an index store (eg. AWS DynamoDB, Google BigTable, Cassandra, ...) and an object store (eg. AWS S3, Google GCS, ...) as backend storage.

The chunks storage is deprecated. You're encouraged to use the [blocks storage](#blocks-storage) instead.

### Chunk

A chunk is an object containing compressed timestamp-value pairs.

When running Cortex with the chunks storage, a single chunk object contains timestamp-value pairs for a single series, while when running Cortex with the [blocks storage](#blocks-storage) a single chunk contains timestamp-value pairs for several series.

### Churn

Churn is the frequency at which series become idle.

A series become idle once it's not exported anymore by the monitored targets. Typically, series become idle when the monitored target itself disappear (eg. the process or node gets terminated).

### Flushing

Series flushing is the operation run by ingesters to offload time series from memory and store them in the long-term storage.

### HA Tracker

The HA Tracker is a feature of Cortex distributor which is used to deduplicate received series coming from two (or more) Prometheus servers configured in HA pairs.

For more information, please refer to the guide "[Config for sending HA Pairs data to Cortex](../guides/ha-pair-handling.md)".

### Hand-over

Series hand-over is an operation supported by ingesters to transfer their state, on shutdown, to a new ingester in the `JOINING` state. Hand-over is typically used during [ingesters rollouts](./ingesters-rolling-updates.md) and is only supported by the Cortex chunks storage.

For more information, please refer to the guide "[Ingesters rolling updates](./ingesters-rolling-updates.md)".

### Hash ring

The hash ring is a distributed data structure used by Cortex for sharding, replication and service discovery. The hash ring data structure gets shared across Cortex replicas via gossip or a key-value store.

For more information, please refer to the [Architecture](../architecture.md#the-hash-ring) documentation.

### Org

_See [Tenant](#tenant)._

### Ring

_See [Hash ring](#hash-ring)._

### Sample

A sample is a single timestamped value in a time series.

For example, given the series `node_cpu_seconds_total{instance="10.0.0.1",mode="system"}` its stream of values (samples) could be:

```
# Display format: <value> @<timestamp>
11775 @1603812134
11790 @1603812149
11805 @1603812164
11819 @1603812179
11834 @1603812194
```

### Schema config

The schema (or schema config) is a configuration file used by the Cortex chunks storage to configure the backend index and chunks store, and manage storage version upgrades. The schema config is **not** used by the Cortex [blocks storage](#blocks-storage).

For more information, please refer to the [Schema config reference](../chunks-storage/schema-config.md).

### Series

In the Prometheus ecosystem, a series (or time series) is a single stream of timestamped values belonging to the same metric, with the same set of label key-value pairs.

For example, given a single metric `node_cpu_seconds_total` you may have multiple series, each one uniquely identified by the combination of metric name and unique label key-value pairs:

```
node_cpu_seconds_total{instance="10.0.0.1",mode="system"}
node_cpu_seconds_total{instance="10.0.0.1",mode="user"}
node_cpu_seconds_total{instance="10.0.0.2",mode="system"}
node_cpu_seconds_total{instance="10.0.0.2",mode="user"}
```

### Tenant

A tenant (also called "user" or "org") is the owner of a set of series written to and queried from Cortex. Cortex multi-tenancy support allows you to isolate series belonging to different tenants. For example, if you have two tenants `team-A` and `team-B`, `team-A` series will be isolated from `team-B`, and each team will be able to query only their own series.

For more information, please refer to:

- [HTTP API authentication](../api/_index.md#authentication)
- [Tenant ID limitations](./limitations.md#tenant-id-naming)

### Time series

_See [Series](#series)._

### User

_See [Tenant](#tenant)._

### WAL

The Write-Ahead Log (WAL) is an append only log stored on disk used by ingesters to recover their in-memory state after the process gets restarted, either after a clear shutdown or an abruptly termination. Despite the implementation is different, the WAL is supported both by Cortex chunks and blocks storage engines.

For more information, please refer to:

- [Ingesters with WAL](../chunks-storage/ingesters-with-wal.md) when running **chunks storage**.
- [Ingesters with WAL](../blocks-storage/_index.md#the-write-path) when running **blocks storage**.
