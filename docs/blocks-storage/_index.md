---
title: "Blocks Storage"
linkTitle: "Blocks Storage"
weight: 3
menu:
---

The blocks storage is a Cortex storage engine based on [Prometheus TSDB](https://prometheus.io/docs/prometheus/latest/storage/): it stores each tenant's time series into their own TSDB which write out their series to a on-disk block (defaults to 2h block range periods). Each block is composed by chunk files - containing the timestamp-value pairs for multiple series - and an index, which indexes metric names and labels to time series in the chunk files.

The supported backends for the blocks storage are:

* [Amazon S3](https://aws.amazon.com/s3)
* [Google Cloud Storage](https://cloud.google.com/storage/)
* [Microsoft Azure Storage](https://azure.microsoft.com/en-us/services/storage/)
* [OpenStack Swift](https://wiki.openstack.org/wiki/Swift) (experimental)
* [Local Filesystem](https://thanos.io/storage.md/#filesystem) (single node only)

_Internally, some components are based on [Thanos](https://thanos.io), but no Thanos knowledge is required in order to run it._

## Architecture

When running the Cortex blocks storage, the Cortex architecture doesn't significantly change and thus the [general architecture documentation](../architecture.md) applies to the blocks storage as well. However, there are two additional Cortex services when running the blocks storage:

- [Store-gateway](./store-gateway.md)
- [Compactor](./compactor.md)

![Architecture](/images/blocks-storage/architecture.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

The **[store-gateway](./store-gateway.md)** is responsible to query blocks and is used by the [querier](./querier.md) at query time. The store-gateway is required when running the blocks storage.

The **[compactor](./compactor.md)** is responsible to merge and deduplicate smaller blocks into larger ones, in order to reduce the number of blocks stored in the long-term storage for a given tenant and query them more efficiently. It also keeps the [bucket index](./bucket-index.md) updated and, for this reason, it's a required component.

The `alertmanager` and `ruler` components can also use object storage to store its configurations and rules uploaded by users.  In that case a separate bucket should be created to store alertmanager configurations and rules: using the same bucket between ruler/alertmanager and blocks will cause issue with the **[compactor](./compactor.md)**.

### The write path

**Ingesters** receive incoming samples from the distributors. Each push request belongs to a tenant, and the ingester appends the received samples to the specific per-tenant TSDB stored on the local disk. The received samples are both kept in-memory and written to a write-ahead log (WAL) and used to recover the in-memory series in case the ingester abruptly terminates. The per-tenant TSDB is lazily created in each ingester as soon as the first samples are received for that tenant.

The in-memory samples are periodically flushed to disk - and the WAL truncated - when a new TSDB block is created, which by default occurs every 2 hours. Each newly created block is then uploaded to the long-term storage and kept in the ingester until the configured `-blocks-storage.tsdb.retention-period` expires, in order to give [queriers](./querier.md) and [store-gateways](./store-gateway.md) enough time to discover the new block on the storage and download its index-header.

In order to effectively use the **WAL** and being able to recover the in-memory series upon ingester abruptly termination, the WAL needs to be stored to a persistent disk which can survive in the event of an ingester failure (ie. AWS EBS volume or GCP persistent disk when running in the cloud). For example, if you're running the Cortex cluster in Kubernetes, you may use a StatefulSet with a persistent volume claim for the ingesters. The location on the filesystem where the WAL is stored is the same where local TSDB blocks (compacted from head) are stored and cannot be decoupled.  See also the [timeline of block uploads](production-tips/#how-to-estimate--querierquery-store-after) and [disk space estimate](production-tips/#ingester-disk-space).

#### Distributor series sharding and replication

The series sharding and replication done by the distributor doesn't change based on the storage engine.

It's important to note that due to the replication factor N (typically 3), each time series is stored by N ingesters. Since each ingester writes its own block to the long-term storage, this leads a storage utilization N times more. [Compactor](./compactor.md) solves this problem by merging blocks from multiple ingesters into a single block, and removing duplicated samples. After blocks compaction, the storage utilization is significantly smaller.

For more information, please refer to the following dedicated sections:

- [Compactor](./compactor.md)
- [Production tips](./production-tips.md)

### The read path

[Queriers](./querier.md) and [store-gateways](./store-gateway.md) periodically iterate over the storage bucket to discover blocks recently uploaded by ingesters.

For each discovered block, queriers only download the block's `meta.json` file (containing some metadata including min and max timestamp of samples within the block), while store-gateways download the `meta.json` as well as the index-header, which is a small subset of the block's index used by the store-gateway to lookup series at query time.

Queriers use the blocks metadata to compute the list of blocks that need to be queried at query time and fetch matching series from the store-gateway instances holding the required blocks.

For more information, please refer to the following dedicated sections:

- [Querier](./querier.md)
- [Store-gateway](./store-gateway.md)
- [Production tips](./production-tips.md)

## Configuration

The general [configuration documentation](../configuration/config-file-reference.md) also applies to a Cortex cluster running the blocks storage. The blocks storage can be enabled switching the storage `engine` to `blocks`:

```yaml
storage:
  # The storage engine to use. Use "blocks" for the blocks storage.
  # CLI flag: -store.engine
  engine: blocks
```

## Known issues

GitHub issues tagged with the [`storage/blocks`](https://github.com/cortexproject/cortex/issues?q=is%3Aopen+is%3Aissue+label%3Astorage%2Fblocks) label are the best source of currently known issues affecting the blocks storage.
