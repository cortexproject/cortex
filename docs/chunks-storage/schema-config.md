---
title: "Schema Configuration"
linkTitle: "Schema Configuration"
weight: 2
slug: schema-configuration
---

**Warning: the chunks storage is deprecated. You're encouraged to use the [blocks storage](../blocks-storage/_index.md).**

Cortex chunks storage stores indexes and chunks in table-based data storages. When such a storage type is used, multiple tables are created over the time: each table - also called periodic table - contains the data for a specific time range. The table-based storage layout is configured through a configuration file called **schema config**.

_The schema config is used only by the chunks storage, while it's **not** used by the [blocks storage](../blocks-storage/_index.md) engine._

## Design

The table based design brings two main benefits:

1. **Schema config changes**<br />
   Each table is bounded to a schema config and version, so that changes can be introduced over the time and multiple schema configs can coexist.
2. **Retention**<br />
   The retention is implemented deleting an entire table, which allows to have fast delete operations.

The [**table-manager**](./table-manager.md) is the Cortex service responsible for creating a periodic table before its time period begins, and deleting it once its data time range exceeds the retention period.

## Periodic tables

A periodic table stores the index or chunks relative to a specific period of time. The duration of the time range of the data stored in a single table and its storage type is configured in the `configs` block of the [schema config](#schema-config) file.

The `configs` block can contain multiple entries. Each config defines the storage used between the day set in `from` (in the format `yyyy-mm-dd`) and the next config, or "now" in the case of the last schema config entry.

This allows to have multiple non-overlapping schema configs over the time, in order to perform schema version upgrades or change storage settings (including changing the storage type).

![Schema config - periodic table](/images/chunks-storage/schema-config-periodic-tables.png)
<!-- Diagram source at https://docs.google.com/presentation/d/1bHp8_zcoWCYoNU2AhO2lSagQyuIrghkCncViSqn14cU/edit -->

The write path hits the table where the sample timestamp falls into (usually the last table, except short periods close to the end of a table and the beginning of the next one), while the read path hits the tables containing data for the query time range.

## Schema versioning

Cortex supports multiple schema version (currently there are 11) but we recommend running with the **v9 schema** for most use cases and **v10 schema** if you expect to have very high cardinality metrics. You can move from one schema to another if a new schema fits your purpose better, but you still need to configure Cortex to make sure it can read the old data in the old schemas.

## Schema config

The path to the schema config YAML file can be specified to Cortex via the CLI flag `-schema-config-file` and has the following structure.

```yaml
configs: []<period_config>
```

### `<period_config>`

The `period_config` configures a single period during which the storage is using a specific schema version and backend storage.

```yaml
# The starting date in YYYY-MM-DD format (eg. 2020-03-01).
from: <string>

# The key-value store to use for the index. Supported values are:
# aws-dynamo, bigtable, bigtable-hashed, cassandra, boltdb.
store: <string>

# The object store to use for the chunks. Supported values are:
# s3, aws-dynamo, bigtable, bigtable-hashed, gcs, cassandra, filesystem.
# If none is specified, "store" is used for storing chunks as well.
[object_store: <string>]

# The schema version to use. Supported versions are: v1, v2, v3, v4, v5,
# v6, v9, v10, v11. We recommended v9 for most use cases, alternatively
# v10 if you expect to have very high cardinality metrics.
schema: <string>

index: <periodic_table_config>
chunks: <periodic_table_config>
```

### `periodic_table_config`

The `periodic_table_config` configures the tables for a single period.

```yaml
# The prefix to use for the table names.
prefix: <string>

# The duration for each table. A new table is created every "period", which also
# represents the granularity with which retention is enforced. Typically this value
#is set to 1w (1 week). Must be a multiple of 24h.
period: <duration>

# The tags to be set on the created table.
tags: <map[string]string>
```

## Schema config example

The following example shows an advanced schema file covering different changes over the course of a long period. It starts with v9 and just Bigtable. Later it was migrated to GCS as the object store, and finally moved to v10.

_This is a complex schema file showing several changes changes over the time, while a typical schema config file usually has just one or two schema versions._

```
configs:
  # Starting from 2018-08-23 Cortex should store chunks and indexes
  # on Google BigTable using weekly periodic tables. The chunks table
  # names will be prefixed with "dev_chunks_", while index tables will be
  # prefixed with "dev_index_".
  - from: "2018-08-23"
    schema: v9
    chunks:
        period: 1w
        prefix: dev_chunks_
    index:
        period: 1w
        prefix: dev_index_
    store: gcp-columnkey

  # Starting 2019-02-13 we moved from BigTable to GCS for storing the chunks.
  - from: "2019-02-13"
    schema: v9
    chunks:
        period: 1w
        prefix: dev_chunks_
    index:
        period: 1w
        prefix: dev_index_
    object_store: gcs
    store: gcp-columnkey

  # Starting 2019-02-24 we moved our index from bigtable-columnkey to bigtable-hashed
  # which improves the distribution of keys.
  - from: "2019-02-24"
    schema: v9
    chunks:
        period: 1w
        prefix: dev_chunks_
    index:
        period: 1w
        prefix: dev_index_
    object_store: gcs
    store: bigtable-hashed

  # Starting 2019-03-05 we moved from v9 schema to v10 schema.
  - from: "2019-03-05"
    schema: v10
    chunks:
        period: 1w
        prefix: dev_chunks_
    index:
        period: 1w
        prefix: dev_index_
    object_store: gcs
    store: bigtable-hashed
```
