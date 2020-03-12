---
title: "Schema Configuration"
linkTitle: "Schema Configuration"
weight: 1
slug: schema-configuration
---

Cortex uses a NoSQL Store to store its index and optionally an Object store to store its chunks. Cortex has overtime evolved its schema to be more optimal and better fit the use cases and query patterns that arose. 

Currently there are 9 schemas that are used in production but we recommend running with `v9` schema when possible. You can move from one schema to another if a new schema fits your purpose better, but you still need to configure Cortex to make sure it can read the old data in the old schemas.

You can configure the schemas using a YAML config file, that you can point to using the `-schema-config-file` flag. It has the following YAML spec:

```yaml
configs: []<period_config>
```

Where `period_config` is
```
# In YYYY-MM-DD format, for example: 2020-03-01.
from: <string>
# The index client to use, valid options: aws-dynamo, bigtable, bigtable-hashed, cassandra, boltdb.
store: <string>
# The object client to use. If none is specified, `store` is used for storing chunks as well. Valid options: s3, aws-dynamo, bigtable, bigtable-hashed, gcs, cassandra, filesystem.
object_store: <string>
# The schema version to use. Valid ones are v1, v2, v3,... v6, v9, v10, v11. Recommended for production: v9.
schema: <string>
index: <periodic_table_config>
chunks: <periodic_table_config>
```

Where `periodic_table_config` is
```
# The prefix to use for the tables.
prefix: <string>
# We typically run Cortex with new tables every week to keep the index size low and to make retention easier. This sets the period at which new tables are created and used. Typically 1w (1week).
period: <duration>
# The tags that can be set on the dynamo table.
tags: <map[string]string>
```

Now an example of this file (also something recommended when starting out) is:
```
configs:
  - from: "2020-03-01" # Or typically a week before the Cortex cluster was created.
    schema: v9
    index:
      period: 1w 
      prefix: cortex_index_
    # Chunks section is optional and required only if you're not using a
    # separate object store.
    chunks:
      period: 1w 
      prefix: cortex_chunks
    store: aws-dynamo/bigtable-hashed/cassandra/boltdb
    object_store: <above options>/s3/gcs/azure/filesystem
```

An example of an advanced schema file with a lot of changes:
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

  # Starting 2018-02-13 we moved from BigTable to GCS for storing the chunks.
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

Note how we started out with v9 and just Bigtable, but later migrated to GCS as the object store, finally moving to v10. This is a complex schema file showing several changes changes over the time, while a typical schema config file usually has just one or two schema versions.

### Migrating from flags to schema file

Legacy versions of Cortex did support the ability to configure schema via flags. If you are still using flags, you need to migrate your configuration from flags to the config file.

If you're using:

* `chunk.storage-client`: then set the corresponding `object_store` field correctly in the schema file.
* `dynamodb.daily-buckets-from`: then set the corresponding `from` date with `v2` schema.
* `dynamodb.base64-buckets-from`: then set the corresponding `from` date with `v3` schema.
* `dynamodb.v{4,5,6,9}-schema-from`: then set the corresponding `from` date with schema `v{4,5,6,9}`
* `bigtable.column-key-from`: then set the corresponding `from` date and use the `store` as `bigtable-columnkey`.
* `dynamodb.use-periodic-tables`: then set the right `index` and `chunk` fields with corresponding values from `dynamodb.periodic-table.{prefix, period, tag}` and `dynamodb.chunk-table.{prefix, period, tag}` flags. Note that the default period is 7 days, so please set the `period` as `168h` in the config file if none is set in the flags.
