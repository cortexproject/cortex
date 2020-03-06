---
title: "Schema Configuration"
linkTitle: "Schema Configuration"
weight: 1
slug: schema-configuration
---

Cortex uses a NoSQL Store to store its index and optionally an Object store to store its chunks. Cortex has overtime evolved its schema to be more optimal and better fit the use-cases that arised. Currently there are 10 schemas that are used in production. You can move from one schema to another if a new schema fits your purpose better, but you still need to configure Cortex to make sure it can read the old data in the old schemas.

You can configure the schemas using a YAML config file, that you can point to using the `-schema-config-file` flag. It has the following YAML spec:

```yaml
configs: []<period_config>
```

Where `period_config` is
```
from: <Date>
store: <string> // The index client to use.
object_store: <string> // The object client to use. If none is specified, `store` is used for storing chunks as well.
schema: <string> // The schema version to use. Valid ones are v1, v2, v3,..., v11
index: <periodic_table_config>
chunks: <periodic_table_config>
```

Where `periodic_table_config` is
```
prefix: <string> // The prefix to use for the tables.
period: <time.Duration> // The period at which new tables are created and used. Typically 168h
tags: <map[string]string> // The tags that can be set on the dynamo table. 
```

Now an example of this file (also something recommended when starting out) is:
```
configs:
  - from: "2020-03-01" // Or typically a week before the Cortex cluster was created.
    schema: v9
    index:
      period: 168h
      prefix: cortex_index_
    chunks:                     // Chunks section is optional and required only if you're not using a separate object store.
      period: 168h
      prefix: cortex_chunks  
    store: aws-dynamo/bigtable-hashed/cassandra/boltdb
    object_store: <above options>/s3/gcs/azure/filesystem
```

An example of an advanced schema file with a lot of changes:
```
configs:
  - from: "2018-08-23"
    schema: v9
    chunks:
        period: 168h0m0s
        prefix: dev_chunks_
    index:
        period: 168h0m0s
        prefix: dev_index_
    store: gcp-columnkey
  - from: "2019-02-13"
    schema: v9
    chunks:
        period: 168h
        prefix: dev_chunks_
    index:
        period: 168h
        prefix: dev_index_
    object_store: gcs
    store: gcp-columnkey
  - from: "2019-02-24"
    schema: v9
    chunks:
        period: 168h
        prefix: dev_chunks_
    index:
        period: 168h
        prefix: dev_index_
    object_store: gcs
    store: bigtable-hashed
  - from: "2019-03-05"
    schema: v10
    chunks:
        period: 168h
        prefix: dev_chunks_
    index:
        period: 168h
        prefix: dev_index_
    object_store: gcs
    store: bigtable-hashed
```

Note how we started out with v9 and just Bigtable, but later added GCS as the object store, finally moving to v10. This is a complex schema file from out dev cluster that saw a lot of changes dating all the way back to 2018, a typical schema config file usually has one or two schema versions.

### Migrating from flags to schema file

We've recently removed the ability to configure schema via flags. If you were using flags, you need to port your config from flags to the config file.

If you're using:
* `chunk.storage-client`: then set the corresponding `object_store` field correctly in the schema file.
* `dynamodb.daily-buckets-from`: then set the corresponding `from` date with `v2` schema.
* `dynamodb.base64-buckets-from`: then set the corresponding `from` date with `v3` schema.
* `dynamodb.v{4,5,6,9}-schema-from`: then set the corresponding `from` date with schema `v{4,5,6,9}`
* `bigtable.column-key-from`: then set the corresponding `from` date and use the `store` as `bigtable-columnkey`.
* `dynamodb.use-periodic-tables`: then set the right `index` and `chunk` fields with corresponding values from `dynamodb.periodic-table.*` and `dynamodb.chunk-table.*` flags. Note that the default period is 7 days, so please set the `period` as `168h` in the config file if none is set in the flags.
