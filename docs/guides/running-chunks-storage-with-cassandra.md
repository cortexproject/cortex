---
title: "Running Cortex chunks storage with Cassandra"
linkTitle: "Running Cortex chunks storage with Cassandra"
weight: 2
slug: running-chunks-storage-with-cassandra
---

This guide covers how to run a single local Cortex instance - with the [**chunks storage**](../chunks-storage/_index.md) engine - storing time series chunks and index in Cassandra.

In this guide we're going to:

1. Setup a locally running Cassandra
2. Configure Cortex to store chunks and index on Cassandra
3. Configure Prometheus to send series to Cortex
4. Configure Grafana to visualise metrics

## Setup a locally running Cassandra

Run Cassandra with the following command:

```
docker run -d --name cassandra --rm -p 9042:9042 cassandra:3.11
```

Use Docker to execute the Cassandra Query Language (CQL) shell in the container:

```
docker exec -it <container_id> cqlsh
```

Create a new Cassandra keyspace for Cortex metrics:

A keyspace is an object that is used to hold column families, user defined types. A keyspace is like RDBMS database which contains column families, indexes, user defined types.

```
CREATE KEYSPACE cortex WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
```
## Configure Cortex to store chunks and index on Cassandra

Now, we have to configure Cortex to store the chunks and index in Cassandra. Create a config file called `single-process-config.yaml`, then add the content below. Make sure to replace the following placeholders:
- `LOCALHOST`: Addresses of your Cassandra instance. This can accept multiple addresses by passing them as comma separated values.
- `KEYSPACE`: The name of the Cassandra keyspace used to store the metrics.

`single-process-config.yaml`
```
# Configuration for running Cortex in single-process mode.
# This should not be used in production.  It is only for getting started
# and development.

# Disable the requirement that every request to Cortex has a
# X-Scope-OrgID header. `fake` will be substituted in instead.
auth_enabled: false

server:
  http_listen_port: 9009

  # Configure the server to allow messages up to 100MB.
  grpc_server_max_recv_msg_size: 104857600
  grpc_server_max_send_msg_size: 104857600
  grpc_server_max_concurrent_streams: 1000

distributor:
  shard_by_all_labels: true
  pool:
    health_check_ingesters: true

ingester_client:
  grpc_client_config:
    # Configure the client to allow messages up to 100MB.
    max_recv_msg_size: 104857600
    max_send_msg_size: 104857600
    grpc_compression: gzip

ingester:
  lifecycler:
    # The address to advertise for this ingester. Will be autodiscovered by
    # looking up address on eth0 or en0; can be specified if this fails.
    address: 127.0.0.1

    # We want to start immediately and flush on shutdown.
    join_after: 0
    final_sleep: 0s
    num_tokens: 512

    # Use an in memory ring store, so we don't need to launch a Consul.
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1

# Use cassandra as storage -for both index store and chunks store.
schema:
  configs:
  - from: 2019-07-29
    store: cassandra
    object_store: cassandra
    schema: v10
    index:
      prefix: index_
      period: 168h
    chunks:
      prefix: chunk_
      period: 168h

storage:
  cassandra:
    addresses: LOCALHOST # configure cassandra addresses here.
    keyspace: KEYSPACE   # configure desired keyspace here.
```

The latest tag is not published for the Cortex docker image. Visit quay.io/repository/cortexproject/cortex
to find the latest stable version tag and use it in the command below (currently it is `v1.8.1`).

Run Cortex using the latest stable version:

```
docker run -d --name=cortex -v $(pwd)/single-process-config.yaml:/etc/single-process-config.yaml -p 9009:9009  quay.io/cortexproject/cortex:v1.8.1 -config.file=/etc/single-process-config.yaml
```
In case you prefer to run the master version, please follow this [documentation](../getting-started/getting-started-chunks.md) on how to build Cortex from source.

### Configure the index and chunk table options

In order to create index and chunk tables on Cassandra, Cortex will use the default table options of your Cassandra.
If you want to configure the table options, use the `storage.cassandra.table_options` property or `cassandra.table-options` flag.
This configuration property is just `string` type and this value used as plain text on `WITH` option of table creation query.
It is recommended to enclose the value of `table_options` in double-quotes because you should enclose strings of table options in quotes on Cassandra.

For example, suppose the name of index(or chunk) table is 'test_table'.
Details about column definitions of the table are omitted.
If no table options configured, then Cortex will generate the query to create a table without a `WITH` clause to use default table options:

```
CREATE TABLE IF NOT EXISTS cortex.test_table (...)
```

If table options configured with `table_options` as below:

```
storage:
  cassandra:
    addresses: 127.0.0.1
    keyspace: cortex
    table_options: "gc_grace_seocnds = 86400
      AND comments = 'this is a test table'
      AND COMPACT STORAGE
      AND caching = { 'keys': 'ALL', 'rows_per_partition': 1024 }"
```

Then Cortex will generate the query to create a table with a `WITH` clause as below:

```
CREATE TABLE IF NOT EXISTS cortex.test_table (...) WITH gc_grace_seocnds = 86400 AND comments = 'this is a test table' AND COMPACT STORAGE AND caching = { 'keys': 'ALL', 'rows_per_partition': 1024 }
```

Available settings of the table options on Cassandra depend on Cassandra version or storage which is compatible.
For details about table options, see the official document of storage you are using.

**WARNING**: Make sure there are no incorrect options and mistakes. Misconfigured table options may cause a failure in creating a table by [table-manager](../chunks-storage/table-manager.md) at runtime and seriously affect your Cortex.

## Configure Prometheus to send series to Cortex

Now that Cortex is up, it should be running on `http://localhost:9009`.

Add the following section to your Prometheus configuration file. This will configure the remote write to send metrics to Cortex.

```
remote_write:
   - url: http://localhost:9009/api/prom/push
```
## Configure Grafana to visualise metrics

Run grafana to visualise metrics from Cortex:

```
docker run -d --name=grafana -p 3000:3000 grafana/grafana
```

Add a data source in Grafana by selecting Prometheus as the data source type and use the Cortex URL to query metrics: `http://localhost:9009/api/prom`.

Finally, You can monitor Cortex's reads & writes by creating the dashboard. If you're looking for ready to use dashboards, you can take a look at Grafana's [Cortex dashboards and alerts](https://github.com/grafana/cortex-jsonnet/) (Jsonnet) or Weaveworks's [Cortex dashboards](https://github.com/weaveworks/cortex-dashboards) (Python).
