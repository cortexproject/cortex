---
title: "Running Cortex with Cassandra"
linkTitle: "Running Cortex with Cassandra"
weight: 7
slug: cassandra
---

This guide covers how to run a single local Cortex instance - with the chunks storage engine - storing time series chunks and index in Cassandra.

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
    use_gzip_compression: true

ingester:
  lifecycler:
    # The address to advertise for this ingester. Will be autodiscovered by
    # looking up address on eth0 or en0; can be specified if this fails.
    address: 127.0.0.1

    # We want to start immediately and flush on shutdown.
    join_after: 0
    claim_on_rollout: false
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
to find the latest stable version tag and use it in the command bellow (currently it is `v0.7.0`).

Run Cortex using the latest stable version:

```
docker run -d --name=cortex -v $(pwd)/single-process-config.yaml:/etc/single-process-config.yaml -p 9009:9009  quay.io/cortexproject/cortex:v0.7.0 -config.file=/etc/single-process-config.yaml
```
In case you prefer to run the master version, please follow this [documentation](../getting-started/getting-started-chunks.md) on how to build Cortex from source.


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

Finally, You can monitor Cortex's reads & writes by creating the dashboard. You can follow this [documentation](https://github.com/cortexproject/cortex/tree/master/production/dashboards) to do so.
