## Running Cortex with Cassandra

This guide covers how to run a single local Cortex instance - with the chunks storage engine - storing time series chunks and index in Cassandra.

We can run Cortex as a single binary either by running it as an container or as a process by building it from scratch.

Now we are using Cassandra as backend storage. Let's create a new Cassandra instance by running the below commands or you can use the existing Cassandra setup:

In this guide we're going to:

1. Setup a locally running Cassandra
2. Configure Cortex to store chunks and index on Cassandra
3. Configure Prometheus to send series to Cortex
4. Configure Grafana to visualise metrics

Run Cassandra with following cmd

```
docker run -d --name cassandra --rm -p 9042:9042 cassandra:3.11
```

Get a cql shell in the Cassandra container

```
docker exec -it <container_id> cqlsh
```

create a new keyspace for Cortex metrics in Cassandra by using the below command

```
CREATE KEYSPACE cortex WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
```

Now Cortex needs configuration details. Now create a file called
"single-process-config.yaml" add below content into it and make sure to add Cassandra instance details i.e

Replace

 **LOCALHOST** = Addresses of your Cassandra instance, This can accept multiple addresses by passing them as comma separated values.

 **KEYSPACE**  = Add the desired keyspace for storing metrics from Cortex.



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

storage:
  cassandra:
    addresses: LOCALHOST # configure cassandra addresses here.
    keyspace: KEYSPACE   # configure desired keyspace here.
```

Running Cortex as an image

```
docker run -d --name=cortex -v $(pwd)/single-process-config.yaml:/etc/single-process-config.yaml -p 9009:9009  quay.io/cortexproject/cortex -config.file=/etc/single-process-config.yaml
```
In case you prefer to run the master version, please follow this [documentation](https://github.com/cortexproject/cortex/blob/master/docs/getting_started.md) on how to build Cortex from sources.

Now the Cortex is running on http://localhost:9009

Add remote write configuration for your prometheus config file. This writes the metrics data to your Cortex. 

```
remote_write:
   - url: http://localhost:9009/api/prom/push
```

Run grafana to visualise metrics from Cortex service.

```
docker run -d --name=grafana -p 3000:3000 grafana/grafana
```

Add a data source in grafana by picking the prometheus type as data source and configure Cortex url to query metrics. 
i.e http://localhost:9009/api/prom

Now you can monitor the Cortex reads & writes by creating the dashboard's using this [documentation](https://github.com/cortexproject/cortex/tree/master/production/dashboards).
