## Running cortex with cassandra


Cortex works as a backend store for prometheus that offers long term storage, multi-tenancy & global aggregation of metrics. 

We can run cortex as a single binary either by running it as an container or as a process by building it from scratch.

Now we are using cassandra as backend storage. Let's create a new cassandra instance by running the below commands or you can use the existing cassandra setup:

Run cassandra with following cmd

```
docker run --name cassandra --rm -p 9042:9042 cassandra:3.11
```

ssh into cassandra container 

```
docker exec -it <container_id> bash
```

enter into cqlsh prompt

```
cqlsh
```

create a new keyspace for cortex metrics in cassandra by using the below command

```
CREATE KEYSPACE cortex WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};
```

Now cortex needs configuration details. Now create a file called
"single-process-config.yaml" add below content into it and make sure to add cassandra instance details i.e

Replace

 **LOCALHOST** = Addresses of your cassandra instance, This can accept multiple addresses by passing them as comma separated values.

 **KEYSPACE**  = Add the desired keyspace for storing metrics from cortex.



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

Running cortex as an image

```
docker run -d --name=cortex -v $(pwd)/single-process-config.yaml:/etc/single-process-config.yaml -p 9009:9009  quay.io/cortexproject/cortex -config.file=/etc/single-process-config.yaml
```

Running cortex as a executable by building it from source

clone the cortex project by using

```
git clone https://github.com/cortexproject/cortex.git
```

Build the cortex from source using

```
go build ./cmd/cortex
```

Now run the cortex using the executable generated and by passing single-process-config.yaml that we built in the previous steps.

```
./cortex -config.file=./single-process-config.yaml
```

Now the cortex is running on http://localhost:9009

Add remote write configuration for your prometheus config file. This writes the metrics data to your cortex. 

```
remote_write:
   - url: http://localhost:9009/api/prom/push
```

Run grafana to visualise metrics from cortex service.

```
docker run -d --name=grafana -p 3000:3000 grafana/grafana
```

Add a data source in grafana by picking the prometheus type as data source and configure cortex url to query metrics. 
i.e http://localhost:9009/api/prom

Now you can monitor the cortex reads & writes by creating the dashboard for cortex.

clone grafonnet project using the below cmd

```
git clone https://github.com/grafana/grafonnet-lib.git
```

Now configure the grafonnet path in the below commands. This will generate cortex read, write path dashboard for grafana.

You can find cortex-read.jsonnet at
https://github.com/cortexproject/cortex/blob/master/production/dashboards/cortex-read.jsonnet

You can find cortex-write.jsonnet at 
https://github.com/cortexproject/cortex/blob/master/production/dashboards/cortex-write.jsonnet

```
jsonnet -J <grafonnet-path> -o cortex-read.json cortex-read.jsonnet
jsonnet -J <grafonnet-path> -o cortex-write.json cortex-write.jsonnet
```

Happy Monitoring!