# Getting Started

Cortex can be runs as a single binary or as multiple independent microservices.
The single-binary mode is easier to deploy and is aimed mainly at users wanting to try out Cortex or develop on it.
The microservices mode is intended for production usage, as it allows you to independently scale different services and isolate failures.
This document will focus on single-process Cortex.
See [the architecture doc](architecture.md) For more information about the microservices.

Separately from single process vs microservices decision, Cortex can be configured to use local storage or cloud storage (DynamoDB, Bigtable, Cassandra, S3, GCS etc).
This document will focus on using local storage.
Local storage is explicity not production ready at this time.
Cortex can also make use of external memcacheds for caching and although these are not mandatory, they should be used in production.

## Single instance, single process.

For simplicity & to get started, we'll run it as a single process with no dependencies:

```sh
$ make protos # Build all the protobuffer definitions.
$ go build ./cmd/cortex
$ ./cortex -config.file=./cmd/cortex/single-process-config.yaml
```

This starts a single Cortex node storing chunks and index to your local filesystem in `/tmp/cortex`.
It is not intended for production use.

Add the following to your prometheus config:

```yaml
remote_write:
- url: http://localhost:9009/api/prom/push
```

And start Prometheus with that config file:

```sh
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

Your Prometheus instance will now start pushing data to Cortex.  To query that data, start a Grafana instance:

```sh
$ docker run -p 3000:3000 grafana/grafana
```

In [the Grafana UI](http://localhost:3000) (username/password admin/admin), add a Prometheus datasource for Cortex (`http://host.docker.internal:9009/api/prom`).

## Horizontally scale out

Next we're going to show how you can run a scale out Cortex cluster using Docker.
We'll need:
- A built Cortex image.
- A Docker network to put these containers on so they can resolve each other by name.
- A single node Consul instance to coordinate the Cortex cluster.


```sh
$ make ./cmd/cortex/.uptodate
$ docker network create cortex
$ docker run -d --name=consul --network=cortex -e CONSUL_BIND_INTERFACE=eth0 consul
```

Next we'll run a couple of Cortex instances pointed at that Consul.  You'll note with Cortex configuration can be specified in either a config file or overridden on the command line.  See [the arguments documentation](arguments.md) for more information about Cortex configuration options.

```sh
$ docker run -d --name=cortex1 --network=cortex -p 9001:9009 quay.io/cortexproject/cortex -config.file=/etc/single-process-config.yaml -ring.store=consul -consul.hostname=consul:8500
$ docker run -d --name=cortex2 --network=cortex -p 9002:9009 quay.io/cortexproject/cortex -config.file=/etc/single-process-config.yaml -ring.store=consul -consul.hostname=consul:8500
```

If you go to http://localhost:9001/ring (or http://localhost:9002/ring) you should see both Cortex nodes join the ring.

To demonstrate the correct operation of Cortex clustering, we'll send samples to one of the instances
and queries to another.  In production, you'd want to load balance both pushes and queries evenly among
all the nodes.

Point Prometheus at the first:

```yaml
remote_write:
- url: http://localhost:9001/api/prom/push
```

```sh
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

And Grafana at the second:

```sh
$ docker run -d --network=cortex -p 3000:3000 grafana/grafana
```

In [the Grafana UI](http://localhost:3000) (username/password admin/admin), add a Prometheus datasource for Cortex (`http://cortex2:9009/api/prom`).