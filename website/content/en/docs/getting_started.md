---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 3
---

Cortex can be runs as a single binary or as multiple independent microservices.
The single-binary mode is easier to deploy and is aimed mainly at users wanting to try out Cortex or develop on it.
The microservices mode is intended for production usage, as it allows you to independently scale different services and isolate failures.
This document will focus on single-process Cortex.
See [the architecture doc](architecture.md) For more information about the microservices.

Separately from single process vs microservices decision, Cortex can be configured to use local storage or cloud storage (DynamoDB, Bigtable, Cassandra, S3, GCS etc).
This document will focus on using local storage.
Local storage is explicitly not production ready at this time.
Cortex can also make use of external memcacheds for caching and although these are not mandatory, they should be used in production.

## Single instance, single process

For simplicity & to get started, we'll run it as a single process with no dependencies:

```sh
$ go build ./cmd/cortex
$ ./cortex -config.file=./docs/single-process-config.yaml
```

This starts a single Cortex node storing chunks and index to your local filesystem in `/tmp/cortex`.
It is not intended for production use.

Add the following to your Prometheus config (documentation/examples/prometheus.yml in Prometheus repo):

```yaml
remote_write:
- url: http://localhost:9009/api/prom/push
```

And start Prometheus with that config file:

```sh
$ git clone https://github.com/prometheus/prometheus
$ cd prometheus
$ go build ./cmd/prometheus
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

Your Prometheus instance will now start pushing data to Cortex.  To query that data, start a Grafana instance:

```sh
$ docker run -d --name=grafana -p 3000:3000 grafana/grafana
```

In [the Grafana UI](http://localhost:3000) (username/password admin/admin), add a Prometheus datasource for Cortex (`http://host.docker.internal:9009/api/prom`).

**To clean up:** press CTRL-C in both terminals (for Cortex and Promrtheus) and run `docker rm -f grafana`.

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
$ docker run -d --name=cortex1 --network=cortex \
    -v $(pwd)/docs/single-process-config.yaml:/etc/single-process-config.yaml \
    -p 9001:9009 \
    quay.io/cortexproject/cortex \
    -config.file=/etc/single-process-config.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500
$ docker run -d --name=cortex2 --network=cortex \
    -v $(pwd)/docs/single-process-config.yaml:/etc/single-process-config.yaml \
    -p 9002:9009 \
    quay.io/cortexproject/cortex \
    -config.file=/etc/single-process-config.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500
```

If you go to http://localhost:9001/ring (or http://localhost:9002/ring) you should see both Cortex nodes join the ring.

To demonstrate the correct operation of Cortex clustering, we'll send samples
to one of the instances and queries to another.  In production, you'd want to
load balance both pushes and queries evenly among all the nodes.

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
$ docker run -d --name=grafana --network=cortex -p 3000:3000 grafana/grafana
```

In [the Grafana UI](http://localhost:3000) (username/password admin/admin), add a Prometheus datasource for Cortex (`http://cortex2:9009/api/prom`).

**To clean up:** CTRL-C the Prometheus process and run:

```
$ docker rm -f cortex1 cortex2 consul grafana
$ docker network remove cortex
```

## High availability with replication

In this last demo we'll show how Cortex can replicate data among three nodes,
and demonstrate Cortex can tolerate a node failure without affecting reads and writes.

First, create a network and run a new Consul and Grafana:

```sh
$ docker network create cortex
$ docker run -d --name=consul --network=cortex -e CONSUL_BIND_INTERFACE=eth0 consul
$ docker run -d --name=grafana --network=cortex -p 3000:3000 grafana/grafana
```

Finally, launch 3 Cortex nodes with replication factor 3:

```sh
$ docker run -d --name=cortex1 --network=cortex \
    -v $(pwd)/docs/single-process-config.yaml:/etc/single-process-config.yaml \
    -p 9001:9009 \
    quay.io/cortexproject/cortex \
    -config.file=/etc/single-process-config.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500 \
    -distributor.replication-factor=3
$ docker run -d --name=cortex2 --network=cortex \
    -v $(pwd)/docs/single-process-config.yaml:/etc/single-process-config.yaml \
    -p 9002:9009 \
    quay.io/cortexproject/cortex \
    -config.file=/etc/single-process-config.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500 \
    -distributor.replication-factor=3
$ docker run -d --name=cortex3 --network=cortex \
    -v $(pwd)/docs/single-process-config.yaml:/etc/single-process-config.yaml \
    -p 9003:9009 \
    quay.io/cortexproject/cortex \
    -config.file=/etc/single-process-config.yaml \
    -ring.store=consul \
    -consul.hostname=consul:8500 \
    -distributor.replication-factor=3
```

Configure Prometheus to send data to the first replica:

```yaml
remote_write:
- url: http://localhost:9001/api/prom/push
```

```sh
$ ./prometheus --config.file=./documentation/examples/prometheus.yml
```

In Grafana, add a datasource for the 3rd Cortex replica (`http://cortex3:9009/api/prom`)
and verify the same data appears in both Prometheus and Cortex.

To show that Cortex can tolerate a node failure, hard kill one of the Cortex replicas:

```
$ docker rm -f cortex2
```

You should see writes and queries continue to work without error.

**To clean up:** CTRL-C the Prometheus process and run:

```
$ docker rm -f cortex1 cortex2 cortex3 consul grafana
$ docker network remove cortex
```
