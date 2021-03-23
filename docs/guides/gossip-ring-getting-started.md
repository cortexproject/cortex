---
title: "Getting started with gossiped ring"
linkTitle: "Getting started with a gossip ring cluster"
weight: 4
slug: getting-started-with-gossiped-ring
---

Cortex requires Key-Value (KV) store to store the ring. It can use traditional KV stores like Consul or Etcd,
but it can also build its own KV store on top of memberlist library using a gossip algorithm.

This short guide shows how to start Cortex in [single-binary mode](../architecture.md) with memberlist-based ring.
To reduce number of required dependencies in this guide, it will use [blocks storage](../blocks-storage/_index.md) with no shipping to external stores.
Storage engine and external storage configuration are not dependant on the ring configuration.

## Single-binary, two Cortex instances

For simplicity and to get started, we'll run it as a two instances of Cortex on local computer.
We will use prepared configuration files ([file 1](../../configuration/single-process-config-blocks-gossip-1.yaml), [file 2](../../configuration/single-process-config-blocks-gossip-2.yaml)), with no external
dependencies.

Build Cortex first:
```sh
$ go build ./cmd/cortex
```

Run two instances of Cortex, each one with its own dedicated config file:
```
$ ./cortex -config.file docs/configuration/single-process-config-blocks-gossip-1.yaml
$ ./cortex -config.file docs/configuration/single-process-config-blocks-gossip-2.yaml
```

Download Prometheus and configure it to use our first Cortex instance for remote writes.

```yaml
remote_write:
- url: http://localhost:9109/api/prom/push
```

After starting Prometheus, it will now start pushing data to Cortex. Distributor component in Cortex will
distribute incoming samples between the two instances.

To query that data, you can configure your Grafana instance to use http://localhost:9109/api/prom (first Cortex) as a Prometheus data source.

## How it works

The two instances we started earlier should be able to find each other via memberlist configuration (already present in the config files):

```yaml
memberlist:
  # defaults to hostname
  node_name: "Ingester 1"
  bind_port: 7946
  join_members:
    - localhost:7947
  abort_if_cluster_join_fails: false
```

This tells memberlist to listen on port 7946, and connect to localhost:7947, which is the second instance.
Port numbers are reversed in the second configuration file.
We also need to configure `node_name` and also ingester ID (`ingester.lifecycler.id` field), because default to hostname,
but we are running both Cortex instances on the same host.

To make sure that both ingesters generate unique tokens, we configure `join_after` and `observe_period` to 10 seconds.
First option tells Cortex to wait 10 seconds before joining the ring.  This option is normally used to tell Cortex ingester
how long to wait for a potential tokens and data transfer from leaving ingester, but we also use it here to increase
the chance of finding other gossip peers. When Cortex joins the ring, it generates tokens and writes them to the ring.
If multiple Cortex instances do this at the same time, they can generate conflicting tokens. This can be a problem
when using gossiped ring (instances may simply not see each other yet), so we use `observe_period` to watch the ring for token conflicts.
If conflict is detected, new tokens are generated instead of conflicting tokens, and observe period is restarted.
If no conflict is detected within the observe period, ingester switches to ACTIVE state.

We are able to observe ring state on [http://localhost:9109/ring](http://localhost:9109/ring) and [http://localhost:9209/ring](http://localhost:9209/ring).
The two instances may see slightly different views (eg. different timestamps), but should converge to a common state soon, with both instances
being ACTIVE and ready to receive samples.

## How to add another instance?

To add another Cortex to the small cluster, copy `docs/configuration/single-process-config-blocks-gossip-1.yaml` to a new file,
and make following modifications. We assume that third Cortex will run on the same machine again, so we change node name and ingester ID as well. Here
is annotated diff:

```diff
...

 server:
+  # These ports need to be unique.
-  http_listen_port: 9109
-  grpc_listen_port: 9195
+  http_listen_port: 9309
+  grpc_listen_port: 9395

...

 ingester:
   lifecycler:
     # Defaults to hostname, but we run both ingesters in this demonstration on the same machine.
-    id: "Ingester 1"
+    id: "Ingester 3"

...

 memberlist:
    # defaults to hostname
-   node_name: "Ingester 1"
+   node_name: "Ingester 3"

    # bind_port needs to be unique
-   bind_port: 7946
+   bind_port: 7948

...

+# Directory names in the `blocks_storage` > `tsdb` config ending with `...1` to end with `...3`. This is to avoid different instances
+# writing in-progress data to the same directories.
 blocks_storage:
   tsdb:
-    dir: /tmp/cortex/tsdb-ing1
+    dir: /tmp/cortex/tsdb-ing3
    bucket_store:
-     sync_dir: /tmp/cortex/tsdb-sync-querier1
+     sync_dir: /tmp/cortex/tsdb-sync-querier3

...
```

We don't need to change or add `memberlist.join_members` list. This new instance will simply join to the second one (listening on port 7947), and
will discover other peers through it. When using kubernetes, suggested setup is to have a headless service pointing to all pods
that want to be part of gossip cluster, and then point `join_members` to this headless service.

We also don't need to change `/tmp/cortex/storage` directory in `blocks_storage.filesystem.dir` field. This is directory where all ingesters will
"upload" finished blocks. This can also be an S3 or GCP storage, but for simplicity, we use local filesystem in this example.

After these changes, we can start another Cortex instance using the modified configuration file. This instance will join the ring
and will start receiving samples after it enters into ACTIVE state.
