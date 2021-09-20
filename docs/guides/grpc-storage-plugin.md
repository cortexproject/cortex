---
title: "gRPC storage plugin"
linkTitle: "gRPC storage plugin"
weight: 10
slug: grpc-based-plugin
---

_This feature is currently experimental and is only supported for Chunks storage (deprecated)._

Cortex chunks storage supports a **gRPC-based plugin system** to use alternative backends for the index and chunks store.
A store plugin is a gRPC-based server which implements the methods required by the index and chunks store. Cortex chunks storage schema is then configured to use the plugin as backend system and gRPC will be used to communicate between Cortex and the plugin.
For example, if you're deploying your Cortex cluster on Kubernetes, the plugin would run as a sidecar container of your Cortex pods and the Cortex's `-grpc-store.server-address` should be configured to the endpoint exposed by the sidecar plugin (eg. `localhost:<port>`).

### How it works

In the cortex configuration file, add `store` and `object_store` as `grpc-store` and configure storage with plugin server endpoint (ie. the address to the gRPC server which implements the cortex chunk store methods).

```
schema:
  configs:
  - from: 2019-07-29
    store: grpc-store
    object_store: grpc-store
    schema: v10
    index:
      prefix: index_
      period: 168h
    chunks:
      prefix: chunk_
      period: 168h

storage:
  grpc_store:
    # gRPC server address
    server_address: localhost:6666
```

## Community plugins


The following list shows Cortex storage plugins built and shared by the community:

1. [gRPC based Cortex chunk store for Mongo](https://github.com/VineethReddy02/cortex-mongo-store)
2. [gRPC based Cortex chunk store for Mysql](https://github.com/VineethReddy02/cortex-mysql-store)
