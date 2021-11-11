---
title: "Config for horizontally scaling the Ruler"
linkTitle: "Config for horizontally scaling the Ruler"
weight: 10
slug: ruler-sharding
---

## Context

One option to scale the ruler is by scaling it horizontally. However, with multiple ruler instances running they will need to coordinate to determine which instance will evaluate which rule. Similar to the ingesters, the rulers establish a hash ring to divide up the responsibilities of evaluating rules.

## Config

In order to enable sharding in the ruler the following flag needs to be set:

```
  -ruler.enable-sharding=true
```

In addition the ruler requires it's own ring to be configured, for instance:

```
  -ruler.ring.consul.hostname=consul.dev.svc.cluster.local:8500
```

The only configuration that is required is to enable sharding and configure a key value store. From there the rulers will shard and handle the division of rules automatically.

Unlike ingesters, rulers do not hand over responsibility: all rules are re-sharded randomly every time a ruler is added to or removed from the ring.

## Ruler Storage

The ruler supports six kinds of storage (configdb, azure, gcs, s3, swift, local).  Most kinds of storage work with the sharded ruler configuration in an obvious way.  i.e. configure all rulers to use the same backend.

The local implementation reads [Prometheus recording rules](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/) off of the local filesystem.  This is a read only backend that does not support the creation and deletion of rules through [the API](../api/_index.md#ruler).  Despite the fact that it reads the local filesystem this method can still be used in a sharded ruler configuration if the operator takes care to load the same rules to every ruler.  For instance this could be accomplished by mounting a [Kubernetes ConfigMap](https://kubernetes.io/docs/concepts/configuration/configmap/) onto every ruler pod.

A typical local config may look something like:
```
  -ruler-storage.backend=local
  -ruler-storage.local.directory=/tmp/cortex/rules
```

With the above configuration the ruler would expect the following layout:
```
/tmp/cortex/rules/<tenant id>/rules1.yaml
                             /rules2.yaml
```
Yaml files are expected to be in the [Prometheus format](https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#recording-rules).

