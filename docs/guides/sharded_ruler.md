---
title: "Config for horizontally scaling the Ruler"
linkTitle: "Config for horizontally scaling the Ruler"
weight: 4
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
