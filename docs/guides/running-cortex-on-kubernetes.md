---
title: "Running Cortex on Kubernetes"
linkTitle: "Running Cortex on Kubernetes"
weight: 3
slug: running-cortex-on-kubernetes
---

Because Cortex is designed to run multiple instances of each component
(ingester, querier, etc.), you probably want to automate the placement
and shepherding of these instances. Most users choose Kubernetes to do
this, but this is not mandatory.

## Configuration

### Resource requests

If using Kubernetes, each container should specify resource requests
so that the scheduler can place them on a node with sufficient capacity.

For example, an ingester might request:

```
        resources:
          requests:
            cpu: 4
            memory: 10Gi
```

The specific values here should be adjusted based on your own
experiences running Cortex - they are very dependent on the rate of data
arriving and other factors such as series churn.

### Take extra care with ingesters

Ingesters hold hours of timeseries data in memory; you can configure
Cortex to replicate the data, but you should take steps to avoid losing
all replicas at once:

- Don't run multiple ingesters on the same node.
- Don't run ingesters on preemptible/spot nodes.
- Spread out ingesters across racks / availability zones / whatever
  applies in your datacenters.

You can ask Kubernetes to avoid running on the same node like this:

```
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              labelSelector:
                matchExpressions:
                - key: name
                  operator: In
                  values:
                  - ingester
              topologyKey: "kubernetes.io/hostname"
```

Give plenty of time for an ingester to hand over or flush data to
store when shutting down; for Kubernetes, this looks like:

```
      terminationGracePeriodSeconds: 2400
```

Ask Kubernetes to limit rolling updates to one ingester at a time and
signal the old one to stop before the new one is ready:

```
  strategy:
    rollingUpdate:
      maxSurge: 0
      maxUnavailable: 1
```

Ingesters provide an HTTP hook to signal readiness when all is well;
this is valuable because it stops a rolling update at the first
problem:

```
        readinessProbe:
          httpGet:
            path: /ready
            port: 80
```

We do not recommend configuring a liveness probe on ingesters -
killing them is a last resort and should not be left to a machine.
