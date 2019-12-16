---
title: "Running Cortex in Production"
linkTitle: "Running Cortex in Production"
weight: 1
slug: running-in-production
---

This document assumes you have read the
[architecture](architecture.md) document.

In addition to the general advice in this document, please see these
platform-specific notes:

- [AWS](aws-specific.md)

## Planning

### Tenants

If you will run Cortex as a multi-tenant system, you need to give each
tenant a unique ID - this can be any string. Managing tenants and
allocating IDs must be done outside of Cortex. You must also configure
[Authentication and Authorisation](auth.md).

### Storage

Cortex requires a scalable storage back-end.  Commercial cloud options
are DynamoDB and Bigtable: the advantage is you don't have to know how
to manage them, but the downside is they have specific costs.
Alternatively you can choose Cassandra, which you will have to install
and manage.

### Components

Every Cortex installation will need Distributor, Ingester and Querier.
Alertmanager, Ruler and Query-frontend are optional.

### Other dependencies

Cortex needs a KV store to track sharding of data between
processes. This can be either Etcd or Consul.

If you want to configure recording and alerting rules (i.e. if you
will run the Ruler and Alertmanager components) then a Postgres
database is required to store configs.

Memcached is not essential but highly recommended.

### Ingester replication factor

The standard replication factor is three, so that we can drop one
replica and be unconcerned, as we still have two copies of the data
left for redundancy. This is configurable: you can run with more
redundancy or less, depending on your risk appetite.

### Schema

#### Schema periodic table

The periodic table from argument (`-dynamodb.periodic-table.from=<date>` if
using command line flags, the `from` field for the first schema entry if using
YAML) should be set to the date the oldest metrics you will be sending to
Cortex. Generally that means set it to the date you are first deploying this
instance. If you use an example date from years ago table-manager will create
hundreds of tables. You can also avoid creating too many tables by setting a
reasonable retention in the table-manager
(`-table-manager.retention-period=<duration>`).

#### Schema version

Choose schema version 9 in most cases; version 10 if you expect
hundreds of thousands of timeseries under a single name.  Anything
older than v9 is much less efficient.

### Chunk encoding

Standard choice would be Bigchunk, which is the most flexible chunk
encoding. You may get better compression from Varbit, if many of your
timeseries do not change value from one day to the next.

### Sizing

You will want to estimate how many nodes are required, how many of
each component to run, and how much storage space will be required.
In practice, these will vary greatly depending on the metrics being
sent to Cortex.

Some key parameters are:

 1. The number of active series. If you have Prometheus already you
 can query `prometheus_tsdb_head_series` to see this number.
 2. Sampling rate, e.g. a new sample for each series every 15
 seconds. Multiply this by the number of active series to get the
 total rate at which samples will arrive at Cortex.
 3. The rate at which series are added and removed. This can be very
 high if you monitor objects that come and go - for example if you run
 thousands of batch jobs lasting a minute or so and capture metrics
 with a unique ID for each one. [Read how to analyse this on
 Prometheus](https://www.robustperception.io/using-tsdb-analyze-to-investigate-churn-and-cardinality)
 4. How compressible the time-series data are. If a metric stays at
 the same value constantly, then Cortex can compress it very well, so
 12 hours of data sampled every 15 seconds would be around 2KB.  On
 the other hand if the value jumps around a lot it might take 10KB.
 There are not currently any tools available to analyse this.
 5. How long you want to retain data for, e.g. 1 month or 2 years.

Other parameters which can become important if you have particularly
high values:

 6. Number of different series under one metric name.
 7. Number of labels per series.
 8. Rate and complexity of queries.

Now, some rules of thumb:

 1. Each million series in an ingester takes 15GB of RAM. Total number
 of series in ingesters is number of active series times the
 replication factor. This is with the default of 12-hour chunks - RAM
 required will reduce if you set `-ingester.max-chunk-age` lower
 (trading off more back-end database IO)
 2. Each million series (including churn) consumes 15GB of chunk
 storage and 4GB of index, per day (so multiply by the retention
 period).
 3. Each 100,000 samples/sec arriving takes 1 CPU in distributors.
 Distributors don't need much RAM.

If you turn on compression between distributors and ingesters (for
example to save on inter-zone bandwidth charges at AWS) they will use
significantly more CPU (approx 100% more for distributor and 50% more
for ingester).

### Caching

Cortex can retain data in-process or in Memcached to speed up various
queries by caching:

 * individual chunks
 * index lookups for one label on one day
 * the results of a whole query

You should always include Memcached in your Cortex install so results
from one process can be re-used by another. In-process caching can cut
fetch times slightly and reduce the load on Memcached.

Ingesters can also be configured to use Memcached to avoid re-writing
index and chunk data which has already been stored in the back-end
database. Again, highly recommended.

### Orchestration

Because Cortex is designed to run multiple instances of each component
(ingester, querier, etc.), you probably want to automate the placement
and shepherding of these instances. Most users choose Kubernetes to do
this, but this is not mandatory.

## Configuration

### Resource requests

If using Kubernetes, each container should specify resource requests
so that the scheduler can place them on a node with sufficient capacity.

For example an ingester might request:

```
        resources:
          requests:
            cpu: 4
            memory: 10Gi
```

The specific values here should be adjusted based on your own
experiences running Cortex - they are very dependent on rate of data
arriving and other factors such as series churn.

### Take extra care with ingesters

Ingesters hold hours of timeseries data in memory; you can configure
Cortex to replicate the data but you should take steps to avoid losing
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
store when shutting down; for Kubernetes this looks like:

```
      terminationGracePeriodSeconds: 2400
```

Ask Kubernetes to limit rolling updates to one ingester at a time, and
signal the old one to stop before the new one is ready:

```
  strategy:
    rollingUpdate:
      maxSurge: 0
      maxUnavailable: 1
```

Ingesters provide an http hook to signal readiness when all is well;
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


## Optimising

### Optimising Storage

These ingester options reduce the chance of storing multiple copies of
the same data:

        -ingester.spread-flushes=true
        -ingester.chunk-age-jitter=0

Add a chunk cache via `-memcached.hostname` to allow writes to be de-duplicated.

As recommended under [Chunk encoding](#chunk-encoding), use Bigchunk:

        -ingester.chunk-encoding=3 # bigchunk
