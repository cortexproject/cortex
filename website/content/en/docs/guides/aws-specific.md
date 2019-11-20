---
title: "Running Cortex at AWS"
linkTitle: "Running Cortex at AWS"
weight: 3
slug: aws-specific.md
---

[this is a work in progress]

See also the [Running in Production]({{< relref "running.md" >}}) document.

## Credentials

You can supply credentials to Cortex by setting environment variables
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (and `AWS_SESSION_TOKEN`
if you use MFA), or use a short-term token solution such as
[kiam](https://github.com/uswitch/kiam).

## Should I use S3 or DynamoDB ?

Note that the choices are: "chunks" of timeseries data in S3 and index
in DynamoDB, or everything in DynamoDB. Using just S3 is not an option.

Broadly S3 is much more expensive to read and write, while DynamoDB is
much more expensive to store over months.  S3 charges differently, so
the cross-over will depend on the size of your chunks, and how long
you keep them.  Very roughly: for 3KB chunks if you keep them longer
than 8 months then S3 is cheaper.


## DynamoDB capacity provisioning

By default, the Cortex Tablemanager will provision tables with 1,000
units of write capacity and 300 read - these numbers are chosen to be
high enough that most trial installations won't see a bottleneck on
storage, but do note that that AWS will charge you approximately $60
per day for this capacity.

To match your costs to requirements, observe the actual capacity
utilisation via CloudWatch or Prometheus metrics, then adjust the
Tablemanager provision via command-line options
`-dynamodb.chunk-table.write-throughput`, `read-throughput` and
similar with `.periodic-table` which controls the index table.

Tablemanager can even adjust the capacity dynamically, by watching
metrics for DynamoDB throttling and ingester queue length. Here is an
example set of command-line parameters from a fairly modest install:

```
   -target=table-manager
   -metrics.url=http://prometheus.monitoring.svc.cluster.local./api/prom/
   -metrics.target-queue-length=100000
   -dynamodb.url=dynamodb://us-east-1/
   -dynamodb.use-periodic-tables=true

   -dynamodb.periodic-table.prefix=cortex_index_
   -dynamodb.periodic-table.from=2019-05-02
   -dynamodb.periodic-table.write-throughput=1000
   -dynamodb.periodic-table.write-throughput.scale.enabled=true
   -dynamodb.periodic-table.write-throughput.scale.min-capacity=200
   -dynamodb.periodic-table.write-throughput.scale.max-capacity=2000
   -dynamodb.periodic-table.write-throughput.scale.out-cooldown=300 # 5 minutes between scale ups
   -dynamodb.periodic-table.inactive-enable-ondemand-throughput-mode=true
   -dynamodb.periodic-table.read-throughput=300
   -dynamodb.periodic-table.tag=product_area=cortex

   -dynamodb.chunk-table.from=2019-05-02
   -dynamodb.chunk-table.prefix=cortex_data_
   -dynamodb.chunk-table.write-throughput=800
   -dynamodb.chunk-table.write-throughput.scale.enabled=true
   -dynamodb.chunk-table.write-throughput.scale.min-capacity=200
   -dynamodb.chunk-table.write-throughput.scale.max-capacity=1000
   -dynamodb.chunk-table.write-throughput.scale.out-cooldown=300 # 5 minutes between scale ups
   -dynamodb.chunk-table.inactive-enable-ondemand-throughput-mode=true
   -dynamodb.chunk-table.read-throughput=300
   -dynamodb.chunk-table.tag=product_area=cortex
```

Several things to note here:

- `-metrics.url` points at a Prometheus server running within the
   cluster, scraping Cortex.  Currently it is not possible to use
   Cortex itself as the target here.
- `-metrics.target-queue-length`: when the ingester queue is below
   this level, Tablemanager will not scale up. When the queue is
   growing above this level, Tablemanager will scale up whatever
   table is being throttled.
- The plain `throughput` values are used when the tables are first
   created. Scale-up to any level up to this value will be very quick,
   but if you go higher than this initial value, AWS may take tens of
   minutes to finish scaling. In the config above they are set.
- `ondemand-throughput-mode` tells AWS to charge for what you use, as
   opposed to continuous provisioning. This mode is cost-effective for
   older data, which is never written and only read sporadically.