---
title: "Capacity Planning"
linkTitle: "Capacity Planning"
weight: 104
slug: capacity-planning
---


You will want to estimate how many nodes are required, how many of
each component to run, and how much storage space will be required.
In practice, these will vary greatly depending on the metrics being
sent to Cortex.

Some key parameters are:

 1. The number of active series. If you have Prometheus already you
 can query `prometheus_tsdb_head_series` to see this number.
 2. Sampling rate, e.g. a new sample for each series every minute
 (the default Prometheus [scrape_interval](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)).
 Multiply this by the number of active series to get the
 total rate at which samples will arrive at Cortex.
 3. The rate at which series are added and removed. This can be very
 high if you monitor objects that come and go - for example if you run
 thousands of batch jobs lasting a minute or so and capture metrics
 with a unique ID for each one. [Read how to analyse this on
 Prometheus](https://www.robustperception.io/using-tsdb-analyze-to-investigate-churn-and-cardinality).
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
example to save on inter-zone bandwidth charges at AWS/GCP) they will use
significantly more CPU (approx 100% more for distributor and 50% more
for ingester).
