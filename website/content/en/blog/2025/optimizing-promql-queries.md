---
date: 2025-04-29
title: "Optimizing PromQL queries: A deep dive"
linkTitle: Optimizing PromQL queries
tags: [ "blog", "cortex", "query", "optimization" ]
categories: [ "blog" ]
projects: [ "cortex" ]
description: >
  This guide explains how Cortex evaluates PromQL queries, details how time series data is stored and retrieved, and offers strategies to write performant queries—particularly in high-cardinality environments.
author: Harry John ([@harry671003](https://github.com/harry671003))
---


## Introduction

This guide explains how Cortex evaluates PromQL queries, details how time series data is stored and retrieved, and offers strategies to write performant queries — particularly in high-cardinality environments.

Note: If you are new to PromQL, it is recommended to start with the [Querying basics documentation](https://prometheus.io/docs/prometheus/latest/querying/basics/).

## Prometheus Concepts

### Data Model

Prometheus employs a straightforward data model:

* Each time series is uniquely identified by a metric name and a set of label-value pairs.
* Each sample includes:
    * A millisecond precision timestamp
    * A 64 bit floating point value.

### Label Matchers

Label matchers define the selection criteria for time series within the TSDB. Consider the following PromQL expression:

```
http_requests_total{cluster="prod", job="envoy"}
```

the label matchers are:

*  `__name__="http_requests_total"`
* `cluster="prod"`
* `job="envoy"`


Prometheus supports four types of label matchers:

|Type	|Syntax	|Example	|
|---	|---	|---	|
|Equal	|label="value"	|job="envoy"	|
|Not Equal	|label!="value"	|job!="prometheus"	|
|Regex Equal	|label=~"regex"	|job=~"env.*"	|
|Regex Not Equal	|label!~"regex"	|status!~"4.."	|

## Time Series Storage in Cortex

Cortex uses Prometheus's Time Series Database (TSDB) for storing time series data. The Prometheus TSDB is time partitioned into blocks. Each TSDB block is made up of the following files:

* `ID` - ID of the block ([ULID](https://github.com/ulid/spec))
* `meta.json` - Contains the metadata of the block
* `index` - A binary file that contains the index
* `chunks` - Directory containing the chunk segment files

More details: [TSDB format docs](https://github.com/prometheus/prometheus/blob/5630a3906ace8f2ecd16e7af7fb184e4f4dd853d/tsdb/docs/format/README.md)

### Index File

The `index` file contains two key mappings for query processing:

* **Postings Offset Table and Postings**: Maps label-value pairs to Series IDs
* **Series Section**: Maps series IDs to label sets and chunk references

#### Example

Given the following time series:

```
http_requests_total{cluster="prod", job="envoy", status="200"} -> SeriesID(1)
http_requests_total{cluster="prod", job="envoy", status="400"} -> SeriesID(2)
http_requests_total{cluster="prod", job="envoy", status="500"} -> SeriesID(3)
http_requests_total{cluster="prod", job="prometheus", status="200"} -> SeriesID(4)
```

The index file would store mappings such as:

```
__name__=http_requests_total → [1, 2, 3, 4]
cluster=prod                 → [1, 2, 3, 4]
job=envoy                    → [1, 2, 3]
job=prometheus               → [4]
status=200                   → [1, 4]
status=400                   → [2]
status=500                   → [3]
```

### Chunks

Each chunk segment file can store up to **512MB** of data. Each chunk in the segment file typically holds up to **120 samples**.

## Query Execution in Cortex

To optimize PromQL queries effectively, it is essential to understand how queries are executed within Cortex. Consider the following example:

```
sum(rate(http_requests_total{cluster="prod", job="envoy"}[5m]))
```

### Block Selection

Cortex first identifies the TSDB blocks that fall within the query’s time range. This process is very fast in Cortex and will not add a huge overhead on query execution.

### Series Selection

Next, Cortex uses the inverted index to retrieve the set of matching series IDs for each label matcher. For example:

```
__name__="http_requests_total" → [1, 2, 3, 4]
cluster="prod"                 → [1, 2, 3, 4]
job="envoy"                    → [1, 2, 3]
```

The intersection of these sets yields:

```
http_requests_total{cluster=“prod”, job=“envoy”, status=“200”}
http_requests_total{cluster=“prod”, job=“envoy”, status=“400”}
http_requests_total{cluster=“prod”, job=“envoy”, status=“500”}
```

### Sample Selection

The mapping from series to chunks is used to identify the relevant chunks from the chunk segment files. These chunks are decoded to retrieve the underlying time series samples.

### PromQL evaluation

Using the retrieved series and samples, the PromQL engine evaluates the query. There are two modes of running queries:

* **Instant queries** – Evaluated at a single timestamp
* **Range queries** – Evaluated at regular intervals over a defined time range

## Common Causes of Slow Queries and Optimization Techniques

Several factors influence the latency and resource usage of PromQL queries. This section highlights the key contributors and practical strategies for improving performance.

### Query Cardinality

High cardinality increases the number of time series that must be scanned and evaluated.

#### Recommendations

* Eliminate unnecessary labels from metrics.
* Use selective label matchers to reduce the number of series returned.

### Number of samples processed

The number of samples fetched impacts both memory usage and CPU time for decoding and processing.

#### Recommendations

Until downsampling is implemented, reducing the scrape interval can help lower the amount of samples to be processed. But this comes at the cost of reduced resolution.

### Number of evaluation steps

The number of evaluation steps for a range query is computed as:

```
num of steps = 1 + (end - start) / step
```

**Example:** A 24-hour query with a 1-minute step results in 1,441 evaluation steps.

#### Recommendations

Grafana can automatically set the step size based on the time range. If a query is slow, manually increasing the step parameter can reduce computational overhead.

### Time range of the query

Wider time ranges amplify the effects of cardinality, sample volume, and evaluation steps.

#### Recommendations

* Use shorter time ranges (e.g., 1h) in dashboards.
* Default to instant queries during metric exploration to reduce load.

### Query Complexity

Subqueries, nested expressions, and advanced functions may lead to substantial CPU consumption.

#### Recommendations

* Simplify complex expressions where feasible.

### Regular Expressions

While Prometheus has optimized regex matching, such queries remain CPU-intensive.

#### Recommendations

* Avoid regex matchers in high-frequency queries.
* Where possible, use equality matchers instead.

### Query Result Size

Queries returning large datasets (>100MB) can incur significant serialization and network transfer costs.

#### Example

```
pod_container_info #No aggregation
sum by (pod) (rate(container_cpu_seconds_total[1m])) # High cardinality result
```

#### Recommendations

* Scoping the query using additional label matchers reduces result size and improves performance.

## Summary

The key optimization techniques are:

* Use selective label matchers to limit cardinality.
* Increase the step value in long-range queries.
* Simplify complex or nested PromQL expressions.
* Avoid regex matchers unless strictly necessary.
* Favor instant queries for interactive use cases.
* Scope queries to minimize the result size.

