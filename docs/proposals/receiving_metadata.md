---
title: "Support metadata API"
linkTitle: "Support metadata API"
weight: 1
slug: support-metadata-api
---

- Author: @gotjosh
- Reviewers: @gouthamve, @pracucci
- Date: March 2020
- Status: Accepted

## Problem Statement
Prometheus holds metric metadata alongside the contents of a scrape. This metadata (`HELP`, `TYPE`, `UNIT` and `METRIC_NAME`) enables [some Prometheus API](https://github.com/prometheus/prometheus/issues/6395) endpoints to output the metadata for integrations (e.g. [Grafana](https://github.com/grafana/grafana/pull/21124)) to consume it.

At the moment of writing, Cortex does not support the `api/v1/metadata` endpoint that Prometheus implements as metadata was never propagated via remote write. Recent [work is done in Prometheus](https://github.com/prometheus/prometheus/pull/6815/files) enables the propagation of metadata.

With this in place, remote write integrations such as Cortex can now receive this data and implement the API endpoint. This results in Cortex users being able to enjoy a tiny bit more insight on their metrics.

## Potential Solutions
Before we delve into the solutions, let's set a baseline about how the data is received. This applies almost equally for the two.

Metadata from Prometheus is sent in the same [`WriteRequest` proto message](https://github.com/prometheus/prometheus/blob/master/prompb/remote.proto) that the samples use. It is part of a different field (#3 given #2 is already [used interally](https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex.proto#L36)), the data is a set identified by the metric name - that means it is aggregated across targets, and is sent all at once. Implying, Cortex will receive a single `WriteRequest` containing a set of the metadata for that instance at an specified interval.

. It is also important to note that this current process is an intermediary step. Eventually, metadata in a request will be sent alongside samples and only for those included. The solutions proposed, take this nuance into account to avoid coupling between the current and future state of Prometheus, and hopefully do something now that also works for the future.

As a reference, these are some key numbers regarding the size (and send timings) of the data at hand from our clusters at Grafana Labs:

- On average, metadata (a combination of `HELP`, `TYPE`, `UNIT` and `METRIC_NAME`) is ~55 bytes uncompressed.
- at GL, on an instance with about 2.6M active series, we hold ~1241 unique metrics in total.
- with that, we can assume that on a worst-case scenario the metadata set for that instance is ~68 kilobytes uncompressed.
- by default, this data is only propagated once every minute (aligning with the default scrape interval), but this can be adjusted.
- Finally, what this gives us is a baseline worst-case scenario formula for the data to store per tenant: `~68KB * Replication Factor * # of Instances`. Keeping in mind that typically, there's a very high overlap of metadata across instances, and we plan to deduplicate in the ingesters.

### Write Path

1. Store the metadata directly from the distributors into a cache (e.g. Memcached)

Since metadata is received all at once, we could directly store into an external cache using the tenant ID as a key, and still, avoid a read-modify-write.  However, a very common use case of Cortex is to have multiple Prometheus sending data for the same tenant ID. This complicates things, as it adds a need to have an intermediary merging phase and thus making a read-modify-write inevitable.

2. Keep metadata in memory within the ingesters

Similarly to what we do with sample data, we can keep the metadata in-memory in the ingesters and apply similar semantics. I propose to use the tenant ID as a hash key, distribute it to the ingesters (taking into account the replication factor), using a hash map to keep a set of the metadata across all instances for a single tenant, and implement a configurable time-based purge process to deal with metadata churn. Given, we need to ensure fair-use we also propose implementing limits for both the number of metadata entries we can receive and the size of a single entry.

### Read Path

In my eyes, the read path seems to only have one option. At the moment of writing, Cortex uses a [`DummyTargetRetriever`](https://github.com/cortexproject/cortex/blob/master/pkg/querier/dummy.go#L11-L20) as a way to signal that these API endpoints are not implemented. We'd need to modify the Prometheus interface to support a `Context` and extract the tenant ID from there. Then, use the tenant ID to query the ingesters for the data, deduplicate it and serve it.

## Conclusions

I conclude that solution #2 is ideal for this work on the write path. It allows us to use similar semantics to samples, thus reducing operational complexity, and lays a groundwork for when we start receiving metadata alongside samples.

There's one last piece to address: Allowing metadata to survive rolling restarts. Option #1 handles this well, given the aim would be to use an external cache such as Memcached. Option #2 lacks this, as it does not include any plans to persist this data. Given Prometheus (by default) sends metadata every minute, and we don't need a high level of consistency. We expect that an eventual consistency of up to 1 minute on the default case is deemed acceptable.

## References
- [Prometheus Propagate metadata via Remote Write Design Doc](https://docs.google.com/document/d/1LoCWPAIIbGSq59NG3ZYyvkeNb8Ymz28PUKbg_yhAzvE/edit#)
- [Prometheus Propagate metadata via Remote Write Design Issue](https://github.com/prometheus/prometheus/issues/6395)
