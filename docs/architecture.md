# Cortex Architecture

> This document is a work in progress. Check back for updates in the coming weeks.

Cortex consists of multiple horizontally scalable microservices. Each microservice uses the most appropriate technique for horizontal scaling; most are stateless and can handle requests for any users while some (namely the [ingesters](#ingester)) are semi-stateful and depend on consistent hashing. This document provides a basic overview of Cortex's architecture.

## The role of Prometheus

Prometheus instances scrape samples from various targets and then push them to Cortex (using Prometheus' [remote storage API](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)). Cortex handles the incoming samples using the [distributor](#distributor).

## Services

Cortex has a fundamentally service-based architecture, in which the overall system is split up into a variety of components that perform specific tasks and run separately (and potentially in parallel).

### Distributor

The **distributor** service is responsible for handling samples written by Prometheus. It's essentially the "first stop" in the write path for Prometheus samples, *unless* you run an optional [query frontend](#query-frontend), in which case the distributor takes writes from the query frontend rather than from Prometheus instances directly.

Once the distributor receives samples from Prometheus, it splits them into chunks, replicates them (based on a configurable replication factor), and then sends them to the [ingester](#ingester) service.

Distributors use consistent hashing, in conjunction with the (configurable) replication factor, to determine *which* instance of the ingester service receives each sample.

### Ingester

The **ingester** service is responsible for writing sample data to long-term storage backends (DynamoDB, S3, Cassandra, etc.). Each instance of the ingester service receives a subset of the total samples 

### Querier

The **querier** service handles the actual [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/) evaluation.

It embeds the chunk store client code for fetching data from long-term storage and communicates with [ingesters](#ingester) for more recent data.

### Ruler

The **ruler** service is responsible for handling alerts produced by [Alertmanager](https://prometheus.io/docs/alerting/alertmanager/).

### Query frontend

The **query frontend** is an optional service that accepts HTTP requests, queues them by tenant ID, and retries in case of errors.

> As mentioned [above](#distributor), the query frontend is completely optional; you can use queriers directly. To use the query frontend, direct incoming authenticated traffic at them and set the `-querier.frontend-address` flag on the queriers.

#### Queueing

Queuing performs a number of functions for the query frontend:

* It ensures that any large queries don't case an out-of-memory (OOM) error in the querier. This allows administrators to over-provision querier parallelism.
* It prevents multiple large requests from being convoyed on a single querier by distributing them first-in/first-out (FIFO) across all queriers.
* It prevents a single tenant from denial-of-service-ing (DoSing) other tenants by fairly scheduling queries between tenants.

#### Splitting

The query frontend splits multi-day queries into multiple single-day queries, executing these queries in parallel on downstream queriers and stitching the results back together again. This prevents large, multi-day queries from OOMing a single querier and helps them execute faster.

#### Caching

The query frontend caches query results and reuses them on subsequent queries. If the cached results are incomplete, the query frontend calculates the required queries and executes them in parallel on downstream queries. The query frontend can optionally align queries with their step parameter to improve the cacheability of the query results.

#### Parallelism

The query frontend job accepts gRPC streaming requests from the queriers, which then "pull" requests from the frontend. For high availability it's recommended that you run multiple frontends; the queriers will connect to---and pull requests from---all of them. To reap the benefit of fair scheduling, it is recommended that you run fewer frontends than queriers. Two should suffice in most cases.

## See also

For more details on Cortex's architecture, you should read/watch:

- "[Project Frankenstein: A multi tenant, scale out Prometheus](https://docs.google.com/document/d/1C7yhMnb1x2sfeoe45f4mnnKConvroWhJ8KQZwIHJOuw/edit#heading=h.nimsq29kl184)" (the original design doc)
- "[Multitenant, Scale-Out Prometheus](https://promcon.io/2016-berlin/talks/multitenant-scale-out-prometheus/)" (PromCon 2016 talk)
- "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" (KubeCon Prometheus Day talk) [slides](http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service) [video](https://www.youtube.com/watch?v=9Uctgnazfwk)
- "[Cortex: Prometheus as a Service, One Year On](https://promcon.io/2017-munich/talks/cortex-prometheus-as-a-service-one-year-on/)" (PromCon 2017 Talk)
- "Horizontally Scalable, Multi-tenant Prometheus" [slides](https://docs.google.com/presentation/d/190oIFgujktVYxWZLhLYN4q8p9dtQYoe4sxHgn4deBSI/edit#slide=id.g3b8e2d6f7e_0_6) (CNCF TOC Presentation)
- [Cortex Query Woes](https://docs.google.com/document/d/1lsvSkv0tiAMPQv-V8vI2LZ8f4i9JuTRsuPI_i-XcAqY) provides details on Cortex design considerations.

## Roadmap

In the future, query splitting, query alignment, and query result caching will be added to the frontend.