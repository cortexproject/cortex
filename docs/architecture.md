# Cortex Architecture

> This document is a work in progress. Check back for updates in the coming weeks.

Cortex consists of multiple horizontally scalable microservices. Each microservice uses the most appropriate technique for horizontal scaling; most are stateless and can handle requests for any users while some (namely the [ingesters](#ingester)) are semi-stateful and depend on consistent hashing. This document provides a basic overview of Cortex's architecture.

<p align="center"><img src="architecture.png" alt="Cortex Architecture"></p>

## The role of Prometheus

Prometheus instances scrape samples from various targets and then push them to Cortex (using Prometheus' [remote write API](https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations)). That remote write API emits batched [Snappy](https://google.github.io/snappy/)-compressed [Protocol Buffer](https://developers.google.com/protocol-buffers/) messages inside the body of an HTTP `PUT` request.

Cortex requires that each HTTP request bear a header specifying a tenant ID for the request. Request authentication and authorization are handled by an external reverse proxy.

Incoming samples (writes from Prometheus) are handled by the [distributor](#distributor) while incoming reads (PromQL queries) are handled by the [query frontend](#query-frontend).

## Services

Cortex has a service-based architecture, in which the overall system is split up into a variety of components that perform specific tasks and run separately (and potentially in parallel).

### Distributor

The **distributor** service is responsible for handling samples written by Prometheus. It's essentially the "first stop" in the write path for Prometheus samples. Once the distributor receives samples from Prometheus, it splits them into batches and then sends them to multiple [ingesters](#ingester) in parallel.

Distributors communicate with ingesters via [gRPC](https://grpc.io). They are stateless and can be scaled up and down as needed.

#### Hashing

Distributors use consistent hashing, in conjunction with the (configurable) replication factor, to determine *which* instances of the ingester service receive each sample.

The hash itself is based on one of two schemes:

1. The metric name and tenant ID
2. All the series labels and tenant ID

The trade-off associated with the latter is that writes are more balanced but they must involve every ingester in each query.

> This hashing scheme was originally chosen to reduce the number of required ingesters on the query path. The trade-off, however, is that the write load on the ingesters is less even.

#### The hash ring

A consistent hash ring is stored in [Consul](https://www.consul.io/) as a single key-value pair, with the ring data structure also encoded as a [Protobuf](https://developers.google.com/protocol-buffers/) message. The consistent hash ring consists of a list of tokens and ingesters. Hashed values are looked up in the ring; the replication set is built for the closest unique ingesters by token. One of the benefits of this system is that adding and remove ingesters results in only 1/_N_ of the series being moved (where _N_ is the number of ingesters).

#### Quorum consistency

All distributors share access to the same hash ring, which means that write requests can be sent to any distributor.

To ensure consistent query results, Cortex uses [Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)-style quorum consistency on reads and writes. This means that the distributor will wait for a positive response of at least one half plus one of the ingesters to send the sample to before responding to the user.

#### Load balancing across distributors

We recommend randomly load balancing write requests across distributor instances, ideally by running the distributors as a Kubernetes [Service](https://kubernetes.io/docs/concepts/services-networking/service/).

### Ingester

The **ingester** service is responsible for writing sample data to long-term storage backends (DynamoDB, S3, Cassandra, etc.).

#### Shared-nothing processes

Ingesters are, for the most part, shared-nothing processes. They don't coordinate or communicate with each other *under normal operation*. The exception is on rolling updates, during which ingesters transfer their in-memory data from the ingester exiting the group to the ingester joining the group.

#### State

Ingesters are semi-stateful in that they always retain the last 12 hours worth of samples. When restarting or upgrading ingesters, care must be taken to avoid losing that data.

As *semi*-stateful processes, ingesters are *not* designed to be long-term data stores. In Cortex, that role is played by the [chunk store](#chunk-store).

#### Write de-amplification

Ingesters store the last 12 hours worth of samples in order to perform **write de-amplification**, i.e. batching and compressing samples for the same series and flushing them out to the [chunk store](#chunk-store). Under normal operations, there should be *many* orders of magnitude fewer queries per second (QPS) worth of writes to the chunk store than to the ingesters.

Write de-amplification is the main source of Cortex's low total cost of ownership (TCO).

### Ruler

The **ruler** service is responsible for handling alerts produced by [Alertmanager](https://prometheus.io/docs/alerting/alertmanager/).

### Query frontend

The **query frontend** is an optional service that accepts HTTP requests, queues them by tenant ID, and retries in case of errors.

> The query frontend is completely optional; you can use queriers directly. To use the query frontend, direct incoming authenticated reads at them and set the `-querier.frontend-address` flag on the queriers.

#### Queueing

Queuing performs a number of functions for the query frontend:

* It ensures that large queries that cause an out-of-memory (OOM) error in the querier will be retried. This allows administrators to under-provision memory for queries, or optimistically run more small queries in parallel, which helps to reduce TCO.
* It prevents multiple large requests from being convoyed on a single querier by distributing them first-in/first-out (FIFO) across all queriers.
* It prevents a single tenant from denial-of-service-ing (DoSing) other tenants by fairly scheduling queries between tenants.

#### Splitting

The query frontend splits multi-day queries into multiple single-day queries, executing these queries in parallel on downstream queriers and stitching the results back together again. This prevents large, multi-day queries from OOMing a single querier and helps them execute faster.

#### Caching

The query frontend caches query results and reuses them on subsequent queries. If the cached results are incomplete, the query frontend calculates the required subqueries and executes them in parallel on downstream queriers. The query frontend can optionally align queries with their step parameter to improve the cacheability of the query results.

#### Parallelism

The query frontend job accepts gRPC streaming requests from the queriers, which then "pull" requests from the frontend. For high availability it's recommended that you run multiple frontends; the queriers will connect to—and pull requests from—all of them. To reap the benefit of fair scheduling, it is recommended that you run fewer frontends than queriers. Two should suffice in most cases.

### Querier

The **querier** service handles the actual [PromQL](https://prometheus.io/docs/prometheus/latest/querying/basics/) evaluation of samples stored in long-term storage.

It embeds the chunk store client code for fetching data from long-term storage and communicates with [ingesters](#ingester) for more recent data.

## Chunk store

The **chunk store** is Cortex's long-term data store, designed to support interactive querying and sustained writing without the need for background maintenance tasks. It consists of:

* An index for the chunks. This index can be backed by [DynamoDB from Amazon Web Services](https://aws.amazon.com/dynamodb), [Bigtable from Google Cloud Platform](https://cloud.google.com/bigtable), [Apache Cassandra](https://cassandra.apache.org), or [Amazon S3](https://aws.amazon.com/s3).
* A key-value (KV) store for the chunk data itself

> Unlike the other core components of Cortex, the chunk store is not a separate service, job, or process, but rather a library embedded in the three services that need to access Cortex data: the [ingester](#ingester), [querier](#querier), and [ruler](#ruler).

The chunk store relies on a unified interface to the "[NoSQL](https://en.wikipedia.org/wiki/NoSQL)" stores—DynamoDB, Bigtable, and Cassandra—that can be used to back the chunk store index. This interface assumes that the index is a collection of entries keyed by:

* A **hash key**. This is required for *all* reads and writes.
* A **range key**. This is required for writes and can be omitted for reads, which can be queried by prefix or range.

The interface works somewhat differently across the supported databases:

* DynamoDB supports range and hash keys natively. Index entries are thus modelled directly as DynamoDB entries, with the hash key as the distribution key and the range as the range key.
* For Bigtable and Cassandra, index entries are modelled as individual column values. The hash key becomes the row key and the range key becomes the column key.

A set of schemas are used to map the matchers and label sets used on reads and writes to the chunk store into appropriate operations on the index. Schemas have been added as Cortex has evolved, mainly in an attempt to better load balance writes and improve query performance.

> The current schema recommendation is the **v10 schema**.