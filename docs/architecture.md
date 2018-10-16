# Cortex Architecture

*NB this document is a work-in-progress.*

The Cortex architecture consists of multiple, horizontally scalable microservices.  Each microservice uses the most appropriate technique for horizontal scaling; most are stateless and can handle requests for any users, and some (the ingesters) are semi-stateful and depend on consistent hashing.

For more details on the Cortex architecture, you should read / watch:
- The original design doc "[Project Frankenstein: A multi tenant, scale out Prometheus](https://docs.google.com/document/d/1C7yhMnb1x2sfeoe45f4mnnKConvroWhJ8KQZwIHJOuw/edit#heading=h.nimsq29kl184)"
- PromCon 2016 Talk: "[Multitenant, Scale-Out Prometheus](https://promcon.io/2016-berlin/talks/multitenant-scale-out-prometheus/)"
- KubeCon Prometheus Day talk "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" [slides](http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service) [video](https://www.youtube.com/watch?v=9Uctgnazfwk)
- PromCon 2017 Talk: "[Cortex: Prometheus as a Service, One Year On](https://promcon.io/2017-munich/talks/cortex-prometheus-as-a-service-one-year-on/)"
- CNCF TOC Presentation; "Horizontally Scalable, Multi-tenant Prometheus" [slides](https://docs.google.com/presentation/d/190oIFgujktVYxWZLhLYN4q8p9dtQYoe4sxHgn4deBSI/edit#slide=id.g3b8e2d6f7e_0_6)

## Write Path

Writes for Cortex come in via the Prometheus remote write API; that is, as snappy compressed protobuffer encoded batches of samples inside the body of a HTTP PUT request.
A HTTP header containing the tenant ID is required; Cortex relies on an external reverse proxy for authentication and authorisation.

### The Distributor

The first job writes hit is the Distributor; it is responsible for distributing samples to Ingesters for scalability, and replication them amongst Ingesters for availability.
A write batch will contains samples for multiple series.
Cortex ensures samples for a given series all end up on the same (sub) set of Ingesters.
You can configure how big the replication set for series is by setting the replication factor on the Distributor.
To ensure consistent query results, Cortex used Dynamo-style quorum consistency on reads and writes; that means the distributor will wait for a positive response of at least half plus one of the Ingesters it send the sample to before responding to the user.

For scalability, the Distributor will pick the replication set of Ingesters for a given series based on a consistent hash of the series metric name and tenant ID.
This was originally chosen as the input to the hash to reduce the number of Ingesters that are needed on the query path, with the trade off of less even write load on the Ingesters.
Optionally, the Distributor can use all the series' labels as an input to the hash; this results in better load balancing on the write path, but requires all Ingesters are consulted for every query.

A consistent hash ring is stored in Consul in a single key value pair, with the ring data structure also encoded as a Protobuffer.
The consistent hash ring consists of a list of tokens and Ingesters; hashed values are looked up in the ring, and the replication set is built for the closest unique Ingesters by token.
A consistent hash ring is used so that adding and removing Ingesters only results in 1/N of the series being moved.

The Distributors communicate with the Ingesters via a gRPC interface.
All Distributors share access to the same ring and write requests can be sent to any Distributor.
The Distributors are stateless, and can be scaled up and down as needed.
A Kubernetes service should be used to load balance requests randomly to the Distributors.
For high availability you need at least two Distributors.

### The Ingester

Ingesters are shared-nothing, semi-stateful "write de-amplification" machines that hold the most recent 12 hours worth of samples for some set of series.
Shared-nothing means Ingesters do not coordinate or communicate with each other under normal operation (the exception is on rolling updates, when the transfer their in memory data from the leave Ingester to the Joining one).
Semi-stateful mean the Ingesters store state - the last 12 hours worth of samples - and that  care is needed when restarting and upgrading them to avoid loosing that data.
"Semi" implies that they are not designed to be a long-term store of data though; the Chunk Store is for that.
"Write de-amplification" means their main role is to batch and compress samples for the same series and flush them out to the chunk store; in normal operation there should be _many_ orders of magniture fewer writes QPS to the chunk store than to the ingesters.
It is from this behaviour that Cortex derives its low total cost of ownership (TCO).

### Transfers and Rolling Upgrades

TODO

## Query Path

### Query Frontend

The query frontend is an optional job which accepts HTTP requests and queues them by tenant ID, retrying them on errors. This allow for the occasional large query which would otherwise cause a querier OOM, allowing us to over-provision querier parallelism. Also, it prevents multiple large requests from being convoyed on a single querier by distributing them FIFO across all queriers. And finally, it prevent a single tenant from DoSing other tenants by fairly scheduling queries between tenants.

The query frontend also splits multi-day queries into multiple single-day queries, executing these queries in parallel on downstream queriers, and stitching the results back together again.  This prevents large, multi-day queries from OOMing a single querier, and helps them execute faster.

Finally, the query frontend also caches query results and reuse the on subsequent queries.  If the cached results are incomplete, the query frontend will calculate the required sub queries and execute them in parallel on downstream queries.  The query frontend can optionally align queries with their step parameter, to improve the cacheability of the query results.

The query frontend job accepts gRPC streaming requests from the queriers, which then "pull" requests from the frontend. For HA it is recommended you run multiple frontends - the queriers will connect to (and pull requests from) all of them. To get the benefit of the fair scheduling, it is recommended you run fewer frontends than queriers - two should suffice.

See the document "[Cortex Query Woes](https://docs.google.com/document/d/1lsvSkv0tiAMPQv-V8vI2LZ8f4i9JuTRsuPI_i-XcAqY)" for more details design discussion. In the future, query splitting, query alignment and query results caching will be added to the frontend.

The query frontend is completely optional - you can continue to use the queriers directly.  If you want to use the query frontend, direct incoming authenticated traffic at them and set the `-querier.frontend-address` flag on the queriers.

### Queriers

The queriers handled the actual PromQL evaluation.  They embed the chunk store client code for fetching data from long-term storage, and communicate with the Ingesters for more recent data.

## Chunk Store

The Chunk Store is the long term store of Cortex data.
It is designed to support interactive querying and sustained writing without the need to background maintenance tasks.
The Chunk Store is not a separate job or process, but instead a library embedded in jobs which need to access the data; the Ingester, the Querier and the Ruler.

The Chunk Store consists of an index and a KV store of the chunks themselves.
The index can be backed by AWS DynamoDB, GCP Bigtable or Cassandra.
A abstraction layer exists in Cortex to unify the interface to these NOSQL stores; the interface assumes an index is a collection of entries keyed by a hash key and a range key.
The hash key is required for all reads and writes; the range key is required for writes, but can be omitted for reads or queried by prefix or range.
For DynamoDB, index entries are modelled directly as DynamoDB entries, we the hash key as the distribution key and the range key as the range key.
For BigTable, index entries are modelled as individual column values; the hash key becomes and row key and the range key the column key.
A similar model is used for Cassandra.

A set of schemas are used to map the matchers and label sets used on reads and writes to the chunk store into appropriate operations on the index.
Schemas have been added as Cortex has evolved, mainly in an attempt to better load balance writes and improve query performance.
The current recommendation is the v6 schema.
