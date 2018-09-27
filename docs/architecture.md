# Cortex Architecture

*NB this document is a work-in-progress.*

The Cortex architecture consists of multiple, horizontally scalable microservices.  Each microservice uses the most appropriate technique for horizontal scaling; most are stateless and can handle requests for any users, and some (the ingesters) are semi-stateful and depend on consistent hashing.

For more details on the Cortex architecture, you should read / watch:
- The original design doc "[Project Frankenstein: A multi tenant, scale out Prometheus](https://docs.google.com/document/d/1C7yhMnb1x2sfeoe45f4mnnKConvroWhJ8KQZwIHJOuw/edit#heading=h.nimsq29kl184)"
- PromCon 2016 Talk: "[Multitenant, Scale-Out Prometheus](https://promcon.io/2016-berlin/talks/multitenant-scale-out-prometheus/)"
- KubeCon Prometheus Day talk "Weave Cortex: Multi-tenant, horizontally scalable Prometheus as a Service" [slides](http://www.slideshare.net/weaveworks/weave-cortex-multitenant-horizontally-scalable-prometheus-as-a-service) [video](https://www.youtube.com/watch?v=9Uctgnazfwk)
- PromCon 2017 Talk: "[Cortex: Prometheus as a Service, One Year On](https://promcon.io/2017-munich/talks/cortex-prometheus-as-a-service-one-year-on/)"
- CNCF TOC Presentation; "Horizontally Scalable, Multi-tenant Prometheus" [slides](https://docs.google.com/presentation/d/190oIFgujktVYxWZLhLYN4q8p9dtQYoe4sxHgn4deBSI/edit#slide=id.g3b8e2d6f7e_0_6)

## Query Path

### Query Frontend

The query frontend is an optional job which accepts HTTP requests and queues them by tenant ID, retrying them on errors. This allow for the occasional large query which would otherwise cause a querier OOM, allowing us to over-provision querier parallelism. Also, it prevents multiple large requests from being convoyed on a single querier by distributing them FIFO across all queriers. And finally, it prevent a single tenant from DoSing other tenants by fairly scheduling queries between tenants.

The query frontend will also split multi-day queries into multiple single-day queries, executing these queries in parallel on downstream queriers, and stitching the results back together again.  This prevents large, multi-day queries from OOMing a single querier, and helps them execute faster.

Finally, the query frontend will also cache query results and reuse the on subsequent queries.  If the cached results are incomplete, the query frontend will calculate the required queries and execute them in parallel on downstream queries.  The query frontend can optionally align queries with their step parameter, to improve the cacheability of the query results.

The query frontend job accepts gRPC streaming requests from the queriers, which then "pull" requests from the frontend. For HA it is recommended you run multiple frontends - the queriers will connect to (and pull requests from) all of them. To get the benefit of the fair scheduling, it is recommended you run fewer frontends than queriers - two should suffice.

See the document "[Cortex Query Woes](https://docs.google.com/document/d/1lsvSkv0tiAMPQv-V8vI2LZ8f4i9JuTRsuPI_i-XcAqY)" for more details design discussion. In the future, query splitting, query alignment and query results caching will be added to the frontend.

The query frontend is completely optional - you can continue to use the queriers directly.  If you want to use the query frontend, direct incoming authenticated traffic at them and set the `-querier.frontend-address` flag on the queriers.

### Queriers

The queriers handled the actual PromQL evaluation.  They embed the chunk store client code for fetching data from long-term storage, and communicate with the ingesters for more recent data.
