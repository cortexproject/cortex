# A First Look at Distributed Query Execution in Cortex

> One of the persistent challenges in Cortex has been dealing with resource contention in a single querier node, which occurs when too much data is pulled. While implementing safeguards through limits has been effective as a temporary solution, we needed to address this issue at its root to allow Cortex to process more data efficiently. This is where distributed query execution comes into play, offering a solution that both expands query limits and improves efficiency through parallel processing and result aggregation.

## Background

### Current Query Splitting Strategies

In the current system, Cortex supports two main splitting strategies: time sharding and vertical sharding by string-rewriting. Time sharding allows us to split queries by time periods, such as dividing a 7-day query into seven 1-day requests. Vertical sharding, on the other hand, works with shard-by patterns. While both methods help reduce the load on individual querier nodes, they come with significant limitations when it comes to query scaling. Time-sharded queries can still fail if they have high cardinality within a single day, and vertical sharding is restricted to specific patterns while still being vulnerable to complexity issues. The current query splitting approaches, while useful, remain constrained at the query-level. For example, even with thanos-engine enabled, Cortex can reach its processing limits on high-cardinality queries spanning just 6 hours or even 15 minutes.

## Distributed Query Execution

Our new distributed query execution approach takes a more sophisticated route by breaking queries down to executable fragments at the expression level. Unlike traditional query-level processing where results are merged only at the query frontend, this fine-grained approach enables dynamic collaboration between execution units during runtime. Each fragment operates independently while maintaining the ability to exchange partial results with other units as needed. This enhanced granularity not only increases opportunities for parallel processing but also enables deeper query optimization. The system ultimately combines these distributed results to produce the final output, achieving better resource utilization and performance compared to conventional query splitting strategies.

![Comparison between previous query splitting strategy v.s. distributed query execution](/images/blog/2025/distributed-exec-splittingStrat.png)

## New Changes
![(Previous Cortex query path v.s. New distributed execution query path)](/images/blog/2025/distributed-exec-queryPath.png)

### Query Frontend: Distributed Query Middleware

The Query Frontend has been enhanced with a new distributed query middleware that fundamentally changes how queries are processed. At its core, this middleware first parses incoming PromQL query expressions into a tree of operators, creating a logical plan that represents the query's structure. The newly implemented Distributed Optimizer then analyzes this logical plan to identify opportunities for parallel execution, strategically inserting "remote nodes" to mark where execution can be distributed across separate queriers. The current optimization strategy primarily focuses on binary expressions, which are common in monitoring scenarios, such as for success rates.

### Query Scheduler: Fragment Coordination

The Query Scheduler implements a sophisticated coordination mechanism that orchestrates the distributed execution of query fragments. It performs a bottom-up traversal of the logical plan, identifying cut points at remote nodes to ensure proper execution order of child-to-root. This approach guarantees that child fragments are enqueued and processed before their parent fragments, maintaining data dependency requirements and ensure there are not too many idle querier.

### Querier: Child-root Execution

The Querier component has undergone significant changes to support distributed execution while maintaining compatibility with the existing Thanos engine. When processing a logical plan, the querier traverses the tree of operators bottom-up, performing necessary data manipulations along the way. For each operator, it first calls series() to understand the shape of incoming data by examining labels, followed by next() calls to fetch the actual data.

The introduction of remote nodes brings a new dimension to this process. When a querier encounters a remote node, instead of fetching data from local operators, it invokes a remote execution operator. This operator uses a new gRPC interface to stream both series() and next() data from other querier processes at target addresses.
![Previous query execution v.s. Distributed execution pull-based model)](/images/blog/2025/distributed-exec-pullBasedModel.png)

## Results

Distributed query execution has demonstrated significant improvements in query processing by effectively reducing resource contention. This enhancement is achieved by distributing query workloads across multiple queriers, effectively increasing the practical memory limit for high-cardinality queries that previously failed due to memory constraints.

Enabling this feature is straightforward, requiring only a single configuration flag in your Cortex setup:

```yaml
querier:
  distributed_exec_enabled: true
```

Real-World Example: SLO Calculations

To understand the practical benefits, let's examine how distributed execution handles Service Level Objective (SLO) calculations. Consider a typical SLO query that involves dividing two sum aggregations: sum(errors) / sum(total). In traditional execution, both sum operations would be processed on a single querier node, potentially causing memory pressure and performance bottlenecks.

With distributed execution, the query is automatically split into separate components. The first sum operation runs on one querier while the second sum executes on another. Each querier processes its portion independently, working with a smaller dataset and requiring less memory. Once both calculations complete, the results are combined for the final division operation. This distributed approach significantly reduces the memory footprint on individual queriers and enables parallel processing.

## Future Work
While the current implementation of distributed query execution already offers benefits, it represents just the beginning of Cortex's optimization journey. To fully realize its potential, several key enhancements are needed:

### Enhance Distributed Optimizer
Distributed optimizer currently support binary expressions, but in the future it should manage more complex operations, including complicated join functions and advanced query patterns.

### Storage Layer Sharding
Implementing more sophisticated storage sharding strategies can better distribute data across the cluster. For example, allow max(A) to be split to max(A[shard=0]) and max(A[shard=1]). Initial experiments with ingester storage have already demonstrated impressive results: binary expressions show 1.7-2.8x performance improvements, while multiple binary expressions achieve up to 5x speed.While not included in the initial release, we invite contributors to continue to develop these sharding capabilities for both ingestor and store-gateway components.
![single binary expressions latency comparison](/images/blog/2025/distributed-exec-binaryExprLatencies.png)
![multiple binary expressions latency comparison](/images/blog/2025/distributed-exec-multipleBinary.png)


### Cardinality Estimation
Building on storage sharding, we hope to implement intelligent cardinality estimation to dynamically determine optimal split ratios, moving beyond the simple binary split approach. This enhancement will ensure queries are divided into appropriately sized chunks based on their cardinality, guaranteeing that each querier can efficiently process its assigned shards. The result will be a truly scalable system where queries can grow indefinitely in complexity while maintaining consistent performance, as they will always be automatically split into manageable, executable sizes.

This distributed query execution framework establishes a strong foundation for future development. With the core architecture in place, we can more easily implement additional optimizations and features. These planned improvements will work in concert to create a robust, scalable system that can handle growing demands while delivering reliable, high-performance query processing.