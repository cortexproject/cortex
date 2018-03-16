# Design

In this document, we first describe the system at a high level.
Then, we introduce constraints and invariants to reify the problem domain.
We move methodically toward a concrete solution, describing key components and behaviors along the way.

## Producers and consumers

We have a large and dynamic set of producers emitting a stream of log records.
Those records should be made available for searching by a consumer.

```
     +-----------+
P -> |           |
P -> |     ?     | -> C
P -> |           |
     +-----------+
```

The producers are concerned with getting their records consumed and persisted as quickly as possible.
In case of failure, some use cases prefer backpressure (e.g. event logs) and others buffering and dropping (e.g. application logs).
But in both cases, the immediate receiving component should be optimized for fast sequential writes.

The consumers are concerned with getting responses to their queries as quickly and accurately as possible.
Because we've defined queries to require time bounds, we claim we can solve the problem with grep over time-partitioned data files.
So the final format of records on disk should be a global merge of all producer streams, partitioned by time.

```
     +-------------------+
P -> | R                 |
P -> | R     ?     R R R | -> C
P -> | R                 |
     +-------------------+
```

## Operational details

We will have many producers, order of thousands.
(A producer for us is the application process, plus a forwarding agent.)
Our log system will necessarily be smaller than the production system it is serving.
So we will have multiple ingesters, and each one will need to handle writes from multiple producers.

Also, we want to serve production systems that generate a lot of log data.
Therefore, we won't make reductive assumptions about data volume.
We assume even a minimum working set of log data will be too large for a single node's storage.
Therefore, consumers will necessarily have to query multiple nodes for results.
This implies the final, time-partitioned data set will be distributed and replicated.

```
          +---+           +---+
P -> F -> | I |           | Q | --.
P -> F -> |   |           +---+   |
          +---+           +---+   '->
          +---+     ?     | Q | ----> C
P -> F -> | I |           +---+   .->
P -> F -> |   |           +---+   |
P -> F -> |   |           | Q | --'
          +---+           +---+
```

We have now introduced distribution, which means we must address coördination.

## Coördination

Coördination is the death of distributed systems.
Our log system will be coördination-free.
Let's see what that requires at each stage.

Producers -- or, more accurately, forwarders -- need to be able to connect to any ingester instance and begin emitting records.
Those records should be persisted directly to that ingester's disk, with as little intermediary processing as possible.
If an ingester dies, its forwarders should be able to simply reconnect to other instances and resume.
(During the transition, they may provide backpressure, buffer, or drop records, according to their configuration.)
This is all to say forwarders must not require knowledge about which is the "correct" ingester.
Any ingester must be equally viable.

As an optimization, highly-loaded ingesters can shed load (connections) to other ingesters.
Ingesters will gossip load information between each other, like number of connections, iops, etc.
Then, highly-loaded ingesters can refuse new connections, and thus redirect forwarders to more-lightly-loaded peers.
Extremely highly-loaded ingesters can even terminate existing connections, if necessary.
This needs to be carefully managed to prevent inadvertant denial of service.
For example, no more than a handful of ingesters should be refusing connections at a given time.

Consumers need to be able to make queries without any prior knowledge about time partitions, replica allocation, etc.
Without that knowledge, this implies the queries will always be scattered to each query node, gathered, and deduplicated.
Query nodes may die at any time, or start up and be empty, so query operations must manage partial results gracefully.

As an optimization, consumers can perform read-repair.
A query should return N copies of each matching record, where N is the replication factor.
Any records with fewer than N copies returned may be under-replicated.
A new segment with the suspect records can be created and replicated to the cluster.
As a further optimization, a separate process can perform sequential queries of the timespace, for the explicit purpose of read repair.

The transfer of data between the ingest and query tier also needs care.
Ideally, any ingest node should be able to transfer segments to any/all query nodes arbitrarily.
That transfer should accomplish work that makes forward progress in the system as a whole.
And we must recover gracefully from failure; for example, network partitions at any stage of the transaction.

Let's now look at how to shuffle data safely from the ingest tier to the query tier.

## Ingest segments

Ingesters receive N independent streams of records, from N forwarders.
Each record shall be prefixed by the ingester with a time UUID, a [ULID](https://github.com/oklog/ulid).
It's important that each record have a timestamp with reasonable precision, to create some global order.
But it's not important that the clocks are globally synced, or that the records are e.g. strictly linearizable.
Also, it's fine if records that arrive in the same minimum time window to appear out-of-order, as long as that order is stable.

Incoming records are written to a so-called active segment, which is a file on disk.

```
          +---+
P -> F -> | I | -> Active: R R R...
P -> F -> |   |
P -> F -> |   |
          +---+
```

Once it has written B bytes, or been active for S seconds, the active segment is flushed to disk.

```
          +---+
P -> F -> | I | -> Active:  R R R...
P -> F -> |   |    Flushed: R R R R R R R R R
P -> F -> |   |    Flushed: R R R R R R R R
          +---+
```

The ingester consumes records serially from each forwarder connection.
The next record is consumed when the current record is successfully written to the active segment.
And the active segment is normally synced once it is flushed.
This is the default durability mode, tentative called fast.

Producers can optionally connect to a separate port, whose handler will sync the active segment after each record is written.
This provides stronger durability, at the expense of throughput.
This is a separate durability mode, tentatively called durable.

There can be a third, even higher durability mode, tentatively called bulk.
Forwarders may write entire segment files at once to the ingester.
Each segment file would only be acknowledged when it's been successfully replicated among the storage nodes.
Then, the forwarder is free to send the next complete segment.

Ingesters host an API which serves flushed segments.

- GET /next — returns the oldest flushed segment and marks it pending
- POST /commit?id=ID — deletes a pending segment
- POST /failed?id=ID — returns a pending segment to flushed

Segment state is controlled by file extension, and we leverage the filesystem for atomic renames.
The states are .active, .flushed, or .pending, and there is only ever a single active segment at a time per connected forwarder.

```
          +---+                     
P -> F -> | I | Active              +---+
P -> F -> |   | Active              | Q | --.
          |   |  Flushed            +---+   |        
          +---+                     +---+   '->
          +---+              ?      | Q | ----> C
P -> F -> | I | Active              +---+   .->
P -> F -> |   | Active              +---+   |
P -> F -> |   | Active              | Q | --'
          |   |  Flushed            +---+
          |   |  Flushed
          +---+
```

Observe that ingesters are stateful, so they need a graceful shutdown process.
First, they should terminate connections and close listeners.
Then, they should wait for all flushed segments to be consumed.
Finally, they may be shut down.

### Consume segments

The ingesters act as a sort of queue, buffering records to disk in groups called segments.
While it is protected against e.g. power failure, ultimately, we consider that storage ephemeral.
Segments should be replicated as quickly as possible by the query tier.
Here, we take a page from the Prometheus playbook.
Rather than the ingesters pushing flushed segments to query nodes, the query nodes pull flushed segments from the ingesters.
This enables a coherent model to scale for throughput.
To accept a higher ingest rate, add more ingest nodes, with faster disks.
If the ingest nodes are backing up, add more query nodes to consume from them.

Consumption is modeled with a 3-stage transaction.
The first stage is the read stage.
Each query node regularly asks each ingest node for its oldest flushed segment, via GET /next.
(This can be pure random selection, round-robin, or some more sophisticated algorithm. For now it is pure random.)
Received segments are read record-by-record, and merged into another composite segment.
This process repeats, consuming multiple segments from the ingest tier, and merging them into the same composite segment.

Once the composite segment has reached B bytes, or been active for S seconds, it is closed, and we enter the replication stage.
Replication means writing the composite segment to N distinct query nodes, where N is the replication factor.
At the moment we just POST the segment to a replication endpoint on N random store nodes.

Once the segment is confirmed replicated on N nodes, we enter the commit stage.
The query node commits the original segments on all of the ingest nodes, via POST /commit.
If the composite segment fails to replicate for any reason, the query node fails all of the original segments, via POST /failed.
In either case, the transaction is now complete, and the query node can begin its loop again.

```
Q1        I1  I2  I3
--        --  --  --
|-Next--->|   |   |
|-Next------->|   |
|-Next----------->|
|<-S1-----|   |   |
|<-S2---------|   |
|<-S3-------------|
|
|--.
|  | S1∪S2∪S3 = S4     Q2  Q3
|<-'                   --  --
|-S4------------------>|   |
|-S4---------------------->|
|<-OK------------------|   |
|<-OK----------------------|
|
|         I1  I2  I3
|         --  --  --
|-Commit->|   |   |
|-Commit----->|   |
|-Commit--------->|
|<-OK-----|   |   |
|<-OK---------|   |
|<-OK-------------|
```

Let's consider failure at each stage.

If the query node fails during the read stage, pending segments are in limbo until a timeout elapses.
After which they are made available for consumption by another query node.
If the original query node is dead forever, no problem.
If the original query node comes back, it may still have records that have already been consumed and replicated by other nodes.
In which case, duplicate records will be written to the query tier, and one or more final commits will fail.
If this happens, it's OK: the records are over-replicated, but they will be deduplicated at read time, and eventually compacted.
So commit failures should be noted, but can be safely ignored.

If the query node fails during the replication stage, it's a similar story.
Assuming the node doesn't come back, pending ingest segments will timeout and be retried by other nodes.
And if the node does come back, replication will proceed without failure, and one or more final commits will fail.
Same as before, this is OK: over-replicated records will be deduplicated at read time, and eventually compacted.

If the query node fails during the commit stage, one or more ingest segments will be stuck in pending and, again, eventually time out back to flushed.
Same as before, records will become over-replicated, deduplicated at read time, and eventually compacted.

## Node failure

If an ingest node fails permanently, any records in its segments are lost.
To protect against this class of failure, clients should use the bulk ingest mode.
That won't allow progress until the segment file is replicated into the storage tier.

If a store node fails permanently, records are safe on (replication factor - 1) other nodes.
But to get the lost records back up to the desired replication factor, read repair is required.
A special timespace walking process can be run to perform this recovery.
It can essentially query all logs from the beginning of time, and perform read repair on the underreplicated records.

## Query index

All queries are time-bounded, and segments are written in time-order.
But an additional index is necessary to map a time range to matching segments.
Whenever a query node writes a segment for any reason, it shall read the first and last timestamp (time UUID) from that segment.
It can then update an in-memory index, associating that segment to that time range.
An interval tree is a good data structure here.

An alternative approach is to name each file as FROM-TO, with both FROM and TO as smallest and largest time UUID in the file.
Then, given a query time range of T1–T2, segments can be selected with two simple comparisons to detect time range overlap.
Arrange two ranges (A, B) (C, D) so that A <= B, C <= D, and A <= C.
In our case, the first range is the range specified by the query, and the second range is for a given segment file.
Then, the ranges overlap, and we should query the segment file, if B >= C.

```
A--B         B >= C?
  C--D           yes 
  
A--B         B >= C?
     C--D         no
  
A-----B      B >= C?
  C-D            yes
  
A-B          B >= C?
C----D           yes
```

This gives us a way to select segment files for querying.

## Compaction

Compaction serves two purposes: deduplication of records, and de-overlapping of segments.
Duplication of records can occur during failures e.g. network partitions.
But segments will regularly and naturally overlap.

Consider 3 overlapping segment files on a given query node.

```
t0             t1
+-------+       |
|   A   |       |
+-------+       |
|  +---------+  |
|  |    B    |  |
|  +---------+  |
|     +---------+
|     |    C    |
|     +---------+
```

Compaction will first merge these overlapping segments into a single aggregate segment in memory.
During the merge, duplicate records can be detected by ULID and dropped.
Then, compaction will split the aggregate segment to achieve desired file sizes, and produce new, non-overlapping segments.

```
t0             t1
+-------+-------+
|       |       |
|   D   |   E   |
|       |       |
+-------+-------+
```

Compaction reduces the number of segments necessary to read in order to serve queries for a given time.
In the ideal state, each time will map to exactly 1 segment.
This helps query performance by reducing the number of reads.

Observe that compaction improves query performance, but doesn't affect either correctness or space utilization.
Compression can be applied to segments completely orthogonally to the process described here.
Proper compression can dramatically increase retention, at the cost of some CPU burn.
It may also prevent processing of segment files with common UNIX tooling like grep, though this may or may not be important.

Since records are individually addressable, read-time deduplication occurs on a per-record basis.
So the mapping of record to segment can be optimized completely independently by each node, without coördination.

The schedule and aggressiveness of compaction is an important performance consideration.
At the moment, a single compactor thread (goroutine) performs each of the compaction tasks sequentially and perpetually.
It fires at most once per second.
Much more performance analysis and real-world study is necessary here.

## Querying

Each query node hosts a query API, GET /query.
Queries may be sent to any query node API by users.
Upon receipt, the query is broadcast to every node in the query tier.
Responses are gathered, results are merged and deduplicated, and then returned to the user.

The actual grepping work is done by each query node individually.
First, segment files matching the time boundaries of the query are identified.
Then, a per-segment reader is attached to each file, and filters matching records from the file.
Finally, a merging reader takes results from each segment file, orders them, and returns them to the originating query node.
This pipeline is lazily constructed of io.ReadClosers, and costs paid when reads actually occur.
That is, when the HTTP response is written to the originating query node.

Note that the per-segment reader is launched in its own goroutine, and reading/filtering occurs concurrently.
Currently there is no fixed limit on the number of active goroutines allowed to read segment files.
This should be improved.

The query request has several fields.

- From, To time.Time — bounds of query
- Q string — term to grep for, blank is OK and matches all records
- Regex bool — if true, compile and match Q as a regex
- StatsOnly bool — if true, just return stats, without actual results

The query response has several fields.

- NodeCount int — how many query nodes were queried
- SegmentCount int — how many segments were read
- Size int — file size of segments read to produce results
- Results io.Reader — merged and time-ordered results

StatsOnly can be used to "explore" and iterate on a query, until it's been narrowed down to a usable result set.

# Component model

This is a working draft of the components of the system.

## Processes

### forward

- ./my_application | forward ingest.mycorp.local:7651
- Should accept multiple ingest host:ports
- Should contain logic to demux a DNS record to individual instances
- Should contain failover logic in case of connection interruption
- Can choose between fast, durable, or chunked writes
- Post-MVP: more sophisticated (HTTP?) forward/ingest protocol; record enhancement

### ingest

- Receives writes from multiple forwarders
- Each record is defined to be newline \n delimited
- Prefixes each record with time UUID upon ingestion
- Appends to active segment
- Flushes active segment to disk at size and time bounds
- Serves segment API to storage tier, polling semantics
- Gossips (via memberlist) with ingest peer instances to share load stats
- Post-MVP: load spreading/shedding; streaming transmission of segments to storage tier

### store

- Polls ingest tier for flushed segments
- Merges ingest segments together
- Replicates merged segments to other store nodes
- Serves query API to clients
- Performs compaction on some interval
- Post-MVP: streaming collection of segments from ingest tier; more advanced query use-cases

## Libraries

### Ingest Log

- Abstraction for segments in the ingest tier
- Operations include create new active segment, flush, mark as pending, commit
- (I've got a reasonable prototype for this one)
- Note that this is effectively a disk-backed queue, short-term durable storage

### Store Log

- Abstraction for segments in the storage tier
- Operations include collect segments, merge, replicate, compact
- Note that this is meant to be long-term durable storage

### Cluster

- Abstraction to get various nodes talking to each other
- Not much data is necessary to share, just node identity and health
- HashiCorp's memberlist fits the bill
