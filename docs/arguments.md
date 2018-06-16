# Cortex Arguments Explained

## Distributor

- `-distributor.shard-by-metric-name`

   In the original Cortex design, samples were sharded amongst distributors by the combination of (userid, metric name).  Sharding by metric name was designed to reduce the number of ingesters you need to hit on the read path; the downside was that you could hotspot the write path.

   In hindsight, this seems like the wrong choice: we do many orders of magnitude more writes than reads, and ingester reads are in-memory and cheap. It seems the right thing to do is to use all the labels to shard, improving load balancing and support for very high cardinality metrics.

   Set this flag to `false` for the new behaviour.

   **Upgrade notes**: As part of the change which introduced this flag also makes all queries always read from all ingesters, the upgrade path is pretty trivial; just enable the flag (and the disable it later). When you do enable it, you'll see a spike in the number of active series as the writes are "reshuffled" amongst the ingesters, but over the new stale period all the old series will be flushed, and you should end up with much better load balancing. Reads will always catch all the data from all ingesters from this change onwards.
