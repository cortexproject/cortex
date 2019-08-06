# Ingester Hand-over

The [ingester](architecture.md#ingester) holds several hours of sample
data in memory. When we want to shut down an ingester, either for
software version update or to drain a node for maintenance, this data
must not be discarded.

Each ingester goes through different states in its lifecycle. When
working normally, the state is `ACTIVE`.

On start-up, an ingester first goes into state `PENDING`. After a
short time, if nothing happens, it adds itself to the ring and goes
into state ACTIVE.

A running ingester is notified to shut down by Unix signal
`SIGINT`. On receipt of this signal it goes into state `LEAVING` and
looks for an ingester in state `PENDING`. If it finds one, that
ingester goes into state `JOINING` and the leaver transfers all its
in-memory data over to the joiner. On successful transfer the leaver
removes itself from the ring and exits and the joiner changes to
`ACTIVE`, taking over ownership of the leaver's
[ring tokens](architecture.md#hashing).

If a leaving ingester does not find a pending ingester, it will flush
all of its chunks to the backing database, then remove itself from the
ring and exit. This may take tens of minutes to complete.

During hand-over, neither the leaving nor joining ingesters will
accept new samples. Distributors are aware of this, and "spill" the
samples to the next ingester in the ring. This creates a set of extra
"spilled" chunks which will idle out and flush after hand-over is
complete. The sudden increase in flush queue can be alarming!

The following metrics can be used to observe this process:

 - `cortex_member_ring_tokens_owned` - how many tokens each ingester thinks it owns
 - `cortex_ring_tokens_owned` - how many tokens each ingester is seen to own by other components
 - `cortex_ring_member_ownership_percent` same as `cortex_ring_tokens_owned` but expressed as a percentage
 - `cortex_ring_members` - how many ingesters can be seen in each state, by other components
 - `cortex_ingester_sent_chunks` - number of chunks sent by leaving ingester
 - `cortex_ingester_received_chunks` - number of chunks received by joining ingester
 
You can see the current state of the ring via http browser request to
`/ring` on a distributor.
