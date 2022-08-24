---
title: "Ingesters rolling updates"
linkTitle: "Ingesters rolling updates"
weight: 10
slug: ingesters-rolling-updates
---

Cortex [ingesters](architecture.md#ingester) are semi-stateful.
A running ingester holds several hours of time series data in memory, before they're flushed to the long-term storage.
When an ingester shutdowns, because of a rolling update or maintenance, the in-memory data must not be discarded in order to avoid any data loss.

The Cortex [blocks storage](../blocks-storage/_index.md) requires ingesters to run with a persistent disk where the TSDB WAL and blocks are stored (eg. a StatefulSet when deployed on Kubernetes).

During a rolling update, the leaving ingester closes the open TSDBs, synchronize the data to disk (`fsync`) and releases the disk resources.
The new ingester, which is expected to reuse the same disk of the leaving one, will replay the TSDB WAL on startup in order to load back in memory the time series that have not been compacted into a block yet.
