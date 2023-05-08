---
title: "Sharding timeseries by partitions"
linkTitle: "Sharding timeseries by partitions"
weight: 1
slug: shard-by-partitions
---

- Author: [Harry John](https://github.com/harry671003)
- Date: May 2023
- Status: Proposed
---

## Introduction

Cortex currently supports two sharding strategies - default and shuffle sharding. 
Shuffle sharding was introduced with the goal of reducing the blast radius and to provide better tenant isolation. However, the current implementation of shuffle sharding in Cortex cannot tolerate any two nodes in the cluster going down at the same time.

If two nodes in the cluster goes down at the same time, the cluster will start returning errors for writes and reads. The likely hood of two nodes going down in a cluster increases with the size of cluster. This proposal provides an alternative for implementing shuffle sharding which can tolerate multiple nodes in the cluster being down at the same time.

## Problem

The underlying problem in the current sharding strategy lies in how the series are assigned to ingesters. Consider the scenario of assigning 1 million active series of a tenant with a shard size of 30. The current implementation picks a sub-ring of size 30. Then with shard_by_all_labels enabled, each series is then hashed and mapped to three ingesters among the 30.

![Problem](/images/proposals/sharding-by-partitions-problem.png)

Each of the 30 ingesters would now have 100K series assigned to them. The series in each of the ingesters would be very distinct from each other. Now, if we randomly pick two ingesters out of the 30, there will always be at-least one time series which will not have 2 ingester replicas available. This is the reason behind the remote write errors.

## Solution

The fundamental problem behind unavailability is the sharding scheme used here. Each series is hashed an assigned to a different ingesters resulting in 1 million shards. If we can decrease the total number of shards, this problem can be solved.
Let us consider the same scenario of assigning 1 million active series to 30 ingesters. Instead of assigning each time series directly to ingesters, the 1 million series will be split into 10 partitions of size 100K. Each partition can then be assigned to three ingesters.

![Solution](/images/proposals/sharding-by-partitions-solution.png)

Now the condition for availability will change to every partition should have at-least two replicas available. The likelihood of the two random ingesters of the tenant being down having the same partitions on them is much less than before.


### Probability of unavailability when two ingesters are down

If two ingesters are down in the cluster, what is probability of two replicas of any partition being unavailable can be expressed by the following formulae. This formulae was derived by Mr. Johney George a retired mathematics professor.

![Formulae](/images/proposals/sharding-by-partitions-formulae.png)

n - Number of nodes
p - Number of partitions

The following graph plots how the probability of unavailability changes with number of partitions in a cluster with 1000 nodes.

![Graph](/images/proposals/sharding-by-partitions-graph.png)

Sharding series by all labels is a special case of this, where number of partitions is 1 million. Beyond 10^5 partitions, there is 100% probability of unavailability if two ingesters are down.

### Splitting series into partitions

A new sharding mechanism is required to calculate the number of partitions and split series to partitions.

To calculate the number of partitions, we can use the following:
- `num_partitions = CEIL(max_active_series_per_user / partitions_size)`

To split series into partitions there are two options::

* Modulo hashing - `hash (labels) % num_partitions`
* Rendezvous hashing - See: https://en.wikipedia.org/wiki/Rendezvous_hashing

The recommendation is to use Rendezvous hashing because when the number of partitions changes, the required resharding is minimal similar to consistent hashing. The implementation of rendezvous hashing is much simpler and doesn’t need a ring to operate.

### Assign partitions to ingester

Partitions can be assigned to 3 distinct ingesters using the ingester ring. This will work similar to how three replicas are picked for a given time series.

### Write Path

Each distributors can independently determine to which partition a given time series will belong to. Distributor can also determine to which ingester a partition is assigned to. Based on this, remote_write requests can be split and sent to the correct ingesters. Write path will only fail if at-least two replicas of any partition is not available.

### Read Path availability

Each querier can independently determine the partitions of a tenant and to which ingester each partition belong to. The queries will still fan out to all ingester. However, the query will only fail if querying ingesters containing two replicas of the same partition fails.

## Conclusion

Changing the sharding strategy to shard by partitions, would improve the write and read availability in large clusters.
