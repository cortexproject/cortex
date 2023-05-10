---
title: "Ruler HA"
linkTitle: "Ruler HA"
weight: 1
slug: ruler-ha
---

- Author: [Soon-Ping Phang](https://github.com/soonping-amzn)
- Date: June 2022
- Status: Proposed
---

## Introduction

This proposal consolidates multiple existing PRs from the AWS team working on this feature, as well as future work needed to complete support.  The hope is that a more holistic view will make for more productive discussion and review of the individual changes, as well as provide better tracking of overall progress.

The original issue is [#4435](https://github.com/cortexproject/cortex/issues/4435).

## Problem

Rulers in Cortex currently run with a replication factor of 1, wherein each rule group is assigned to exactly 1 ruler.  This lack of redundancy creates the following risks:

- Rule group evaluation
  - Missed evaluations due to a ruler outage, possibly caused by a deployment, noisy neighbour, hardware failure, etc.
  - Missed evaluations due to a ruler brownout due to other tenant rule groups sharing the same ruler (noisy neighbour)
- API
  -inconsistent API results during resharding (e.g. due to a deployment) when rulers are in a transition state loading rule groups

This proposal attempts to mitigate the above risks by enabling a ruler replication factor of greater than 1, allowing multiple rulers to evaluate the same rule group — effectively, the ruler equivalent of ingester HA already supported in Cortex.  To avoid unnecessary duplication of work between replicas, we will use an elected replica strategy (again, similar to HA sample tracking) to ensure only one actual evaluation occurs for each iteration.

## Proposal

### Make ReplicationFactor configurable

ReplicationFactor in Ruler is currently hardcoded to 1.  Making this a configurable parameter is the first step to enabling HA in ruler, and would also be the mechanism for the user to turn the feature on.  The parameter value will be 1 by default, equating to the feature being turned off by default.

A replication factor greater than 1 will result in the same rules being available for evaluation by multiple ruler instances.

This redundancy will allow for missed or skipped rule evaluations from single ruler outages to be covered by other instances able to evaluate the same rule groups.

[PR #4712](https://github.com/cortexproject/cortex/pull/4712) [open]

### Elected ruler replicas for single iteration evaluation

In order to ensure that at each iteration, a rule group is evaluated only once, we use an elected replica strategy similar to that used for HA sample tracking, using a KV store to coordinate between replicas.

As in the existing ruler implementation, each ruler will have a set of rule groups self-assigned to itself, and each rule group will have a GoLang ticker triggering evaluations.  Assuming a replication factor of 3, for a given rule group G, there would be 3 rulers with the same ticker for G.  Upon each tick, each ruler will check if it is the elected leader for G, and if it is, will proceed to evaluate G.  Rulers that are not elected leaders will ignore their ticks.

The leader election algorithm itself will be identical to that used by distributor HA tracker, but with cluster being replaced by the unique rule group identifier (namespace/rulegroupName), and with the ruler pod ID serving as the replica identifier.

#### Why not have replicated evaluation?

We tested replicated concurrent evaluation of rule groups, and observed duplication of the following:

- recording rule metrics (which ingestion deduplication took care of thanks to [slotted intervals](https://github.com/prometheus/prometheus/blob/b878527151e6503d24ac5b667b86e8794eb79ff7/rules/manager.go#L509))
- alerts (which could be worked around by syncing alert states, but adds complexity)
- rule group evaluation logging
- operational metrics

Apart from the increased resources and cost of redundant evaluations, duplicated logging and operational metrics, in particular, becomes difficult to monitor and alert on, given that single errors may no longer signal actual failures from the tenants' perspective.

#### KV store data

| key          | value        |
|--------------|--------------|
| ${userID}/${namespace}/${groupName} | ReplicaDesc with Replica == ruler pod ID |

#### Configuration changes

Since we will be essentially copying distributor's HA tracker implementation, we will add the same configuration parameters to ruler:

```
ha_tracker:
  # Explicitly enable the ruler HA tracker to track ruler replica leadership
  # state. This is useful when migrating from replication_factor=1 to
  # replication_factor>1, as part of a 2-phase deployment where an admin
  # enables HA tracking with replication_factor=1 as the first step, followed
  # by increasing ruler replication in the second step.
  # Note that this parameter defaults to true if ruler.ring.replication_factor > 1.
  # CLI flag: -ruler.ha-tracker.enable
  [enable_ha_tracker: <boolean> | default = false]

  # Update the timestamp in the KV store for a given cluster/replica only after
  # this amount of time has passed since the current stored timestamp.
  # CLI flag: -ruler.ha-tracker.update-timeout
  [ha_tracker_update_timeout: <duration> | default = 15s]

  # Maximum jitter applied to the update timeout, in order to spread the HA
  # heartbeats over time.
  # CLI flag: -ruler.ha-tracker.update-timeout-jitter-max
  [ha_tracker_update_timeout_jitter_max: <duration> | default = 5s]

  # If we don't receive any samples from the accepted replica for a cluster in
  # this amount of time we will failover to the next replica we receive a sample
  # from. This value must be greater than the update timeout
  # CLI flag: -ruler.ha-tracker.failover-timeout
  [ha_tracker_failover_timeout: <duration> | default = 30s]

  # Backend storage to use for the ring. Please be aware that memberlist is not
  # supported by the HA tracker since gossip propagation is too slow for HA
  # purposes.
  kvstore:
    # Backend storage to use for the ring. Supported values are: consul, etcd,
    # inmemory, memberlist, multi.
    # CLI flag: -ruler.ha-tracker.store
    [store: <string> | default = "consul"]

    # The prefix for the keys in the store. Should end with a /.
    # CLI flag: -ruler.ha-tracker.prefix
    [prefix: <string> | default = "ruler.ha-tracker/"]

    dynamodb:
      # Region to access dynamodb.
      # CLI flag: -ruler.ha-tracker.dynamodb.region
      [region: <string> | default = ""]

      # Table name to use on dynamodb.
      # CLI flag: -ruler.ha-tracker.dynamodb.table-name
      [table_name: <string> | default = ""]

      # Time to expire items on dynamodb.
      # CLI flag: -ruler.ha-tracker.dynamodb.ttl-time
      [ttl: <duration> | default = 0s]

    # The consul_config configures the consul client.
    # The CLI flags prefix for this block config is: ruler.ha-tracker
    [consul: <consul_config>]

    # The etcd_config configures the etcd client.
    # The CLI flags prefix for this block config is: ruler.ha-tracker
    [etcd: <etcd_config>]

    multi:
      # Primary backend storage used by multi-client.
      # CLI flag: -ruler.ha-tracker.multi.primary
      [primary: <string> | default = ""]

      # Secondary backend storage used by multi-client.
      # CLI flag: -ruler.ha-tracker.multi.secondary
      [secondary: <string> | default = ""]

      # Mirror writes to secondary store.
      # CLI flag: -ruler.ha-tracker.multi.mirror-enabled
      [mirror_enabled: <boolean> | default = false]

      # Timeout for storing value to secondary store.
      # CLI flag: -ruler.ha-tracker.multi.mirror-timeout
      [mirror_timeout: <duration> | default = 2s]
```

#### Technical implementation notes

Implementation will leverage a new generalized Prometheus ruler callback introduced in PR [#11885](https://github.com/prometheus/prometheus/pull/11885) to perform leader checks and evaluation skipping, and will use a modified version of distributor.haTracker for leader election logic.

### Weak and Strong Quorum in ListRules and ListAlerts

ListRules/ListAlerts will return inconsistent responses while a new configuration propagates to multiple ruler HA instances. For most users, this is an acceptable side-effect of an eventually consistent HA architecture. However, some use-cases have stronger consistency requirements and are willing to sacrifice some availability for those use-cases. For example, an alerts client might want to verify that a change has propagated to a majority of instances before informing the user that the update succeeded. To enable this, we propose adding an optional quorum API parameter with the following behaviour:

- quorum=weak (default)
  - Biased towards availability, ListRules will perform a best-effort merge of the results from at least $i = tenantShardSize - replicationFactor + 1$ instances. If a rule group definition does not satisfy a quorum of $q = \lfloor{replicationFactor / 2}\rfloor + 1$ copies, it will choose the most recently evaluated version of that rule group for the final result set. An error response will be returned only if all instances return an error.  Note that a side-effect of this rule is that the API will return a result even if all but one ruler instances in the tenant's shard is unavailable.

- quorum=strong
  - Biased towards consistency, ListRules will query at least $i = tenantShardSize - \lfloor{replicationFactor / 2}\rfloor + 1$ instances. If any rule group does not satisfy a quorum of $q = \lfloor{replicationFactor / 2}\rfloor + 1$ copies, a 503 error will be returned.

[PR #4768](https://github.com/cortexproject/cortex/pull/4768) [open]

#### Alternatives to a quorum API parameter

##### Weak or strong quorum by default

Another option for making ListRules work in HA mode is to implement one of the quorum rules (weak or strong) as the default, with no control to select between the two, outside of maybe a configuration parameter.  AWS itself runs multitenant Cortex instances, and we have an internal use-case for the strong quorum implementation, but we do not want to impose the subsequent availability hit on our customers, particularly given that replication_factor is not currently a tenant-specific parameter in Cortex, for ingesters, alert manager, or ruler.

Making HA availability the default, while giving users the choice to knowingly request for more consistency at the cost of more error-handling seems like a good balance.
