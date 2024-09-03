---
title: "Ruler High Availability"
linkTitle: "Ruler High Availability"
weight: 1
slug: ruler-high-availability
---

- Author: [Anand Rajagopal](https://github.com/rajagopalanand)
- Date: Aug 2024
- Status: Proposed
---

## Problem

Rulers in Cortex currently run with a replication factor of 1, wherein each RuleGroup is assigned to exactly 1 ruler.  This lack of redundancy creates the following risks:

- Rule group evaluation
    - Missed evaluations due to a ruler outage, possibly caused by a deployment, noisy neighbour, hardware failure, etc.
    - Missed evaluations due to a ruler brownout due to other tenant rule groups sharing the same ruler (noisy neighbour)
- API
  - Inconsistent API results during resharding (e.g. due to a deployment) when rulers are in a transition state loading rule groups

This proposal attempts to mitigate the above risks by enabling a ruler replication factor of greater than 1, allowing multiple rulers to evaluate the same rule group — effectively.

## Proposal

### Make ReplicationFactor configurable

ReplicationFactor in Ruler is currently hardcoded to 1.  Making this a configurable parameter is the first step to enabling HA in ruler.  The parameter value will be 1 by default. To enable Ruler HA for rule group evaluation, a new flag will be created

A replication factor greater than 1 will result in the following

 - Ring will pick R rulers for a rule group where R=RF
 - The primary ruler (R1), when active, will take ownership of the rule group
 - Non-primary ruler R2 will check if R1 is active. If R1 is not active, R2 will take ownership of the rule group
 - Non-primary ruler R3 (if RF=3) will check if R1 and R2 are active. If they are both inactive/unhealthy, then R3 will take owership of the rule group
 - Non-primary rulers will drop their ownership when R1 becomes active after an outage

With this redundancy, the maximum duration of missed evaluations will be limited to the sync interval of the rule groups, reducing the impact of primary Ruler unavailability.

### Prometheus change

No Prometheus change is required for this proposal

### API HA

An interim solution is addressed in this [#5773](https://github.com/cortexproject/cortex/issues/5773) PR. This will be modified such that the replicas will return both active and passive rule groups and the API handler will continue to de-duplicate the results.
The difference is that after Ruler HA, the replicas could potentially return proper rule group state if those replicas evaluated the rule group

PRs:

* For Rule evaluation [#6129](https://github.com/cortexproject/cortex/pull/6129)
* For API HA [#5773](https://github.com/cortexproject/cortex/issues/5773)
