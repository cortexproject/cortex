---
title: "Ruler HA"
linkTitle: "Ruler HA"
weight: 1
slug: ruler-ha
---

- Author: [Anand Rajagopal](https://github.com/rajagopalanand)
- Date: April 2024
- Status: Proposed
---

## Problem

Rulers in Cortex currently run with a replication factor of 1, wherein each RuleGroup is assigned to exactly 1 ruler.  This lack of redundancy creates the following risks:

- Rule group evaluation
    - Missed evaluations due to a ruler outage, possibly caused by a deployment, noisy neighbour, hardware failure, etc.
    - Missed evaluations due to a ruler brownout due to other tenant rule groups sharing the same ruler (noisy neighbour)
- API
  - inconsistent API results during resharding (e.g. due to a deployment) when rulers are in a transition state loading rule groups

This proposal attempts to mitigate the above risks by enabling a ruler replication factor of greater than 1, allowing multiple rulers to evaluate the same rule group — effectively.

## Proposal

### Make ReplicationFactor configurable

ReplicationFactor in Ruler is currently hardcoded to 1.  Making this a configurable parameter is the first step to enabling HA in ruler, and would also be the mechanism for the user to turn the feature on.  The parameter value will be 1 by default, equating to the feature being turned off by default.

A replication factor greater than 1 will result in multiple rulers loading the same rule groups but only one ruler evaluating the rule group. The replicas are in "passive" state until it is necessary for them to become active

This redundancy will allow for missed rule evaluations from single ruler outages to be covered by other instances evaluating the same rule groups.

To avoid inconsistent rule group state, which is maintained by Prometheus, the author proposes making a change in Prometheus rule group evaluation logic as described below

### Prometheus change

The author proposes making a change to Prometheus to allow for pausing and resuming (or activating and deactivating) a rule group as described [here](https://github.com/prometheus/prometheus/issues/13630)

If the proposal is not accepted by Prometheus community, the proposal is to maintain a fork of Prometheus for Cortex with modified rule group evaluation behavior. This [draft PR](https://github.com/prometheus/prometheus/pull/13858)
shows the changes required in Prometheus to support pausing and resuming a rule group evaluation

### API HA

An interim solution is addressed in this [#5773](https://github.com/cortexproject/cortex/issues/5773) PR. This will be modified such that the replicas will return both active and passive rule groups and the API handler will continue to de-duplicate the results.
The difference is that after Ruler HA, the replicas could potentially return proper rule group state if those replicas evaluated the rule group

PRs:

* Prometheus PR [#13858](https://github.com/prometheus/prometheus/pull/13858) [draft]
* For API HA [#5773](https://github.com/cortexproject/cortex/issues/5773)
