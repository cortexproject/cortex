---
title: "Synchronize Namespace On-Demand"
linkTitle: "Synchronize Namespace On-Demand"
weight: 1
slug: sync-namespace
---

- Author: [Anand Rajagopal](https://github.com/rajagopalanand)
- Date: Jul 2023
- Status: Proposed
---

## Background

Currently, when a user creates a new namespace or updates rule groups within a namespace, the Ruler saves the rule group files and returns a 202 status. A period synchronization process within each Ruler subsequently loads all the rule groups for all the users, determines ownership, and synchronizes the rules to Prometheus for evaluation. However, this method is somewhat slow,  as users must wait a full minute for the update, even in namespaces with limited number of rule groups

## Proposal

### Introduce a new SyncNamespace API that would facilitate on-demand synchronization of a namespace

The SyncNamespace API would perform the following actions:

1. The Ruler that receives the call determines which Rulers need to be notified to sync/reload the namespace. This can be accomplished by:
   1. Querying the ring for the Rulers that can service the user’s namespaces. For instance, when shuffle sharding is utilized, calling ring.ShuffleShard(userID, shardSize), would return the sub-ring for the user
   2. Listing all the rule groups for that namespace by calling ListRuleGroupsForUserAndNamespace
   3. Further filtering the Rulers it needs to notify by asking the sub-ring for Rulers for each rule group from the previous step
2. The Ruler notifies all the other Rulers from the previous step to sync/reload the namespace
3. The Rulers that were notified to sync/reload the namespace would:
   1. List all the rule groups for that namespace by calling ListRuleGroupsForUserAndNamespace
   2. Filter for the rule groups it owns
   3. Loads the rule groups from storage by invoking `LoadRuleGroups`
   4. Synchronize the rule groups to Prometheus. While syncing a specific namespace, the Ruler can read the rest of the namespace files from local file system and update Prometheus with a combined list of all the namespace files. This is necessary to prevent Prometheus from stopping/removing rule groups from namespaces that are not being updated. A change will also need to be made to prevent Cortex from removing other users from the Ruler
   5. As an alternative to previous step, we could propose a change to Prometheus to enable updating of a single namespace. At present, Prometheus manager in rules package has an `Update` method that not only refreshes the rule groups but also removes rule groups not present in the specified list of files. This is why Cortex must provide all the files for a user to Prometheus. Introduction of a method to update a single namespace without affecting other namespaces would be advantageous

![SyncNamespace On-Demand](/website/static/images/proposals/sync-namespace-on-demand.png)

### API

POST `/api/v1/rules/{namespace}/sync`

### Scenarios:

#### Competing SyncNamespace Calls:

When a single Ruler receives multiple SyncNamespace calls, the Ruler will block while updating the local files and synchronizing with Prometheus. This should prevent multiple threads from potentially corrupting rule files


#### Simultaneous SyncNamespace and Periodic Sync:

A new buffered channel will be created on the Ruler and the existing syncRules function has a for/select clause. A new case statement will be added to the existing select clause. This will prevent SyncNamespace and period sync from running concurrently


#### New pods scaling up during the sync and the Ring has not yet been updated:

In this situation, the new Ruler pod will sync all the rule groups for all the users it is responsible for at the start up. Eventually, the Ring will be updated, triggering a separate sync which could cause resharding.


#### Concurrent SyncNamespace and DeleteRuleGroup/DeleteNamespace Calls:

This should be fine as current sync might catch some of the rule groups that are deleted and either the next on-demand sync or the regular period sync will catch the rest of deletes
