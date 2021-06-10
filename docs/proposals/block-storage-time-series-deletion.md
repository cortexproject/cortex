---
title: "Time Series Deletion from Blocks Storage"
linkTitle: "Time Series Deletion from Blocks Storage"
weight: 1
slug: block-storage-time-series-deletion
---

- Author: [Ilan Gofman](https://github.com/ilangofman)
- Date: June 2021
- Status: Proposal

## Problem

Currently, Cortex only implements a time series deletion API for chunk storage.  We present a design for implementing time series deletion with block storage. We would like to have the same API for deleting series as currently implemented in Prometheus and in Cortex with chunk storage. 


This can be very important for users to have as confidential or accidental data might have been incorrectly pushed and needs to be removed. 

## Related Works

As previously mentioned, the deletion feature is already implemented with chunk storage. The main functionality is implemented through the purger service. It accepts requests for deletion and processes them.  At first, when a deletion request is made, a tombstone is created. This is used to filter out the data for queries. After some time, a deletion plan is executed where the data is permanently removed from chunk storage. 

Can find more info here:

- [Cortex documentation for chunk store deletion](https://cortexmetrics.io/docs/guides/deleting-series/)
- [Chunk deletion proposal](https://docs.google.com/document/d/1PeKwP3aGo3xVrR-2qJdoFdzTJxT8FcAbLm2ew_6UQyQ/edit)



## Background

With a block-storage configuration, Cortex stores data that could be potentially deleted by a user in:

- Object store (GCS, S3, etc..) for long term storage of blocks
- Ingestors for more recent data that should be eventually transferred to the object store
- Cache 
    - Index cache
    - Metadata cache
    - Chunks cache (stores the potentially to be deleted data) 
    - Query results cache  (stores the potentially to be deleted data) 
    - Compactor during the compaction process
    - Store-gateway


## Proposal

The deletion will not happen right away. Initially, the data will be filtered out from queries using tombstones and will be deleted afterward. This will allow the user some time to cancel the delete request. 

### API Endpoints

The existing purger service will be used to process the incoming requests for deletion. The API will follow the same structure as the chunk storage endpoints for deletion, which is also based on the Prometheus deletion API. 

This will enable the following endpoints for Cortex when using block storage:

`POST /api/v1/admin/tsdb/delete_series` - Accepts [Prometheus style delete request](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series)  for deleting series.

Parameters:

- `start=<rfc3339 | unix_timestamp>`
    - Optional. If not provided, will be set to minimum possible time. 
- `end=<rfc3339 | unix_timestamp> `
    - Optional. If not provided, will be set to maximum possible time (time when request was made). End time cannot be greater than the current UTC time. 
- `match[]=<series_selector>`
    - Cannot be empty, must contain at least one label matcher argument. 


`POST /api/v1/admin/tsdb/cancel_delete_request`  - To cancel a request if it has not been processed yet for permanent deletion. I.e. it is in the query filtering stage (see the deletion lifecycle below). 

Parameters:

- `request_id`

`GET /api/v1/admin/tsdb/delete_series` - Get all delete requests id’s and their current status. 

Prometheus also implements a [clean_tombstones](https://prometheus.io/docs/prometheus/latest/querying/api/#clean-tombstones) API which is not included in this proposal. The tombstones will be deleted automatically once the permanent deletion has taken place which is described in the section below. By default, this should take approximately 24 hours. 

### Deletion Lifecycle

The deletion request lifecycle can follow the same states as currently implemented for chunk storage:

1. StatusReceived - No actions are done on request yet, just doing query time filtering
2. StatusBuildingPlan - Request picked up for processing and building plans for it, still doing query time filtering
3. StatusDeleting - Plans built already, running delete operations and still doing query time filtering
4. StatusProcessed - All requested data deleted, not considering this for query time filtering

(copied from the chunk storage series deletion proposal)

With the current chunk store implementation, the amount of time for the request to move from StatusReceived to StatusBuildingPlan is dependent on a config option:  `-purger.delete-request-cancel-period`. The purpose of this is to allow the user some time to cancel the deletion request if it was made by mistake. 


#### Current entities in the Purger that can be reused

The following entities already exist for the chunk store and can be adapted to work for block storage:

- `DeleteStore` - Handles storing and fetching delete requests, delete plans, cache gen numbers
- `TombstonesLoader` - Helps loading delete requests and cache gen. Also keeps checking for updates.
- `CacheGenMiddleware` - Adds generation number as a prefix to cache keys before doing Store/Fetch operations.

(copied from the chunk storage series deletion proposal)



### Filtering data during queries while not yet deleted:

This will be done during the `DeleteRequest = StatusReceived` and `DeleteRequest = StatusBuildingPlan` parts of the deletion lifecycle.

Once a deletion request is received, a tombstone entry will be created. The object store such as S3, GCS, Azure storage, can be used to store all the deletion requests. See the section below for more detail on how the tombstones will be stored. Using the tombstones, the querier will be able to filter the to-be-deleted data initially. In addition, the existing cache will be invalidated using cache generation numbers, which are described in the later sections. 

From the purger service, the existing TombstonesLoader will periodically check for new tombstones in the object-store and load the new requests for deletion periodically using the modified DeleteStore. They will be loaded in memory periodically instead of retrieving all the tombstones when performing queries. Currently, with chunk storage, the TombstonesLoader is configured to check for updates to the DeleteStore every 5 minutes. 

Similar to the chunk storage deletion implementation, the initial filtering of the deleted data will be done inside the Querier. This will allow filtering the data read from both the store gateway and the ingestor. This functionality already exists for the chunk storage implementation. By implementing it in the querier, this would mean that the ruler can also utilize this to filter out the various metrics for the alert manager (read from the store gateway).  


#### Storing Tombstones in Object Store 


The Purger will store the tombstone entries in a separate folder called “series_deletion” in the object store (e.g. S3 bucket) in the respective tenant folder. Each tombstone can have a separate JSON file outlining all the necessary information about the deletion request such as the parameters passed in the request, as well as the current status and some meta-data such as the creation date of the request.  The name of the file can be a hash of the API parameters (start, end, markers). This way if a user calls the API twice by accident with the same parameters, it will only create one tombstone.  

The tombstone will be stored in a single file per request: 

- `/<tenantId>/series_deletion/requests/<request_id>.json`


The schema of the JSON file is:


```{
  "requestId": <string>,
  "startTime": <int>,
  "endTime": <int>,
  "creationTime": <int>,
  "matchers": [
    "<string matcher 1>",
    .., 
    "<string matcher n>"
    ]
  },
  "userID": <string>,
  "deleteRequestStatus": <received | buildingPlan | deleting | processed> 
}
```


Pros:

- Design is similar to the existing chunk storage deletion 
    - Lots of code can be reused inside the purger component. 
- Allows deletion and un-delete to be done in a single operation. 

Cons:
- Negative impact on query performance when there are active tombstones. As in the chunk storage implementation, all the queries made will have to be compared to the matchers contained in the active tombstone files. The impact on performance should be the same as the deletion would have with chunk storage.


#### Invalidating Cache

Using block store, the different caches available are:
- Index cache
- Metadata cache
- Chunks cache (stores the potentially to be deleted chunks of data) 
- Query results cache (stores the potentially to be deleted data) 

The filtering using tombstones is only requested by the querier. By using the purger, the querier filters out the data received from the ingestors and store-gateway. The cache not being processed through the querier needs to be invalidated to prevent deleted data from coming up in queries. There are two potential caches that could contain deleted data, the chunks cache, and the query results cache. 


Firstly, the query results cache needs to be invalidated for each new delete request. This can be done using the same mechanism currently used for chunk storage by utilizing the cache generation numbers. For each tenant, their cache is prefixed with a cache generation number. This is already implemented into the middleware and would be easy to use for invalidating the cache. When the cache needs to be invalidated due to a delete or cancel delete request, the cache generation numbers would be increased (to the current timestamp), which would invalidate all the cache entries for a given tenant. The cache generation numbers are currently being stored in an Index table  (e.g. DynamoDB or Bigtable). One option for block store is to store a per tenant JSON file with the corresponding cache generation numbers. 

Furthermore, since the chunks cache  is retrieved from the store gateway and passed to the querier, it will be filtered out like the rest of the time series data in the querier using the tombstones, with the mechanism described in the previous section. However, some issues may arise if the tombstone is deleted but the data to-be-deleted still exists in the chunks cache. This is why it is also best to use cache generation numbers for this cache in order to invalidate it the same way. 

This file can be stored in: 

- `/<tenantId>/series_deletion/cache_generation_numbers.json`

An example of the schema for the JSON file:

```
{
  "userID": <string>,
  "resultsCache": {
      "generationNum": <Int>
    }, 
  "storeCache": {
      "generationNum": <Int>
    }
} 
```

### Permanently deleting the data

To delete the data permanently from the block storage, deletion marker files will be used. The proposed approach is to perform the deletions from the compactor. A new background service inside the compactor called _DeletedSeriesCleaner_ can be created and is responsible for executing deletion.  The process of permanently deleting the data can be separated into 2 stages, preprocessing and processing. 

#### Pre-processing 

This will happen after a grace period has passed once the API request has been made. By default, this should be 24 hours. The Purger will outline all the blocks that may contain data to be deleted. For each separate block that the deletion may be applicable to, the Purger will begin the process by adding a series deletion marker inside the series-deletion-marker.json file. The JSON file will contain an array of deletions that need to be applied to the block, which allows the ability to handle the situation when there are multiple tombstones that could be applicable to a particular block.

#### Processing


This will happen when the `DeleteRequest = StatusDeleting` in the deletion lifecycle. A background task can be created to process the permanent deletion of time series using the information inside the series-deletion-marker.json files. This can be done each hour. 

To delete the data from the blocks, the same logic as the [Bucket Rewrite Tool](https://thanos.io/tip/components/tools.md/#bucket-rewrite
) from Thanos can be leveraged. This tool does the following: `tools bucket rewrite rewrites chosen blocks in the bucket, while deleting or modifying series`. The tool itself is a CLI tool that we won’t be using, but instead we can utilize the logic inside it. For more information about the way this tool runs, please see the code [here](https://github.com/thanos-io/thanos/blob/d8b21e708bee6d19f46ca32b158b0509ca9b7fed/cmd/thanos/tools_bucket.go#L809).

The compactor’s _DeletedSeriesCleaner_ will apply this logic on individual blocks and each time it is run, it creates a new block without the data that matched the deletion request. The original individual blocks containing the data that was requested to be deleted, need to be marked for deletion by the compactor. 

One important thing to note regarding this tool is that it should not be used at the same time as when another compactor is touching a block. If the tool is run at the same time as compaction on a particular block, it can cause overlap and the data marked for deletion can already be part of the compacted block. To mitigate such issues, these are some of the proposed solutions:

Option 1: Only apply the deletion once the blocks are in the final state of compaction.

Pros:
- Simpler implementation as everything is contained within the DeletedSeriesCleaner.

Cons:
- Might have to wait for a longer period of time for the compaction to be finished. 
    - This would mean the earliest time to be able to run the deletion would be once the last time from the block_ranges in the [compactor_config](https://cortexmetrics.io/docs/blocks-storage/compactor/#compactor-configuration) has passed. By default this value is 24 hours, so only once 24 hours have passed and the new compacted blocks have been created, then the rewrite can be safely run. 




Option 2: For blocks that still need to be compacted further after the deletion request cancel period is over, the deletion logic can be applied before the blocks are compacted. This will generate a new block which can then be used instead for compaction with other blocks. 

Pros:
- The deletion can be applied earlier than the previous options. 
    - Only applies if the deletion request cancel period is less than the last time interval for compaction is. 
Cons:
- Added coupling between the compaction and the DeletedSeriesCleaner. 
- Might block compaction for a short time while doing the deletion. 

In the newly created block without the deleted time series data, the information about the deletion is added to the meta.json file. This will indicate which deletion requests have been filtered out of this new block. This is necessary because it will let the Purger service know that this block doesn’t need to be rewritten again. 

To determine when a deletion request is complete, the purger will iterate through all the applicable blocks that might have data to be deleted. If there are any blocks that don’t have the tombstone ID in the meta.json of the block indicating the deletion has been complete, then the purger will add the series deletion markers to those blocks (if it doesn’t already exist). If after iterating through all blocks, it doesn’t find any such blocks, then that means the compactor has finished executing.

Once all the applicable blocks have been rewritten without the deleted data, the deletion request moves to `DeleteRequest = StatusProcessed` and the tombstone is deleted. 



##### Handling failed/unfinished delete jobs:

Deletions will be completed and the tombstones will be deleted only when the Purger iterates over all blocks that match the time interval and confirms that they have been re-written without the deleted data.  Otherwise, it will keep creating the markers indicating which blocks are remaining for deletion. In case of any failure that causes the deletion to stop, any unfinished deletions will be resumed once the service is restarted. The series deletion markers will remain in the bucket until the new blocks are created without the deleted data. Meaning that the compactor will continue to process the blocks for deletion that are remaining according to the deletion markers. 


#### Tenant Deletion API

If a request is made to delete a tenant, then all the tombstones will be deleted for that user. For all the tombstones deleted, if there were any series deletion markers for the tombstones deleted, these will also need to be deleted prior to marking the tenant’s blocks for deletion. 



## Current Open Questions:

- If the start and end time is very far apart, it might result in a lot of the data being re-written. Since we create a new block without the deleted data and mark the old one for deletion, there may be a period of time with lots of extra blocks and space used for large deletion queries. 
- Need to outline more clearly how this will work with multiple deletion requests at a time. 



## Alternatives Considered


For processing the actual deletions, an alternative approach is not to wait until the final compaction has been completed and filter out the data during compaction. If the data is marked to be deleted, then don’t include it the new bigger block during compaction. For the remaining blocks where the data wasn’t filtered during compaction, the deletion can be done the same as in the previous section. 

Pros: 

- The deletion can happen sooner.  
- The rewrite tools creates additional blocks. By filtering the metrics during compaction, the intermediary re-written block will be avoided. 

Cons: 

- A more complicated implementation requiring add more logic to the compactor
- Slower compaction if it needs to filter all the data
- Need to manage which blocks should be deleted with the rewrite vs which blocks already had data filtered during compaction. 
- Would need to run the rewrite logic during and outside of compaction because some blocks that might need to be deleted are already in the final compaction state. So that would mean the deletion functionality has to be implemented in multiple places.
- Won’t be leveraging the rewrites tools from Thanos for all the deletion, so potentially more work is duplicated

