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


This can be very important for users to have as confidential or accidental data might have been incorrectly pushed and needs to be removed. As well as potentially removing high cardinality data that is causing inefficient queries. 

## Related works

As previously mentioned, the deletion feature is already implemented with chunk storage. The main functionality is implemented through the purger service. It accepts requests for deletion and processes them.  At first, when a deletion request is made, a tombstone is created. This is used to filter out the data for queries. After some time, a deletion plan is executed where the data is permanently removed from chunk storage. 

Can find more info here:

- [Cortex documentation for chunk store deletion](https://cortexmetrics.io/docs/guides/deleting-series/)
- [Chunk deletion proposal](https://docs.google.com/document/d/1PeKwP3aGo3xVrR-2qJdoFdzTJxT8FcAbLm2ew_6UQyQ/edit)



## Background on current storage

With a block-storage configuration, Cortex stores data that could be potentially deleted by a user in:

- Object store (GCS, S3, etc..) for long term storage of blocks
- Ingesters for more recent data that should be eventually transferred to the object store
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


`POST /api/v1/admin/tsdb/cancel_delete_request`  - To cancel a request if it has not been processed yet for permanent deletion. This can only be done before the `-purger.delete-request-cancel-period` has passed. 
Parameters:

- `request_id`

`GET /api/v1/admin/tsdb/delete_series` - Get all delete requests id’s and their current status. 

Prometheus also implements a [clean_tombstones](https://prometheus.io/docs/prometheus/latest/querying/api/#clean-tombstones) API which is not included in this proposal. The tombstones will be deleted automatically once the permanent deletion has taken place which is described in the section below. By default, this should take approximately 24 hours. 

### Deletion Lifecycle

The deletion request lifecycle can follow these 3 states:

1. Pending - Tombstone file is created, just doing query time filtering. No data has been deleted yet. 
2. Deleting - Running delete operations and still doing query time filtering.
4. Processed - All requested data deleted, chunks cache should contain new blocks. Initially, will still need to do query time filtering while waiting for the bucket index and store-gateway to pick up the new blocks and replace the old chunks cache. Once that period has passed, will no longer require any query time filtering. 

The amount of time for the request to move from `Pending` to `Deleting` is dependent on a config option: `-purger.delete-request-cancel-period`. The purpose of this is to allow the user some time to cancel the deletion request if it was made by mistake. 


### Filtering data during queries while not yet deleted:

Once a deletion request is received, a tombstone entry will be created. The object store such as S3, GCS, Azure storage, can be used to store all the deletion requests. See the section below for more detail on how the tombstones will be stored. Using the tombstones, the querier will be able to filter the to-be-deleted data initially. If a cancel delete request is made, then the tombstone file will be deleted. In addition, the existing cache will be invalidated using cache generation numbers, which are described in the later sections. 

The compactor will scan for new tombstone files and will update the bucket-index with the tombstone information regarding the deletion requests. This will enable the querier to periodically check the bucket index if there are any new tombstone files that are required to be used for filtering. One drawback of this approach is the time it could take to start filtering the data. Since the compactor will update the bucket index with the new tombstones every `-compactor.cleanup-interval` (default 15 min). Then the cached bucket index is refreshed in the querier every `-blocks-storage.bucket-store.sync-interval` (default 15 min). Potentially could take almost 30 min for queriers to start filtering deleted data when using the default values. If the information requested for deletion is confidential/classified, the time delay is something that the user should be aware of, in addition to the time that the data has already been in Cortex.

An additional thing to consider is that this would mean that the bucket-index would have to be enabled for this API to work. Since the plan is to make to the bucket-index mandatory in the future for block storage, this shouldn't be an issue.  

Similar to the chunk storage deletion implementation, the initial filtering of the deleted data will be done inside the Querier. This will allow filtering the data read from both the store gateway and the ingester. This functionality already exists for the chunk storage implementation. By implementing it in the querier, this would mean that the ruler will be supported too (ruler internally runs the querier).

#### Storing tombstones in object store 


The Purger will write the new tombstone entries in a separate folder called `tombstones` in the object store (e.g. S3 bucket) in the respective tenant folder. Each tombstone can have a separate JSON file outlining all the necessary information about the deletion request such as the parameters passed in the request, as well as some meta-data such as the creation date of the file.  The name of the file can be a hash of the API parameters (start, end, markers). This way if a user calls the API twice by accident with the same parameters, it will only create one tombstone. To keep track of the request state, filename extensions can be used. This will allow the tombstone files to be immutable. The 3 different file extensions will be `pending, deleting, processed`. Each time the deletion request moves to a new state, a new file will be added with the same content but a different extension to indicate the new state. The file containing the previous state will be deleted once the new one is created. 

Updating the states will be done from the compactor. Inside the new _DeletedSeriesCleaner_ service, it will periodically check all the tombstones to see if their current state is ready to be upgraded. If it is determined that the request should move to the next state, then it will first write a new file containing the tombstone information. The information inside the file will be the same except the `creationTime`, which is replaced with the current timestamp. The extension of the new file will be different to reflect the new state. If the new file is successfully written, the file with the previous state is deleted. If the write of the new file fails, then the previous file is not going to be deleted. Next time the service runs to check the state of each tombstone, it will retry creating the new file with the updated state. If the write is successful but the deletion of the old file is unsuccessful then there will be 2 tombstone files with the same filename but different extension. When writing to the bucket index, the compactor will check for duplicate tombstone files but with different extensions. It will use the tombstone with the most recently updated state and try to delete the file with the older state. 

The tombstone will be stored in a single JSON file per request and state: 

- `/<tenantId>/tombstones/<request_id>.json.<state>`


The schema of the JSON file is:


```
{
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
}
```


Pros:
- Allows deletion and un-delete to be done in a single operation. 

Cons:

- Negative impact on query performance when there are active tombstones. As in the chunk storage implementation, all the series will have to be compared to the matchers contained in the active tombstone files. The impact on performance should be the same as the deletion would have with chunk storage.

- With the default config, potential 30 minute wait for the data to begin filtering if using the default configuration. 

#### Invalidating cache

Using block store, the different caches available are:
- Index cache
- Metadata cache
- Chunks cache (stores the potentially to be deleted chunks of data) 
- Query results cache (stores the potentially to be deleted data) 

There are two potential caches that could contain deleted data, the chunks cache, and the query results cache. Using the tombstones, the queriers filter out the data received from the ingesters and store-gateway. The cache not being processed through the querier needs to be invalidated to prevent deleted data from coming up in queries. 

Firstly, the query results cache needs to be invalidated for each new delete request or a cancel delete request. This can be done using the same mechanism currently used for chunk storage by utilizing the cache generation numbers. For each tenant, their cache is prefixed with a cache generation number. This is already implemented into the middleware and would be easy to use for invalidating the cache. When the cache needs to be invalidated due to a delete or cancel delete request, the cache generation numbers would be increased (to the current timestamp), which would invalidate all the cache entries for a given tenant. With chunk store, the cache generation numbers are currently being stored in an Index table  (e.g. DynamoDB or Bigtable). One option for block store is to save a per tenant key using the KV-store with the ring backend and propagate it using a Compare-And-Set/Swap (CAS) operation. Inside the query frontend, if the current cache generation number is older than the one in KV-store or it is empty, then the cache is invalidated and the cache generation number is updated. However, since the tombstones are uploaded asynchronously to the queriers, the results from the queriers might contain deleted data after the cache has been already invalidated. That would mean that the results can't be cached until there is a guarantee that the querier's responses have been processed using the most up to date tombstones. Here is the proposed method for ensuring this:

- When a deletion request is made or cancelled, the cache generation number is incremented for the given tenant in the KV-store to the current timestamp. 
- Inside the query frontend, the cache generation number will be outdated, so the cache is invalidated. The query frontend will now store the most recent cache generation number.
- When the compactor writes the tombstones to the bucket index, it will include the timestamp of when the write occurred. When the querier reads from the bucket index, it will store this timestamp.  
- Inside the query frontend, if a response is not found in cache, it will ask at least one querier. Inside the response of each querier, it will include the timestamp of when the compactor wrote to the bucket index. The query frontend will compare the minimum timestamp that was returned by the queriers to the cache generation number (timestamp). If the minimum timestamp is larger than the cache generation number, that means that all the queriers loaded the bucket index after the compactor has updated it with the most up-to-date tombstones.  If this is the case, then the response from each of the queriers should have the correct data and the results can be cached inside the query frontend. Otherwise, the response of the queriers might contain data that was not processed using the most up-to-date tombstones, and the results should not be cached. Using the minimum timestamp returned by the queriers, we can guarantee that the queriers have the most recent tombstones. 

Furthermore, since the chunks cache  is retrieved from the store gateway and passed to the querier, it will be filtered out like the rest of the time series data in the querier using the tombstones, with the mechanism described in the previous section. However, some issues may arise if the tombstone is deleted but the data to-be-deleted still exists in the chunks cache. To prevent this, we keep performing query time filtering using the tombstones once all the data has been deleted for an additional period of time. The filtering would be required until the store-gateway picks up the new blocks and the chunks cache is able to be refreshed with the new blocks without the deleted data. The `processed` state will begin as soon as all the requested data has been permanently deleted from the block store. Once in this state, the query time filtering will last for a length of `-compactor.deletion-delay + -compactor.cleanup-interval +  -blocks-storage.bucket-store.sync-interval`. Once that time period has passed, the chunks cache should not have any of the deleted data. Then the tombstone will no longer be used for query time filtering. 

### Permanently deleting the data

The proposed approach is to perform the deletions from the compactor. A new background service inside the compactor called _DeletedSeriesCleaner_ can be created and is responsible for executing the deletion.  

#### Processing


This will happen after a grace period has passed once the API request has been made. By default this should be 24 hours. The state of the request becomes `Deleting`. A background task can be created to process the permanent deletion of time series. This background task can be executed each hour. 

To delete the data from the blocks, the same logic as the [Bucket Rewrite Tool](https://thanos.io/tip/components/tools.md/#bucket-rewrite
) from Thanos can be leveraged. This tool does the following: `tools bucket rewrite rewrites chosen blocks in the bucket, while deleting or modifying series`. The tool itself is a CLI tool that we won’t be using, but instead we can utilize the logic inside it. For more information about the way this tool runs, please see the code [here](https://github.com/thanos-io/thanos/blob/d8b21e708bee6d19f46ca32b158b0509ca9b7fed/cmd/thanos/tools_bucket.go#L809).

The compactor’s _DeletedSeriesCleaner_ will apply this logic on individual blocks and each time it is run, it creates a new block without the data that matched the deletion request. The original individual blocks containing the data that was requested to be deleted, need to be marked for deletion by the compactor. 

While deleting the data permanently from the block storage, the `meta.json` files will be used to keep track of the deletion progress. Inside each `meta.json` file, we will add a new field called `tombstonesFiltered`. This will store an array of deletion request id's that were used to create this block. Once the rewrite logic is applied to a block, the new block's `meta.json` file will append the deletion request id(s) used for the rewrite operation inside this field. This will let the _DeletedSeriesCleaner_ know that this block has already processed the particular deletions requests listed in this field. Assuming that the deletion requests are quite rare, the size of the meta.json files should remain small. 

The _DeletedSeriesCleaner_ can iterate through all the blocks that the deletion request could apply to. For each of these blocks, if the deletion request ID isn't inside the meta.json `tombstonesFiltered` field, then the compactor can apply the rewrite logic to this block. If there are multiple tombstones in the `Deleting` state that apply to a particular block, then the _DeletedSeriesCleaner_ will process both at the same time to prevent additional blocks from being created. If after iterating through all the blocks, it doesn’t find any such blocks requiring deletion, then the `Deleting` state is complete and the request progresses to the `Processed` state.

One important thing to note regarding this rewrite tool is that it should not be used at the same time as when another compactor is touching a block. If the tool is run at the same time as compaction on a particular block, it can cause overlap and the data marked for deletion can already be part of the compacted block. To mitigate such issues, these are some of the proposed solutions:

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



Once all the applicable blocks have been rewritten without the deleted data, the deletion request state moves to `Processed`. Once in this state, the queriers will still have to perform query time filtering using the tombstones for a period of time. This period of time is described in the cache invalidation section. Once the time period is over, the tombstone will no longer be used.  



#### Handling failed/unfinished delete jobs:

Deletions will be completed and the tombstones will be deleted only when the DeletedSeriesCleaner iterates over all blocks that match the time interval and confirms that they have been re-written without the deleted data.  Otherwise, it will keep iterating over the blocks and process the blocks that haven't been rewritten according to the information in the `meta.json` file. In case of any failure that causes the deletion to stop, any unfinished deletions will be resumed once the service is restarted. If the block rewrite was not completed on a particular block, then the block will not be marked for deletion. The compactor will continue to iterate over the blocks and process the block again. 


#### Tenant Deletion API

If a request is made to delete a tenant, then all the tombstones will be deleted for that user. 



## Current Open Questions:

- If the start and end time is very far apart, it might result in a lot of the data being re-written. Since we create a new block without the deleted data and mark the old one for deletion, there may be a period of time with lots of extra blocks and space used for large deletion queries. 
- There will be a delay between the deletion request and the deleted data being filtered during queries. 
    - In Prometheus, there is no delay. 
    - One way to filter out immediately is to load the tombstones during query time but this will cause a negative performance impact. 
- Adding limits to the API such as:
    - The number of deletion requests per day,
    - Number of requests allowed at a time
    - How wide apart the start and end time can be. 

## Alternatives Considered


#### Adding a Pre-processing State

The process of permanently deleting the data can be separated into 2 stages, preprocessing and processing. 

Pre-processing will begin after the `-purger.delete-request-cancel-period` has passed since the API request has been made. The deletion request will move to a new state called `BuildingPlan`. The compactor will outline all the blocks that may contain data to be deleted. For each separate block that the deletion may be applicable to, the compactor will begin the process by adding a series deletion marker inside the series-deletion-marker.json file. The JSON file will contain an array of deletion request id's that need to be applied to the block, which allows the ability to handle the situation when there are multiple tombstones that could be applicable to a particular block. Then during the processing step, instead of checking the meta.json file, we only need to check if a marker file exists with a specific deletion request id. If the marker file exists, then we apply the rewrite logic.

#### Alternative Permanent Deletion Processing

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

