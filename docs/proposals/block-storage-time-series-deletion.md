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

1. Pending - Tombstone file is created. During this state, the queriers will be performing query time filtering. The initial time period configured by `-purger.delete-request-cancel-period`, no data will be deleted. Once this period is over, permanent deletion processing will begin and the request is no longer cancellable.
2. Processed - All requested data has been deleted. Initially, will still need to do query time filtering while waiting for the bucket index and store-gateway to pick up the new blocks. Once that period has passed, will no longer require any query time filtering.
3. Deleted - The deletion request was cancelled. A grace period configured by `-purger.delete-request-cancel-period` will allow the user some time to cancel the deletion request if it was made by mistake. The request is no longer cancelable after this period has passed.



### Filtering data during queries while not yet deleted:

Once a deletion request is received, a tombstone entry will be created. The object store such as S3, GCS, Azure storage, can be used to store all the deletion requests. See the section below for more detail on how the tombstones will be stored. Using the tombstones, the querier will be able to filter the to-be-deleted data initially. If a cancel delete request is made, then the tombstone file will be deleted. In addition, the existing cache will be invalidated using cache generation numbers, which are described in the later sections.

The compactor's _BlocksCleaner_ service will scan for new tombstone files and will update the bucket-index with the tombstone information regarding the deletion requests. This will enable the querier to periodically check the bucket index if there are any new tombstone files that are required to be used for filtering. One drawback of this approach is the time it could take to start filtering the data. Since the compactor will update the bucket index with the new tombstones every `-compactor.cleanup-interval` (default 15 min). Then the cached bucket index is refreshed in the querier every `-blocks-storage.bucket-store.sync-interval` (default 15 min). Potentially could take almost 30 min for queriers to start filtering deleted data when using the default values. If the information requested for deletion is confidential/classified, the time delay is something that the user should be aware of, in addition to the time that the data has already been in Cortex.

An additional thing to consider is that this would mean that the bucket-index would have to be enabled for this API to work. Since the plan is to make to the bucket-index mandatory in the future for block storage, this shouldn't be an issue.

Similar to the chunk storage deletion implementation, the initial filtering of the deleted data will be done inside the Querier. This will allow filtering the data read from both the store gateway and the ingester. This functionality already exists for the chunk storage implementation. By implementing it in the querier, this would mean that the ruler will be supported too (ruler internally runs the querier).

#### Storing tombstones in object store


The Purger will write the new tombstone entries in a separate folder called `tombstones` in the object store (e.g. S3 bucket) in the respective tenant folder. Each tombstone can have a separate JSON file outlining all the necessary information about the deletion request such as the parameters passed in the request, as well as some meta-data such as the creation date of the file.  The name of the file can be a hash of the API parameters (start, end, markers). This way if a user calls the API twice by accident with the same parameters, it will only create one tombstone. To keep track of the request state, filename extensions can be used. This will allow the tombstone files to be immutable. The 3 different file extensions will be `pending, processed, deleted`. Each time the deletion request moves to a new state, a new file will be added with the same deletion information but a different extension to indicate the new state. The file containing the previous state will be deleted once the new one is created. If a deletion request is cancelled, then a tombstone file with the `.deleted` filename extension will be created.

When it is determined that the request should move to the next state, then it will first write a new file containing the tombstone information to the object store. The information inside the file will be the same except the `stateCreationTime`, which is replaced with the current timestamp. The extension of the new file will be different to reflect the new state. If the new file is successfully written, the file with the previous state is deleted. If the write of the new file fails, then the previous file is not going to be deleted. Next time the service runs to check the state of each tombstone, it will retry creating the new file with the updated state. If the write is successful but the deletion of the old file is unsuccessful then there will be 2 tombstone files with the same filename but different extension. When `BlocksCleaner` writes the tombstones to the bucket index, the compactor will check for duplicate tombstone files but with different extensions. It will use the tombstone with the most recently updated state and try to delete the file with the older state. There could be a scenario where there are two files with the same request ID but different extensions: {`.pending`, `.processed`} or {`.pending`, `.deleted`}. In this case, the `.processed` or `.deleted ` file will be selected as it is always the later state compared to the `pending` state.

The tombstone will be stored in a single JSON file per request and state:

- `/<tenantId>/tombstones/<request_id>.json.<state>`


The schema of the JSON file is:


```
{
  "requestId": <string>,
  "startTime": <int>,
  "endTime": <int>,
  "requestCreationTime": <int>,
  "stateCreationTime": <int>,
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

Firstly, the query results cache needs to be invalidated for each new delete request or a cancellation of one. This can be accomplished by utilizing cache generation numbers. For each tenant, their cache is prefixed with a cache generation number. When the query front-end discovers a cache generation number that is greater than the previous generation number, then it knows to invalidate the query results cache. However, the cache can only be invalidated once the queriers have loaded the tombstones from the bucket index and have begun filtering the data. Otherwise, to-be deleted data might show up in queries and be cached again. One of the way to guarantee that all the queriers are using the new tombstones is to wait until the bucket index staleness period has passed from the time the tombstones have been written to the bucket index. The staleness period can be configured using the following flag: `-blocks-storage.bucket-store.bucket-index.max-stale-period`. We can use the bucket index staleness period as the delay to wait before the cache generation number is increased. A query will fail inside the querier, if the bucket index last update is older the staleness period. Once this period is over, all the queriers should have the updated tombstones and the query results cache can be invalidated.  Here is the proposed method for accomplishing this:


- The cache generation number will be a timestamp. The default value will be 0.
- The bucket index will store the cache generation number.  The query front-end will periodically fetch the bucket index.
- Inside the compactor, the _BlocksCleaner_ will load the tombstones from object store and update the bucket index accordingly. It will calculate the cache generation number by iterating through all the tombstones and their respective times (next bullet point) and selecting the maximum timestamp that is less than (current time minus  `-blocks-storage.bucket-store.bucket-index.max-stale-period`). This would mean that if a deletion request is made or cancelled, the compactor will only update the cache generation number once the staleness period is over, ensuring that all queriers have the updated tombstones.
- For requests in a pending or processed state, the `requestCreationTime` will be used when comparing the maximum timestamps. If a request is in a deleted state, it will use the `stateCreationTime` for comparing the timestamps. This means that the cache gets invalidated only once it has been created or deleted, and the bucket index staleness period has passed. The cache will not be invalidated again when a request advances from pending to processed state.
- The query front-end will fetch the cache generation number from the bucket index. The query front end will compare it to the current cache generation number stored in the front-end. If the cache generation number from the front-end is less than the one from bucket index, then the cache is invalidated.

In regards to the chunks cache, since it is retrieved from the store gateway and passed to the querier, it will be filtered out like the rest of the time series data in the querier using the tombstones, with the mechanism described in the previous section.

### Permanently deleting the data

The proposed approach is to perform the deletions from the compactor. A new background service inside the compactor called _DeletedSeriesCleaner_ can be created and is responsible for executing the deletion.

#### Processing


This will happen after a grace period has passed once the API request has been made. By default this should be 24 hours. A background task can be created to process the permanent deletion of time series. This background task can be executed each hour.

To delete the data from the blocks, the same logic as the [Bucket Rewrite Tool](https://thanos.io/tip/components/tools.md/#bucket-rewrite
) from Thanos can be leveraged. This tool does the following: `tools bucket rewrite rewrites chosen blocks in the bucket, while deleting or modifying series`. The tool itself is a CLI tool that we won’t be using, but instead we can utilize the logic inside it. For more information about the way this tool runs, please see the code [here](https://github.com/thanos-io/thanos/blob/d8b21e708bee6d19f46ca32b158b0509ca9b7fed/cmd/thanos/tools_bucket.go#L809).

The compactor’s _DeletedSeriesCleaner_ will apply this logic on individual blocks and each time it is run, it creates a new block without the data that matched the deletion request. The original individual blocks containing the data that was requested to be deleted, need to be marked for deletion by the compactor.

While deleting the data permanently from the block storage, the `meta.json` files will be used to keep track of the deletion progress. Inside each `meta.json` file, we will add a new field called `tombstonesFiltered`. This will store an array of deletion request id's that were used to create this block. Once the rewrite logic is applied to a block, the new block's `meta.json` file will append the deletion request id(s) used for the rewrite operation inside this field. This will let the _DeletedSeriesCleaner_ know that this block has already processed the particular deletions requests listed in this field. Assuming that the deletion requests are quite rare, the size of the meta.json files should remain small.

The _DeletedSeriesCleaner_ can iterate through all the blocks that the deletion request could apply to. For each of these blocks, if the deletion request ID isn't inside the meta.json `tombstonesFiltered` field, then the compactor can apply the rewrite logic to this block. If there are multiple tombstones that are currently being processing for deletions and apply to a particular block, then the _DeletedSeriesCleaner_ will process both at the same time to prevent additional blocks from being created. If after iterating through all the blocks, it doesn’t find any such blocks requiring deletion, then the `Pending` state is complete and the request progresses to the `Processed` state.

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



Once all the applicable blocks have been rewritten without the deleted data, the deletion request state moves to the `Processed` state. Once in this state, the queriers will still have to perform query time filtering using the tombstones until the old blocks that were marked for deletion are no longer queried by the queriers. This will mean that the query time filtering will last for an additional length of `-compactor.deletion-delay + -compactor.cleanup-interval +  -blocks-storage.bucket-store.sync-interval` in the `Processed` state. Once that time period has passed, the queriers should no longer be querying any of the old blocks that were marked for deletion. The tombstone will no longer be used after this.


#### Cancelled Delete Requests

If a request was successfully cancelled, then a tombstone file a `.deleted` extension is created. This is done to help ensure that the cache generation number is updated and the query results cache is invalidated. The compactor's blocks cleaner can take care of cleaning up `.deleted` tombstones after a period of time of when they are no longer required for cache invalidation. This can be done after 10 times the bucket index max staleness time period has passed. Before removing the file from the object store, the current cache generation number must greater than or equal to when the tombstone was cancelled.

#### Handling failed/unfinished delete jobs:

Deletions will be completed and the tombstones will be deleted only when the DeletedSeriesCleaner iterates over all blocks that match the time interval and confirms that they have been re-written without the deleted data.  Otherwise, it will keep iterating over the blocks and process the blocks that haven't been rewritten according to the information in the `meta.json` file. In case of any failure that causes the deletion to stop, any unfinished deletions will be resumed once the service is restarted. If the block rewrite was not completed on a particular block, then the original block will not be marked for deletion. The compactor will continue to iterate over the blocks and process the block again.


#### Tenant Deletion API

If a request is made to delete a tenant, then all the tombstones will be deleted for that user.


## Current Open Questions:

- If the start and end time is very far apart, it might result in a lot of the data being re-written. Since we create a new block without the deleted data and mark the old one for deletion, there may be a period of time with lots of extra blocks and space used for large deletion queries.
- There will be a delay between the deletion request and the deleted data being filtered during queries.
    - In Prometheus, there is no delay.
    - One way to filter out immediately is to load the tombstones during query time but this will cause a negative performance impact.
- Adding limits to the API such as:
    - Max number of deletion requests allowed in the last 24 hours for a given tenent.
    - Max number of pending tombstones for a given tenant.


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

