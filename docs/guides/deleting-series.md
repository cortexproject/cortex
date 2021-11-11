---
title: "Deleting Series"
linkTitle: "Deleting Series"
weight: 10
slug: deleting-series
---

_This feature is currently experimental and is only supported for Chunks storage (deprecated)._

Cortex supports deletion of series using [Prometheus compatible API](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series).
It however does not support [Prometheuses Clean Tombstones](https://prometheus.io/docs/prometheus/latest/querying/api/#clean-tombstones) API because Cortex uses a different mechanism to manage deletions.

### How it works

A new service called `purger` is added which exposes deletion APIs and does the processing of the requests.
To store the requests, and some additional information while performing deletions, the purger requires configuring an index and object store respectively for it.
For more information about the `purger` configuration, please refer to the [config file reference](../configuration/config-file-reference.md#purger_config) documentation.

All the requests specified below needs to be sent to `purger`.

**Note:** If you have enabled multi-tenancy in your Cortex cluster then deletion APIs requests require to have the `X-Scope-OrgID` header set like for any other Cortex API.

#### Requesting Deletion

By calling the `/api/v1/admin/tsdb/delete_series` API like how it is done in [Prometheus](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series), you can request the deletion of series.
Delete Series requests are immediately honored by eliminating series requested for deletion from query responses without actually deleting them from storage.
The actual data is not deleted from storage until period configured for `-purger.delete-request-cancel-period` CLI flag or its respective YAML config option which helps operators take informed decision about continuing with the deletion or cancelling the request.

Cortex would keep eliminating series requested for deletion until the `purger` is done processing the delete request or the delete request gets cancelled.

_Sample cURL command:_
```
curl -X POST \
  '<purger_addr>/api/v1/admin/tsdb/delete_series?match%5B%5D=up&start=1591616227&end=1591619692' \
  -H 'x-scope-orgid: <tenant-id>'
```

#### Cancellation of Delete Request

Cortex allows cancellation of delete requests until they are not picked up for processing, which is controlled by the `-purger.delete-request-cancel-period` CLI flag or its respective YAML config option.
Since Cortex does query time filtering of data request for deletion until it is actually deleted, you can take an informed decision to cancel the delete request by calling the API defined below:

```
POST /api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>
PUT /api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>
```

_Sample cURL command:_
```
curl -X POST \
  '<purger_addr>/api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>' \
  -H 'x-scope-orgid: <tenant-id>'
```

You can find the id of the request that you want to cancel by using the GET `delete_series` API defined below.

#### Listing Delete Requests

You can list the created delete requests using following API:

```
GET /api/v1/admin/tsdb/delete_series
```

_Sample cURL command:_
```
curl -X GET \
  <purger_addr>/api/v1/admin/tsdb/delete_series \
  -H 'x-scope-orgid: <orgid>'
```

**NOTE:** List API returns both processed and un-processed requests except the cancelled ones since they are removed from the store.

