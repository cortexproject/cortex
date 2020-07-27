---
title: "Deleting Series"
linkTitle: "Deleting Series"
weight: 3
slug: deleting-series
---

:warning: This feature is currently experimental and is only supported for Chunks storage. :warning:

Cortex supports deletion of series using [Prometheus compatible API](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series).
It however does not support [Prometheuses Clean Tombstones](https://prometheus.io/docs/prometheus/latest/querying/api/#clean-tombstones) API by trading it in for another feature for allowing cancellation of delete requests.

### How it works

A new service called `purger` is added which handles deletion APIs and does the processing of the requests.
To store the requests, and some additional information while performing deletion it requires configuring an index and object store respectively for it.
`purger` can be configured using the config defined [here](../configuration/config-file-reference.md#purger_config)

All the requests specified below needs to be sent to `purger`.

**Note:** If you have enabled auth then same as other APIs it is required to have `X-Scope-OrgID` header set in the requests.

#### Requesting Deletion

By calling the `delete_series` API defined [here](https://prometheus.io/docs/prometheus/latest/querying/api/#delete-series), you can request the deletion of series.
The data would be deleted based on the configuration for `-purger.delete-request-cancel-period` flag which defaults to 24 hours.
The delete requests would be picked up for processing only after they have aged for the duration configured using that flag.
Cortex would do query time filtering of the data requested for deletion until the `purger` is done processing the delete requests.

_Sample cURL command:_
```
curl -X POST \
  '<purger_addr>/api/v1/admin/tsdb/delete_series?match%5B%5D=up&start=1591616227&end=1591619692' \
  -H 'x-scope-orgid: <orgid>'
```

#### Cancellation of Delete Request

Cortex allows cancellation of delete requests until they are not picked up for processing, which is controlled by a flag as explained in `Requesting Deletion` section.
Since Cortex does query time filtering of data request for deletion until it is actually deleted, you can take an informed decision to cancel the delete request by calling the API defined below:

```
POST /api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>
PUT /api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>
```

_Sample cURL command:_
```
curl -X POST \
  '<purger_addr>/api/v1/admin/tsdb/cancel_delete_request?request_id=<request_id>' \
  -H 'x-scope-orgid: <orgid>'
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

**NOTE:** Cancelled delete requests are removed from the store, so they would not be there in the list of requests returned by the API.

