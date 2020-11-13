---
title: "Migrate the storage from Thanos and Prometheus"
linkTitle: "Migrate the storage from Thanos and Prometheus"
weight: 7
slug: migrate-storage-from-thanos-and-prometheus
---

The Cortex blocks storage engine stores series in TSDB blocks uploaded in the storage bucket. This makes very easy to migrate the storage from Thanos and/or Prometheus to Cortex, when running the blocks storage.

## Cortex blocks storage requirements

The Cortex blocks storage has few requirements that should be considered when migrating TSDB blocks from Thanos / Prometheus to Cortex:

1. **The blocks in the bucket should be located at `bucket://<tenant-id>/`**<br />
   Cortex isolates blocks on a per-tenant basis in the bucket and, for this reason, each tenant blocks should be uploaded to a different location in the bucket. The bucket prefix, where a specific tenant blocks should be uploaded, is `/<tenant-id>/`; if Cortex is running with auth disabled (no multi-tenancy) then the `<tenant-id>` to use is `fake`.
2. **Remove Thanos external labels and inject `__org_id__` into each block's `meta.json`**<br />
   Every block has a little metadata file named `meta.json`. Thanos stores external labels at `thanos` > `labels`, which should be all removed when migrating to Cortex, while the `"__org_id__": "<tenant-id>"` added.

## How to migrate the storage

Currently, no tool is provided to migrate TSDB blocks from Thanos / Prometheus to Cortex, but writing an automation should be fairly easy. This automation could do the following:

1. Upload TSDB blocks from Thanos / Prometheus to Cortex bucket
2. Manipulate `meta.json` file for each block in the Cortex bucket

### Upload TSDB blocks to Cortex bucket

TSDB blocks stored in Prometheus local disk or Thanos bucket should be copied/uploaded to the Cortex bucket at the location `bucket://<tenant-id>/` (when Cortex is running with auth disabled then `<tenant-id>` must be `fake`).

### Manipulate `meta.json` file

For each block copied/uploaded to the Cortex bucket, the `meta.json` should be manipulated. The easiest approach would be iterating the tenants and blocks in the bucket and for each block:

1. Download the `meta.json` to the local filesystem
2. Decode the JSON
3. Manipulate the data structure (_see below_)
4. Re-encode the JSON
5. Re-upload it to the bucket (overwriting the previous version of the `meta.json` file)

The `meta.json` should be manipulated in order to ensure:

- It contains the `thanos` root-level entry
- The `thanos` > `labels` do not contain any Thanos-specific external label
- The `thanos` > `labels` contain the Cortex-specific external label `"__org_id__": "<tenant-id>"`

#### When migrating from Thanos

When migrating from Thanos, the easiest approach would be keep the existing `thanos` root-level entry as is, except:

1. Completely remove the content of `thanos` > `labels`
2. Add `"__org_id__": "<tenant-id>"` to `thanos` > `labels`

For example, when migrating a block from Thanos for the tenant `user-1`, the `thanos` root-level property within the `meta.json` file will look like:

```json
{
	"thanos": {
		"labels": {
			"__org_id__": "user-1"
		},
		"downsample": {
			"resolution": 0
		},
		"source": "compactor"
	}
}
```

#### When migrating from Prometheus

When migrating from Prometheus, the `meta.json` file will not contain any `thanos` root-level entry and, for this reason, it would need to be generated:

1. Create the `thanos` root-level entry (_see below_)
2. Add `"__org_id__": "<tenant-id>"` to `thanos` > `labels`

For example, when migrating a block from Prometheus for the tenant `user-1`, the `thanos` root-level property within the `meta.json` file should be as follow:

```json
{
	"thanos": {
		"labels": {
			"__org_id__": "user-1"
		},
		"downsample": {
			"resolution": 0
		},
		"source": "compactor"
	}
}
```
