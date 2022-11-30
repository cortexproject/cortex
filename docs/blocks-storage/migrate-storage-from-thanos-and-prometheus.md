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

### Upload TSDB blocks to Cortex bucket

TSDB blocks stored in Prometheus local disk or Thanos bucket should be copied/uploaded to the Cortex bucket at the location `bucket://<tenant-id>/` (when Cortex is running with auth disabled then `<tenant-id>` must be `fake`).

### Migrate block metadata (`meta.json`) to Cortex

For each block copied/uploaded to the Cortex bucket, there are a few changes required to the `meta.json`.

#### Automatically migrate metadata using the `thanosconvert` tool

`thanosconvert` can iterate over a Cortex bucket and make sure that each `meta.json` has the correct `thanos` > `labels` layout.

⚠ Warning ⚠ `thanosconvert` will modify files in the bucket you specify. It's recommended that you have backups or enable object versioning before running this tool.

To run `thanosconvert`, you need to provide it with the bucket configuration in the same format as the [blocks storage bucket configuration](../configuration/config-file-reference.md#blocks_storage_config).
```yaml
# bucket-config.yaml
backend: s3
s3:
  endpoint: s3.us-east-1.amazonaws.com
  bucket_name: my-cortex-bucket
```

You can run thanosconvert directly using Go:
```bash
go install github.com/cortexproject/cortex/cmd/thanosconvert
thanosconvert
```

Or use the provided docker image:
```bash
docker run quay.io/cortexproject/thanosconvert
```

You can run the tool in dry-run mode first to find out what which blocks it will migrate:

```bash
thanosconvert -config ./bucket-config.yaml -dry-run
```

Once you're happy with the results, you can run without dry run to migrate blocks:
```bash
thanosconvert -config ./bucket-config.yaml
```

You can cancel a conversion in progress (with Ctrl+C) and rerun `thanosconvert`. It won't change any blocks which have been written by Cortex or already converted from Thanos, so you can run `thanosconvert` multiple times.


#### Migrate metadata manually

If you need to migrate the block metadata manually, you need to:

1. Download the `meta.json` to the local filesystem
2. Decode the JSON
3. Manipulate the data structure (_see below_)
4. Re-encode the JSON
5. Re-upload it to the bucket (overwriting the previous version of the `meta.json` file)

The `meta.json` should be manipulated in order to ensure:

- It contains the `thanos` root-level entry
- The `thanos` > `labels` do not contain any Thanos-specific external label
- The `thanos` > `labels` contain the Cortex-specific external label `"__org_id__": "<tenant-id>"`


##### When migrating from Thanos

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

Right now Cortex doesn't support downsampling so Thanos downsampled blocks are not supported. Downsampled blocks will be simply skipped by the `thanosconvert` tool.

If downsampled blocks are uploaded to the Cortex bucket, they cannot be queried so please exclude them when migrating TSDB blocks.

##### When migrating from Prometheus

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
