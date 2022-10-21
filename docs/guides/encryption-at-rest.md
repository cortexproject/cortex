---
title: "Encryption at Rest"
linkTitle: "Encryption at Rest"
weight: 10
slug: encryption-at-rest
---

Cortex supports data encryption at rest for some storage backends.

## S3

The Cortex S3 client supports the following server-side encryption (SSE) modes:

- [SSE-S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html)
- [SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingKMSEncryption.html)

### Blocks storage

The [blocks storage](../blocks-storage/_index.md) S3 server-side encryption can be configured as follows.

### `s3_sse_config`

The `s3_sse_config` configures the S3 server-side encryption.

```yaml
sse:
  # Enable AWS Server Side Encryption. Supported values: SSE-KMS, SSE-S3.
  # CLI flag: -<prefix>.s3.sse.type
  [type: <string> | default = ""]

  # KMS Key ID used to encrypt objects in S3
  # CLI flag: -<prefix>.s3.sse.kms-key-id
  [kms_key_id: <string> | default = ""]

  # KMS Encryption Context used for object encryption. It expects JSON formatted
  # string.
  # CLI flag: -<prefix>.s3.sse.kms-encryption-context
  [kms_encryption_context: <string> | default = ""]
```

### Ruler

The ruler S3 server-side encryption can be configured similarly to the blocks storage. The per-tenant overrides are supported when using the storage backend configurable the `-ruler-storage.` flag prefix (or their respective YAML config options).

### Alertmanager

The alertmanager S3 server-side encryption can be configured similarly to the blocks storage. The per-tenant overrides are supported when using the storage backend configurable the `-alertmanager-storage.` flag prefix (or their respective YAML config options).

### Per-tenant config overrides

The S3 client used by the blocks storage, ruler and alertmanager supports S3 SSE config overrides on a per-tenant basis, using the [runtime configuration file](../configuration/arguments.md#runtime-configuration-file).
The following settings can ben overridden for each tenant:

- **`s3_sse_type`**<br />
  S3 server-side encryption type. It must be set to enable the SSE config override for a given tenant.
- **`s3_sse_kms_key_id`**<br />
  S3 server-side encryption KMS Key ID. Ignored if the SSE type override is not set or the type is not `SSE-KMS`.
- **`s3_sse_kms_encryption_context`**<br />
  S3 server-side encryption KMS encryption context. If unset and the key ID override is set, the encryption context will not be provided to S3. Ignored if the SSE type override is not set or the type is not `SSE-KMS`.

## Other storages

Other storage backends may support encryption at rest configuring it directly at the storage level.
