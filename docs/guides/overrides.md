---
title: "User Overrides API"
linkTitle: "User Overrides API"
weight: 11
slug: overrides
---

The User Overrides API provides a RESTful interface for managing tenant-specific limit overrides at runtime without requiring manual edits to the runtime configuration file or service restarts.

## Context

Cortex is a multi-tenant system that applies resource limits to each tenant to prevent any single tenant from using too many resources. These limits can be configured globally via the `limits` section in the main configuration file, or per-tenant via the `runtime_config` file.

Traditionally, updating per-tenant limits required:
1. Manually editing the runtime configuration file
2. Waiting for Cortex to reload the configuration (based on `reload-period`)
3. Direct access to the runtime configuration storage

The User Overrides API simplifies this process by providing HTTP endpoints that allow authorized users or systems to:
- View current tenant overrides
- Set or update specific limit overrides
- Delete all overrides for a tenant

## Architecture

The overrides module runs as a service within Cortex and provides three main capabilities:

1. **API Endpoints**: RESTful HTTP endpoints for managing overrides
2. **Validation**: Enforces allowed limits and hard limits from runtime configuration
3. **Merge Behavior**: Preserves existing overrides when updating specific limits

## Configuration

### Enabling the Overrides Module

The `overrides` module must be explicitly enabled in Cortex. It is not included in the `all` target by default.

```bash
# Run only the overrides module
cortex -target=overrides -runtime-config.file=runtime.yaml

# Include overrides with other modules
cortex -target=overrides,query-frontend,querier -runtime-config.file=runtime.yaml
```

### Runtime Configuration File

The runtime configuration file controls which limits can be modified via the API and sets upper bounds (hard limits) for tenant overrides.

#### Basic Configuration

```yaml
# file: runtime.yaml

# Current tenant overrides
overrides:
  tenant1:
    ingestion_rate: 50000
    max_global_series_per_user: 1000000

# Limits that can be modified via the API
api_allowed_limits:
  - ingestion_rate
  - ingestion_burst_size
  - max_global_series_per_user
  - max_global_series_per_metric
  - ruler_max_rules_per_rule_group
  - ruler_max_rule_groups_per_tenant
```

#### Configuration with Hard Limits

Hard limits prevent tenants from setting overrides above a specified maximum value:

```yaml
# file: runtime.yaml

# Current tenant overrides
overrides:
  tenant1:
    ingestion_rate: 50000
    max_global_series_per_user: 500000

# Allowed limits that can be modified via API
api_allowed_limits:
  - ingestion_rate
  - ingestion_burst_size
  - max_global_series_per_user
  - max_global_series_per_metric
  - ruler_max_rules_per_rule_group
  - ruler_max_rule_groups_per_tenant

# Hard limits (maximum values) per tenant
hard_overrides:
  tenant1:
    ingestion_rate: 100000
    max_global_series_per_user: 2000000
  tenant2:
    ingestion_rate: 200000
    max_global_series_per_user: 5000000
```

### Storage Backend

The overrides module uses the same storage backend as the runtime config. Configure it using the `runtime-config` section:

```yaml
runtime_config:
  period: 10s
  file: runtime.yaml

  # For S3 backend
  backend: s3
  s3:
    bucket_name: cortex-runtime-config
    endpoint: s3.amazonaws.com
    access_key_id: ${AWS_ACCESS_KEY_ID}
    secret_access_key: ${AWS_SECRET_ACCESS_KEY}

  # For GCS backend
  # backend: gcs
  # gcs:
  #   bucket_name: cortex-runtime-config

  # For filesystem backend (default)
  # backend: filesystem
  # filesystem:
  #   dir: /etc/cortex
```

## API Reference

All endpoints require authentication using the `X-Scope-OrgID` header with the tenant ID.

### Get User Overrides

```http
GET /api/v1/user-overrides
X-Scope-OrgID: tenant1
```

Returns the current overrides for the authenticated tenant in JSON format.

**Response (200 OK):**
```json
{
  "ingestion_rate": 50000,
  "max_global_series_per_user": 500000,
  "ruler_max_rules_per_rule_group": 100
}
```

**Response (404 Not Found):**
If the tenant has no overrides configured, an empty object is returned:
```json
{}
```

### Set User Overrides

```http
POST /api/v1/user-overrides
X-Scope-OrgID: tenant1
Content-Type: application/json

{
  "ingestion_rate": 75000
}
```

Sets or updates specific overrides for the authenticated tenant. This operation **merges** with existing overrides rather than replacing them entirely.

**Merge Behavior Example:**

Current state:
```json
{
  "ingestion_rate": 50000,
  "max_global_series_per_user": 500000,
  "ruler_max_rules_per_rule_group": 100
}
```

Request:
```json
{
  "ingestion_rate": 75000
}
```

Result:
```json
{
  "ingestion_rate": 75000,
  "max_global_series_per_user": 500000,
  "ruler_max_rules_per_rule_group": 100
}
```

**Response (200 OK):**
Returns success with no body.

**Response (400 Bad Request):**
- Invalid limit names (not in `api_allowed_limits`)
- Values exceeding hard limits
- Invalid JSON format

### Delete User Overrides

```http
DELETE /api/v1/user-overrides
X-Scope-OrgID: tenant1
```

Removes all overrides for the authenticated tenant. The tenant will revert to using global default values.

**Response (200 OK):**
Returns success with no body.

## API Allowed Limits

The `api_allowed_limits` configuration in the runtime config file controls which limits can be modified via the API. This provides an additional security layer to prevent unauthorized modification of critical limits.

### Configuring Allowed Limits

```yaml
api_allowed_limits:
  - ingestion_rate
  - ingestion_burst_size
  - max_global_series_per_user
  - max_global_series_per_metric
  - ruler_max_rules_per_rule_group
  - ruler_max_rule_groups_per_tenant
```

### Validation Behavior

When a POST request is made:

1. **Allowed limits check**: Each limit in the request is validated against `api_allowed_limits`
2. **Rejection**: If any limit is not in the allowed list, the entire request is rejected with a 400 error

**Example - Rejected Request:**

Runtime config:
```yaml
api_allowed_limits:
  - ingestion_rate
  - max_global_series_per_user
```

Request:
```json
{
  "ingestion_rate": 50000,
  "max_series_per_query": 100000
}
```

Response (400 Bad Request):
```
the following limits cannot be modified via the overrides API: max_series_per_query
```

### Available Limit Names

Limit names correspond to fields in the [`limits_config`](../configuration/config-file-reference.md#limits_config) section. Common examples include:

- `ingestion_rate`
- `ingestion_burst_size`
- `max_global_series_per_user`
- `max_global_series_per_metric`
- `max_local_series_per_user`
- `max_local_series_per_metric`
- `max_series_per_query`
- `max_samples_per_query`
- `ruler_max_rules_per_rule_group`
- `ruler_max_rule_groups_per_tenant`
- `max_label_names_per_series`
- `max_label_name_length`
- `max_label_value_length`

## Hard Limits

Hard limits provide per-tenant upper bounds for override values. They prevent tenants from setting limits that exceed their allocated capacity.

### Configuring Hard Limits

Hard limits are specified per tenant in the `hard_overrides` section:

```yaml
hard_overrides:
  tenant1:
    ingestion_rate: 100000
    max_global_series_per_user: 2000000
  tenant2:
    ingestion_rate: 500000
    max_global_series_per_user: 10000000
```

### Validation Behavior

When a POST request is made:

1. **Hard limit lookup**: System checks if the tenant has hard limits configured
2. **Value comparison**: Each requested override value is compared against its hard limit
3. **Rejection**: If any value exceeds its hard limit, the entire request is rejected

**Example - Rejected Request:**

Runtime config:
```yaml
hard_overrides:
  tenant1:
    ingestion_rate: 100000
    max_global_series_per_user: 2000000
```

Request:
```json
{
  "ingestion_rate": 150000
}
```

Response (400 Bad Request):
```
limit ingestion_rate exceeds hard limit: 150000 > 100000
```

### Hard Limits vs. Default Limits

| Configuration | Purpose | Scope |
|--------------|---------|-------|
| Default limits (`limits_config`) | Global defaults for all tenants | All tenants |
| Overrides (`overrides`) | Per-tenant custom limits | Specific tenants |
| Hard limits (`hard_overrides`) | Maximum allowed override values | Specific tenants |

**Hierarchy:**
```
Default Limits (global)
  ↓
Tenant Overrides (per-tenant, within hard limits)
  ↓
Hard Limits (per-tenant maximum)
```

## Usage Examples

### Example 1: Initial Override Setup

Set initial overrides for a new tenant:

```bash
curl -X POST http://cortex:8080/api/v1/user-overrides \
  -H "X-Scope-OrgID: tenant1" \
  -H "Content-Type: application/json" \
  -d '{
    "ingestion_rate": 50000,
    "max_global_series_per_user": 1000000,
    "ruler_max_rules_per_rule_group": 50
  }'
```

### Example 2: Update Specific Limit

Update only the ingestion rate while preserving other overrides:

```bash
curl -X POST http://cortex:8080/api/v1/user-overrides \
  -H "X-Scope-OrgID: tenant1" \
  -H "Content-Type: application/json" \
  -d '{
    "ingestion_rate": 75000
  }'
```

Result: `ingestion_rate` updated to 75000, other limits remain unchanged.

### Example 3: View Current Overrides

```bash
curl -X GET http://cortex:8080/api/v1/user-overrides \
  -H "X-Scope-OrgID: tenant1"
```

Response:
```json
{
  "ingestion_rate": 75000,
  "max_global_series_per_user": 1000000,
  "ruler_max_rules_per_rule_group": 50
}
```

### Example 4: Remove All Overrides

```bash
curl -X DELETE http://cortex:8080/api/v1/user-overrides \
  -H "X-Scope-OrgID: tenant1"
```

Result: Tenant reverts to global default limits.

### Example 5: Handling Validation Errors

Attempt to set a disallowed limit:

```bash
curl -X POST http://cortex:8080/api/v1/user-overrides \
  -H "X-Scope-OrgID: tenant1" \
  -H "Content-Type: application/json" \
  -d '{
    "ingestion_rate": 50000,
    "some_invalid_limit": 100
  }'
```

Response (400):
```
the following limits cannot be modified via the overrides API: some_invalid_limit
```

Attempt to exceed hard limit:

```bash
curl -X POST http://cortex:8080/api/v1/user-overrides \
  -H "X-Scope-OrgID: tenant1" \
  -H "Content-Type: application/json" \
  -d '{
    "ingestion_rate": 200000
  }'
```

Response (400):
```
limit ingestion_rate exceeds hard limit: 200000 > 100000
```

## Operational Considerations

### Security

- **Authentication**: All endpoints require valid tenant authentication via `X-Scope-OrgID` header
- **Authorization**: Tenants can only manage their own overrides
- **Allowed limits**: Use `api_allowed_limits` to restrict which limits can be modified
- **Hard limits**: Use `hard_overrides` to enforce maximum values per tenant

### High Availability

- The overrides module can run on multiple instances for high availability
- All instances read/write to the same runtime configuration storage backend
- Changes are eventually consistent based on `runtime-config.period`

### Best Practices

1. **Use hard limits**: Always configure `hard_overrides` to prevent runaway resource usage
2. **Restrict allowed limits**: Only expose limits via `api_allowed_limits` that are safe for self-service
3. **Monitor changes**: Track when overrides are modified and by whom
4. **Version control**: Keep runtime configuration in version control for audit trail
5. **Gradual rollout**: Test override changes in non-production environments first

## Troubleshooting

### "user not found" error when getting overrides

This means the tenant has no overrides configured. This is normal for new tenants. An empty JSON object `{}` is returned.

### Changes not taking effect immediately

The runtime config is reloaded periodically based on `runtime-config.period` (default: 10s). Changes may take up to this duration to be applied.

### "failed to validate hard limits" error

This indicates an issue reading or parsing the runtime configuration file. Check:
- Runtime config file is accessible
- YAML syntax is valid
- Storage backend is properly configured

### Overrides being reset

If overrides are unexpectedly reset, check:
- Multiple instances are not writing conflicting configurations
- Runtime config file is not being manually edited simultaneously
- Storage backend has sufficient permissions

## See Also

- [Limits Configuration Reference](../configuration/config-file-reference.md#limits_config)
- [Runtime Configuration](../configuration/arguments.md#runtime-configuration-file)
- [Overrides Exporter](./overrides-exporter.md)
- [API Documentation](../api/_index.md#overrides)
