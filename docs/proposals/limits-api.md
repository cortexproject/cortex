---
title: "Limits API"
linkTitle: "Limits API"
weight: 1
slug: limits-api
---

- Author: Bogdan Stancu
- Date: June 2025
- Status: Proposed

## Overview

This proposal outlines the design for a new API endpoint that will allow users to modify their current limits in Cortex. Currently, limits can only be changed by administrators modifying the runtime configuration file and waiting for it to be reloaded.

## Problem

Currently, when users need limit adjustments, they must:
1. Manually editing the runtime configuration file
2. Coordinating with users to verify the changes
3. Potentially repeating this process multiple times to find the right balance

This manual process is time-consuming, error-prone, and doesn't scale well with a large number of users. By offering a self-service API, users can adjust their own limits within predefined boundaries, reducing the administrative overhead and improving the user experience.

## Proposed API Design

### Endpoints

#### 1. GET /api/v1/limits/{tenant_id}
Returns the current limits configuration for a specific tenant.

Response format:
```json
{
  "ingestion_rate": 10000,
  "ingestion_burst_size": 20000,
  "max_global_series_per_user": 1000000,
  "max_global_series_per_metric": 200000,
  ...
}
```

#### 2. PUT /api/v1/limits/{tenant_id}
Updates limits for a specific tenant. The request body should contain only the limits that need to be updated.

Request body:
```json
{
  "ingestion_rate": 10000,
  "max_series_per_metric": 100000
}
```

#### 3. DELETE /api/v1/limits/{tenant_id}
Removes tenant-specific limits, reverting to default limits.

### Implementation Details

1. The API will be integrated into the existing Cortex components to:
   - Read the current runtime config from the configured storage backend
   - Apply changes to the in-memory configuration
   - Persist changes back to the storage backend
   - Trigger a reload of the runtime config

2. Security:
   - The API will require admin-level authentication
   - Rate limiting will be implemented to prevent abuse
   - Changes will be validated before being applied

3. Error Handling:
   - Invalid limit values will return 400 Bad Request
   - Storage backend errors will return 500 Internal Server Error

