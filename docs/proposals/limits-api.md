---
title: "User Overrides API"
linkTitle: "User Overrides API"
weight: 1
slug: overrides-api
---

- Author: Bogdan Stancu
- Date: June 2025
- Status: Proposed

## Overview

This proposal outlines the design for a new API endpoint that will allow users to modify their current limits in Cortex. Currently, overrides can only be changed by administrators modifying the runtime configuration file and waiting for it to be reloaded.

## Problem

Currently, when users need limit adjustments, they must:
1. Manually editing the runtime configuration file
2. Coordinate with users to verify the changes
3. Potentially repeating this process multiple times to find the right balance

This manual process is time-consuming, error-prone, and doesn't scale well with a large number of users. By offering a self-service API, users can adjust their own limits within predefined boundaries, reducing the administrative overhead and improving the user experience.

## Proposed API Design

### Endpoints

#### 1. GET /api/v1/user-overrides
Returns the current overrides configuration for a specific tenant.

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

#### 2. PUT /api/v1/user-overrides
Updates overrides for a specific tenant. The request body should contain only the overrides that need to be updated.

Request body:
```json
{
  "ingestion_rate": 10000,
  "max_series_per_metric": 100000
}
```

#### 3. DELETE /api/v1/user-overrides
Removes tenant-specific overrides, reverting to default overrides.

### Implementation Details

1. The API will be integrated into the cortex-overrides component to:
   - Read the current runtime config from the configured storage backend
   - Persist changes back to the storage backend
   - The API will only work with configurations stored in block storage backends.

2. Security:
   - Rate limiting will be implemented to prevent abuse
   - Changes will be validated before being applied
   - A hard limit configuration will be implemented  
   Hard limits will not be changable through the API  
   Example:
```yaml
   # file: runtime.yaml
   # In this example, we're overriding ingestion limits for a single tenant.
   overrides:
   "user1":
      ingestion_burst_size: 350000
      ingestion_rate: 350000
      max_global_series_per_metric: 300000
      max_global_series_per_user: 300000
      max_series_per_metric: 0
      max_series_per_user: 0
      max_samples_per_query: 100000
      max_series_per_query: 100000
   configurable-overrides: # still not sure about the naming for this section
   "user1":
      ingestion_rate: 700000
      max_global_series_per_user: 700000
```

3. Error Handling:
   - Invalid limit values will return 400 Bad Request
   - Storage backend errors will return 500 Internal Server Error

