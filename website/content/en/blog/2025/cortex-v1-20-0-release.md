---
date: 2025-11-10
title: "Cortex v1.20.0 Released: Enhanced Scalability, New Storage Formats, and Improved Observability"
linkTitle: Cortex v1.20.0 Release
tags: [ "release", "cortex", "prometheus", "metrics", "observability" ]
categories: [ "releases" ]
projects: [ "cortex" ]
description: >
  We're excited to announce the release of Cortex v1.20.0, bringing experimental support for Prometheus Remote Write 2.0, Parquet-based storage, advanced query federation, and more. Discover the new features and how they benefit Cortex users.
author: The Cortex Team
---

We're thrilled to announce the release of **Cortex v1.20.0**! This version introduces several experimental features and enhancements that push the boundaries of scalable, multi-tenant metrics storage for Prometheus and OpenTelemetry. With 371 contributions from 38 contributors, including 14 new ones, this release focuses on improving performance, flexibility, and operational efficiency.

## Key Highlights

### Experimental Prometheus Remote Write 2.0 Support
Cortex now supports the experimental Prometheus Remote Write 2.0 protocol. This update enables more efficient data transmission, reducing overhead and improving ingestion performance for high-volume metrics environments. Users can benefit from better compression and faster writes, making it ideal for large-scale deployments.

### Parquet Format Support (Experimental)
A groundbreaking addition is experimental support for Parquet-based block storage. This includes:
- A new **Parquet converter service** to transform TSDB blocks into Parquet format.
- A **Parquet querier** for querying Parquet files directly.

Parquet's columnar storage offers superior compression and query performance, especially for analytical workloads. This feature allows users to optimize storage costs and query speeds for historical data, potentially reducing infrastructure expenses by up to 50% in certain scenarios.

### Advanced Query Federation with Regex Tenant Resolver
The experimental regex tenant resolver enhances multi-tenant query federation. By enabling the `-tenant-federation.regex-matcher-enabled` flag, users can use regex patterns in the `X-Scope-OrgID` header. This simplifies querying across multiple tenants, improving operational workflows for organizations with complex tenant structures.

### gRPC Stream Push for Distributor-Ingester Communication
An experimental feature introduces gRPC stream connections for push requests between distributors and ingesters. This reduces connection overhead and improves reliability in high-throughput environments, leading to more stable and efficient metric ingestion.

### Enhanced Native Histogram Support
Building on previous histogram capabilities, v1.20.0 adds:
- Out-of-order native histogram ingestion (when `-ingester.out-of-order-time-window > 0` and native histograms are enabled).
- Per-tenant native histogram ingestion configuration.
- New metrics and limits for active native histogram series.

These improvements provide better observability for histogram-based metrics, allowing users to capture more nuanced data distributions without sacrificing performance.

### Resource-Based Monitoring and Limiting
A new `ResourceMonitor` module collects CPU and heap usage metrics across Cortex components. Paired with `ResourceBasedLimiter` in ingesters and store gateways, this protects services from overload by throttling requests when resource limits are approached. Users gain automatic safeguards against cascading failures, ensuring higher availability and predictable performance.

### UTF-8 Name Validation
Support for UTF-8 metric names via the `-name-validation-scheme` flag aligns Cortex with modern Prometheus standards, enabling more expressive and internationalized metric naming.

## Additional Improvements
- **Dynamic Query Splitting**: Experimental flags for adaptive query interval sizing to optimize shard management.
- **Percentage-Based Sharding**: For rulers and compactors, improving load distribution.
- **Prometheus Upgrade**: Updated to v3.6.0, bringing the latest features and fixes.
- **Alertmanager Enhancements**: Upgraded to v0.28.0 with new integrations (MSTeams v2, Jira, RocketChat) and better limits.
- **Query Frontend Additions**: New APIs for query formatting and parsing, plus enhanced metrics.
- Numerous bug fixes, performance optimizations, and operational improvements.

## Benefits for Cortex Users
- **Cost Efficiency**: Parquet storage and improved compression reduce storage and bandwidth costs.
- **Scalability**: New sharding and limiting features handle larger workloads more gracefully.
- **Flexibility**: Experimental features provide options for advanced use cases like cross-tenant queries.
- **Reliability**: Resource monitoring and stream pushes enhance system stability.
- **Future-Proofing**: Support for Remote Write 2.0 and UTF-8 prepares for evolving standards.

## Upgrading
As always, review the changelog and upgrade guide before upgrading. Experimental features should be tested in staging environments first.

## Community and Thanks
This release wouldn't be possible without our vibrant community. Special thanks to all contributors, especially the 14 new ones making their first contributions. Join the conversation on Slack or GitHub Discussions.

Stay tuned for more updates, and happy monitoring!

The Cortex Team