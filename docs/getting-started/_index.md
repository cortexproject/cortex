---
title: "Getting Started"
linkTitle: "Getting Started"
weight: 1
no_section_index_title: true
slug: "getting-started"
---

Welcome to Cortex! This guide will help you get a Cortex environment up and running quickly.

## What is Cortex?

Cortex is a horizontally scalable, highly available, multi-tenant, long-term storage solution for Prometheus and OpenTelemetry Metrics. It can be run in two modes:

- **Single Binary Mode**: All components run in a single process - ideal for testing, development, and learning
- **Microservices Mode**: Components run as independent services - designed for production deployments

Both deployment modes use [blocks storage](../blocks-storage/_index.md), which is based on Prometheus TSDB and stores data in object storage (S3, GCS, Azure, or compatible services).

## Choose Your Guide

| Mode | Time | Use Case | Guide |
|------|------|----------|-------|
| **Single Binary** | ~15 min | Learning, Development, Testing | [Start Here →](single-binary.md) |
| **Microservices** | ~30 min | Production-like Environment, Kubernetes | [Start Here →](microservices.md) |

### Single Binary Mode

Perfect for your first experience with Cortex. Runs all components in one process with minimal dependencies.

**What you'll set up:**
- Cortex (single process)
- Prometheus (sending metrics via remote_write)
- Grafana (visualizing metrics)
- SeaweedFS (S3-compatible storage)

**Requirements:**
- Docker & Docker Compose
- 4GB RAM, 10GB disk

[Get Started with Single Binary Mode →](single-binary.md)

### Microservices Mode

Experience Cortex as it runs in production. Each component runs as a separate service in Kubernetes.

**What you'll set up:**
- Cortex (distributed: ingester, querier, distributor, compactor, etc.)
- Prometheus (sending metrics via remote_write)
- Grafana (visualizing metrics)
- SeaweedFS (S3-compatible storage)

**Requirements:**
- Kind, kubectl, Helm
- 8GB RAM, 20GB disk

[Get Started with Microservices Mode →](microservices.md)

## Key Concepts

Before you begin, it's helpful to understand these core concepts:

- **Blocks Storage**: Cortex's storage engine based on Prometheus TSDB. Metrics are stored in 2-hour blocks in object storage.
- **Multi-tenancy**: Cortex isolates metrics by tenant ID (sent via `X-Scope-OrgID` header). In these guides, we use `cortex` as the tenant ID.
- **Remote Write**: Prometheus protocol for sending metrics to remote storage systems like Cortex.
- **Components**: In microservices mode, Cortex runs as separate services (distributor, ingester, querier, etc.). In single binary mode, all run together.

## Data Flow

```
Prometheus → remote_write → Cortex → Object Storage (S3)
                               ↓
                            Grafana (queries via PromQL)
```

## Need Help?

- **Documentation**: Explore the [Architecture guide](../architecture.md) to understand Cortex's design
- **Community**: Join the [CNCF Slack #cortex channel](https://cloud-native.slack.com/archives/cortex)
- **Issues**: Report problems on [GitHub](https://github.com/cortexproject/cortex/issues)
