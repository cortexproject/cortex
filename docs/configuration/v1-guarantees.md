---
title: "v1.x Guarantees"
linkTitle: "v1.x Guarantees"
weight: 6
slug: v1guarantees
---

For the v1.0 release, we want to provide the following guarantees:

## Flags, Config and minor version upgrades

Upgrading cortex from one minor version to the next should "just work"; that being said, we don't want to bump the major version every time we remove a flag, so we will will keep deprecated flags around for 2 minor release.  There is a metric (`cortex_deprecated_flags_inuse_total`) you can alert on to find out if you're using a deprecated  flag.

Similarly to flags, minor version upgrades using config files should "just work".  If we do need to change config, we will keep the old way working for two minor version.  There will be a metric you can alert on for this too.

These guarantees don't apply for [experimental features](#experimental-features).

## Reading old data

The Cortex maintainers commit to ensuring future version of Cortex can read data written by versions up to two years old. In practice we expect to be able to read more, but this is our guarantee.

## API Compatibility

Cortex strives to be 100% API compatible with Prometheus (under `/api/prom/*`); any deviation from this is considered a bug, except:

- Requiring the `__name__` label on queries.
- Additional API endpoints for creating, removing and modifying alerts and recording rules.
- Additional API around pushing metrics (under `/api/push`).
- Additional API endpoints for management of Cortex itself, such as the ring.  These APIs are not part of the any compatibility guarantees.

## Experimental features

Cortex is an actively developed project and we want to encourage the introduction of new features and capability.  As such, not everything in each release of Cortex is considered "production-ready". We don't provide any backwards compatibility guarantees on these and the config and flags might break.

Currently experimental features are:

- TSDB block storage.
- Ingester chunk WAL.
- Cassandra storage engine.
- Azure blob storage.
- Zone awareness based replication.
- Gossip based ring.
- User subrings.
- Ruler API (to PUT rules).
- Memcached client DNS-based service discovery.
- Delete series APIs.
- In-memory (FIFO) and Redis cache.
