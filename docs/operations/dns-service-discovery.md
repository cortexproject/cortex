---
title: "DNS service discovery"
linkTitle: "DNS service discovery"
weight: 2
slug: dns-service-discovery
---

Some clients in Cortex support service discovery via DNS to find addresses of backend servers to connect to (ie. caching servers). The clients supporting it are:

- [Blocks storage's memcached index cache](blocks-storage.md#memcached-index-cache)

## Supported discovery modes

The DNS service discovery supports different discovery modes. A discovery mode is selected adding a specific prefix to the address. The supported prefixes are:

- **`dns+`**<br />
  The domain name after the prefix is looked up as an A/AAAA query. For example: `dns+memcached.local:11211`
- **`dnssrv+`**<br />
  The domain name after the prefix is looked up as a SRV query, and then each SRV record is resolved as an A/AAAA record. For example: `dnssrv+memcached.namespace.svc.cluster.local`
- **`dnssrvnoa+`**<br />
  The domain name after the prefix is looked up as a SRV query, with no A/AAAA lookup made after that. For example: `dnssrvnoa+memcached.namespace.svc.cluster.local`
