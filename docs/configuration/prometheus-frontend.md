---
title: "Prometheus Frontend"
linkTitle: "Prometheus Frontend"
weight: 2
slug: prometheus-frontend.md
---


You can use the Cortex query frontend with any Prometheus-API compatible
service, including Prometheus and Thanos.  Use this config file to get
the benefits of query parallelisation and caching.

```yaml
# Disable the requirement that every request to Cortex has a
# X-Scope-OrgID header. `fake` will be substituted in instead.
auth_enabled: false

# We only want to run the query-frontend module.
target: query-frontend

# We don't want the usual /api/prom prefix.
http_prefix:

server:
  http_listen_port: 9091

frontend:
  log_queries_longer_than: 1s
  split_queries_by_day: true
  align_queries_with_step: true
  cache_results: true
  compress_responses: true

  results_cache:
    max_freshness: 1m
    cache:

      # We're going to use the in-process "FIFO" cache, but you can enable
      # memcached below.
      enable_fifocache: true
      fifocache:
        size: 1024
        validity: 24h

      # If you want to use a memcached cluster, configure a headless service
      # in Kubernetes and Cortex will discover the individual instances using
      # a SRV DNS query.  Cortex will then do client-side hashing to spread
      # the load evenly.
      # memcached:
      #   memcached_client:
      #     host: memcached.default.svc.cluster.local
      #     service: memcached
      #     consistent_hash: true
```