---
title: "Prometheus Frontend"
linkTitle: "Prometheus Frontend"
weight: 3
slug: prometheus-frontend
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

query_range:
  split_queries_by_interval: 24h
  align_queries_with_step: true
  cache_results: true

  results_cache:
    cache:

      # We're going to use the in-process "FIFO" cache, but you can enable
      # memcached below.
      enable_fifocache: true
      fifocache:
        size: 1024
        validity: 24h

      # If you want to use a memcached cluster, you can either configure a
      # headless service in Kubernetes and Cortex will discover the individual
      # instances using a SRV DNS query (host) or list comma separated
      # memcached addresses.
      # host + service: this is the config you should set when you use the
      # SRV DNS (this is considered stable)
      # addresses: this is experimental and supports service discovery
      # (https://cortexmetrics.io/docs/configuration/arguments/#dns-service-discovery)
      # so it could either be a list of single addresses, or a SRV record
      # prefixed with dnssrvnoa+. Cortex will then do client-side hashing to
      # spread the load evenly.

      # memcached:
      #   expiration : 24h
      # memcached_client:
      #   host: memcached.default.svc.cluster.local
      #   service: memcached
      #   addresses: ""
      #   consistent_hash: true

frontend:
  log_queries_longer_than: 1s
  compress_responses: true

  # The Prometheus URL to which the query-frontend should connect to.
  downstream_url: http://prometheus.mydomain.com
```
