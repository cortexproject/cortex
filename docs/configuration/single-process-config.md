---
title: "Single-process"
linkTitle: "Single-process"
weight: 5
slug: single-process-config
---

Configuration for running Cortex in single-process mode.
This should not be used in production.  It is only for getting started
and development.

[embedmd]:# (./single-process-config.yaml)
```yaml

# Configuration for running Cortex in single-process mode.
# This configuration should not be used in production.
# It is only for getting started and development.

# Disable the requirement that every request to Cortex has a
# X-Scope-OrgID header. `fake` will be substituted in instead.
auth_enabled: false

server:
  http_listen_port: 9009

  # Configure the server to allow messages up to 100MB.
  grpc_server_max_recv_msg_size: 104857600
  grpc_server_max_send_msg_size: 104857600
  grpc_server_max_concurrent_streams: 1000

distributor:
  shard_by_all_labels: true
  pool:
    health_check_ingesters: true

ingester_client:
  grpc_client_config:
    # Configure the client to allow messages up to 100MB.
    max_recv_msg_size: 104857600
    max_send_msg_size: 104857600
    grpc_compression: gzip

ingester:
  # We want our ingesters to flush chunks at the same time to optimise
  # deduplication opportunities.
  spread_flushes: true
  chunk_age_jitter: 0

  walconfig:
    wal_enabled: true
    recover_from_wal: true
    wal_dir: /tmp/cortex/wal

  lifecycler:
    # The address to advertise for this ingester.  Will be autodiscovered by
    # looking up address on eth0 or en0; can be specified if this fails.
    # address: 127.0.0.1

    # We want to start immediately and flush on shutdown.
    join_after: 0
    min_ready_duration: 0s
    final_sleep: 0s
    num_tokens: 512
    tokens_file_path: /tmp/cortex/wal/tokens

    # Use an in memory ring store, so we don't need to launch a Consul.
    ring:
      kvstore:
        store: inmemory
      replication_factor: 1

# Use local storage - BoltDB for the index, and the filesystem
# for the chunks.
schema:
  configs:
  - from: 2019-07-29
    store: boltdb
    object_store: filesystem
    schema: v10
    index:
      prefix: index_
      period: 1w

storage:
  boltdb:
    directory: /tmp/cortex/index

  filesystem:
    directory: /tmp/cortex/chunks

  delete_store:
    store: boltdb

purger:
  object_store_type: filesystem

frontend_worker:
  # Configure the frontend worker in the querier to match worker count
  # to max_concurrent on the queriers.
  match_max_concurrent: true

# Configure the ruler to scan the /tmp/cortex/rules directory for prometheus
# rules: https://prometheus.io/docs/prometheus/latest/configuration/recording_rules/#recording-rules
ruler:
  enable_api: true
  enable_sharding: false

ruler_storage:
  backend: local
  local:
    directory: /tmp/cortex/rules
```
