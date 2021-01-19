{
  server_config:: {
    new(): {
      http_listen_address: "",
      http_listen_port: 80,
      http_listen_conn_limit: 0,
      grpc_listen_address: "",
      grpc_listen_port: 9095,
      grpc_listen_conn_limit: 0,
      http_tls_config: {
        cert_file: "",
        key_file: "",
        client_auth_type: "",
        client_ca_file: ""
      },
      grpc_tls_config: {
        cert_file: "",
        key_file: "",
        client_auth_type: "",
        client_ca_file: ""
      },
      register_instrumentation: true,
      graceful_shutdown_timeout: "30s",
      http_server_read_timeout: "30s",
      http_server_write_timeout: "30s",
      http_server_idle_timeout: "2m0s",
      grpc_server_max_recv_msg_size: 4194304,
      grpc_server_max_send_msg_size: 4194304,
      grpc_server_max_concurrent_streams: 100,
      grpc_server_max_connection_idle: "2562047h47m16.854775807s",
      grpc_server_max_connection_age: "2562047h47m16.854775807s",
      grpc_server_max_connection_age_grace: "2562047h47m16.854775807s",
      grpc_server_keepalive_time: "2h0m0s",
      grpc_server_keepalive_timeout: "20s",
      grpc_server_min_time_between_pings: "5m0s",
      grpc_server_ping_without_stream_allowed: false,
      log_format: "logfmt",
      log_level: "info",
      log_source_ips_enabled: false,
      log_source_ips_header: "",
      log_source_ips_regex: "",
      http_path_prefix: ""
    }
  },
  distributor_config:: {
    new(): {
      pool: {
        client_cleanup_period: "15s",
        health_check_ingesters: true
      },
      ha_tracker: {
        enable_ha_tracker: false,
        ha_tracker_update_timeout: "15s",
        ha_tracker_update_timeout_jitter_max: "5s",
        ha_tracker_failover_timeout: "30s",
        kvstore: {
          store: "consul",
          prefix: "ha-tracker/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        }
      },
      max_recv_msg_size: 104857600,
      remote_timeout: "2s",
      extra_queue_delay: "0s",
      sharding_strategy: "default",
      shard_by_all_labels: false,
      ring: {
        kvstore: {
          store: "consul",
          prefix: "collectors/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "5s",
        heartbeat_timeout: "1m0s",
        instance_interface_names: ["eth0", "en0"]
      }
    }
  },
  consul_config:: {
    new(): {
      host: "localhost:8500",
      acl_token: "",
      http_client_timeout: "20s",
      consistent_reads: false,
      watch_rate_limit: 1.000000,
      watch_burst_size: 1
    }
  },
  etcd_config:: {
    new(): {
      endpoints: [],
      dial_timeout: "10s",
      max_retries: 10,
      tls_enabled: false,
      tls_cert_path: "",
      tls_key_path: "",
      tls_ca_path: "",
      tls_insecure_skip_verify: false
    }
  },
  querier_config:: {
    new(): {
      max_concurrent: 20,
      timeout: "2m0s",
      iterators: false,
      batch_iterators: true,
      ingester_streaming: true,
      max_samples: 50000000,
      query_ingesters_within: "0s",
      query_store_for_labels_enabled: false,
      query_store_after: "0s",
      max_query_into_future: "10m0s",
      default_evaluation_interval: "1m0s",
      active_query_tracker_dir: "./active-query-tracker",
      lookback_delta: "5m0s",
      store_gateway_addresses: "",
      store_gateway_client: {
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      },
      second_store_engine: "",
      use_second_store_before_time: "0",
      shuffle_sharding_ingesters_lookback_period: "0s"
    }
  },
  ingester_config:: {
    new(): {
      walconfig: {
        wal_enabled: false,
        checkpoint_enabled: true,
        recover_from_wal: false,
        wal_dir: "wal",
        checkpoint_duration: "30m0s",
        flush_on_shutdown_with_wal_enabled: false
      },
      lifecycler: {
        ring: {
          kvstore: {
            store: "consul",
            prefix: "collectors/",
            consul: {
              host: "localhost:8500",
              acl_token: "",
              http_client_timeout: "20s",
              consistent_reads: false,
              watch_rate_limit: 1.000000,
              watch_burst_size: 1
            },
            etcd: {
              endpoints: [],
              dial_timeout: "10s",
              max_retries: 10,
              tls_enabled: false,
              tls_cert_path: "",
              tls_key_path: "",
              tls_ca_path: "",
              tls_insecure_skip_verify: false
            },
            multi: {
              primary: "",
              secondary: "",
              mirror_enabled: false,
              mirror_timeout: "2s"
            }
          },
          heartbeat_timeout: "1m0s",
          replication_factor: 3,
          zone_awareness_enabled: false,
          extend_writes: true
        },
        num_tokens: 128,
        heartbeat_period: "5s",
        observe_period: "0s",
        join_after: "0s",
        min_ready_duration: "1m0s",
        interface_names: ["eth0", "en0"],
        final_sleep: "30s",
        tokens_file_path: "",
        availability_zone: "",
        unregister_on_shutdown: true
      },
      max_transfer_retries: 10,
      flush_period: "1m0s",
      retain_period: "5m0s",
      max_chunk_idle_time: "5m0s",
      max_stale_chunk_idle_time: "2m0s",
      flush_op_timeout: "1m0s",
      max_chunk_age: "12h0m0s",
      chunk_age_jitter: "0s",
      concurrent_flushes: 50,
      spread_flushes: true,
      metadata_retain_period: "10m0s",
      rate_update_period: "15s",
      active_series_metrics_enabled: false,
      active_series_metrics_update_period: "1m0s",
      active_series_metrics_idle_timeout: "10m0s"
    }
  },
  flusher_config:: {
    new(): {
      wal_dir: "wal",
      concurrent_flushes: 50,
      flush_op_timeout: "2m0s",
      exit_after_flush: true
    }
  },
  storage_config:: {
    new(): {
      engine: "chunks",
      aws: {
        dynamodb: {
          dynamodb_url: "",
          api_limit: 2.000000,
          throttle_limit: 10.000000,
          metrics: {
            url: "",
            target_queue_length: 100000,
            scale_up_factor: 1.300000,
            ignore_throttle_below: 1.000000,
            queue_length_query: "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))",
            write_throttle_query: "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) \u003e 0",
            write_usage_query: "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) \u003e 0",
            read_usage_query: "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) \u003e 0",
            read_error_query: "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) \u003e 0"
          },
          chunk_gang_size: 10,
          chunk_get_max_parallelism: 32,
          backoff_config: {
            min_period: "100ms",
            max_period: "50s",
            max_retries: 20
          }
        },
        s3: "",
        s3forcepathstyle: false,
        bucketnames: "",
        endpoint: "",
        region: "",
        access_key_id: "",
        secret_access_key: "",
        insecure: false,
        sse_encryption: false,
        http_config: {
          idle_conn_timeout: "1m30s",
          response_header_timeout: "0s",
          insecure_skip_verify: false
        },
        signature_version: "v4"
      },
      azure: {
        environment: "AzureGlobal",
        container_name: "cortex",
        account_name: "",
        account_key: "",
        download_buffer_size: 512000,
        upload_buffer_size: 256000,
        upload_buffer_count: 1,
        request_timeout: "30s",
        max_retries: 5,
        min_retry_delay: "10ms",
        max_retry_delay: "500ms"
      },
      bigtable: {
        project: "",
        instance: "",
        grpc_client_config: {
          max_recv_msg_size: 104857600,
          max_send_msg_size: 16777216,
          grpc_compression: "",
          rate_limit: 0.000000,
          rate_limit_burst: 0,
          backoff_on_ratelimits: false,
          backoff_config: {
            min_period: "100ms",
            max_period: "10s",
            max_retries: 10
          }
        },
        table_cache_enabled: true,
        table_cache_expiration: "30m0s"
      },
      gcs: {
        bucket_name: "",
        chunk_buffer_size: 0,
        request_timeout: "0s"
      },
      cassandra: {
        addresses: "",
        port: 9042,
        keyspace: "",
        consistency: "QUORUM",
        replication_factor: 3,
        disable_initial_host_lookup: false,
        SSL: false,
        host_verification: true,
        CA_path: "",
        tls_cert_path: "",
        tls_key_path: "",
        auth: false,
        username: "",
        password: "",
        password_file: "",
        custom_authenticators: [],
        timeout: "2s",
        connect_timeout: "5s",
        reconnect_interval: "1s",
        max_retries: 0,
        retry_max_backoff: "10s",
        retry_min_backoff: "100ms",
        query_concurrency: 0,
        num_connections: 2,
        convict_hosts_on_failure: true,
        table_options: ""
      },
      boltdb: {
        directory: ""
      },
      filesystem: {
        directory: ""
      },
      swift: {
        auth_version: 0,
        auth_url: "",
        username: "",
        user_domain_name: "",
        user_domain_id: "",
        user_id: "",
        password: "",
        domain_id: "",
        domain_name: "",
        project_id: "",
        project_name: "",
        project_domain_id: "",
        project_domain_name: "",
        region_name: "",
        container_name: "",
        max_retries: 3,
        connect_timeout: "10s",
        request_timeout: "5s"
      },
      index_cache_validity: "5m0s",
      index_queries_cache_config: {
        enable_fifocache: false,
        default_validity: "0s",
        background: {
          writeback_goroutines: 10,
          writeback_buffer: 10000
        },
        memcached: {
          expiration: "0s",
          batch_size: 1024,
          parallelism: 100
        },
        memcached_client: {
          host: "",
          service: "memcached",
          addresses: "",
          timeout: "100ms",
          max_idle_conns: 16,
          update_interval: "1m0s",
          consistent_hash: true,
          circuit_breaker_consecutive_failures: 10,
          circuit_breaker_timeout: "10s",
          circuit_breaker_interval: "10s"
        },
        redis: {
          endpoint: "",
          master_name: "",
          timeout: "500ms",
          expiration: "0s",
          db: 0,
          pool_size: 0,
          password: "",
          tls_enabled: false,
          tls_insecure_skip_verify: false,
          idle_timeout: "0s",
          max_connection_age: "0s"
        },
        fifocache: {
          max_size_bytes: "",
          max_size_items: 0,
          validity: "0s",
          size: 0
        }
      },
      delete_store: {
        store: "",
        requests_table_name: "delete_requests",
        table_provisioning: {
          enable_ondemand_throughput_mode: false,
          provisioned_write_throughput: 1,
          provisioned_read_throughput: 300,
          write_scale: {
            enabled: false,
            role_arn: "",
            min_capacity: 3000,
            max_capacity: 6000,
            out_cooldown: 1800,
            in_cooldown: 1800,
            target: 80.000000
          },
          read_scale: {
            enabled: false,
            role_arn: "",
            min_capacity: 3000,
            max_capacity: 6000,
            out_cooldown: 1800,
            in_cooldown: 1800,
            target: 80.000000
          },
          tags: {}
        }
      },
      grpc_store: {
        server_address: ""
      }
    }
  },
  memcached_config:: {
    new(): {
      expiration: "0s",
      batch_size: 1024,
      parallelism: 100
    }
  },
  memcached_client_config:: {
    new(): {
      host: "",
      service: "memcached",
      addresses: "",
      timeout: "100ms",
      max_idle_conns: 16,
      update_interval: "1m0s",
      consistent_hash: true,
      circuit_breaker_consecutive_failures: 10,
      circuit_breaker_timeout: "10s",
      circuit_breaker_interval: "10s"
    }
  },
  redis_config:: {
    new(): {
      endpoint: "",
      master_name: "",
      timeout: "500ms",
      expiration: "0s",
      db: 0,
      pool_size: 0,
      password: "",
      tls_enabled: false,
      tls_insecure_skip_verify: false,
      idle_timeout: "0s",
      max_connection_age: "0s"
    }
  },
  fifo_cache_config:: {
    new(): {
      max_size_bytes: "",
      max_size_items: 0,
      validity: "0s",
      size: 0
    }
  },
  chunk_store_config:: {
    new(): {
      chunk_cache_config: {
        enable_fifocache: false,
        default_validity: "0s",
        background: {
          writeback_goroutines: 10,
          writeback_buffer: 10000
        },
        memcached: {
          expiration: "0s",
          batch_size: 1024,
          parallelism: 100
        },
        memcached_client: {
          host: "",
          service: "memcached",
          addresses: "",
          timeout: "100ms",
          max_idle_conns: 16,
          update_interval: "1m0s",
          consistent_hash: true,
          circuit_breaker_consecutive_failures: 10,
          circuit_breaker_timeout: "10s",
          circuit_breaker_interval: "10s"
        },
        redis: {
          endpoint: "",
          master_name: "",
          timeout: "500ms",
          expiration: "0s",
          db: 0,
          pool_size: 0,
          password: "",
          tls_enabled: false,
          tls_insecure_skip_verify: false,
          idle_timeout: "0s",
          max_connection_age: "0s"
        },
        fifocache: {
          max_size_bytes: "",
          max_size_items: 0,
          validity: "0s",
          size: 0
        }
      },
      write_dedupe_cache_config: {
        enable_fifocache: false,
        default_validity: "0s",
        background: {
          writeback_goroutines: 10,
          writeback_buffer: 10000
        },
        memcached: {
          expiration: "0s",
          batch_size: 1024,
          parallelism: 100
        },
        memcached_client: {
          host: "",
          service: "memcached",
          addresses: "",
          timeout: "100ms",
          max_idle_conns: 16,
          update_interval: "1m0s",
          consistent_hash: true,
          circuit_breaker_consecutive_failures: 10,
          circuit_breaker_timeout: "10s",
          circuit_breaker_interval: "10s"
        },
        redis: {
          endpoint: "",
          master_name: "",
          timeout: "500ms",
          expiration: "0s",
          db: 0,
          pool_size: 0,
          password: "",
          tls_enabled: false,
          tls_insecure_skip_verify: false,
          idle_timeout: "0s",
          max_connection_age: "0s"
        },
        fifocache: {
          max_size_bytes: "",
          max_size_items: 0,
          validity: "0s",
          size: 0
        }
      },
      cache_lookups_older_than: "0s",
      max_look_back_period: "0s"
    }
  },
  limits_config:: {
    new(): {
      ingestion_rate: 25000.000000,
      ingestion_rate_strategy: "local",
      ingestion_burst_size: 50000,
      accept_ha_samples: false,
      ha_cluster_label: "cluster",
      ha_replica_label: "__replica__",
      ha_max_clusters: 0,
      drop_labels: [],
      max_label_name_length: 1024,
      max_label_value_length: 2048,
      max_label_names_per_series: 30,
      max_metadata_length: 1024,
      reject_old_samples: false,
      reject_old_samples_max_age: "336h0m0s",
      creation_grace_period: "10m0s",
      enforce_metadata_metric_name: true,
      enforce_metric_name: true,
      ingestion_tenant_shard_size: 0,
      metric_relabel_configs: [],
      max_series_per_query: 100000,
      max_samples_per_query: 1000000,
      max_series_per_user: 5000000,
      max_series_per_metric: 50000,
      max_global_series_per_user: 0,
      max_global_series_per_metric: 0,
      min_chunk_length: 0,
      max_metadata_per_user: 8000,
      max_metadata_per_metric: 10,
      max_global_metadata_per_user: 0,
      max_global_metadata_per_metric: 0,
      max_chunks_per_query: 2000000,
      max_query_lookback: "0s",
      max_query_length: "0s",
      max_query_parallelism: 14,
      cardinality_limit: 100000,
      max_cache_freshness: "1m0s",
      max_queriers_per_tenant: 0,
      ruler_evaluation_delay_duration: "0s",
      ruler_tenant_shard_size: 0,
      ruler_max_rules_per_rule_group: 0,
      ruler_max_rule_groups_per_tenant: 0,
      store_gateway_tenant_shard_size: 0,
      per_tenant_override_config: "",
      per_tenant_override_period: "10s"
    }
  },
  frontend_worker_config:: {
    new(): {
      frontend_address: "",
      scheduler_address: "",
      dns_lookup_duration: "10s",
      parallelism: 10,
      match_max_concurrent: false,
      id: "",
      grpc_client_config: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      }
    }
  },
  query_frontend_config:: {
    new(): {
      log_queries_longer_than: "0s",
      max_body_size: 10485760,
      query_stats_enabled: false,
      max_outstanding_per_tenant: 100,
      scheduler_address: "",
      scheduler_dns_lookup_period: "10s",
      scheduler_worker_concurrency: 5,
      grpc_client_config: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      },
      instance_interface_names: ["eth0", "en0"],
      compress_responses: false,
      downstream_url: ""
    }
  },
  query_range_config:: {
    new(): {
      split_queries_by_interval: "0s",
      split_queries_by_day: false,
      align_queries_with_step: false,
      results_cache: {
        cache: {
          enable_fifocache: false,
          default_validity: "0s",
          background: {
            writeback_goroutines: 10,
            writeback_buffer: 10000
          },
          memcached: {
            expiration: "0s",
            batch_size: 1024,
            parallelism: 100
          },
          memcached_client: {
            host: "",
            service: "memcached",
            addresses: "",
            timeout: "100ms",
            max_idle_conns: 16,
            update_interval: "1m0s",
            consistent_hash: true,
            circuit_breaker_consecutive_failures: 10,
            circuit_breaker_timeout: "10s",
            circuit_breaker_interval: "10s"
          },
          redis: {
            endpoint: "",
            master_name: "",
            timeout: "500ms",
            expiration: "0s",
            db: 0,
            pool_size: 0,
            password: "",
            tls_enabled: false,
            tls_insecure_skip_verify: false,
            idle_timeout: "0s",
            max_connection_age: "0s"
          },
          fifocache: {
            max_size_bytes: "",
            max_size_items: 0,
            validity: "0s",
            size: 0
          }
        },
        compression: ""
      },
      cache_results: false,
      max_retries: 5,
      parallelise_shardable_queries: false
    }
  },
  table_manager_config:: {
    new(): {
      throughput_updates_disabled: false,
      retention_deletes_enabled: false,
      retention_period: "0s",
      poll_interval: "2m0s",
      creation_grace_period: "10m0s",
      index_tables_provisioning: {
        enable_ondemand_throughput_mode: false,
        provisioned_write_throughput: 1000,
        provisioned_read_throughput: 300,
        write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        enable_inactive_throughput_on_demand_mode: false,
        inactive_write_throughput: 1,
        inactive_read_throughput: 300,
        inactive_write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_write_scale_lastn: 4,
        inactive_read_scale_lastn: 4
      },
      chunk_tables_provisioning: {
        enable_ondemand_throughput_mode: false,
        provisioned_write_throughput: 1000,
        provisioned_read_throughput: 300,
        write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        enable_inactive_throughput_on_demand_mode: false,
        inactive_write_throughput: 1,
        inactive_read_throughput: 300,
        inactive_write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_write_scale_lastn: 4,
        inactive_read_scale_lastn: 4
      }
    }
  },
  blocks_storage_config:: {
    new(): {
      backend: "s3",
      s3: {
        endpoint: "",
        bucket_name: "",
        secret_access_key: "",
        access_key_id: "",
        insecure: false,
        signature_version: "v4",
        http: {
          idle_conn_timeout: "1m30s",
          response_header_timeout: "2m0s",
          insecure_skip_verify: false
        }
      },
      gcs: {
        bucket_name: "",
        service_account: ""
      },
      azure: {
        account_name: "",
        account_key: "",
        container_name: "",
        endpoint_suffix: "",
        max_retries: 20
      },
      swift: {
        auth_version: 0,
        auth_url: "",
        username: "",
        user_domain_name: "",
        user_domain_id: "",
        user_id: "",
        password: "",
        domain_id: "",
        domain_name: "",
        project_id: "",
        project_name: "",
        project_domain_id: "",
        project_domain_name: "",
        region_name: "",
        container_name: "",
        max_retries: 3,
        connect_timeout: "10s",
        request_timeout: "5s"
      },
      filesystem: {
        dir: ""
      },
      bucket_store: {
        sync_dir: "tsdb-sync",
        sync_interval: "5m0s",
        max_chunk_pool_bytes: 2147483648,
        max_concurrent: 100,
        tenant_sync_concurrency: 10,
        block_sync_concurrency: 20,
        meta_sync_concurrency: 20,
        consistency_delay: "0s",
        index_cache: {
          backend: "inmemory",
          inmemory: {
            max_size_bytes: 1073741824
          },
          memcached: {
            addresses: "",
            timeout: "100ms",
            max_idle_connections: 16,
            max_async_concurrency: 50,
            max_async_buffer_size: 10000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 0,
            max_item_size: 1048576
          },
          postings_compression_enabled: false
        },
        chunks_cache: {
          backend: "",
          memcached: {
            addresses: "",
            timeout: "100ms",
            max_idle_connections: 16,
            max_async_concurrency: 50,
            max_async_buffer_size: 10000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 0,
            max_item_size: 1048576
          },
          subrange_size: 16000,
          max_get_range_requests: 3,
          attributes_ttl: "168h0m0s",
          subrange_ttl: "24h0m0s"
        },
        metadata_cache: {
          backend: "",
          memcached: {
            addresses: "",
            timeout: "100ms",
            max_idle_connections: 16,
            max_async_concurrency: 50,
            max_async_buffer_size: 10000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 0,
            max_item_size: 1048576
          },
          tenants_list_ttl: "15m0s",
          tenant_blocks_list_ttl: "5m0s",
          chunks_list_ttl: "24h0m0s",
          metafile_exists_ttl: "2h0m0s",
          metafile_doesnt_exist_ttl: "5m0s",
          metafile_content_ttl: "24h0m0s",
          metafile_max_size_bytes: 1048576,
          metafile_attributes_ttl: "168h0m0s",
          block_index_attributes_ttl: "168h0m0s",
          bucket_index_content_ttl: "5m0s",
          bucket_index_max_size_bytes: 1048576
        },
        ignore_deletion_mark_delay: "6h0m0s",
        bucket_index: {
          enabled: false,
          update_on_error_interval: "1m0s",
          idle_timeout: "1h0m0s",
          max_stale_period: "1h0m0s"
        }
      },
      tsdb: {
        dir: "tsdb",
        block_ranges_period: ["2h0m0s"],
        retention_period: "6h0m0s",
        ship_interval: "1m0s",
        ship_concurrency: 10,
        head_compaction_interval: "1m0s",
        head_compaction_concurrency: 5,
        head_compaction_idle_timeout: "1h0m0s",
        head_chunks_write_buffer_size_bytes: 4194304,
        stripe_size: 16384,
        wal_compression_enabled: false,
        wal_segment_size_bytes: 134217728,
        flush_blocks_on_shutdown: false,
        close_idle_tsdb_timeout: "0s",
        max_tsdb_opening_concurrency_on_startup: 10
      }
    }
  },
  compactor_config:: {
    new(): {
      block_ranges: ["2h0m0s", "12h0m0s", "24h0m0s"],
      block_sync_concurrency: 20,
      meta_sync_concurrency: 20,
      consistency_delay: "0s",
      data_dir: "./data",
      compaction_interval: "1h0m0s",
      compaction_retries: 3,
      compaction_concurrency: 1,
      cleanup_interval: "15m0s",
      cleanup_concurrency: 20,
      deletion_delay: "12h0m0s",
      tenant_cleanup_delay: "6h0m0s",
      block_deletion_marks_migration_enabled: true,
      enabled_tenants: "",
      disabled_tenants: "",
      sharding_enabled: false,
      sharding_ring: {
        kvstore: {
          store: "consul",
          prefix: "collectors/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "5s",
        heartbeat_timeout: "1m0s",
        wait_stability_min_duration: "1m0s",
        wait_stability_max_duration: "5m0s",
        instance_interface_names: ["eth0", "en0"]
      }
    }
  },
  store_gateway_config:: {
    new(): {
      sharding_enabled: false,
      sharding_ring: {
        kvstore: {
          store: "consul",
          prefix: "collectors/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "15s",
        heartbeat_timeout: "1m0s",
        replication_factor: 3,
        tokens_file_path: "",
        zone_awareness_enabled: false,
        instance_interface_names: ["eth0", "en0"],
        instance_availability_zone: ""
      },
      sharding_strategy: "default"
    }
  },
  purger_config:: {
    new(): {
      enable: false,
      num_workers: 2,
      object_store_type: "",
      delete_request_cancel_period: "24h0m0s"
    }
  },
  ruler_config:: {
    new(): {
      external_url: "",
      ruler_client: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      },
      evaluation_interval: "1m0s",
      poll_interval: "1m0s",
      storage: {
        type: "configdb",
        configdb: {
          configs_api_url: "",
          client_timeout: "5s",
          tls_cert_path: "",
          tls_key_path: "",
          tls_ca_path: "",
          tls_insecure_skip_verify: false
        },
        azure: {
          environment: "AzureGlobal",
          container_name: "cortex",
          account_name: "",
          account_key: "",
          download_buffer_size: 512000,
          upload_buffer_size: 256000,
          upload_buffer_count: 1,
          request_timeout: "30s",
          max_retries: 5,
          min_retry_delay: "10ms",
          max_retry_delay: "500ms"
        },
        gcs: {
          bucket_name: "",
          chunk_buffer_size: 0,
          request_timeout: "0s"
        },
        s3: {
          s3: "",
          s3forcepathstyle: false,
          bucketnames: "",
          endpoint: "",
          region: "",
          access_key_id: "",
          secret_access_key: "",
          insecure: false,
          sse_encryption: false,
          http_config: {
            idle_conn_timeout: "1m30s",
            response_header_timeout: "0s",
            insecure_skip_verify: false
          },
          signature_version: "v4"
        },
        swift: {
          auth_version: 0,
          auth_url: "",
          username: "",
          user_domain_name: "",
          user_domain_id: "",
          user_id: "",
          password: "",
          domain_id: "",
          domain_name: "",
          project_id: "",
          project_name: "",
          project_domain_id: "",
          project_domain_name: "",
          region_name: "",
          container_name: "",
          max_retries: 3,
          connect_timeout: "10s",
          request_timeout: "5s"
        },
        'local': {
          directory: ""
        }
      },
      rule_path: "/rules",
      alertmanager_url: "",
      enable_alertmanager_discovery: false,
      alertmanager_refresh_interval: "1m0s",
      enable_alertmanager_v2: false,
      notification_queue_capacity: 10000,
      notification_timeout: "10s",
      for_outage_tolerance: "1h0m0s",
      for_grace_period: "10m0s",
      resend_delay: "1m0s",
      enable_sharding: false,
      sharding_strategy: "default",
      search_pending_for: "5m0s",
      ring: {
        kvstore: {
          store: "consul",
          prefix: "rulers/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "5s",
        heartbeat_timeout: "1m0s",
        instance_interface_names: ["eth0", "en0"],
        num_tokens: 128
      },
      flush_period: "1m0s",
      enable_api: false
    }
  },
  ingester_client_config:: {
    new(): {
      configs_api_url: "",
      client_timeout: "5s",
      tls_cert_path: "",
      tls_key_path: "",
      tls_ca_path: "",
      tls_insecure_skip_verify: false
    }
  },
  configs_config:: {
    new(): {
      database: {
        uri: "postgres://postgres@configs-db.weave.local/configs?sslmode=disable",
        migrations_dir: "",
        password_file: ""
      },
      api: {
        notifications: {
          disable_email: false,
          disable_webhook: false
        }
      }
    }
  },
  alertmanager_config:: {
    new(): {
      data_dir: "data/",
      retention: "120h0m0s",
      external_url: "",
      poll_interval: "15s",
      cluster_bind_address: "0.0.0.0:9094",
      cluster_advertise_address: "",
      peers: [],
      peer_timeout: "15s",
      fallback_config_file: "",
      auto_webhook_root: "",
      storage: {
        type: "configdb",
        configdb: {
          configs_api_url: "",
          client_timeout: "5s",
          tls_cert_path: "",
          tls_key_path: "",
          tls_ca_path: "",
          tls_insecure_skip_verify: false
        },
        azure: {
          environment: "AzureGlobal",
          container_name: "cortex",
          account_name: "",
          account_key: "",
          download_buffer_size: 512000,
          upload_buffer_size: 256000,
          upload_buffer_count: 1,
          request_timeout: "30s",
          max_retries: 5,
          min_retry_delay: "10ms",
          max_retry_delay: "500ms"
        },
        gcs: {
          bucket_name: "",
          chunk_buffer_size: 0,
          request_timeout: "0s"
        },
        s3: {
          s3: "",
          s3forcepathstyle: false,
          bucketnames: "",
          endpoint: "",
          region: "",
          access_key_id: "",
          secret_access_key: "",
          insecure: false,
          sse_encryption: false,
          http_config: {
            idle_conn_timeout: "1m30s",
            response_header_timeout: "0s",
            insecure_skip_verify: false
          },
          signature_version: "v4"
        },
        'local': {
          path: ""
        }
      },
      enable_api: false
    }
  },
  memberlist_config:: {
    new(): {
      node_name: "",
      randomize_node_name: true,
      stream_timeout: "0s",
      retransmit_factor: 0,
      pull_push_interval: "0s",
      gossip_interval: "0s",
      gossip_nodes: 0,
      gossip_to_dead_nodes_time: "0s",
      dead_node_reclaim_time: "0s",
      join_members: [],
      min_join_backoff: "1s",
      max_join_backoff: "1m0s",
      max_join_retries: 10,
      abort_if_cluster_join_fails: true,
      rejoin_interval: "0s",
      left_ingesters_timeout: "5m0s",
      leave_timeout: "5s",
      message_history_buffer_bytes: 0,
      bind_addr: [],
      bind_port: 7946,
      packet_dial_timeout: "5s",
      packet_write_timeout: "5s"
    }
  },
  flags:: {
    target: "target",
    auth_enabled: "auth.enabled",
    http_prefix: "http.prefix",
    api: {
      response_compression_enabled: "api.response-compression-enabled",
      alertmanager_http_prefix: "http.alertmanager-http-prefix",
      prometheus_http_prefix: "http.prometheus-http-prefix"
    },
    server: {
      http_listen_address: "server.http-listen-address",
      http_listen_port: "server.http-listen-port",
      http_listen_conn_limit: "server.http-conn-limit",
      grpc_listen_address: "server.grpc-listen-address",
      grpc_listen_port: "server.grpc-listen-port",
      grpc_listen_conn_limit: "server.grpc-conn-limit",
      http_tls_config: {
        cert_file: "server.http-tls-cert-path",
        key_file: "server.http-tls-key-path",
        client_auth_type: "server.http-tls-client-auth",
        client_ca_file: "server.http-tls-ca-path"
      },
      grpc_tls_config: {
        cert_file: "server.grpc-tls-cert-path",
        key_file: "server.grpc-tls-key-path",
        client_auth_type: "server.grpc-tls-client-auth",
        client_ca_file: "server.grpc-tls-ca-path"
      },
      register_instrumentation: "server.register-instrumentation",
      graceful_shutdown_timeout: "server.graceful-shutdown-timeout",
      http_server_read_timeout: "server.http-read-timeout",
      http_server_write_timeout: "server.http-write-timeout",
      http_server_idle_timeout: "server.http-idle-timeout",
      grpc_server_max_recv_msg_size: "server.grpc-max-recv-msg-size-bytes",
      grpc_server_max_send_msg_size: "server.grpc-max-send-msg-size-bytes",
      grpc_server_max_concurrent_streams: "server.grpc-max-concurrent-streams",
      grpc_server_max_connection_idle: "server.grpc.keepalive.max-connection-idle",
      grpc_server_max_connection_age: "server.grpc.keepalive.max-connection-age",
      grpc_server_max_connection_age_grace: "server.grpc.keepalive.max-connection-age-grace",
      grpc_server_keepalive_time: "server.grpc.keepalive.time",
      grpc_server_keepalive_timeout: "server.grpc.keepalive.timeout",
      grpc_server_min_time_between_pings: "server.grpc.keepalive.min-time-between-pings",
      grpc_server_ping_without_stream_allowed: "server.grpc.keepalive.ping-without-stream-allowed",
      log_format: "log.format",
      log_level: "log.level",
      log_source_ips_enabled: "server.log-source-ips-enabled",
      log_source_ips_header: "server.log-source-ips-header",
      log_source_ips_regex: "server.log-source-ips-regex",
      http_path_prefix: "server.path-prefix"
    },
    distributor: {
      pool: {
        client_cleanup_period: "distributor.client-cleanup-period",
        health_check_ingesters: "distributor.health-check-ingesters"
      },
      ha_tracker: {
        enable_ha_tracker: "distributor.ha-tracker.enable",
        ha_tracker_update_timeout: "distributor.ha-tracker.update-timeout",
        ha_tracker_update_timeout_jitter_max: "distributor.ha-tracker.update-timeout-jitter-max",
        ha_tracker_failover_timeout: "distributor.ha-tracker.failover-timeout",
        kvstore: {
          store: "distributor.ha-tracker.store",
          prefix: "distributor.ha-tracker.prefix",
          consul: {
            host: "distributor.ha-tracker.consul.hostname",
            acl_token: "distributor.ha-tracker.consul.acl-token",
            http_client_timeout: "distributor.ha-tracker.consul.client-timeout",
            consistent_reads: "distributor.ha-tracker.consul.consistent-reads",
            watch_rate_limit: "distributor.ha-tracker.consul.watch-rate-limit",
            watch_burst_size: "distributor.ha-tracker.consul.watch-burst-size"
          },
          etcd: {
            endpoints: "distributor.ha-tracker.etcd.endpoints",
            dial_timeout: "distributor.ha-tracker.etcd.dial-timeout",
            max_retries: "distributor.ha-tracker.etcd.max-retries",
            tls_enabled: "distributor.ha-tracker.etcd.tls-enabled",
            tls_cert_path: "distributor.ha-tracker.etcd.tls-cert-path",
            tls_key_path: "distributor.ha-tracker.etcd.tls-key-path",
            tls_ca_path: "distributor.ha-tracker.etcd.tls-ca-path",
            tls_insecure_skip_verify: "distributor.ha-tracker.etcd.tls-insecure-skip-verify"
          },
          multi: {
            primary: "distributor.ha-tracker.multi.primary",
            secondary: "distributor.ha-tracker.multi.secondary",
            mirror_enabled: "distributor.ha-tracker.multi.mirror-enabled",
            mirror_timeout: "distributor.ha-tracker.multi.mirror-timeout"
          }
        }
      },
      max_recv_msg_size: "distributor.max-recv-msg-size",
      remote_timeout: "distributor.remote-timeout",
      extra_queue_delay: "distributor.extra-query-delay",
      sharding_strategy: "distributor.sharding-strategy",
      shard_by_all_labels: "distributor.shard-by-all-labels",
      ring: {
        kvstore: {
          store: "distributor.ring.store",
          prefix: "distributor.ring.prefix",
          consul: {
            host: "distributor.ring.consul.hostname",
            acl_token: "distributor.ring.consul.acl-token",
            http_client_timeout: "distributor.ring.consul.client-timeout",
            consistent_reads: "distributor.ring.consul.consistent-reads",
            watch_rate_limit: "distributor.ring.consul.watch-rate-limit",
            watch_burst_size: "distributor.ring.consul.watch-burst-size"
          },
          etcd: {
            endpoints: "distributor.ring.etcd.endpoints",
            dial_timeout: "distributor.ring.etcd.dial-timeout",
            max_retries: "distributor.ring.etcd.max-retries",
            tls_enabled: "distributor.ring.etcd.tls-enabled",
            tls_cert_path: "distributor.ring.etcd.tls-cert-path",
            tls_key_path: "distributor.ring.etcd.tls-key-path",
            tls_ca_path: "distributor.ring.etcd.tls-ca-path",
            tls_insecure_skip_verify: "distributor.ring.etcd.tls-insecure-skip-verify"
          },
          multi: {
            primary: "distributor.ring.multi.primary",
            secondary: "distributor.ring.multi.secondary",
            mirror_enabled: "distributor.ring.multi.mirror-enabled",
            mirror_timeout: "distributor.ring.multi.mirror-timeout"
          }
        },
        heartbeat_period: "distributor.ring.heartbeat-period",
        heartbeat_timeout: "distributor.ring.heartbeat-timeout",
        instance_interface_names: "distributor.ring.instance-interface-names"
      }
    },
    querier: {
      max_concurrent: "querier.max-concurrent",
      timeout: "querier.timeout",
      iterators: "querier.iterators",
      batch_iterators: "querier.batch-iterators",
      ingester_streaming: "querier.ingester-streaming",
      max_samples: "querier.max-samples",
      query_ingesters_within: "querier.query-ingesters-within",
      query_store_for_labels_enabled: "querier.query-store-for-labels-enabled",
      query_store_after: "querier.query-store-after",
      max_query_into_future: "querier.max-query-into-future",
      default_evaluation_interval: "querier.default-evaluation-interval",
      active_query_tracker_dir: "querier.active-query-tracker-dir",
      lookback_delta: "querier.lookback-delta",
      store_gateway_addresses: "querier.store-gateway-addresses",
      store_gateway_client: {
        tls_cert_path: "querier.store-gateway-client.tls-cert-path",
        tls_key_path: "querier.store-gateway-client.tls-key-path",
        tls_ca_path: "querier.store-gateway-client.tls-ca-path",
        tls_insecure_skip_verify: "querier.store-gateway-client.tls-insecure-skip-verify"
      },
      second_store_engine: "querier.second-store-engine",
      use_second_store_before_time: "querier.use-second-store-before-time",
      shuffle_sharding_ingesters_lookback_period: "querier.shuffle-sharding-ingesters-lookback-period"
    },
    ingester_client: {
      grpc_client_config: {
        max_recv_msg_size: "ingester.client.grpc-max-recv-msg-size",
        max_send_msg_size: "ingester.client.grpc-max-send-msg-size",
        grpc_compression: "ingester.client.grpc-compression",
        rate_limit: "ingester.client.grpc-client-rate-limit",
        rate_limit_burst: "ingester.client.grpc-client-rate-limit-burst",
        backoff_on_ratelimits: "ingester.client.backoff-on-ratelimits",
        backoff_config: {
          min_period: "ingester.client.backoff-min-period",
          max_period: "ingester.client.backoff-max-period",
          max_retries: "ingester.client.backoff-retries"
        },
        tls_cert_path: "ingester.client.tls-cert-path",
        tls_key_path: "ingester.client.tls-key-path",
        tls_ca_path: "ingester.client.tls-ca-path",
        tls_insecure_skip_verify: "ingester.client.tls-insecure-skip-verify"
      }
    },
    ingester: {
      walconfig: {
        wal_enabled: "ingester.wal-enabled",
        checkpoint_enabled: "ingester.checkpoint-enabled",
        recover_from_wal: "ingester.recover-from-wal",
        wal_dir: "ingester.wal-dir",
        checkpoint_duration: "ingester.checkpoint-duration",
        flush_on_shutdown_with_wal_enabled: "ingester.flush-on-shutdown-with-wal-enabled"
      },
      lifecycler: {
        ring: {
          kvstore: {
            store: "ring.store",
            prefix: "ring.prefix",
            consul: {
              host: "consul.hostname",
              acl_token: "consul.acl-token",
              http_client_timeout: "consul.client-timeout",
              consistent_reads: "consul.consistent-reads",
              watch_rate_limit: "consul.watch-rate-limit",
              watch_burst_size: "consul.watch-burst-size"
            },
            etcd: {
              endpoints: "etcd.endpoints",
              dial_timeout: "etcd.dial-timeout",
              max_retries: "etcd.max-retries",
              tls_enabled: "etcd.tls-enabled",
              tls_cert_path: "etcd.tls-cert-path",
              tls_key_path: "etcd.tls-key-path",
              tls_ca_path: "etcd.tls-ca-path",
              tls_insecure_skip_verify: "etcd.tls-insecure-skip-verify"
            },
            multi: {
              primary: "multi.primary",
              secondary: "multi.secondary",
              mirror_enabled: "multi.mirror-enabled",
              mirror_timeout: "multi.mirror-timeout"
            }
          },
          heartbeat_timeout: "ring.heartbeat-timeout",
          replication_factor: "distributor.replication-factor",
          zone_awareness_enabled: "distributor.zone-awareness-enabled",
          extend_writes: "distributor.extend-writes"
        },
        num_tokens: "ingester.num-tokens",
        heartbeat_period: "ingester.heartbeat-period",
        observe_period: "ingester.observe-period",
        join_after: "ingester.join-after",
        min_ready_duration: "ingester.min-ready-duration",
        interface_names: "ingester.lifecycler.interface",
        final_sleep: "ingester.final-sleep",
        tokens_file_path: "ingester.tokens-file-path",
        availability_zone: "ingester.availability-zone",
        unregister_on_shutdown: "ingester.unregister-on-shutdown"
      },
      max_transfer_retries: "ingester.max-transfer-retries",
      flush_period: "ingester.flush-period",
      retain_period: "ingester.retain-period",
      max_chunk_idle_time: "ingester.max-chunk-idle",
      max_stale_chunk_idle_time: "ingester.max-stale-chunk-idle",
      flush_op_timeout: "ingester.flush-op-timeout",
      max_chunk_age: "ingester.max-chunk-age",
      chunk_age_jitter: "ingester.chunk-age-jitter",
      concurrent_flushes: "ingester.concurrent-flushes",
      spread_flushes: "ingester.spread-flushes",
      metadata_retain_period: "ingester.metadata-retain-period",
      rate_update_period: "ingester.rate-update-period",
      active_series_metrics_enabled: "ingester.active-series-metrics-enabled",
      active_series_metrics_update_period: "ingester.active-series-metrics-update-period",
      active_series_metrics_idle_timeout: "ingester.active-series-metrics-idle-timeout"
    },
    flusher: {
      wal_dir: "flusher.wal-dir",
      concurrent_flushes: "flusher.concurrent-flushes",
      flush_op_timeout: "flusher.flush-op-timeout",
      exit_after_flush: "flusher.exit-after-flush"
    },
    storage: {
      engine: "store.engine",
      aws: {
        dynamodb: {
          dynamodb_url: "dynamodb.url",
          api_limit: "dynamodb.api-limit",
          throttle_limit: "dynamodb.throttle-limit",
          metrics: {
            url: "metrics.url",
            target_queue_length: "metrics.target-queue-length",
            scale_up_factor: "metrics.scale-up-factor",
            ignore_throttle_below: "metrics.ignore-throttle-below",
            queue_length_query: "metrics.queue-length-query",
            write_throttle_query: "metrics.write-throttle-query",
            write_usage_query: "metrics.usage-query",
            read_usage_query: "metrics.read-usage-query",
            read_error_query: "metrics.read-error-query"
          },
          chunk_gang_size: "dynamodb.chunk-gang-size",
          chunk_get_max_parallelism: "dynamodb.chunk.get-max-parallelism",
          backoff_config: {
            min_period: "dynamodb.min-backoff",
            max_period: "dynamodb.max-backoff",
            max_retries: "dynamodb.max-retries"
          }
        },
        s3: "s3.url",
        s3forcepathstyle: "s3.force-path-style",
        bucketnames: "s3.buckets",
        endpoint: "s3.endpoint",
        region: "s3.region",
        access_key_id: "s3.access-key-id",
        secret_access_key: "s3.secret-access-key",
        insecure: "s3.insecure",
        sse_encryption: "s3.sse-encryption",
        http_config: {
          idle_conn_timeout: "s3.http.idle-conn-timeout",
          response_header_timeout: "s3.http.response-header-timeout",
          insecure_skip_verify: "s3.http.insecure-skip-verify"
        },
        signature_version: "s3.signature-version"
      },
      azure: {
        environment: "azure.environment",
        container_name: "azure.container-name",
        account_name: "azure.account-name",
        account_key: "azure.account-key",
        download_buffer_size: "azure.download-buffer-size",
        upload_buffer_size: "azure.upload-buffer-size",
        upload_buffer_count: "azure.download-buffer-count",
        request_timeout: "azure.request-timeout",
        max_retries: "azure.max-retries",
        min_retry_delay: "azure.min-retry-delay",
        max_retry_delay: "azure.max-retry-delay"
      },
      bigtable: {
        project: "bigtable.project",
        instance: "bigtable.instance",
        grpc_client_config: {
          max_recv_msg_size: "bigtable.grpc-max-recv-msg-size",
          max_send_msg_size: "bigtable.grpc-max-send-msg-size",
          grpc_compression: "bigtable.grpc-compression",
          rate_limit: "bigtable.grpc-client-rate-limit",
          rate_limit_burst: "bigtable.grpc-client-rate-limit-burst",
          backoff_on_ratelimits: "bigtable.backoff-on-ratelimits",
          backoff_config: {
            min_period: "bigtable.backoff-min-period",
            max_period: "bigtable.backoff-max-period",
            max_retries: "bigtable.backoff-retries"
          }
        },
        table_cache_enabled: "bigtable.table-cache.enabled",
        table_cache_expiration: "bigtable.table-cache.expiration"
      },
      gcs: {
        bucket_name: "gcs.bucketname",
        chunk_buffer_size: "gcs.chunk-buffer-size",
        request_timeout: "gcs.request-timeout"
      },
      cassandra: {
        addresses: "cassandra.addresses",
        port: "cassandra.port",
        keyspace: "cassandra.keyspace",
        consistency: "cassandra.consistency",
        replication_factor: "cassandra.replication-factor",
        disable_initial_host_lookup: "cassandra.disable-initial-host-lookup",
        SSL: "cassandra.ssl",
        host_verification: "cassandra.host-verification",
        CA_path: "cassandra.ca-path",
        tls_cert_path: "cassandra.tls-cert-path",
        tls_key_path: "cassandra.tls-key-path",
        auth: "cassandra.auth",
        username: "cassandra.username",
        password: "cassandra.password",
        password_file: "cassandra.password-file",
        custom_authenticators: "cassandra.custom-authenticator",
        timeout: "cassandra.timeout",
        connect_timeout: "cassandra.connect-timeout",
        reconnect_interval: "cassandra.reconnent-interval",
        max_retries: "cassandra.max-retries",
        retry_max_backoff: "cassandra.retry-max-backoff",
        retry_min_backoff: "cassandra.retry-min-backoff",
        query_concurrency: "cassandra.query-concurrency",
        num_connections: "cassandra.num-connections",
        convict_hosts_on_failure: "cassandra.convict-hosts-on-failure",
        table_options: "cassandra.table-options"
      },
      boltdb: {
        directory: "boltdb.dir"
      },
      filesystem: {
        directory: "local.chunk-directory"
      },
      swift: {
        auth_version: "swift.auth-version",
        auth_url: "swift.auth-url",
        username: "swift.username",
        user_domain_name: "swift.user-domain-name",
        user_domain_id: "swift.user-domain-id",
        user_id: "swift.user-id",
        password: "swift.password",
        domain_id: "swift.domain-id",
        domain_name: "swift.domain-name",
        project_id: "swift.project-id",
        project_name: "swift.project-name",
        project_domain_id: "swift.project-domain-id",
        project_domain_name: "swift.project-domain-name",
        region_name: "swift.region-name",
        container_name: "swift.container-name",
        max_retries: "swift.max-retries",
        connect_timeout: "swift.connect-timeout",
        request_timeout: "swift.request-timeout"
      },
      index_cache_validity: "store.index-cache-validity",
      index_queries_cache_config: {
        enable_fifocache: "store.index-cache-read.cache.enable-fifocache",
        default_validity: "store.index-cache-read.default-validity",
        background: {
          writeback_goroutines: "store.index-cache-read.background.write-back-concurrency",
          writeback_buffer: "store.index-cache-read.background.write-back-buffer"
        },
        memcached: {
          expiration: "store.index-cache-read.memcached.expiration",
          batch_size: "store.index-cache-read.memcached.batchsize",
          parallelism: "store.index-cache-read.memcached.parallelism"
        },
        memcached_client: {
          host: "store.index-cache-read.memcached.hostname",
          service: "store.index-cache-read.memcached.service",
          addresses: "store.index-cache-read.memcached.addresses",
          timeout: "store.index-cache-read.memcached.timeout",
          max_idle_conns: "store.index-cache-read.memcached.max-idle-conns",
          update_interval: "store.index-cache-read.memcached.update-interval",
          consistent_hash: "store.index-cache-read.memcached.consistent-hash",
          circuit_breaker_consecutive_failures: "store.index-cache-read.memcached.circuit-breaker-consecutive-failures",
          circuit_breaker_timeout: "store.index-cache-read.memcached.circuit-breaker-timeout",
          circuit_breaker_interval: "store.index-cache-read.memcached.circuit-breaker-interval"
        },
        redis: {
          endpoint: "store.index-cache-read.redis.endpoint",
          master_name: "store.index-cache-read.redis.master-name",
          timeout: "store.index-cache-read.redis.timeout",
          expiration: "store.index-cache-read.redis.expiration",
          db: "store.index-cache-read.redis.db",
          pool_size: "store.index-cache-read.redis.pool-size",
          password: "store.index-cache-read.redis.password",
          tls_enabled: "store.index-cache-read.redis.tls-enabled",
          tls_insecure_skip_verify: "store.index-cache-read.redis.tls-insecure-skip-verify",
          idle_timeout: "store.index-cache-read.redis.idle-timeout",
          max_connection_age: "store.index-cache-read.redis.max-connection-age"
        },
        fifocache: {
          max_size_bytes: "store.index-cache-read.fifocache.max-size-bytes",
          max_size_items: "store.index-cache-read.fifocache.max-size-items",
          validity: "store.index-cache-read.fifocache.duration",
          size: "store.index-cache-read.fifocache.size"
        }
      },
      delete_store: {
        store: "deletes.store",
        requests_table_name: "deletes.requests-table-name",
        table_provisioning: {
          enable_ondemand_throughput_mode: "deletes.table.enable-ondemand-throughput-mode",
          provisioned_write_throughput: "deletes.table.write-throughput",
          provisioned_read_throughput: "deletes.table.read-throughput",
          write_scale: {
            enabled: "deletes.table.write-throughput.scale.enabled",
            role_arn: "deletes.table.write-throughput.scale.role-arn",
            min_capacity: "deletes.table.write-throughput.scale.min-capacity",
            max_capacity: "deletes.table.write-throughput.scale.max-capacity",
            out_cooldown: "deletes.table.write-throughput.scale.out-cooldown",
            in_cooldown: "deletes.table.write-throughput.scale.in-cooldown",
            target: "deletes.table.write-throughput.scale.target-value"
          },
          read_scale: {
            enabled: "deletes.table.read-throughput.scale.enabled",
            role_arn: "deletes.table.read-throughput.scale.role-arn",
            min_capacity: "deletes.table.read-throughput.scale.min-capacity",
            max_capacity: "deletes.table.read-throughput.scale.max-capacity",
            out_cooldown: "deletes.table.read-throughput.scale.out-cooldown",
            in_cooldown: "deletes.table.read-throughput.scale.in-cooldown",
            target: "deletes.table.read-throughput.scale.target-value"
          },
          tags: "deletes.table.tags"
        }
      },
      grpc_store: {
        server_address: "grpc-store.server-address"
      }
    },
    chunk_store: {
      chunk_cache_config: {
        enable_fifocache: "store.chunks-cache.cache.enable-fifocache",
        default_validity: "store.chunks-cache.default-validity",
        background: {
          writeback_goroutines: "store.chunks-cache.background.write-back-concurrency",
          writeback_buffer: "store.chunks-cache.background.write-back-buffer"
        },
        memcached: {
          expiration: "store.chunks-cache.memcached.expiration",
          batch_size: "store.chunks-cache.memcached.batchsize",
          parallelism: "store.chunks-cache.memcached.parallelism"
        },
        memcached_client: {
          host: "store.chunks-cache.memcached.hostname",
          service: "store.chunks-cache.memcached.service",
          addresses: "store.chunks-cache.memcached.addresses",
          timeout: "store.chunks-cache.memcached.timeout",
          max_idle_conns: "store.chunks-cache.memcached.max-idle-conns",
          update_interval: "store.chunks-cache.memcached.update-interval",
          consistent_hash: "store.chunks-cache.memcached.consistent-hash",
          circuit_breaker_consecutive_failures: "store.chunks-cache.memcached.circuit-breaker-consecutive-failures",
          circuit_breaker_timeout: "store.chunks-cache.memcached.circuit-breaker-timeout",
          circuit_breaker_interval: "store.chunks-cache.memcached.circuit-breaker-interval"
        },
        redis: {
          endpoint: "store.chunks-cache.redis.endpoint",
          master_name: "store.chunks-cache.redis.master-name",
          timeout: "store.chunks-cache.redis.timeout",
          expiration: "store.chunks-cache.redis.expiration",
          db: "store.chunks-cache.redis.db",
          pool_size: "store.chunks-cache.redis.pool-size",
          password: "store.chunks-cache.redis.password",
          tls_enabled: "store.chunks-cache.redis.tls-enabled",
          tls_insecure_skip_verify: "store.chunks-cache.redis.tls-insecure-skip-verify",
          idle_timeout: "store.chunks-cache.redis.idle-timeout",
          max_connection_age: "store.chunks-cache.redis.max-connection-age"
        },
        fifocache: {
          max_size_bytes: "store.chunks-cache.fifocache.max-size-bytes",
          max_size_items: "store.chunks-cache.fifocache.max-size-items",
          validity: "store.chunks-cache.fifocache.duration",
          size: "store.chunks-cache.fifocache.size"
        }
      },
      write_dedupe_cache_config: {
        enable_fifocache: "store.index-cache-write.cache.enable-fifocache",
        default_validity: "store.index-cache-write.default-validity",
        background: {
          writeback_goroutines: "store.index-cache-write.background.write-back-concurrency",
          writeback_buffer: "store.index-cache-write.background.write-back-buffer"
        },
        memcached: {
          expiration: "store.index-cache-write.memcached.expiration",
          batch_size: "store.index-cache-write.memcached.batchsize",
          parallelism: "store.index-cache-write.memcached.parallelism"
        },
        memcached_client: {
          host: "store.index-cache-write.memcached.hostname",
          service: "store.index-cache-write.memcached.service",
          addresses: "store.index-cache-write.memcached.addresses",
          timeout: "store.index-cache-write.memcached.timeout",
          max_idle_conns: "store.index-cache-write.memcached.max-idle-conns",
          update_interval: "store.index-cache-write.memcached.update-interval",
          consistent_hash: "store.index-cache-write.memcached.consistent-hash",
          circuit_breaker_consecutive_failures: "store.index-cache-write.memcached.circuit-breaker-consecutive-failures",
          circuit_breaker_timeout: "store.index-cache-write.memcached.circuit-breaker-timeout",
          circuit_breaker_interval: "store.index-cache-write.memcached.circuit-breaker-interval"
        },
        redis: {
          endpoint: "store.index-cache-write.redis.endpoint",
          master_name: "store.index-cache-write.redis.master-name",
          timeout: "store.index-cache-write.redis.timeout",
          expiration: "store.index-cache-write.redis.expiration",
          db: "store.index-cache-write.redis.db",
          pool_size: "store.index-cache-write.redis.pool-size",
          password: "store.index-cache-write.redis.password",
          tls_enabled: "store.index-cache-write.redis.tls-enabled",
          tls_insecure_skip_verify: "store.index-cache-write.redis.tls-insecure-skip-verify",
          idle_timeout: "store.index-cache-write.redis.idle-timeout",
          max_connection_age: "store.index-cache-write.redis.max-connection-age"
        },
        fifocache: {
          max_size_bytes: "store.index-cache-write.fifocache.max-size-bytes",
          max_size_items: "store.index-cache-write.fifocache.max-size-items",
          validity: "store.index-cache-write.fifocache.duration",
          size: "store.index-cache-write.fifocache.size"
        }
      },
      cache_lookups_older_than: "store.cache-lookups-older-than",
      max_look_back_period: "store.max-look-back-period"
    },
    limits: {
      ingestion_rate: "distributor.ingestion-rate-limit",
      ingestion_rate_strategy: "distributor.ingestion-rate-limit-strategy",
      ingestion_burst_size: "distributor.ingestion-burst-size",
      accept_ha_samples: "distributor.ha-tracker.enable-for-all-users",
      ha_cluster_label: "distributor.ha-tracker.cluster",
      ha_replica_label: "distributor.ha-tracker.replica",
      ha_max_clusters: "distributor.ha-tracker.max-clusters",
      drop_labels: "distributor.drop-label",
      max_label_name_length: "validation.max-length-label-name",
      max_label_value_length: "validation.max-length-label-value",
      max_label_names_per_series: "validation.max-label-names-per-series",
      max_metadata_length: "validation.max-metadata-length",
      reject_old_samples: "validation.reject-old-samples",
      reject_old_samples_max_age: "validation.reject-old-samples.max-age",
      creation_grace_period: "validation.create-grace-period",
      enforce_metadata_metric_name: "validation.enforce-metadata-metric-name",
      enforce_metric_name: "validation.enforce-metric-name",
      ingestion_tenant_shard_size: "distributor.ingestion-tenant-shard-size",
      metric_relabel_configs: "",
      max_series_per_query: "ingester.max-series-per-query",
      max_samples_per_query: "ingester.max-samples-per-query",
      max_series_per_user: "ingester.max-series-per-user",
      max_series_per_metric: "ingester.max-series-per-metric",
      max_global_series_per_user: "ingester.max-global-series-per-user",
      max_global_series_per_metric: "ingester.max-global-series-per-metric",
      min_chunk_length: "ingester.min-chunk-length",
      max_metadata_per_user: "ingester.max-metadata-per-user",
      max_metadata_per_metric: "ingester.max-metadata-per-metric",
      max_global_metadata_per_user: "ingester.max-global-metadata-per-user",
      max_global_metadata_per_metric: "ingester.max-global-metadata-per-metric",
      max_chunks_per_query: "store.query-chunk-limit",
      max_query_lookback: "querier.max-query-lookback",
      max_query_length: "store.max-query-length",
      max_query_parallelism: "querier.max-query-parallelism",
      cardinality_limit: "store.cardinality-limit",
      max_cache_freshness: "frontend.max-cache-freshness",
      max_queriers_per_tenant: "frontend.max-queriers-per-tenant",
      ruler_evaluation_delay_duration: "ruler.evaluation-delay-duration",
      ruler_tenant_shard_size: "ruler.tenant-shard-size",
      ruler_max_rules_per_rule_group: "ruler.max-rules-per-rule-group",
      ruler_max_rule_groups_per_tenant: "ruler.max-rule-groups-per-tenant",
      store_gateway_tenant_shard_size: "store-gateway.tenant-shard-size",
      per_tenant_override_config: "limits.per-user-override-config",
      per_tenant_override_period: "limits.per-user-override-period"
    },
    frontend_worker: {
      frontend_address: "querier.frontend-address",
      scheduler_address: "querier.scheduler-address",
      dns_lookup_duration: "querier.dns-lookup-period",
      parallelism: "querier.worker-parallelism",
      match_max_concurrent: "querier.worker-match-max-concurrent",
      id: "querier.id",
      grpc_client_config: {
        max_recv_msg_size: "querier.frontend-client.grpc-max-recv-msg-size",
        max_send_msg_size: "querier.frontend-client.grpc-max-send-msg-size",
        grpc_compression: "querier.frontend-client.grpc-compression",
        rate_limit: "querier.frontend-client.grpc-client-rate-limit",
        rate_limit_burst: "querier.frontend-client.grpc-client-rate-limit-burst",
        backoff_on_ratelimits: "querier.frontend-client.backoff-on-ratelimits",
        backoff_config: {
          min_period: "querier.frontend-client.backoff-min-period",
          max_period: "querier.frontend-client.backoff-max-period",
          max_retries: "querier.frontend-client.backoff-retries"
        },
        tls_cert_path: "querier.frontend-client.tls-cert-path",
        tls_key_path: "querier.frontend-client.tls-key-path",
        tls_ca_path: "querier.frontend-client.tls-ca-path",
        tls_insecure_skip_verify: "querier.frontend-client.tls-insecure-skip-verify"
      }
    },
    frontend: {
      log_queries_longer_than: "frontend.log-queries-longer-than",
      max_body_size: "frontend.max-body-size",
      query_stats_enabled: "frontend.query-stats-enabled",
      max_outstanding_per_tenant: "querier.max-outstanding-requests-per-tenant",
      scheduler_address: "frontend.scheduler-address",
      scheduler_dns_lookup_period: "frontend.scheduler-dns-lookup-period",
      scheduler_worker_concurrency: "frontend.scheduler-worker-concurrency",
      grpc_client_config: {
        max_recv_msg_size: "frontend.grpc-client-config.grpc-max-recv-msg-size",
        max_send_msg_size: "frontend.grpc-client-config.grpc-max-send-msg-size",
        grpc_compression: "frontend.grpc-client-config.grpc-compression",
        rate_limit: "frontend.grpc-client-config.grpc-client-rate-limit",
        rate_limit_burst: "frontend.grpc-client-config.grpc-client-rate-limit-burst",
        backoff_on_ratelimits: "frontend.grpc-client-config.backoff-on-ratelimits",
        backoff_config: {
          min_period: "frontend.grpc-client-config.backoff-min-period",
          max_period: "frontend.grpc-client-config.backoff-max-period",
          max_retries: "frontend.grpc-client-config.backoff-retries"
        },
        tls_cert_path: "frontend.grpc-client-config.tls-cert-path",
        tls_key_path: "frontend.grpc-client-config.tls-key-path",
        tls_ca_path: "frontend.grpc-client-config.tls-ca-path",
        tls_insecure_skip_verify: "frontend.grpc-client-config.tls-insecure-skip-verify"
      },
      instance_interface_names: "frontend.instance-interface-names",
      compress_responses: "querier.compress-http-responses",
      downstream_url: "frontend.downstream-url"
    },
    query_range: {
      split_queries_by_interval: "querier.split-queries-by-interval",
      split_queries_by_day: "querier.split-queries-by-day",
      align_queries_with_step: "querier.align-querier-with-step",
      results_cache: {
        cache: {
          enable_fifocache: "frontend.cache.enable-fifocache",
          default_validity: "frontend.default-validity",
          background: {
            writeback_goroutines: "frontend.background.write-back-concurrency",
            writeback_buffer: "frontend.background.write-back-buffer"
          },
          memcached: {
            expiration: "frontend.memcached.expiration",
            batch_size: "frontend.memcached.batchsize",
            parallelism: "frontend.memcached.parallelism"
          },
          memcached_client: {
            host: "frontend.memcached.hostname",
            service: "frontend.memcached.service",
            addresses: "frontend.memcached.addresses",
            timeout: "frontend.memcached.timeout",
            max_idle_conns: "frontend.memcached.max-idle-conns",
            update_interval: "frontend.memcached.update-interval",
            consistent_hash: "frontend.memcached.consistent-hash",
            circuit_breaker_consecutive_failures: "frontend.memcached.circuit-breaker-consecutive-failures",
            circuit_breaker_timeout: "frontend.memcached.circuit-breaker-timeout",
            circuit_breaker_interval: "frontend.memcached.circuit-breaker-interval"
          },
          redis: {
            endpoint: "frontend.redis.endpoint",
            master_name: "frontend.redis.master-name",
            timeout: "frontend.redis.timeout",
            expiration: "frontend.redis.expiration",
            db: "frontend.redis.db",
            pool_size: "frontend.redis.pool-size",
            password: "frontend.redis.password",
            tls_enabled: "frontend.redis.tls-enabled",
            tls_insecure_skip_verify: "frontend.redis.tls-insecure-skip-verify",
            idle_timeout: "frontend.redis.idle-timeout",
            max_connection_age: "frontend.redis.max-connection-age"
          },
          fifocache: {
            max_size_bytes: "frontend.fifocache.max-size-bytes",
            max_size_items: "frontend.fifocache.max-size-items",
            validity: "frontend.fifocache.duration",
            size: "frontend.fifocache.size"
          }
        },
        compression: "frontend.compression"
      },
      cache_results: "querier.cache-results",
      max_retries: "querier.max-retries-per-request",
      parallelise_shardable_queries: "querier.parallelise-shardable-queries"
    },
    table_manager: {
      throughput_updates_disabled: "table-manager.throughput-updates-disabled",
      retention_deletes_enabled: "table-manager.retention-deletes-enabled",
      retention_period: "table-manager.retention-period",
      poll_interval: "table-manager.poll-interval",
      creation_grace_period: "table-manager.periodic-table.grace-period",
      index_tables_provisioning: {
        enable_ondemand_throughput_mode: "table-manager.index-table.enable-ondemand-throughput-mode",
        provisioned_write_throughput: "table-manager.index-table.write-throughput",
        provisioned_read_throughput: "table-manager.index-table.read-throughput",
        write_scale: {
          enabled: "table-manager.index-table.write-throughput.scale.enabled",
          role_arn: "table-manager.index-table.write-throughput.scale.role-arn",
          min_capacity: "table-manager.index-table.write-throughput.scale.min-capacity",
          max_capacity: "table-manager.index-table.write-throughput.scale.max-capacity",
          out_cooldown: "table-manager.index-table.write-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.index-table.write-throughput.scale.in-cooldown",
          target: "table-manager.index-table.write-throughput.scale.target-value"
        },
        read_scale: {
          enabled: "table-manager.index-table.read-throughput.scale.enabled",
          role_arn: "table-manager.index-table.read-throughput.scale.role-arn",
          min_capacity: "table-manager.index-table.read-throughput.scale.min-capacity",
          max_capacity: "table-manager.index-table.read-throughput.scale.max-capacity",
          out_cooldown: "table-manager.index-table.read-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.index-table.read-throughput.scale.in-cooldown",
          target: "table-manager.index-table.read-throughput.scale.target-value"
        },
        enable_inactive_throughput_on_demand_mode: "table-manager.index-table.inactive-enable-ondemand-throughput-mode",
        inactive_write_throughput: "table-manager.index-table.inactive-write-throughput",
        inactive_read_throughput: "table-manager.index-table.inactive-read-throughput",
        inactive_write_scale: {
          enabled: "table-manager.index-table.inactive-write-throughput.scale.enabled",
          role_arn: "table-manager.index-table.inactive-write-throughput.scale.role-arn",
          min_capacity: "table-manager.index-table.inactive-write-throughput.scale.min-capacity",
          max_capacity: "table-manager.index-table.inactive-write-throughput.scale.max-capacity",
          out_cooldown: "table-manager.index-table.inactive-write-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.index-table.inactive-write-throughput.scale.in-cooldown",
          target: "table-manager.index-table.inactive-write-throughput.scale.target-value"
        },
        inactive_read_scale: {
          enabled: "table-manager.index-table.inactive-read-throughput.scale.enabled",
          role_arn: "table-manager.index-table.inactive-read-throughput.scale.role-arn",
          min_capacity: "table-manager.index-table.inactive-read-throughput.scale.min-capacity",
          max_capacity: "table-manager.index-table.inactive-read-throughput.scale.max-capacity",
          out_cooldown: "table-manager.index-table.inactive-read-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.index-table.inactive-read-throughput.scale.in-cooldown",
          target: "table-manager.index-table.inactive-read-throughput.scale.target-value"
        },
        inactive_write_scale_lastn: "table-manager.index-table.inactive-write-throughput.scale-last-n",
        inactive_read_scale_lastn: "table-manager.index-table.inactive-read-throughput.scale-last-n"
      },
      chunk_tables_provisioning: {
        enable_ondemand_throughput_mode: "table-manager.chunk-table.enable-ondemand-throughput-mode",
        provisioned_write_throughput: "table-manager.chunk-table.write-throughput",
        provisioned_read_throughput: "table-manager.chunk-table.read-throughput",
        write_scale: {
          enabled: "table-manager.chunk-table.write-throughput.scale.enabled",
          role_arn: "table-manager.chunk-table.write-throughput.scale.role-arn",
          min_capacity: "table-manager.chunk-table.write-throughput.scale.min-capacity",
          max_capacity: "table-manager.chunk-table.write-throughput.scale.max-capacity",
          out_cooldown: "table-manager.chunk-table.write-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.chunk-table.write-throughput.scale.in-cooldown",
          target: "table-manager.chunk-table.write-throughput.scale.target-value"
        },
        read_scale: {
          enabled: "table-manager.chunk-table.read-throughput.scale.enabled",
          role_arn: "table-manager.chunk-table.read-throughput.scale.role-arn",
          min_capacity: "table-manager.chunk-table.read-throughput.scale.min-capacity",
          max_capacity: "table-manager.chunk-table.read-throughput.scale.max-capacity",
          out_cooldown: "table-manager.chunk-table.read-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.chunk-table.read-throughput.scale.in-cooldown",
          target: "table-manager.chunk-table.read-throughput.scale.target-value"
        },
        enable_inactive_throughput_on_demand_mode: "table-manager.chunk-table.inactive-enable-ondemand-throughput-mode",
        inactive_write_throughput: "table-manager.chunk-table.inactive-write-throughput",
        inactive_read_throughput: "table-manager.chunk-table.inactive-read-throughput",
        inactive_write_scale: {
          enabled: "table-manager.chunk-table.inactive-write-throughput.scale.enabled",
          role_arn: "table-manager.chunk-table.inactive-write-throughput.scale.role-arn",
          min_capacity: "table-manager.chunk-table.inactive-write-throughput.scale.min-capacity",
          max_capacity: "table-manager.chunk-table.inactive-write-throughput.scale.max-capacity",
          out_cooldown: "table-manager.chunk-table.inactive-write-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.chunk-table.inactive-write-throughput.scale.in-cooldown",
          target: "table-manager.chunk-table.inactive-write-throughput.scale.target-value"
        },
        inactive_read_scale: {
          enabled: "table-manager.chunk-table.inactive-read-throughput.scale.enabled",
          role_arn: "table-manager.chunk-table.inactive-read-throughput.scale.role-arn",
          min_capacity: "table-manager.chunk-table.inactive-read-throughput.scale.min-capacity",
          max_capacity: "table-manager.chunk-table.inactive-read-throughput.scale.max-capacity",
          out_cooldown: "table-manager.chunk-table.inactive-read-throughput.scale.out-cooldown",
          in_cooldown: "table-manager.chunk-table.inactive-read-throughput.scale.in-cooldown",
          target: "table-manager.chunk-table.inactive-read-throughput.scale.target-value"
        },
        inactive_write_scale_lastn: "table-manager.chunk-table.inactive-write-throughput.scale-last-n",
        inactive_read_scale_lastn: "table-manager.chunk-table.inactive-read-throughput.scale-last-n"
      }
    },
    blocks_storage: {
      backend: "blocks-storage.backend",
      s3: {
        endpoint: "blocks-storage.s3.endpoint",
        bucket_name: "blocks-storage.s3.bucket-name",
        secret_access_key: "blocks-storage.s3.secret-access-key",
        access_key_id: "blocks-storage.s3.access-key-id",
        insecure: "blocks-storage.s3.insecure",
        signature_version: "blocks-storage.s3.signature-version",
        http: {
          idle_conn_timeout: "blocks-storage.s3.http.idle-conn-timeout",
          response_header_timeout: "blocks-storage.s3.http.response-header-timeout",
          insecure_skip_verify: "blocks-storage.s3.http.insecure-skip-verify"
        }
      },
      gcs: {
        bucket_name: "blocks-storage.gcs.bucket-name",
        service_account: "blocks-storage.gcs.service-account"
      },
      azure: {
        account_name: "blocks-storage.azure.account-name",
        account_key: "blocks-storage.azure.account-key",
        container_name: "blocks-storage.azure.container-name",
        endpoint_suffix: "blocks-storage.azure.endpoint-suffix",
        max_retries: "blocks-storage.azure.max-retries"
      },
      swift: {
        auth_version: "blocks-storage.swift.auth-version",
        auth_url: "blocks-storage.swift.auth-url",
        username: "blocks-storage.swift.username",
        user_domain_name: "blocks-storage.swift.user-domain-name",
        user_domain_id: "blocks-storage.swift.user-domain-id",
        user_id: "blocks-storage.swift.user-id",
        password: "blocks-storage.swift.password",
        domain_id: "blocks-storage.swift.domain-id",
        domain_name: "blocks-storage.swift.domain-name",
        project_id: "blocks-storage.swift.project-id",
        project_name: "blocks-storage.swift.project-name",
        project_domain_id: "blocks-storage.swift.project-domain-id",
        project_domain_name: "blocks-storage.swift.project-domain-name",
        region_name: "blocks-storage.swift.region-name",
        container_name: "blocks-storage.swift.container-name",
        max_retries: "blocks-storage.swift.max-retries",
        connect_timeout: "blocks-storage.swift.connect-timeout",
        request_timeout: "blocks-storage.swift.request-timeout"
      },
      filesystem: {
        dir: "blocks-storage.filesystem.dir"
      },
      bucket_store: {
        sync_dir: "blocks-storage.bucket-store.sync-dir",
        sync_interval: "blocks-storage.bucket-store.sync-interval",
        max_chunk_pool_bytes: "blocks-storage.bucket-store.max-chunk-pool-bytes",
        max_concurrent: "blocks-storage.bucket-store.max-concurrent",
        tenant_sync_concurrency: "blocks-storage.bucket-store.tenant-sync-concurrency",
        block_sync_concurrency: "blocks-storage.bucket-store.block-sync-concurrency",
        meta_sync_concurrency: "blocks-storage.bucket-store.meta-sync-concurrency",
        consistency_delay: "blocks-storage.bucket-store.consistency-delay",
        index_cache: {
          backend: "blocks-storage.bucket-store.index-cache.backend",
          inmemory: {
            max_size_bytes: "blocks-storage.bucket-store.index-cache.inmemory.max-size-bytes"
          },
          memcached: {
            addresses: "blocks-storage.bucket-store.index-cache.memcached.addresses",
            timeout: "blocks-storage.bucket-store.index-cache.memcached.timeout",
            max_idle_connections: "blocks-storage.bucket-store.index-cache.memcached.max-idle-connections",
            max_async_concurrency: "blocks-storage.bucket-store.index-cache.memcached.max-async-concurrency",
            max_async_buffer_size: "blocks-storage.bucket-store.index-cache.memcached.max-async-buffer-size",
            max_get_multi_concurrency: "blocks-storage.bucket-store.index-cache.memcached.max-get-multi-concurrency",
            max_get_multi_batch_size: "blocks-storage.bucket-store.index-cache.memcached.max-get-multi-batch-size",
            max_item_size: "blocks-storage.bucket-store.index-cache.memcached.max-item-size"
          },
          postings_compression_enabled: "blocks-storage.bucket-store.index-cache.postings-compression-enabled"
        },
        chunks_cache: {
          backend: "blocks-storage.bucket-store.chunks-cache.backend",
          memcached: {
            addresses: "blocks-storage.bucket-store.chunks-cache.memcached.addresses",
            timeout: "blocks-storage.bucket-store.chunks-cache.memcached.timeout",
            max_idle_connections: "blocks-storage.bucket-store.chunks-cache.memcached.max-idle-connections",
            max_async_concurrency: "blocks-storage.bucket-store.chunks-cache.memcached.max-async-concurrency",
            max_async_buffer_size: "blocks-storage.bucket-store.chunks-cache.memcached.max-async-buffer-size",
            max_get_multi_concurrency: "blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-concurrency",
            max_get_multi_batch_size: "blocks-storage.bucket-store.chunks-cache.memcached.max-get-multi-batch-size",
            max_item_size: "blocks-storage.bucket-store.chunks-cache.memcached.max-item-size"
          },
          subrange_size: "blocks-storage.bucket-store.chunks-cache.subrange-size",
          max_get_range_requests: "blocks-storage.bucket-store.chunks-cache.max-get-range-requests",
          attributes_ttl: "blocks-storage.bucket-store.chunks-cache.attributes-ttl",
          subrange_ttl: "blocks-storage.bucket-store.chunks-cache.subrange-ttl"
        },
        metadata_cache: {
          backend: "blocks-storage.bucket-store.metadata-cache.backend",
          memcached: {
            addresses: "blocks-storage.bucket-store.metadata-cache.memcached.addresses",
            timeout: "blocks-storage.bucket-store.metadata-cache.memcached.timeout",
            max_idle_connections: "blocks-storage.bucket-store.metadata-cache.memcached.max-idle-connections",
            max_async_concurrency: "blocks-storage.bucket-store.metadata-cache.memcached.max-async-concurrency",
            max_async_buffer_size: "blocks-storage.bucket-store.metadata-cache.memcached.max-async-buffer-size",
            max_get_multi_concurrency: "blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-concurrency",
            max_get_multi_batch_size: "blocks-storage.bucket-store.metadata-cache.memcached.max-get-multi-batch-size",
            max_item_size: "blocks-storage.bucket-store.metadata-cache.memcached.max-item-size"
          },
          tenants_list_ttl: "blocks-storage.bucket-store.metadata-cache.tenants-list-ttl",
          tenant_blocks_list_ttl: "blocks-storage.bucket-store.metadata-cache.tenant-blocks-list-ttl",
          chunks_list_ttl: "blocks-storage.bucket-store.metadata-cache.chunks-list-ttl",
          metafile_exists_ttl: "blocks-storage.bucket-store.metadata-cache.metafile-exists-ttl",
          metafile_doesnt_exist_ttl: "blocks-storage.bucket-store.metadata-cache.metafile-doesnt-exist-ttl",
          metafile_content_ttl: "blocks-storage.bucket-store.metadata-cache.metafile-content-ttl",
          metafile_max_size_bytes: "blocks-storage.bucket-store.metadata-cache.metafile-max-size-bytes",
          metafile_attributes_ttl: "blocks-storage.bucket-store.metadata-cache.metafile-attributes-ttl",
          block_index_attributes_ttl: "blocks-storage.bucket-store.metadata-cache.block-index-attributes-ttl",
          bucket_index_content_ttl: "blocks-storage.bucket-store.metadata-cache.bucket-index-content-ttl",
          bucket_index_max_size_bytes: "blocks-storage.bucket-store.metadata-cache.bucket-index-max-size-bytes"
        },
        ignore_deletion_mark_delay: "blocks-storage.bucket-store.ignore-deletion-marks-delay",
        bucket_index: {
          enabled: "blocks-storage.bucket-store.bucket-index.enabled",
          update_on_error_interval: "blocks-storage.bucket-store.bucket-index.update-on-error-interval",
          idle_timeout: "blocks-storage.bucket-store.bucket-index.idle-timeout",
          max_stale_period: "blocks-storage.bucket-store.bucket-index.max-stale-period"
        }
      },
      tsdb: {
        dir: "blocks-storage.tsdb.dir",
        block_ranges_period: "blocks-storage.tsdb.block-ranges-period",
        retention_period: "blocks-storage.tsdb.retention-period",
        ship_interval: "blocks-storage.tsdb.ship-interval",
        ship_concurrency: "blocks-storage.tsdb.ship-concurrency",
        head_compaction_interval: "blocks-storage.tsdb.head-compaction-interval",
        head_compaction_concurrency: "blocks-storage.tsdb.head-compaction-concurrency",
        head_compaction_idle_timeout: "blocks-storage.tsdb.head-compaction-idle-timeout",
        head_chunks_write_buffer_size_bytes: "blocks-storage.tsdb.head-chunks-write-buffer-size-bytes",
        stripe_size: "blocks-storage.tsdb.stripe-size",
        wal_compression_enabled: "blocks-storage.tsdb.wal-compression-enabled",
        wal_segment_size_bytes: "blocks-storage.tsdb.wal-segment-size-bytes",
        flush_blocks_on_shutdown: "blocks-storage.tsdb.flush-blocks-on-shutdown",
        close_idle_tsdb_timeout: "blocks-storage.tsdb.close-idle-tsdb-timeout",
        max_tsdb_opening_concurrency_on_startup: "blocks-storage.tsdb.max-tsdb-opening-concurrency-on-startup"
      }
    },
    compactor: {
      block_ranges: "compactor.block-ranges",
      block_sync_concurrency: "compactor.block-sync-concurrency",
      meta_sync_concurrency: "compactor.meta-sync-concurrency",
      consistency_delay: "compactor.consistency-delay",
      data_dir: "compactor.data-dir",
      compaction_interval: "compactor.compaction-interval",
      compaction_retries: "compactor.compaction-retries",
      compaction_concurrency: "compactor.compaction-concurrency",
      cleanup_interval: "compactor.cleanup-interval",
      cleanup_concurrency: "compactor.cleanup-concurrency",
      deletion_delay: "compactor.deletion-delay",
      tenant_cleanup_delay: "compactor.tenant-cleanup-delay",
      block_deletion_marks_migration_enabled: "compactor.block-deletion-marks-migration-enabled",
      enabled_tenants: "compactor.enabled-tenants",
      disabled_tenants: "compactor.disabled-tenants",
      sharding_enabled: "compactor.sharding-enabled",
      sharding_ring: {
        kvstore: {
          store: "compactor.ring.store",
          prefix: "compactor.ring.prefix",
          consul: {
            host: "compactor.ring.consul.hostname",
            acl_token: "compactor.ring.consul.acl-token",
            http_client_timeout: "compactor.ring.consul.client-timeout",
            consistent_reads: "compactor.ring.consul.consistent-reads",
            watch_rate_limit: "compactor.ring.consul.watch-rate-limit",
            watch_burst_size: "compactor.ring.consul.watch-burst-size"
          },
          etcd: {
            endpoints: "compactor.ring.etcd.endpoints",
            dial_timeout: "compactor.ring.etcd.dial-timeout",
            max_retries: "compactor.ring.etcd.max-retries",
            tls_enabled: "compactor.ring.etcd.tls-enabled",
            tls_cert_path: "compactor.ring.etcd.tls-cert-path",
            tls_key_path: "compactor.ring.etcd.tls-key-path",
            tls_ca_path: "compactor.ring.etcd.tls-ca-path",
            tls_insecure_skip_verify: "compactor.ring.etcd.tls-insecure-skip-verify"
          },
          multi: {
            primary: "compactor.ring.multi.primary",
            secondary: "compactor.ring.multi.secondary",
            mirror_enabled: "compactor.ring.multi.mirror-enabled",
            mirror_timeout: "compactor.ring.multi.mirror-timeout"
          }
        },
        heartbeat_period: "compactor.ring.heartbeat-period",
        heartbeat_timeout: "compactor.ring.heartbeat-timeout",
        wait_stability_min_duration: "compactor.ring.wait-stability-min-duration",
        wait_stability_max_duration: "compactor.ring.wait-stability-max-duration",
        instance_interface_names: "compactor.ring.instance-interface-names"
      }
    },
    store_gateway: {
      sharding_enabled: "store-gateway.sharding-enabled",
      sharding_ring: {
        kvstore: {
          store: "store-gateway.sharding-ring.store",
          prefix: "store-gateway.sharding-ring.prefix",
          consul: {
            host: "store-gateway.sharding-ring.consul.hostname",
            acl_token: "store-gateway.sharding-ring.consul.acl-token",
            http_client_timeout: "store-gateway.sharding-ring.consul.client-timeout",
            consistent_reads: "store-gateway.sharding-ring.consul.consistent-reads",
            watch_rate_limit: "store-gateway.sharding-ring.consul.watch-rate-limit",
            watch_burst_size: "store-gateway.sharding-ring.consul.watch-burst-size"
          },
          etcd: {
            endpoints: "store-gateway.sharding-ring.etcd.endpoints",
            dial_timeout: "store-gateway.sharding-ring.etcd.dial-timeout",
            max_retries: "store-gateway.sharding-ring.etcd.max-retries",
            tls_enabled: "store-gateway.sharding-ring.etcd.tls-enabled",
            tls_cert_path: "store-gateway.sharding-ring.etcd.tls-cert-path",
            tls_key_path: "store-gateway.sharding-ring.etcd.tls-key-path",
            tls_ca_path: "store-gateway.sharding-ring.etcd.tls-ca-path",
            tls_insecure_skip_verify: "store-gateway.sharding-ring.etcd.tls-insecure-skip-verify"
          },
          multi: {
            primary: "store-gateway.sharding-ring.multi.primary",
            secondary: "store-gateway.sharding-ring.multi.secondary",
            mirror_enabled: "store-gateway.sharding-ring.multi.mirror-enabled",
            mirror_timeout: "store-gateway.sharding-ring.multi.mirror-timeout"
          }
        },
        heartbeat_period: "store-gateway.sharding-ring.heartbeat-period",
        heartbeat_timeout: "store-gateway.sharding-ring.heartbeat-timeout",
        replication_factor: "store-gateway.sharding-ring.replication-factor",
        tokens_file_path: "store-gateway.sharding-ring.tokens-file-path",
        zone_awareness_enabled: "store-gateway.sharding-ring.zone-awareness-enabled",
        instance_interface_names: "store-gateway.sharding-ring.instance-interface-names",
        instance_availability_zone: "store-gateway.sharding-ring.instance-availability-zone"
      },
      sharding_strategy: "store-gateway.sharding-strategy"
    },
    purger: {
      enable: "purger.enable",
      num_workers: "purger.num-workers",
      object_store_type: "purger.object-store-type",
      delete_request_cancel_period: "purger.delete-request-cancel-period"
    },
    tenant_federation: {
      enabled: "tenant-federation.enabled"
    },
    ruler: {
      external_url: "ruler.external.url",
      ruler_client: {
        max_recv_msg_size: "ruler.client.grpc-max-recv-msg-size",
        max_send_msg_size: "ruler.client.grpc-max-send-msg-size",
        grpc_compression: "ruler.client.grpc-compression",
        rate_limit: "ruler.client.grpc-client-rate-limit",
        rate_limit_burst: "ruler.client.grpc-client-rate-limit-burst",
        backoff_on_ratelimits: "ruler.client.backoff-on-ratelimits",
        backoff_config: {
          min_period: "ruler.client.backoff-min-period",
          max_period: "ruler.client.backoff-max-period",
          max_retries: "ruler.client.backoff-retries"
        },
        tls_cert_path: "ruler.client.tls-cert-path",
        tls_key_path: "ruler.client.tls-key-path",
        tls_ca_path: "ruler.client.tls-ca-path",
        tls_insecure_skip_verify: "ruler.client.tls-insecure-skip-verify"
      },
      evaluation_interval: "ruler.evaluation-interval",
      poll_interval: "ruler.poll-interval",
      storage: {
        type: "ruler.storage.type",
        configdb: {
          configs_api_url: "ruler.configs.url",
          client_timeout: "ruler.configs.client-timeout",
          tls_cert_path: "ruler.configs.tls-cert-path",
          tls_key_path: "ruler.configs.tls-key-path",
          tls_ca_path: "ruler.configs.tls-ca-path",
          tls_insecure_skip_verify: "ruler.configs.tls-insecure-skip-verify"
        },
        azure: {
          environment: "ruler.storage.azure.environment",
          container_name: "ruler.storage.azure.container-name",
          account_name: "ruler.storage.azure.account-name",
          account_key: "ruler.storage.azure.account-key",
          download_buffer_size: "ruler.storage.azure.download-buffer-size",
          upload_buffer_size: "ruler.storage.azure.upload-buffer-size",
          upload_buffer_count: "ruler.storage.azure.download-buffer-count",
          request_timeout: "ruler.storage.azure.request-timeout",
          max_retries: "ruler.storage.azure.max-retries",
          min_retry_delay: "ruler.storage.azure.min-retry-delay",
          max_retry_delay: "ruler.storage.azure.max-retry-delay"
        },
        gcs: {
          bucket_name: "ruler.storage.gcs.bucketname",
          chunk_buffer_size: "ruler.storage.gcs.chunk-buffer-size",
          request_timeout: "ruler.storage.gcs.request-timeout"
        },
        s3: {
          s3: "ruler.storage.s3.url",
          s3forcepathstyle: "ruler.storage.s3.force-path-style",
          bucketnames: "ruler.storage.s3.buckets",
          endpoint: "ruler.storage.s3.endpoint",
          region: "ruler.storage.s3.region",
          access_key_id: "ruler.storage.s3.access-key-id",
          secret_access_key: "ruler.storage.s3.secret-access-key",
          insecure: "ruler.storage.s3.insecure",
          sse_encryption: "ruler.storage.s3.sse-encryption",
          http_config: {
            idle_conn_timeout: "ruler.storage.s3.http.idle-conn-timeout",
            response_header_timeout: "ruler.storage.s3.http.response-header-timeout",
            insecure_skip_verify: "ruler.storage.s3.http.insecure-skip-verify"
          },
          signature_version: "ruler.storage.s3.signature-version"
        },
        swift: {
          auth_version: "ruler.storage.swift.auth-version",
          auth_url: "ruler.storage.swift.auth-url",
          username: "ruler.storage.swift.username",
          user_domain_name: "ruler.storage.swift.user-domain-name",
          user_domain_id: "ruler.storage.swift.user-domain-id",
          user_id: "ruler.storage.swift.user-id",
          password: "ruler.storage.swift.password",
          domain_id: "ruler.storage.swift.domain-id",
          domain_name: "ruler.storage.swift.domain-name",
          project_id: "ruler.storage.swift.project-id",
          project_name: "ruler.storage.swift.project-name",
          project_domain_id: "ruler.storage.swift.project-domain-id",
          project_domain_name: "ruler.storage.swift.project-domain-name",
          region_name: "ruler.storage.swift.region-name",
          container_name: "ruler.storage.swift.container-name",
          max_retries: "ruler.storage.swift.max-retries",
          connect_timeout: "ruler.storage.swift.connect-timeout",
          request_timeout: "ruler.storage.swift.request-timeout"
        },
        'local': {
          directory: "ruler.storage.local.directory"
        }
      },
      rule_path: "ruler.rule-path",
      alertmanager_url: "ruler.alertmanager-url",
      enable_alertmanager_discovery: "ruler.alertmanager-discovery",
      alertmanager_refresh_interval: "ruler.alertmanager-refresh-interval",
      enable_alertmanager_v2: "ruler.alertmanager-use-v2",
      notification_queue_capacity: "ruler.notification-queue-capacity",
      notification_timeout: "ruler.notification-timeout",
      for_outage_tolerance: "ruler.for-outage-tolerance",
      for_grace_period: "ruler.for-grace-period",
      resend_delay: "ruler.resend-delay",
      enable_sharding: "ruler.enable-sharding",
      sharding_strategy: "ruler.sharding-strategy",
      search_pending_for: "ruler.search-pending-for",
      ring: {
        kvstore: {
          store: "ruler.ring.store",
          prefix: "ruler.ring.prefix",
          consul: {
            host: "ruler.ring.consul.hostname",
            acl_token: "ruler.ring.consul.acl-token",
            http_client_timeout: "ruler.ring.consul.client-timeout",
            consistent_reads: "ruler.ring.consul.consistent-reads",
            watch_rate_limit: "ruler.ring.consul.watch-rate-limit",
            watch_burst_size: "ruler.ring.consul.watch-burst-size"
          },
          etcd: {
            endpoints: "ruler.ring.etcd.endpoints",
            dial_timeout: "ruler.ring.etcd.dial-timeout",
            max_retries: "ruler.ring.etcd.max-retries",
            tls_enabled: "ruler.ring.etcd.tls-enabled",
            tls_cert_path: "ruler.ring.etcd.tls-cert-path",
            tls_key_path: "ruler.ring.etcd.tls-key-path",
            tls_ca_path: "ruler.ring.etcd.tls-ca-path",
            tls_insecure_skip_verify: "ruler.ring.etcd.tls-insecure-skip-verify"
          },
          multi: {
            primary: "ruler.ring.multi.primary",
            secondary: "ruler.ring.multi.secondary",
            mirror_enabled: "ruler.ring.multi.mirror-enabled",
            mirror_timeout: "ruler.ring.multi.mirror-timeout"
          }
        },
        heartbeat_period: "ruler.ring.heartbeat-period",
        heartbeat_timeout: "ruler.ring.heartbeat-timeout",
        instance_interface_names: "ruler.ring.instance-interface-names",
        num_tokens: "ruler.ring.num-tokens"
      },
      flush_period: "ruler.flush-period",
      enable_api: "experimental.ruler.enable-api"
    },
    configs: {
      database: {
        uri: "configs.database.uri",
        migrations_dir: "configs.database.migrations-dir",
        password_file: "configs.database.password-file"
      },
      api: {
        notifications: {
          disable_email: "configs.notifications.disable-email",
          disable_webhook: "configs.notifications.disable-webhook"
        }
      }
    },
    alertmanager: {
      data_dir: "alertmanager.storage.path",
      retention: "alertmanager.storage.retention",
      external_url: "alertmanager.web.external-url",
      poll_interval: "alertmanager.configs.poll-interval",
      cluster_bind_address: "cluster.listen-address",
      cluster_advertise_address: "cluster.advertise-address",
      peers: "cluster.peer",
      peer_timeout: "cluster.peer-timeout",
      fallback_config_file: "alertmanager.configs.fallback",
      auto_webhook_root: "alertmanager.configs.auto-webhook-root",
      storage: {
        type: "alertmanager.storage.type",
        configdb: {
          configs_api_url: "alertmanager.configs.url",
          client_timeout: "alertmanager.configs.client-timeout",
          tls_cert_path: "alertmanager.configs.tls-cert-path",
          tls_key_path: "alertmanager.configs.tls-key-path",
          tls_ca_path: "alertmanager.configs.tls-ca-path",
          tls_insecure_skip_verify: "alertmanager.configs.tls-insecure-skip-verify"
        },
        azure: {
          environment: "alertmanager.storage.azure.environment",
          container_name: "alertmanager.storage.azure.container-name",
          account_name: "alertmanager.storage.azure.account-name",
          account_key: "alertmanager.storage.azure.account-key",
          download_buffer_size: "alertmanager.storage.azure.download-buffer-size",
          upload_buffer_size: "alertmanager.storage.azure.upload-buffer-size",
          upload_buffer_count: "alertmanager.storage.azure.download-buffer-count",
          request_timeout: "alertmanager.storage.azure.request-timeout",
          max_retries: "alertmanager.storage.azure.max-retries",
          min_retry_delay: "alertmanager.storage.azure.min-retry-delay",
          max_retry_delay: "alertmanager.storage.azure.max-retry-delay"
        },
        gcs: {
          bucket_name: "alertmanager.storage.gcs.bucketname",
          chunk_buffer_size: "alertmanager.storage.gcs.chunk-buffer-size",
          request_timeout: "alertmanager.storage.gcs.request-timeout"
        },
        s3: {
          s3: "alertmanager.storage.s3.url",
          s3forcepathstyle: "alertmanager.storage.s3.force-path-style",
          bucketnames: "alertmanager.storage.s3.buckets",
          endpoint: "alertmanager.storage.s3.endpoint",
          region: "alertmanager.storage.s3.region",
          access_key_id: "alertmanager.storage.s3.access-key-id",
          secret_access_key: "alertmanager.storage.s3.secret-access-key",
          insecure: "alertmanager.storage.s3.insecure",
          sse_encryption: "alertmanager.storage.s3.sse-encryption",
          http_config: {
            idle_conn_timeout: "alertmanager.storage.s3.http.idle-conn-timeout",
            response_header_timeout: "alertmanager.storage.s3.http.response-header-timeout",
            insecure_skip_verify: "alertmanager.storage.s3.http.insecure-skip-verify"
          },
          signature_version: "alertmanager.storage.s3.signature-version"
        },
        'local': {
          path: "alertmanager.storage.local.path"
        }
      },
      enable_api: "experimental.alertmanager.enable-api"
    },
    runtime_config: {
      period: "runtime-config.reload-period",
      file: "runtime-config.file"
    },
    memberlist: {
      node_name: "memberlist.nodename",
      randomize_node_name: "memberlist.randomize-node-name",
      stream_timeout: "memberlist.stream-timeout",
      retransmit_factor: "memberlist.retransmit-factor",
      pull_push_interval: "memberlist.pullpush-interval",
      gossip_interval: "memberlist.gossip-interval",
      gossip_nodes: "memberlist.gossip-nodes",
      gossip_to_dead_nodes_time: "memberlist.gossip-to-dead-nodes-time",
      dead_node_reclaim_time: "memberlist.dead-node-reclaim-time",
      join_members: "memberlist.join",
      min_join_backoff: "memberlist.min-join-backoff",
      max_join_backoff: "memberlist.max-join-backoff",
      max_join_retries: "memberlist.max-join-retries",
      abort_if_cluster_join_fails: "memberlist.abort-if-join-fails",
      rejoin_interval: "memberlist.rejoin-interval",
      left_ingesters_timeout: "memberlist.left-ingesters-timeout",
      leave_timeout: "memberlist.leave-timeout",
      message_history_buffer_bytes: "memberlist.message-history-buffer-bytes",
      bind_addr: "memberlist.bind-addr",
      bind_port: "memberlist.bind-port",
      packet_dial_timeout: "memberlist.packet-dial-timeout",
      packet_write_timeout: "memberlist.packet-write-timeout"
    },
    query_scheduler: {
      max_outstanding_requests_per_tenant: "query-scheduler.max-outstanding-requests-per-tenant",
      grpc_client_config: {
        max_recv_msg_size: "query-scheduler.grpc-client-config.grpc-max-recv-msg-size",
        max_send_msg_size: "query-scheduler.grpc-client-config.grpc-max-send-msg-size",
        grpc_compression: "query-scheduler.grpc-client-config.grpc-compression",
        rate_limit: "query-scheduler.grpc-client-config.grpc-client-rate-limit",
        rate_limit_burst: "query-scheduler.grpc-client-config.grpc-client-rate-limit-burst",
        backoff_on_ratelimits: "query-scheduler.grpc-client-config.backoff-on-ratelimits",
        backoff_config: {
          min_period: "query-scheduler.grpc-client-config.backoff-min-period",
          max_period: "query-scheduler.grpc-client-config.backoff-max-period",
          max_retries: "query-scheduler.grpc-client-config.backoff-retries"
        },
        tls_cert_path: "query-scheduler.grpc-client-config.tls-cert-path",
        tls_key_path: "query-scheduler.grpc-client-config.tls-key-path",
        tls_ca_path: "query-scheduler.grpc-client-config.tls-ca-path",
        tls_insecure_skip_verify: "query-scheduler.grpc-client-config.tls-insecure-skip-verify"
      }
    }
  },
  defaults:: {
    target: "all",
    auth_enabled: true,
    http_prefix: "/api/prom",
    api: {
      response_compression_enabled: false,
      alertmanager_http_prefix: "/alertmanager",
      prometheus_http_prefix: "/prometheus"
    },
    server: {
      http_listen_address: "",
      http_listen_port: 80,
      http_listen_conn_limit: 0,
      grpc_listen_address: "",
      grpc_listen_port: 9095,
      grpc_listen_conn_limit: 0,
      http_tls_config: {
        cert_file: "",
        key_file: "",
        client_auth_type: "",
        client_ca_file: ""
      },
      grpc_tls_config: {
        cert_file: "",
        key_file: "",
        client_auth_type: "",
        client_ca_file: ""
      },
      register_instrumentation: true,
      graceful_shutdown_timeout: "30s",
      http_server_read_timeout: "30s",
      http_server_write_timeout: "30s",
      http_server_idle_timeout: "2m0s",
      grpc_server_max_recv_msg_size: 4194304,
      grpc_server_max_send_msg_size: 4194304,
      grpc_server_max_concurrent_streams: 100,
      grpc_server_max_connection_idle: "2562047h47m16.854775807s",
      grpc_server_max_connection_age: "2562047h47m16.854775807s",
      grpc_server_max_connection_age_grace: "2562047h47m16.854775807s",
      grpc_server_keepalive_time: "2h0m0s",
      grpc_server_keepalive_timeout: "20s",
      grpc_server_min_time_between_pings: "5m0s",
      grpc_server_ping_without_stream_allowed: false,
      log_format: "logfmt",
      log_level: "info",
      log_source_ips_enabled: false,
      log_source_ips_header: "",
      log_source_ips_regex: "",
      http_path_prefix: ""
    },
    distributor: {
      pool: {
        client_cleanup_period: "15s",
        health_check_ingesters: true
      },
      ha_tracker: {
        enable_ha_tracker: false,
        ha_tracker_update_timeout: "15s",
        ha_tracker_update_timeout_jitter_max: "5s",
        ha_tracker_failover_timeout: "30s",
        kvstore: {
          store: "consul",
          prefix: "ha-tracker/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        }
      },
      max_recv_msg_size: 104857600,
      remote_timeout: "2s",
      extra_queue_delay: "0s",
      sharding_strategy: "default",
      shard_by_all_labels: false,
      ring: {
        kvstore: {
          store: "consul",
          prefix: "collectors/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "5s",
        heartbeat_timeout: "1m0s",
        instance_interface_names: ["eth0", "en0"]
      }
    },
    querier: {
      max_concurrent: 20,
      timeout: "2m0s",
      iterators: false,
      batch_iterators: true,
      ingester_streaming: true,
      max_samples: 50000000,
      query_ingesters_within: "0s",
      query_store_for_labels_enabled: false,
      query_store_after: "0s",
      max_query_into_future: "10m0s",
      default_evaluation_interval: "1m0s",
      active_query_tracker_dir: "./active-query-tracker",
      lookback_delta: "5m0s",
      store_gateway_addresses: "",
      store_gateway_client: {
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      },
      second_store_engine: "",
      use_second_store_before_time: "0",
      shuffle_sharding_ingesters_lookback_period: "0s"
    },
    ingester_client: {
      grpc_client_config: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      }
    },
    ingester: {
      walconfig: {
        wal_enabled: false,
        checkpoint_enabled: true,
        recover_from_wal: false,
        wal_dir: "wal",
        checkpoint_duration: "30m0s",
        flush_on_shutdown_with_wal_enabled: false
      },
      lifecycler: {
        ring: {
          kvstore: {
            store: "consul",
            prefix: "collectors/",
            consul: {
              host: "localhost:8500",
              acl_token: "",
              http_client_timeout: "20s",
              consistent_reads: false,
              watch_rate_limit: 1.000000,
              watch_burst_size: 1
            },
            etcd: {
              endpoints: [],
              dial_timeout: "10s",
              max_retries: 10,
              tls_enabled: false,
              tls_cert_path: "",
              tls_key_path: "",
              tls_ca_path: "",
              tls_insecure_skip_verify: false
            },
            multi: {
              primary: "",
              secondary: "",
              mirror_enabled: false,
              mirror_timeout: "2s"
            }
          },
          heartbeat_timeout: "1m0s",
          replication_factor: 3,
          zone_awareness_enabled: false,
          extend_writes: true
        },
        num_tokens: 128,
        heartbeat_period: "5s",
        observe_period: "0s",
        join_after: "0s",
        min_ready_duration: "1m0s",
        interface_names: ["eth0", "en0"],
        final_sleep: "30s",
        tokens_file_path: "",
        availability_zone: "",
        unregister_on_shutdown: true
      },
      max_transfer_retries: 10,
      flush_period: "1m0s",
      retain_period: "5m0s",
      max_chunk_idle_time: "5m0s",
      max_stale_chunk_idle_time: "2m0s",
      flush_op_timeout: "1m0s",
      max_chunk_age: "12h0m0s",
      chunk_age_jitter: "0s",
      concurrent_flushes: 50,
      spread_flushes: true,
      metadata_retain_period: "10m0s",
      rate_update_period: "15s",
      active_series_metrics_enabled: false,
      active_series_metrics_update_period: "1m0s",
      active_series_metrics_idle_timeout: "10m0s"
    },
    flusher: {
      wal_dir: "wal",
      concurrent_flushes: 50,
      flush_op_timeout: "2m0s",
      exit_after_flush: true
    },
    storage: {
      engine: "chunks",
      aws: {
        dynamodb: {
          dynamodb_url: "",
          api_limit: 2.000000,
          throttle_limit: 10.000000,
          metrics: {
            url: "",
            target_queue_length: 100000,
            scale_up_factor: 1.300000,
            ignore_throttle_below: 1.000000,
            queue_length_query: "sum(avg_over_time(cortex_ingester_flush_queue_length{job=\"cortex/ingester\"}[2m]))",
            write_throttle_query: "sum(rate(cortex_dynamo_throttled_total{operation=\"DynamoDB.BatchWriteItem\"}[1m])) by (table) \u003e 0",
            write_usage_query: "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.BatchWriteItem\"}[15m])) by (table) \u003e 0",
            read_usage_query: "sum(rate(cortex_dynamo_consumed_capacity_total{operation=\"DynamoDB.QueryPages\"}[1h])) by (table) \u003e 0",
            read_error_query: "sum(increase(cortex_dynamo_failures_total{operation=\"DynamoDB.QueryPages\",error=\"ProvisionedThroughputExceededException\"}[1m])) by (table) \u003e 0"
          },
          chunk_gang_size: 10,
          chunk_get_max_parallelism: 32,
          backoff_config: {
            min_period: "100ms",
            max_period: "50s",
            max_retries: 20
          }
        },
        s3: "",
        s3forcepathstyle: false,
        bucketnames: "",
        endpoint: "",
        region: "",
        access_key_id: "",
        secret_access_key: "",
        insecure: false,
        sse_encryption: false,
        http_config: {
          idle_conn_timeout: "1m30s",
          response_header_timeout: "0s",
          insecure_skip_verify: false
        },
        signature_version: "v4"
      },
      azure: {
        environment: "AzureGlobal",
        container_name: "cortex",
        account_name: "",
        account_key: "",
        download_buffer_size: 512000,
        upload_buffer_size: 256000,
        upload_buffer_count: 1,
        request_timeout: "30s",
        max_retries: 5,
        min_retry_delay: "10ms",
        max_retry_delay: "500ms"
      },
      bigtable: {
        project: "",
        instance: "",
        grpc_client_config: {
          max_recv_msg_size: 104857600,
          max_send_msg_size: 16777216,
          grpc_compression: "",
          rate_limit: 0.000000,
          rate_limit_burst: 0,
          backoff_on_ratelimits: false,
          backoff_config: {
            min_period: "100ms",
            max_period: "10s",
            max_retries: 10
          }
        },
        table_cache_enabled: true,
        table_cache_expiration: "30m0s"
      },
      gcs: {
        bucket_name: "",
        chunk_buffer_size: 0,
        request_timeout: "0s"
      },
      cassandra: {
        addresses: "",
        port: 9042,
        keyspace: "",
        consistency: "QUORUM",
        replication_factor: 3,
        disable_initial_host_lookup: false,
        SSL: false,
        host_verification: true,
        CA_path: "",
        tls_cert_path: "",
        tls_key_path: "",
        auth: false,
        username: "",
        password: "",
        password_file: "",
        custom_authenticators: [],
        timeout: "2s",
        connect_timeout: "5s",
        reconnect_interval: "1s",
        max_retries: 0,
        retry_max_backoff: "10s",
        retry_min_backoff: "100ms",
        query_concurrency: 0,
        num_connections: 2,
        convict_hosts_on_failure: true,
        table_options: ""
      },
      boltdb: {
        directory: ""
      },
      filesystem: {
        directory: ""
      },
      swift: {
        auth_version: 0,
        auth_url: "",
        username: "",
        user_domain_name: "",
        user_domain_id: "",
        user_id: "",
        password: "",
        domain_id: "",
        domain_name: "",
        project_id: "",
        project_name: "",
        project_domain_id: "",
        project_domain_name: "",
        region_name: "",
        container_name: "",
        max_retries: 3,
        connect_timeout: "10s",
        request_timeout: "5s"
      },
      index_cache_validity: "5m0s",
      index_queries_cache_config: {
        enable_fifocache: false,
        default_validity: "0s",
        background: {
          writeback_goroutines: 10,
          writeback_buffer: 10000
        },
        memcached: {
          expiration: "0s",
          batch_size: 1024,
          parallelism: 100
        },
        memcached_client: {
          host: "",
          service: "memcached",
          addresses: "",
          timeout: "100ms",
          max_idle_conns: 16,
          update_interval: "1m0s",
          consistent_hash: true,
          circuit_breaker_consecutive_failures: 10,
          circuit_breaker_timeout: "10s",
          circuit_breaker_interval: "10s"
        },
        redis: {
          endpoint: "",
          master_name: "",
          timeout: "500ms",
          expiration: "0s",
          db: 0,
          pool_size: 0,
          password: "",
          tls_enabled: false,
          tls_insecure_skip_verify: false,
          idle_timeout: "0s",
          max_connection_age: "0s"
        },
        fifocache: {
          max_size_bytes: "",
          max_size_items: 0,
          validity: "0s",
          size: 0
        }
      },
      delete_store: {
        store: "",
        requests_table_name: "delete_requests",
        table_provisioning: {
          enable_ondemand_throughput_mode: false,
          provisioned_write_throughput: 1,
          provisioned_read_throughput: 300,
          write_scale: {
            enabled: false,
            role_arn: "",
            min_capacity: 3000,
            max_capacity: 6000,
            out_cooldown: 1800,
            in_cooldown: 1800,
            target: 80.000000
          },
          read_scale: {
            enabled: false,
            role_arn: "",
            min_capacity: 3000,
            max_capacity: 6000,
            out_cooldown: 1800,
            in_cooldown: 1800,
            target: 80.000000
          },
          tags: {}
        }
      },
      grpc_store: {
        server_address: ""
      }
    },
    chunk_store: {
      chunk_cache_config: {
        enable_fifocache: false,
        default_validity: "0s",
        background: {
          writeback_goroutines: 10,
          writeback_buffer: 10000
        },
        memcached: {
          expiration: "0s",
          batch_size: 1024,
          parallelism: 100
        },
        memcached_client: {
          host: "",
          service: "memcached",
          addresses: "",
          timeout: "100ms",
          max_idle_conns: 16,
          update_interval: "1m0s",
          consistent_hash: true,
          circuit_breaker_consecutive_failures: 10,
          circuit_breaker_timeout: "10s",
          circuit_breaker_interval: "10s"
        },
        redis: {
          endpoint: "",
          master_name: "",
          timeout: "500ms",
          expiration: "0s",
          db: 0,
          pool_size: 0,
          password: "",
          tls_enabled: false,
          tls_insecure_skip_verify: false,
          idle_timeout: "0s",
          max_connection_age: "0s"
        },
        fifocache: {
          max_size_bytes: "",
          max_size_items: 0,
          validity: "0s",
          size: 0
        }
      },
      write_dedupe_cache_config: {
        enable_fifocache: false,
        default_validity: "0s",
        background: {
          writeback_goroutines: 10,
          writeback_buffer: 10000
        },
        memcached: {
          expiration: "0s",
          batch_size: 1024,
          parallelism: 100
        },
        memcached_client: {
          host: "",
          service: "memcached",
          addresses: "",
          timeout: "100ms",
          max_idle_conns: 16,
          update_interval: "1m0s",
          consistent_hash: true,
          circuit_breaker_consecutive_failures: 10,
          circuit_breaker_timeout: "10s",
          circuit_breaker_interval: "10s"
        },
        redis: {
          endpoint: "",
          master_name: "",
          timeout: "500ms",
          expiration: "0s",
          db: 0,
          pool_size: 0,
          password: "",
          tls_enabled: false,
          tls_insecure_skip_verify: false,
          idle_timeout: "0s",
          max_connection_age: "0s"
        },
        fifocache: {
          max_size_bytes: "",
          max_size_items: 0,
          validity: "0s",
          size: 0
        }
      },
      cache_lookups_older_than: "0s",
      max_look_back_period: "0s"
    },
    limits: {
      ingestion_rate: 25000.000000,
      ingestion_rate_strategy: "local",
      ingestion_burst_size: 50000,
      accept_ha_samples: false,
      ha_cluster_label: "cluster",
      ha_replica_label: "__replica__",
      ha_max_clusters: 0,
      drop_labels: [],
      max_label_name_length: 1024,
      max_label_value_length: 2048,
      max_label_names_per_series: 30,
      max_metadata_length: 1024,
      reject_old_samples: false,
      reject_old_samples_max_age: "336h0m0s",
      creation_grace_period: "10m0s",
      enforce_metadata_metric_name: true,
      enforce_metric_name: true,
      ingestion_tenant_shard_size: 0,
      metric_relabel_configs: [],
      max_series_per_query: 100000,
      max_samples_per_query: 1000000,
      max_series_per_user: 5000000,
      max_series_per_metric: 50000,
      max_global_series_per_user: 0,
      max_global_series_per_metric: 0,
      min_chunk_length: 0,
      max_metadata_per_user: 8000,
      max_metadata_per_metric: 10,
      max_global_metadata_per_user: 0,
      max_global_metadata_per_metric: 0,
      max_chunks_per_query: 2000000,
      max_query_lookback: "0s",
      max_query_length: "0s",
      max_query_parallelism: 14,
      cardinality_limit: 100000,
      max_cache_freshness: "1m0s",
      max_queriers_per_tenant: 0,
      ruler_evaluation_delay_duration: "0s",
      ruler_tenant_shard_size: 0,
      ruler_max_rules_per_rule_group: 0,
      ruler_max_rule_groups_per_tenant: 0,
      store_gateway_tenant_shard_size: 0,
      per_tenant_override_config: "",
      per_tenant_override_period: "10s"
    },
    frontend_worker: {
      frontend_address: "",
      scheduler_address: "",
      dns_lookup_duration: "10s",
      parallelism: 10,
      match_max_concurrent: false,
      id: "",
      grpc_client_config: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      }
    },
    frontend: {
      log_queries_longer_than: "0s",
      max_body_size: 10485760,
      query_stats_enabled: false,
      max_outstanding_per_tenant: 100,
      scheduler_address: "",
      scheduler_dns_lookup_period: "10s",
      scheduler_worker_concurrency: 5,
      grpc_client_config: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      },
      instance_interface_names: ["eth0", "en0"],
      compress_responses: false,
      downstream_url: ""
    },
    query_range: {
      split_queries_by_interval: "0s",
      split_queries_by_day: false,
      align_queries_with_step: false,
      results_cache: {
        cache: {
          enable_fifocache: false,
          default_validity: "0s",
          background: {
            writeback_goroutines: 10,
            writeback_buffer: 10000
          },
          memcached: {
            expiration: "0s",
            batch_size: 1024,
            parallelism: 100
          },
          memcached_client: {
            host: "",
            service: "memcached",
            addresses: "",
            timeout: "100ms",
            max_idle_conns: 16,
            update_interval: "1m0s",
            consistent_hash: true,
            circuit_breaker_consecutive_failures: 10,
            circuit_breaker_timeout: "10s",
            circuit_breaker_interval: "10s"
          },
          redis: {
            endpoint: "",
            master_name: "",
            timeout: "500ms",
            expiration: "0s",
            db: 0,
            pool_size: 0,
            password: "",
            tls_enabled: false,
            tls_insecure_skip_verify: false,
            idle_timeout: "0s",
            max_connection_age: "0s"
          },
          fifocache: {
            max_size_bytes: "",
            max_size_items: 0,
            validity: "0s",
            size: 0
          }
        },
        compression: ""
      },
      cache_results: false,
      max_retries: 5,
      parallelise_shardable_queries: false
    },
    table_manager: {
      throughput_updates_disabled: false,
      retention_deletes_enabled: false,
      retention_period: "0s",
      poll_interval: "2m0s",
      creation_grace_period: "10m0s",
      index_tables_provisioning: {
        enable_ondemand_throughput_mode: false,
        provisioned_write_throughput: 1000,
        provisioned_read_throughput: 300,
        write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        enable_inactive_throughput_on_demand_mode: false,
        inactive_write_throughput: 1,
        inactive_read_throughput: 300,
        inactive_write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_write_scale_lastn: 4,
        inactive_read_scale_lastn: 4
      },
      chunk_tables_provisioning: {
        enable_ondemand_throughput_mode: false,
        provisioned_write_throughput: 1000,
        provisioned_read_throughput: 300,
        write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        enable_inactive_throughput_on_demand_mode: false,
        inactive_write_throughput: 1,
        inactive_read_throughput: 300,
        inactive_write_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_read_scale: {
          enabled: false,
          role_arn: "",
          min_capacity: 3000,
          max_capacity: 6000,
          out_cooldown: 1800,
          in_cooldown: 1800,
          target: 80.000000
        },
        inactive_write_scale_lastn: 4,
        inactive_read_scale_lastn: 4
      }
    },
    blocks_storage: {
      backend: "s3",
      s3: {
        endpoint: "",
        bucket_name: "",
        secret_access_key: "",
        access_key_id: "",
        insecure: false,
        signature_version: "v4",
        http: {
          idle_conn_timeout: "1m30s",
          response_header_timeout: "2m0s",
          insecure_skip_verify: false
        }
      },
      gcs: {
        bucket_name: "",
        service_account: ""
      },
      azure: {
        account_name: "",
        account_key: "",
        container_name: "",
        endpoint_suffix: "",
        max_retries: 20
      },
      swift: {
        auth_version: 0,
        auth_url: "",
        username: "",
        user_domain_name: "",
        user_domain_id: "",
        user_id: "",
        password: "",
        domain_id: "",
        domain_name: "",
        project_id: "",
        project_name: "",
        project_domain_id: "",
        project_domain_name: "",
        region_name: "",
        container_name: "",
        max_retries: 3,
        connect_timeout: "10s",
        request_timeout: "5s"
      },
      filesystem: {
        dir: ""
      },
      bucket_store: {
        sync_dir: "tsdb-sync",
        sync_interval: "5m0s",
        max_chunk_pool_bytes: 2147483648,
        max_concurrent: 100,
        tenant_sync_concurrency: 10,
        block_sync_concurrency: 20,
        meta_sync_concurrency: 20,
        consistency_delay: "0s",
        index_cache: {
          backend: "inmemory",
          inmemory: {
            max_size_bytes: 1073741824
          },
          memcached: {
            addresses: "",
            timeout: "100ms",
            max_idle_connections: 16,
            max_async_concurrency: 50,
            max_async_buffer_size: 10000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 0,
            max_item_size: 1048576
          },
          postings_compression_enabled: false
        },
        chunks_cache: {
          backend: "",
          memcached: {
            addresses: "",
            timeout: "100ms",
            max_idle_connections: 16,
            max_async_concurrency: 50,
            max_async_buffer_size: 10000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 0,
            max_item_size: 1048576
          },
          subrange_size: 16000,
          max_get_range_requests: 3,
          attributes_ttl: "168h0m0s",
          subrange_ttl: "24h0m0s"
        },
        metadata_cache: {
          backend: "",
          memcached: {
            addresses: "",
            timeout: "100ms",
            max_idle_connections: 16,
            max_async_concurrency: 50,
            max_async_buffer_size: 10000,
            max_get_multi_concurrency: 100,
            max_get_multi_batch_size: 0,
            max_item_size: 1048576
          },
          tenants_list_ttl: "15m0s",
          tenant_blocks_list_ttl: "5m0s",
          chunks_list_ttl: "24h0m0s",
          metafile_exists_ttl: "2h0m0s",
          metafile_doesnt_exist_ttl: "5m0s",
          metafile_content_ttl: "24h0m0s",
          metafile_max_size_bytes: 1048576,
          metafile_attributes_ttl: "168h0m0s",
          block_index_attributes_ttl: "168h0m0s",
          bucket_index_content_ttl: "5m0s",
          bucket_index_max_size_bytes: 1048576
        },
        ignore_deletion_mark_delay: "6h0m0s",
        bucket_index: {
          enabled: false,
          update_on_error_interval: "1m0s",
          idle_timeout: "1h0m0s",
          max_stale_period: "1h0m0s"
        }
      },
      tsdb: {
        dir: "tsdb",
        block_ranges_period: ["2h0m0s"],
        retention_period: "6h0m0s",
        ship_interval: "1m0s",
        ship_concurrency: 10,
        head_compaction_interval: "1m0s",
        head_compaction_concurrency: 5,
        head_compaction_idle_timeout: "1h0m0s",
        head_chunks_write_buffer_size_bytes: 4194304,
        stripe_size: 16384,
        wal_compression_enabled: false,
        wal_segment_size_bytes: 134217728,
        flush_blocks_on_shutdown: false,
        close_idle_tsdb_timeout: "0s",
        max_tsdb_opening_concurrency_on_startup: 10
      }
    },
    compactor: {
      block_ranges: ["2h0m0s", "12h0m0s", "24h0m0s"],
      block_sync_concurrency: 20,
      meta_sync_concurrency: 20,
      consistency_delay: "0s",
      data_dir: "./data",
      compaction_interval: "1h0m0s",
      compaction_retries: 3,
      compaction_concurrency: 1,
      cleanup_interval: "15m0s",
      cleanup_concurrency: 20,
      deletion_delay: "12h0m0s",
      tenant_cleanup_delay: "6h0m0s",
      block_deletion_marks_migration_enabled: true,
      enabled_tenants: "",
      disabled_tenants: "",
      sharding_enabled: false,
      sharding_ring: {
        kvstore: {
          store: "consul",
          prefix: "collectors/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "5s",
        heartbeat_timeout: "1m0s",
        wait_stability_min_duration: "1m0s",
        wait_stability_max_duration: "5m0s",
        instance_interface_names: ["eth0", "en0"]
      }
    },
    store_gateway: {
      sharding_enabled: false,
      sharding_ring: {
        kvstore: {
          store: "consul",
          prefix: "collectors/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "15s",
        heartbeat_timeout: "1m0s",
        replication_factor: 3,
        tokens_file_path: "",
        zone_awareness_enabled: false,
        instance_interface_names: ["eth0", "en0"],
        instance_availability_zone: ""
      },
      sharding_strategy: "default"
    },
    purger: {
      enable: false,
      num_workers: 2,
      object_store_type: "",
      delete_request_cancel_period: "24h0m0s"
    },
    tenant_federation: {
      enabled: false
    },
    ruler: {
      external_url: "",
      ruler_client: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      },
      evaluation_interval: "1m0s",
      poll_interval: "1m0s",
      storage: {
        type: "configdb",
        configdb: {
          configs_api_url: "",
          client_timeout: "5s",
          tls_cert_path: "",
          tls_key_path: "",
          tls_ca_path: "",
          tls_insecure_skip_verify: false
        },
        azure: {
          environment: "AzureGlobal",
          container_name: "cortex",
          account_name: "",
          account_key: "",
          download_buffer_size: 512000,
          upload_buffer_size: 256000,
          upload_buffer_count: 1,
          request_timeout: "30s",
          max_retries: 5,
          min_retry_delay: "10ms",
          max_retry_delay: "500ms"
        },
        gcs: {
          bucket_name: "",
          chunk_buffer_size: 0,
          request_timeout: "0s"
        },
        s3: {
          s3: "",
          s3forcepathstyle: false,
          bucketnames: "",
          endpoint: "",
          region: "",
          access_key_id: "",
          secret_access_key: "",
          insecure: false,
          sse_encryption: false,
          http_config: {
            idle_conn_timeout: "1m30s",
            response_header_timeout: "0s",
            insecure_skip_verify: false
          },
          signature_version: "v4"
        },
        swift: {
          auth_version: 0,
          auth_url: "",
          username: "",
          user_domain_name: "",
          user_domain_id: "",
          user_id: "",
          password: "",
          domain_id: "",
          domain_name: "",
          project_id: "",
          project_name: "",
          project_domain_id: "",
          project_domain_name: "",
          region_name: "",
          container_name: "",
          max_retries: 3,
          connect_timeout: "10s",
          request_timeout: "5s"
        },
        'local': {
          directory: ""
        }
      },
      rule_path: "/rules",
      alertmanager_url: "",
      enable_alertmanager_discovery: false,
      alertmanager_refresh_interval: "1m0s",
      enable_alertmanager_v2: false,
      notification_queue_capacity: 10000,
      notification_timeout: "10s",
      for_outage_tolerance: "1h0m0s",
      for_grace_period: "10m0s",
      resend_delay: "1m0s",
      enable_sharding: false,
      sharding_strategy: "default",
      search_pending_for: "5m0s",
      ring: {
        kvstore: {
          store: "consul",
          prefix: "rulers/",
          consul: {
            host: "localhost:8500",
            acl_token: "",
            http_client_timeout: "20s",
            consistent_reads: false,
            watch_rate_limit: 1.000000,
            watch_burst_size: 1
          },
          etcd: {
            endpoints: [],
            dial_timeout: "10s",
            max_retries: 10,
            tls_enabled: false,
            tls_cert_path: "",
            tls_key_path: "",
            tls_ca_path: "",
            tls_insecure_skip_verify: false
          },
          multi: {
            primary: "",
            secondary: "",
            mirror_enabled: false,
            mirror_timeout: "2s"
          }
        },
        heartbeat_period: "5s",
        heartbeat_timeout: "1m0s",
        instance_interface_names: ["eth0", "en0"],
        num_tokens: 128
      },
      flush_period: "1m0s",
      enable_api: false
    },
    configs: {
      database: {
        uri: "postgres://postgres@configs-db.weave.local/configs?sslmode=disable",
        migrations_dir: "",
        password_file: ""
      },
      api: {
        notifications: {
          disable_email: false,
          disable_webhook: false
        }
      }
    },
    alertmanager: {
      data_dir: "data/",
      retention: "120h0m0s",
      external_url: "",
      poll_interval: "15s",
      cluster_bind_address: "0.0.0.0:9094",
      cluster_advertise_address: "",
      peers: [],
      peer_timeout: "15s",
      fallback_config_file: "",
      auto_webhook_root: "",
      storage: {
        type: "configdb",
        configdb: {
          configs_api_url: "",
          client_timeout: "5s",
          tls_cert_path: "",
          tls_key_path: "",
          tls_ca_path: "",
          tls_insecure_skip_verify: false
        },
        azure: {
          environment: "AzureGlobal",
          container_name: "cortex",
          account_name: "",
          account_key: "",
          download_buffer_size: 512000,
          upload_buffer_size: 256000,
          upload_buffer_count: 1,
          request_timeout: "30s",
          max_retries: 5,
          min_retry_delay: "10ms",
          max_retry_delay: "500ms"
        },
        gcs: {
          bucket_name: "",
          chunk_buffer_size: 0,
          request_timeout: "0s"
        },
        s3: {
          s3: "",
          s3forcepathstyle: false,
          bucketnames: "",
          endpoint: "",
          region: "",
          access_key_id: "",
          secret_access_key: "",
          insecure: false,
          sse_encryption: false,
          http_config: {
            idle_conn_timeout: "1m30s",
            response_header_timeout: "0s",
            insecure_skip_verify: false
          },
          signature_version: "v4"
        },
        'local': {
          path: ""
        }
      },
      enable_api: false
    },
    runtime_config: {
      period: "10s",
      file: ""
    },
    memberlist: {
      node_name: "",
      randomize_node_name: true,
      stream_timeout: "0s",
      retransmit_factor: 0,
      pull_push_interval: "0s",
      gossip_interval: "0s",
      gossip_nodes: 0,
      gossip_to_dead_nodes_time: "0s",
      dead_node_reclaim_time: "0s",
      join_members: [],
      min_join_backoff: "1s",
      max_join_backoff: "1m0s",
      max_join_retries: 10,
      abort_if_cluster_join_fails: true,
      rejoin_interval: "0s",
      left_ingesters_timeout: "5m0s",
      leave_timeout: "5s",
      message_history_buffer_bytes: 0,
      bind_addr: [],
      bind_port: 7946,
      packet_dial_timeout: "5s",
      packet_write_timeout: "5s"
    },
    query_scheduler: {
      max_outstanding_requests_per_tenant: 100,
      grpc_client_config: {
        max_recv_msg_size: 104857600,
        max_send_msg_size: 16777216,
        grpc_compression: "",
        rate_limit: 0.000000,
        rate_limit_burst: 0,
        backoff_on_ratelimits: false,
        backoff_config: {
          min_period: "100ms",
          max_period: "10s",
          max_retries: 10
        },
        tls_cert_path: "",
        tls_key_path: "",
        tls_ca_path: "",
        tls_insecure_skip_verify: false
      }
    }
  },
  types:: {
    target: "string",
    auth_enabled: "boolean",
    http_prefix: "string",
    api: {
      response_compression_enabled: "boolean",
      alertmanager_http_prefix: "string",
      prometheus_http_prefix: "string"
    },
    server: {
      http_listen_address: "string",
      http_listen_port: "int",
      http_listen_conn_limit: "int",
      grpc_listen_address: "string",
      grpc_listen_port: "int",
      grpc_listen_conn_limit: "int",
      http_tls_config: {
        cert_file: "string",
        key_file: "string",
        client_auth_type: "string",
        client_ca_file: "string"
      },
      grpc_tls_config: {
        cert_file: "string",
        key_file: "string",
        client_auth_type: "string",
        client_ca_file: "string"
      },
      register_instrumentation: "boolean",
      graceful_shutdown_timeout: "duration",
      http_server_read_timeout: "duration",
      http_server_write_timeout: "duration",
      http_server_idle_timeout: "duration",
      grpc_server_max_recv_msg_size: "int",
      grpc_server_max_send_msg_size: "int",
      grpc_server_max_concurrent_streams: "int",
      grpc_server_max_connection_idle: "duration",
      grpc_server_max_connection_age: "duration",
      grpc_server_max_connection_age_grace: "duration",
      grpc_server_keepalive_time: "duration",
      grpc_server_keepalive_timeout: "duration",
      grpc_server_min_time_between_pings: "duration",
      grpc_server_ping_without_stream_allowed: "boolean",
      log_format: "string",
      log_level: "string",
      log_source_ips_enabled: "boolean",
      log_source_ips_header: "string",
      log_source_ips_regex: "string",
      http_path_prefix: "string"
    },
    distributor: {
      pool: {
        client_cleanup_period: "duration",
        health_check_ingesters: "boolean"
      },
      ha_tracker: {
        enable_ha_tracker: "boolean",
        ha_tracker_update_timeout: "duration",
        ha_tracker_update_timeout_jitter_max: "duration",
        ha_tracker_failover_timeout: "duration",
        kvstore: {
          store: "string",
          prefix: "string",
          consul: {
            host: "string",
            acl_token: "string",
            http_client_timeout: "duration",
            consistent_reads: "boolean",
            watch_rate_limit: "float",
            watch_burst_size: "int"
          },
          etcd: {
            endpoints: "list of string",
            dial_timeout: "duration",
            max_retries: "int",
            tls_enabled: "boolean",
            tls_cert_path: "string",
            tls_key_path: "string",
            tls_ca_path: "string",
            tls_insecure_skip_verify: "boolean"
          },
          multi: {
            primary: "string",
            secondary: "string",
            mirror_enabled: "boolean",
            mirror_timeout: "duration"
          }
        }
      },
      max_recv_msg_size: "int",
      remote_timeout: "duration",
      extra_queue_delay: "duration",
      sharding_strategy: "string",
      shard_by_all_labels: "boolean",
      ring: {
        kvstore: {
          store: "string",
          prefix: "string",
          consul: {
            host: "string",
            acl_token: "string",
            http_client_timeout: "duration",
            consistent_reads: "boolean",
            watch_rate_limit: "float",
            watch_burst_size: "int"
          },
          etcd: {
            endpoints: "list of string",
            dial_timeout: "duration",
            max_retries: "int",
            tls_enabled: "boolean",
            tls_cert_path: "string",
            tls_key_path: "string",
            tls_ca_path: "string",
            tls_insecure_skip_verify: "boolean"
          },
          multi: {
            primary: "string",
            secondary: "string",
            mirror_enabled: "boolean",
            mirror_timeout: "duration"
          }
        },
        heartbeat_period: "duration",
        heartbeat_timeout: "duration",
        instance_interface_names: "list of string"
      }
    },
    querier: {
      max_concurrent: "int",
      timeout: "duration",
      iterators: "boolean",
      batch_iterators: "boolean",
      ingester_streaming: "boolean",
      max_samples: "int",
      query_ingesters_within: "duration",
      query_store_for_labels_enabled: "boolean",
      query_store_after: "duration",
      max_query_into_future: "duration",
      default_evaluation_interval: "duration",
      active_query_tracker_dir: "string",
      lookback_delta: "duration",
      store_gateway_addresses: "string",
      store_gateway_client: {
        tls_cert_path: "string",
        tls_key_path: "string",
        tls_ca_path: "string",
        tls_insecure_skip_verify: "boolean"
      },
      second_store_engine: "string",
      use_second_store_before_time: "time",
      shuffle_sharding_ingesters_lookback_period: "duration"
    },
    ingester_client: {
      grpc_client_config: {
        max_recv_msg_size: "int",
        max_send_msg_size: "int",
        grpc_compression: "string",
        rate_limit: "float",
        rate_limit_burst: "int",
        backoff_on_ratelimits: "boolean",
        backoff_config: {
          min_period: "duration",
          max_period: "duration",
          max_retries: "int"
        },
        tls_cert_path: "string",
        tls_key_path: "string",
        tls_ca_path: "string",
        tls_insecure_skip_verify: "boolean"
      }
    },
    ingester: {
      walconfig: {
        wal_enabled: "boolean",
        checkpoint_enabled: "boolean",
        recover_from_wal: "boolean",
        wal_dir: "string",
        checkpoint_duration: "duration",
        flush_on_shutdown_with_wal_enabled: "boolean"
      },
      lifecycler: {
        ring: {
          kvstore: {
            store: "string",
            prefix: "string",
            consul: {
              host: "string",
              acl_token: "string",
              http_client_timeout: "duration",
              consistent_reads: "boolean",
              watch_rate_limit: "float",
              watch_burst_size: "int"
            },
            etcd: {
              endpoints: "list of string",
              dial_timeout: "duration",
              max_retries: "int",
              tls_enabled: "boolean",
              tls_cert_path: "string",
              tls_key_path: "string",
              tls_ca_path: "string",
              tls_insecure_skip_verify: "boolean"
            },
            multi: {
              primary: "string",
              secondary: "string",
              mirror_enabled: "boolean",
              mirror_timeout: "duration"
            }
          },
          heartbeat_timeout: "duration",
          replication_factor: "int",
          zone_awareness_enabled: "boolean",
          extend_writes: "boolean"
        },
        num_tokens: "int",
        heartbeat_period: "duration",
        observe_period: "duration",
        join_after: "duration",
        min_ready_duration: "duration",
        interface_names: "list of string",
        final_sleep: "duration",
        tokens_file_path: "string",
        availability_zone: "string",
        unregister_on_shutdown: "boolean"
      },
      max_transfer_retries: "int",
      flush_period: "duration",
      retain_period: "duration",
      max_chunk_idle_time: "duration",
      max_stale_chunk_idle_time: "duration",
      flush_op_timeout: "duration",
      max_chunk_age: "duration",
      chunk_age_jitter: "duration",
      concurrent_flushes: "int",
      spread_flushes: "boolean",
      metadata_retain_period: "duration",
      rate_update_period: "duration",
      active_series_metrics_enabled: "boolean",
      active_series_metrics_update_period: "duration",
      active_series_metrics_idle_timeout: "duration"
    },
    flusher: {
      wal_dir: "string",
      concurrent_flushes: "int",
      flush_op_timeout: "duration",
      exit_after_flush: "boolean"
    },
    storage: {
      engine: "string",
      aws: {
        dynamodb: {
          dynamodb_url: "url",
          api_limit: "float",
          throttle_limit: "float",
          metrics: {
            url: "string",
            target_queue_length: "int",
            scale_up_factor: "float",
            ignore_throttle_below: "float",
            queue_length_query: "string",
            write_throttle_query: "string",
            write_usage_query: "string",
            read_usage_query: "string",
            read_error_query: "string"
          },
          chunk_gang_size: "int",
          chunk_get_max_parallelism: "int",
          backoff_config: {
            min_period: "duration",
            max_period: "duration",
            max_retries: "int"
          }
        },
        s3: "url",
        s3forcepathstyle: "boolean",
        bucketnames: "string",
        endpoint: "string",
        region: "string",
        access_key_id: "string",
        secret_access_key: "string",
        insecure: "boolean",
        sse_encryption: "boolean",
        http_config: {
          idle_conn_timeout: "duration",
          response_header_timeout: "duration",
          insecure_skip_verify: "boolean"
        },
        signature_version: "string"
      },
      azure: {
        environment: "string",
        container_name: "string",
        account_name: "string",
        account_key: "string",
        download_buffer_size: "int",
        upload_buffer_size: "int",
        upload_buffer_count: "int",
        request_timeout: "duration",
        max_retries: "int",
        min_retry_delay: "duration",
        max_retry_delay: "duration"
      },
      bigtable: {
        project: "string",
        instance: "string",
        grpc_client_config: {
          max_recv_msg_size: "int",
          max_send_msg_size: "int",
          grpc_compression: "string",
          rate_limit: "float",
          rate_limit_burst: "int",
          backoff_on_ratelimits: "boolean",
          backoff_config: {
            min_period: "duration",
            max_period: "duration",
            max_retries: "int"
          }
        },
        table_cache_enabled: "boolean",
        table_cache_expiration: "duration"
      },
      gcs: {
        bucket_name: "string",
        chunk_buffer_size: "int",
        request_timeout: "duration"
      },
      cassandra: {
        addresses: "string",
        port: "int",
        keyspace: "string",
        consistency: "string",
        replication_factor: "int",
        disable_initial_host_lookup: "boolean",
        SSL: "boolean",
        host_verification: "boolean",
        CA_path: "string",
        tls_cert_path: "string",
        tls_key_path: "string",
        auth: "boolean",
        username: "string",
        password: "string",
        password_file: "string",
        custom_authenticators: "list of string",
        timeout: "duration",
        connect_timeout: "duration",
        reconnect_interval: "duration",
        max_retries: "int",
        retry_max_backoff: "duration",
        retry_min_backoff: "duration",
        query_concurrency: "int",
        num_connections: "int",
        convict_hosts_on_failure: "boolean",
        table_options: "string"
      },
      boltdb: {
        directory: "string"
      },
      filesystem: {
        directory: "string"
      },
      swift: {
        auth_version: "int",
        auth_url: "string",
        username: "string",
        user_domain_name: "string",
        user_domain_id: "string",
        user_id: "string",
        password: "string",
        domain_id: "string",
        domain_name: "string",
        project_id: "string",
        project_name: "string",
        project_domain_id: "string",
        project_domain_name: "string",
        region_name: "string",
        container_name: "string",
        max_retries: "int",
        connect_timeout: "duration",
        request_timeout: "duration"
      },
      index_cache_validity: "duration",
      index_queries_cache_config: {
        enable_fifocache: "boolean",
        default_validity: "duration",
        background: {
          writeback_goroutines: "int",
          writeback_buffer: "int"
        },
        memcached: {
          expiration: "duration",
          batch_size: "int",
          parallelism: "int"
        },
        memcached_client: {
          host: "string",
          service: "string",
          addresses: "string",
          timeout: "duration",
          max_idle_conns: "int",
          update_interval: "duration",
          consistent_hash: "boolean",
          circuit_breaker_consecutive_failures: "int",
          circuit_breaker_timeout: "duration",
          circuit_breaker_interval: "duration"
        },
        redis: {
          endpoint: "string",
          master_name: "string",
          timeout: "duration",
          expiration: "duration",
          db: "int",
          pool_size: "int",
          password: "string",
          tls_enabled: "boolean",
          tls_insecure_skip_verify: "boolean",
          idle_timeout: "duration",
          max_connection_age: "duration"
        },
        fifocache: {
          max_size_bytes: "string",
          max_size_items: "int",
          validity: "duration",
          size: "int"
        }
      },
      delete_store: {
        store: "string",
        requests_table_name: "string",
        table_provisioning: {
          enable_ondemand_throughput_mode: "boolean",
          provisioned_write_throughput: "int",
          provisioned_read_throughput: "int",
          write_scale: {
            enabled: "boolean",
            role_arn: "string",
            min_capacity: "int",
            max_capacity: "int",
            out_cooldown: "int",
            in_cooldown: "int",
            target: "float"
          },
          read_scale: {
            enabled: "boolean",
            role_arn: "string",
            min_capacity: "int",
            max_capacity: "int",
            out_cooldown: "int",
            in_cooldown: "int",
            target: "float"
          },
          tags: "map of string to string"
        }
      },
      grpc_store: {
        server_address: "string"
      }
    },
    chunk_store: {
      chunk_cache_config: {
        enable_fifocache: "boolean",
        default_validity: "duration",
        background: {
          writeback_goroutines: "int",
          writeback_buffer: "int"
        },
        memcached: {
          expiration: "duration",
          batch_size: "int",
          parallelism: "int"
        },
        memcached_client: {
          host: "string",
          service: "string",
          addresses: "string",
          timeout: "duration",
          max_idle_conns: "int",
          update_interval: "duration",
          consistent_hash: "boolean",
          circuit_breaker_consecutive_failures: "int",
          circuit_breaker_timeout: "duration",
          circuit_breaker_interval: "duration"
        },
        redis: {
          endpoint: "string",
          master_name: "string",
          timeout: "duration",
          expiration: "duration",
          db: "int",
          pool_size: "int",
          password: "string",
          tls_enabled: "boolean",
          tls_insecure_skip_verify: "boolean",
          idle_timeout: "duration",
          max_connection_age: "duration"
        },
        fifocache: {
          max_size_bytes: "string",
          max_size_items: "int",
          validity: "duration",
          size: "int"
        }
      },
      write_dedupe_cache_config: {
        enable_fifocache: "boolean",
        default_validity: "duration",
        background: {
          writeback_goroutines: "int",
          writeback_buffer: "int"
        },
        memcached: {
          expiration: "duration",
          batch_size: "int",
          parallelism: "int"
        },
        memcached_client: {
          host: "string",
          service: "string",
          addresses: "string",
          timeout: "duration",
          max_idle_conns: "int",
          update_interval: "duration",
          consistent_hash: "boolean",
          circuit_breaker_consecutive_failures: "int",
          circuit_breaker_timeout: "duration",
          circuit_breaker_interval: "duration"
        },
        redis: {
          endpoint: "string",
          master_name: "string",
          timeout: "duration",
          expiration: "duration",
          db: "int",
          pool_size: "int",
          password: "string",
          tls_enabled: "boolean",
          tls_insecure_skip_verify: "boolean",
          idle_timeout: "duration",
          max_connection_age: "duration"
        },
        fifocache: {
          max_size_bytes: "string",
          max_size_items: "int",
          validity: "duration",
          size: "int"
        }
      },
      cache_lookups_older_than: "duration",
      max_look_back_period: "duration"
    },
    limits: {
      ingestion_rate: "float",
      ingestion_rate_strategy: "string",
      ingestion_burst_size: "int",
      accept_ha_samples: "boolean",
      ha_cluster_label: "string",
      ha_replica_label: "string",
      ha_max_clusters: "int",
      drop_labels: "list of string",
      max_label_name_length: "int",
      max_label_value_length: "int",
      max_label_names_per_series: "int",
      max_metadata_length: "int",
      reject_old_samples: "boolean",
      reject_old_samples_max_age: "duration",
      creation_grace_period: "duration",
      enforce_metadata_metric_name: "boolean",
      enforce_metric_name: "boolean",
      ingestion_tenant_shard_size: "int",
      metric_relabel_configs: "relabel_config...",
      max_series_per_query: "int",
      max_samples_per_query: "int",
      max_series_per_user: "int",
      max_series_per_metric: "int",
      max_global_series_per_user: "int",
      max_global_series_per_metric: "int",
      min_chunk_length: "int",
      max_metadata_per_user: "int",
      max_metadata_per_metric: "int",
      max_global_metadata_per_user: "int",
      max_global_metadata_per_metric: "int",
      max_chunks_per_query: "int",
      max_query_lookback: "duration",
      max_query_length: "duration",
      max_query_parallelism: "int",
      cardinality_limit: "int",
      max_cache_freshness: "duration",
      max_queriers_per_tenant: "int",
      ruler_evaluation_delay_duration: "duration",
      ruler_tenant_shard_size: "int",
      ruler_max_rules_per_rule_group: "int",
      ruler_max_rule_groups_per_tenant: "int",
      store_gateway_tenant_shard_size: "int",
      per_tenant_override_config: "string",
      per_tenant_override_period: "duration"
    },
    frontend_worker: {
      frontend_address: "string",
      scheduler_address: "string",
      dns_lookup_duration: "duration",
      parallelism: "int",
      match_max_concurrent: "boolean",
      id: "string",
      grpc_client_config: {
        max_recv_msg_size: "int",
        max_send_msg_size: "int",
        grpc_compression: "string",
        rate_limit: "float",
        rate_limit_burst: "int",
        backoff_on_ratelimits: "boolean",
        backoff_config: {
          min_period: "duration",
          max_period: "duration",
          max_retries: "int"
        },
        tls_cert_path: "string",
        tls_key_path: "string",
        tls_ca_path: "string",
        tls_insecure_skip_verify: "boolean"
      }
    },
    frontend: {
      log_queries_longer_than: "duration",
      max_body_size: "int",
      query_stats_enabled: "boolean",
      max_outstanding_per_tenant: "int",
      scheduler_address: "string",
      scheduler_dns_lookup_period: "duration",
      scheduler_worker_concurrency: "int",
      grpc_client_config: {
        max_recv_msg_size: "int",
        max_send_msg_size: "int",
        grpc_compression: "string",
        rate_limit: "float",
        rate_limit_burst: "int",
        backoff_on_ratelimits: "boolean",
        backoff_config: {
          min_period: "duration",
          max_period: "duration",
          max_retries: "int"
        },
        tls_cert_path: "string",
        tls_key_path: "string",
        tls_ca_path: "string",
        tls_insecure_skip_verify: "boolean"
      },
      instance_interface_names: "list of string",
      compress_responses: "boolean",
      downstream_url: "string"
    },
    query_range: {
      split_queries_by_interval: "duration",
      split_queries_by_day: "boolean",
      align_queries_with_step: "boolean",
      results_cache: {
        cache: {
          enable_fifocache: "boolean",
          default_validity: "duration",
          background: {
            writeback_goroutines: "int",
            writeback_buffer: "int"
          },
          memcached: {
            expiration: "duration",
            batch_size: "int",
            parallelism: "int"
          },
          memcached_client: {
            host: "string",
            service: "string",
            addresses: "string",
            timeout: "duration",
            max_idle_conns: "int",
            update_interval: "duration",
            consistent_hash: "boolean",
            circuit_breaker_consecutive_failures: "int",
            circuit_breaker_timeout: "duration",
            circuit_breaker_interval: "duration"
          },
          redis: {
            endpoint: "string",
            master_name: "string",
            timeout: "duration",
            expiration: "duration",
            db: "int",
            pool_size: "int",
            password: "string",
            tls_enabled: "boolean",
            tls_insecure_skip_verify: "boolean",
            idle_timeout: "duration",
            max_connection_age: "duration"
          },
          fifocache: {
            max_size_bytes: "string",
            max_size_items: "int",
            validity: "duration",
            size: "int"
          }
        },
        compression: "string"
      },
      cache_results: "boolean",
      max_retries: "int",
      parallelise_shardable_queries: "boolean"
    },
    table_manager: {
      throughput_updates_disabled: "boolean",
      retention_deletes_enabled: "boolean",
      retention_period: "duration",
      poll_interval: "duration",
      creation_grace_period: "duration",
      index_tables_provisioning: {
        enable_ondemand_throughput_mode: "boolean",
        provisioned_write_throughput: "int",
        provisioned_read_throughput: "int",
        write_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        read_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        enable_inactive_throughput_on_demand_mode: "boolean",
        inactive_write_throughput: "int",
        inactive_read_throughput: "int",
        inactive_write_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        inactive_read_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        inactive_write_scale_lastn: "int",
        inactive_read_scale_lastn: "int"
      },
      chunk_tables_provisioning: {
        enable_ondemand_throughput_mode: "boolean",
        provisioned_write_throughput: "int",
        provisioned_read_throughput: "int",
        write_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        read_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        enable_inactive_throughput_on_demand_mode: "boolean",
        inactive_write_throughput: "int",
        inactive_read_throughput: "int",
        inactive_write_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        inactive_read_scale: {
          enabled: "boolean",
          role_arn: "string",
          min_capacity: "int",
          max_capacity: "int",
          out_cooldown: "int",
          in_cooldown: "int",
          target: "float"
        },
        inactive_write_scale_lastn: "int",
        inactive_read_scale_lastn: "int"
      }
    },
    blocks_storage: {
      backend: "string",
      s3: {
        endpoint: "string",
        bucket_name: "string",
        secret_access_key: "string",
        access_key_id: "string",
        insecure: "boolean",
        signature_version: "string",
        http: {
          idle_conn_timeout: "duration",
          response_header_timeout: "duration",
          insecure_skip_verify: "boolean"
        }
      },
      gcs: {
        bucket_name: "string",
        service_account: "string"
      },
      azure: {
        account_name: "string",
        account_key: "string",
        container_name: "string",
        endpoint_suffix: "string",
        max_retries: "int"
      },
      swift: {
        auth_version: "int",
        auth_url: "string",
        username: "string",
        user_domain_name: "string",
        user_domain_id: "string",
        user_id: "string",
        password: "string",
        domain_id: "string",
        domain_name: "string",
        project_id: "string",
        project_name: "string",
        project_domain_id: "string",
        project_domain_name: "string",
        region_name: "string",
        container_name: "string",
        max_retries: "int",
        connect_timeout: "duration",
        request_timeout: "duration"
      },
      filesystem: {
        dir: "string"
      },
      bucket_store: {
        sync_dir: "string",
        sync_interval: "duration",
        max_chunk_pool_bytes: "int",
        max_concurrent: "int",
        tenant_sync_concurrency: "int",
        block_sync_concurrency: "int",
        meta_sync_concurrency: "int",
        consistency_delay: "duration",
        index_cache: {
          backend: "string",
          inmemory: {
            max_size_bytes: "int"
          },
          memcached: {
            addresses: "string",
            timeout: "duration",
            max_idle_connections: "int",
            max_async_concurrency: "int",
            max_async_buffer_size: "int",
            max_get_multi_concurrency: "int",
            max_get_multi_batch_size: "int",
            max_item_size: "int"
          },
          postings_compression_enabled: "boolean"
        },
        chunks_cache: {
          backend: "string",
          memcached: {
            addresses: "string",
            timeout: "duration",
            max_idle_connections: "int",
            max_async_concurrency: "int",
            max_async_buffer_size: "int",
            max_get_multi_concurrency: "int",
            max_get_multi_batch_size: "int",
            max_item_size: "int"
          },
          subrange_size: "int",
          max_get_range_requests: "int",
          attributes_ttl: "duration",
          subrange_ttl: "duration"
        },
        metadata_cache: {
          backend: "string",
          memcached: {
            addresses: "string",
            timeout: "duration",
            max_idle_connections: "int",
            max_async_concurrency: "int",
            max_async_buffer_size: "int",
            max_get_multi_concurrency: "int",
            max_get_multi_batch_size: "int",
            max_item_size: "int"
          },
          tenants_list_ttl: "duration",
          tenant_blocks_list_ttl: "duration",
          chunks_list_ttl: "duration",
          metafile_exists_ttl: "duration",
          metafile_doesnt_exist_ttl: "duration",
          metafile_content_ttl: "duration",
          metafile_max_size_bytes: "int",
          metafile_attributes_ttl: "duration",
          block_index_attributes_ttl: "duration",
          bucket_index_content_ttl: "duration",
          bucket_index_max_size_bytes: "int"
        },
        ignore_deletion_mark_delay: "duration",
        bucket_index: {
          enabled: "boolean",
          update_on_error_interval: "duration",
          idle_timeout: "duration",
          max_stale_period: "duration"
        }
      },
      tsdb: {
        dir: "string",
        block_ranges_period: "list of duration",
        retention_period: "duration",
        ship_interval: "duration",
        ship_concurrency: "int",
        head_compaction_interval: "duration",
        head_compaction_concurrency: "int",
        head_compaction_idle_timeout: "duration",
        head_chunks_write_buffer_size_bytes: "int",
        stripe_size: "int",
        wal_compression_enabled: "boolean",
        wal_segment_size_bytes: "int",
        flush_blocks_on_shutdown: "boolean",
        close_idle_tsdb_timeout: "duration",
        max_tsdb_opening_concurrency_on_startup: "int"
      }
    },
    compactor: {
      block_ranges: "list of duration",
      block_sync_concurrency: "int",
      meta_sync_concurrency: "int",
      consistency_delay: "duration",
      data_dir: "string",
      compaction_interval: "duration",
      compaction_retries: "int",
      compaction_concurrency: "int",
      cleanup_interval: "duration",
      cleanup_concurrency: "int",
      deletion_delay: "duration",
      tenant_cleanup_delay: "duration",
      block_deletion_marks_migration_enabled: "boolean",
      enabled_tenants: "string",
      disabled_tenants: "string",
      sharding_enabled: "boolean",
      sharding_ring: {
        kvstore: {
          store: "string",
          prefix: "string",
          consul: {
            host: "string",
            acl_token: "string",
            http_client_timeout: "duration",
            consistent_reads: "boolean",
            watch_rate_limit: "float",
            watch_burst_size: "int"
          },
          etcd: {
            endpoints: "list of string",
            dial_timeout: "duration",
            max_retries: "int",
            tls_enabled: "boolean",
            tls_cert_path: "string",
            tls_key_path: "string",
            tls_ca_path: "string",
            tls_insecure_skip_verify: "boolean"
          },
          multi: {
            primary: "string",
            secondary: "string",
            mirror_enabled: "boolean",
            mirror_timeout: "duration"
          }
        },
        heartbeat_period: "duration",
        heartbeat_timeout: "duration",
        wait_stability_min_duration: "duration",
        wait_stability_max_duration: "duration",
        instance_interface_names: "list of string"
      }
    },
    store_gateway: {
      sharding_enabled: "boolean",
      sharding_ring: {
        kvstore: {
          store: "string",
          prefix: "string",
          consul: {
            host: "string",
            acl_token: "string",
            http_client_timeout: "duration",
            consistent_reads: "boolean",
            watch_rate_limit: "float",
            watch_burst_size: "int"
          },
          etcd: {
            endpoints: "list of string",
            dial_timeout: "duration",
            max_retries: "int",
            tls_enabled: "boolean",
            tls_cert_path: "string",
            tls_key_path: "string",
            tls_ca_path: "string",
            tls_insecure_skip_verify: "boolean"
          },
          multi: {
            primary: "string",
            secondary: "string",
            mirror_enabled: "boolean",
            mirror_timeout: "duration"
          }
        },
        heartbeat_period: "duration",
        heartbeat_timeout: "duration",
        replication_factor: "int",
        tokens_file_path: "string",
        zone_awareness_enabled: "boolean",
        instance_interface_names: "list of string",
        instance_availability_zone: "string"
      },
      sharding_strategy: "string"
    },
    purger: {
      enable: "boolean",
      num_workers: "int",
      object_store_type: "string",
      delete_request_cancel_period: "duration"
    },
    tenant_federation: {
      enabled: "boolean"
    },
    ruler: {
      external_url: "url",
      ruler_client: {
        max_recv_msg_size: "int",
        max_send_msg_size: "int",
        grpc_compression: "string",
        rate_limit: "float",
        rate_limit_burst: "int",
        backoff_on_ratelimits: "boolean",
        backoff_config: {
          min_period: "duration",
          max_period: "duration",
          max_retries: "int"
        },
        tls_cert_path: "string",
        tls_key_path: "string",
        tls_ca_path: "string",
        tls_insecure_skip_verify: "boolean"
      },
      evaluation_interval: "duration",
      poll_interval: "duration",
      storage: {
        type: "string",
        configdb: {
          configs_api_url: "url",
          client_timeout: "duration",
          tls_cert_path: "string",
          tls_key_path: "string",
          tls_ca_path: "string",
          tls_insecure_skip_verify: "boolean"
        },
        azure: {
          environment: "string",
          container_name: "string",
          account_name: "string",
          account_key: "string",
          download_buffer_size: "int",
          upload_buffer_size: "int",
          upload_buffer_count: "int",
          request_timeout: "duration",
          max_retries: "int",
          min_retry_delay: "duration",
          max_retry_delay: "duration"
        },
        gcs: {
          bucket_name: "string",
          chunk_buffer_size: "int",
          request_timeout: "duration"
        },
        s3: {
          s3: "url",
          s3forcepathstyle: "boolean",
          bucketnames: "string",
          endpoint: "string",
          region: "string",
          access_key_id: "string",
          secret_access_key: "string",
          insecure: "boolean",
          sse_encryption: "boolean",
          http_config: {
            idle_conn_timeout: "duration",
            response_header_timeout: "duration",
            insecure_skip_verify: "boolean"
          },
          signature_version: "string"
        },
        swift: {
          auth_version: "int",
          auth_url: "string",
          username: "string",
          user_domain_name: "string",
          user_domain_id: "string",
          user_id: "string",
          password: "string",
          domain_id: "string",
          domain_name: "string",
          project_id: "string",
          project_name: "string",
          project_domain_id: "string",
          project_domain_name: "string",
          region_name: "string",
          container_name: "string",
          max_retries: "int",
          connect_timeout: "duration",
          request_timeout: "duration"
        },
        'local': {
          directory: "string"
        }
      },
      rule_path: "string",
      alertmanager_url: "string",
      enable_alertmanager_discovery: "boolean",
      alertmanager_refresh_interval: "duration",
      enable_alertmanager_v2: "boolean",
      notification_queue_capacity: "int",
      notification_timeout: "duration",
      for_outage_tolerance: "duration",
      for_grace_period: "duration",
      resend_delay: "duration",
      enable_sharding: "boolean",
      sharding_strategy: "string",
      search_pending_for: "duration",
      ring: {
        kvstore: {
          store: "string",
          prefix: "string",
          consul: {
            host: "string",
            acl_token: "string",
            http_client_timeout: "duration",
            consistent_reads: "boolean",
            watch_rate_limit: "float",
            watch_burst_size: "int"
          },
          etcd: {
            endpoints: "list of string",
            dial_timeout: "duration",
            max_retries: "int",
            tls_enabled: "boolean",
            tls_cert_path: "string",
            tls_key_path: "string",
            tls_ca_path: "string",
            tls_insecure_skip_verify: "boolean"
          },
          multi: {
            primary: "string",
            secondary: "string",
            mirror_enabled: "boolean",
            mirror_timeout: "duration"
          }
        },
        heartbeat_period: "duration",
        heartbeat_timeout: "duration",
        instance_interface_names: "list of string",
        num_tokens: "int"
      },
      flush_period: "duration",
      enable_api: "boolean"
    },
    configs: {
      database: {
        uri: "string",
        migrations_dir: "string",
        password_file: "string"
      },
      api: {
        notifications: {
          disable_email: "boolean",
          disable_webhook: "boolean"
        }
      }
    },
    alertmanager: {
      data_dir: "string",
      retention: "duration",
      external_url: "url",
      poll_interval: "duration",
      cluster_bind_address: "string",
      cluster_advertise_address: "string",
      peers: "list of string",
      peer_timeout: "duration",
      fallback_config_file: "string",
      auto_webhook_root: "string",
      storage: {
        type: "string",
        configdb: {
          configs_api_url: "url",
          client_timeout: "duration",
          tls_cert_path: "string",
          tls_key_path: "string",
          tls_ca_path: "string",
          tls_insecure_skip_verify: "boolean"
        },
        azure: {
          environment: "string",
          container_name: "string",
          account_name: "string",
          account_key: "string",
          download_buffer_size: "int",
          upload_buffer_size: "int",
          upload_buffer_count: "int",
          request_timeout: "duration",
          max_retries: "int",
          min_retry_delay: "duration",
          max_retry_delay: "duration"
        },
        gcs: {
          bucket_name: "string",
          chunk_buffer_size: "int",
          request_timeout: "duration"
        },
        s3: {
          s3: "url",
          s3forcepathstyle: "boolean",
          bucketnames: "string",
          endpoint: "string",
          region: "string",
          access_key_id: "string",
          secret_access_key: "string",
          insecure: "boolean",
          sse_encryption: "boolean",
          http_config: {
            idle_conn_timeout: "duration",
            response_header_timeout: "duration",
            insecure_skip_verify: "boolean"
          },
          signature_version: "string"
        },
        'local': {
          path: "string"
        }
      },
      enable_api: "boolean"
    },
    runtime_config: {
      period: "duration",
      file: "string"
    },
    memberlist: {
      node_name: "string",
      randomize_node_name: "boolean",
      stream_timeout: "duration",
      retransmit_factor: "int",
      pull_push_interval: "duration",
      gossip_interval: "duration",
      gossip_nodes: "int",
      gossip_to_dead_nodes_time: "duration",
      dead_node_reclaim_time: "duration",
      join_members: "list of string",
      min_join_backoff: "duration",
      max_join_backoff: "duration",
      max_join_retries: "int",
      abort_if_cluster_join_fails: "boolean",
      rejoin_interval: "duration",
      left_ingesters_timeout: "duration",
      leave_timeout: "duration",
      message_history_buffer_bytes: "int",
      bind_addr: "list of string",
      bind_port: "int",
      packet_dial_timeout: "duration",
      packet_write_timeout: "duration"
    },
    query_scheduler: {
      max_outstanding_requests_per_tenant: "int",
      grpc_client_config: {
        max_recv_msg_size: "int",
        max_send_msg_size: "int",
        grpc_compression: "string",
        rate_limit: "float",
        rate_limit_burst: "int",
        backoff_on_ratelimits: "boolean",
        backoff_config: {
          min_period: "duration",
          max_period: "duration",
          max_retries: "int"
        },
        tls_cert_path: "string",
        tls_key_path: "string",
        tls_ca_path: "string",
        tls_insecure_skip_verify: "boolean"
      }
    }
  }
}
