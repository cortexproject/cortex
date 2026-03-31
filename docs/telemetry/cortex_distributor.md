---
title: "Distributor Telemetry Reference"
---

# Distributor Telemetry Reference

This document is auto-generated from the telemetry schema defined in
`telemetry/registry/distributor/`. Do not edit manually.

## Metrics

| Name | Type | Unit | Description | Labels |
|------|------|------|-------------|--------|
| `cortex_distributor_deduped_samples_total` | counter | {sample} | The total number of deduplicated samples. | `user`, `cluster` |
| `cortex_distributor_exemplars_in_total` | counter | {exemplar} | The total number of exemplars that have come in to the distributor, including rejected or deduped exemplars. | `user` |
| `cortex_distributor_inflight_client_requests` | gauge | {request} | Current number of inflight client requests in distributor. | - |
| `cortex_distributor_inflight_push_requests` | gauge | {request} | Current number of inflight push requests in distributor. | - |
| `cortex_distributor_ingester_append_failures_total` | counter | {append} | The total number of failed batch appends sent to ingesters. | `ingester`, `type`, `status` |
| `cortex_distributor_ingester_appends_total` | counter | {append} | The total number of batch appends sent to ingesters. | `ingester`, `type` |
| `cortex_distributor_ingester_partial_data_queries_total` | counter | {query} | The total number of queries sent to ingesters that may have returned partial data. | - |
| `cortex_distributor_ingester_push_timeouts_total` | counter | {timeout} | The total number of push requests to ingesters that were canceled due to timeout. | - |
| `cortex_distributor_ingester_queries_total` | counter | {query} | The total number of queries sent to ingesters. | `ingester` |
| `cortex_distributor_ingester_query_failures_total` | counter | {query} | The total number of failed queries sent to ingesters. | `ingester` |
| `cortex_distributor_ingestion_rate_samples_per_second` | gauge | {sample}/s | Current ingestion rate in samples/sec that distributor is using to limit access. | - |
| `cortex_distributor_instance_limits` | gauge | {limit} | Instance limits used by this distributor. | `limit` |
| `cortex_distributor_latest_seen_sample_timestamp_seconds` | gauge | s | Unix timestamp of latest received sample per user. | `user` |
| `cortex_distributor_metadata_in_total` | counter | {metadata} | The total number of metadata that have come in to the distributor, including rejected. | `user` |
| `cortex_distributor_non_ha_samples_received_total` | counter | {sample} | The total number of received samples for a user that has HA tracking turned on, but the sample didn't contain both HA labels. | `user` |
| `cortex_distributor_query_duration_seconds` | histogram | s | Time spent executing expression and exemplar queries. | `method`, `status_code` |
| `cortex_distributor_received_exemplars_total` | counter | {exemplar} | The total number of received exemplars, excluding rejected and deduped exemplars. | `user` |
| `cortex_distributor_received_metadata_total` | counter | {metadata} | The total number of received metadata, excluding rejected. | `user` |
| `cortex_distributor_received_samples_per_labelset_total` | counter | {sample} | The total number of received samples per label set, excluding rejected and deduped samples. | `user`, `type`, `labelset` |
| `cortex_distributor_received_samples_total` | counter | {sample} | The total number of received samples, excluding rejected and deduped samples. | `user`, `type` |
| `cortex_distributor_replication_factor` | gauge | {factor} | The configured replication factor. | - |
| `cortex_distributor_samples_in_total` | counter | {sample} | The total number of samples that have come in to the distributor, including rejected or deduped samples. | `user`, `type` |
| `cortex_labels_per_sample` | histogram | {label} | Number of labels per sample. | - |