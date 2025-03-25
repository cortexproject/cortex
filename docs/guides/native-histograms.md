---
title: "Native Histograms"
linkTitle: "Native Histograms"
weight: 10
slug: native-histograms
---

## Context
The Prometheus introduces a native histogram, a new sample type, to address several problems that the classic histograms (originally implemented histograms) have.
Please read to [Prometheus Native Histograms Document](https://prometheus.io/docs/specs/native_histograms/) if more detailed information is necessary.

This guide explains how to configure the native histograms on the Cortex.

## How to configure native histograms
This section explains how to configure native histograms on the Cortex.
### Enable Ingestion
To ingest native histogram ingestion, set the flag `-blocks-storage.tsdb.enable-native-histograms`.

And via yaml:
```yaml
blocks_storage:
  tsdb:
    enable_native_histograms: <bool>
```
By default, it is disabled, so make sure to enable it if you need native histograms for your system monitoring.

When a `write request` contains both `samples` and `native histogram`, the ingestion behavior is as follows:
- When it is enabled, the Cortex ingests samples and native histograms. If ingestion is successful, the Cortex returns a 200 status code.
- When it is disabled, the Cortex ingests samples but discards the native histograms. If sample ingestion is successful, the Cortex returns a 200 status code.

You can track the discarded number of native histograms via the `cortex_discarded_samples_total{reason="native-histogram-sample"}` promQL.

### Set Bucket limit
To limit the number of buckets(= sum of the number of positive and negative buckets) native histograms ingested, set the flag `-validation.max-native-histogram-buckets`.

And via yaml:
```yaml
limits:
  max_native_histogram_buckets: <int>
```

The default value is 0, which means no limit. If the total number of positive and negative buckets exceeds the limit, the Cortex emits a validation error with status code 400 (Bad Request).

To limit the number of buckets per tenant, you can utilize a [runtime config](../configuration/arguments.md#runtime-configuration-file).

For example, the following yaml file specifies the number of bucket limit 160 for `user-1` and no limit for `user-2`.

```
overrides:
  user-1:
    max_native_histogram_buckets: 160
  user-2:
    max_native_histogram_buckets: 0
```

## How to enable out-of-order native histograms ingestion
Like samples out-of-order ingestion, the Cortex allows out-of-order ingestion for the native histogram.
It is automatically enabled when `-blocks-storage.tsdb.enable-native-histograms=true` and `-ingester.out-of-order-time-window > 0`.

And via yaml:

```yaml
blocks_storage:
  tsdb:
    enable_native_histograms: true
limits:
  out_of_order_time_window: 5m
```