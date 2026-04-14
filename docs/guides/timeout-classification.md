# Timeout Classification

Timeout classification lets Cortex distinguish between query timeouts caused by expensive user queries (4XX) and those caused by system issues (5XX). When enabled, queries that spend most of their time in PromQL evaluation are returned as `422 Unprocessable Entity` instead of `503 Service Unavailable`, giving callers a clear signal to simplify the query rather than retry.

## How It Works

When a query arrives at the querier, the feature:

1. Subtracts any time the query spent waiting in the scheduler queue from the configured deadline.
2. Sets a proactive context timeout using the remaining budget, so the querier cancels the query slightly before the PromQL engine's own timeout fires.
3. On timeout, inspects phase timings (storage fetch time vs. total time) to compute eval time.
4. If eval time exceeds the configured threshold, the timeout is classified as a user error (4XX). Otherwise it remains a system error (5XX).

This means expensive queries that burn their budget in PromQL evaluation get a `422`, while other queries remain a `5XX`.

* Note that due to different query shards not returning at the same time, the first returned timed out shard gets to decide whether the query will be converted to 4XX.

## Configuration

Enable the feature and set the three related flags:

```yaml
querier:
  timeout_classification_enabled: true
  timeout_classification_deadline: 1m59s
  timeout_classification_eval_threshold: 1m30s
```

| Flag | Default | Description |
|---|---|---|
| `timeout_classification_enabled` | `false` | Enable 5XX-to-4XX conversion based on phase timing. |
| `timeout_classification_deadline` | `1m59s` | Proactive cancellation deadline. Set this a few seconds less than the querier timeout. |
| `timeout_classification_eval_threshold` | `1m30s` | Eval time above which a timeout is classified as user error (4XX). Must be ≤ the deadline. |

### Constraints

- `timeout_classification_deadline` must be positive and strictly less than `querier.timeout`.
- `timeout_classification_eval_threshold` must be positive and ≤ `timeout_classification_deadline`.
- Query stats must be enabled (`query_stats_enabled: true` on the frontend handler) for classification to work.

## Tuning

- The deadline should be close to but below the querier timeout so the proactive cancellation fires first. A gap of 1–2 seconds is typical.
- The eval threshold controls sensitivity. A lower threshold classifies more timeouts as user errors; a higher threshold is more conservative. Start with the default and adjust based on your workload.
- Monitor the `decision` field in the timeout classification log line (`query shard timed out with classification`) to see how queries are being classified before enabling the conversion.

## Observability

When a query times out and query stats is active, the querier emits a warning-level log line containing:

- `queue_wait_time` — time spent in the scheduler queue
- `query_storage_wall_time` — time spent fetching data from storage
- `eval_time` — computed as `total_time - query_storage_wall_time`
- `decision` — `0` for 5XX (system), `1` for 4XX (user)
- `conversion_enabled` — whether the status code conversion is active

These fields are logged regardless of whether conversion is enabled, so you can observe classification behavior in dry-run mode by setting `timeout_classification_enabled: false` and reviewing the logs.
