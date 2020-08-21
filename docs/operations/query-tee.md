---
title: "Query Tee (service)"
linkTitle: "Query Tee (service)"
weight: 3
slug: query-tee
---

The `query-tee` is a standalone service which can be used for testing purposes to compare the query performances of 2+ backend systems (ie. Cortex clusters) ingesting the same exact series.

This service exposes Prometheus-compatible read API endpoints and, for each received request, performs the request against all backends tracking the response time of each backend and then sends back to the client one of the received responses.

## How to run it

You can run `query-tee` in two ways:

- Build it from sources
  ```
  go run ./cmd/query-tee -help
  ```
- Run it via the provided Docker image
  ```
  docker run quay.io/cortexproject/query-tee -help
  ```

The service requires at least 1 backend endpoint (but 2 are required in order to compare performances) configured as comma-separated HTTP(S) URLs via the CLI flag **`-backend.endpoints`**. For each incoming request, `query-tee` will clone the request and send it to each backend, tracking performance metrics for each backend before sending back the response to the client.

## How it works

### API endpoints

The following Prometheus API endpoints are supported by `query-tee`:

- `/api/v1/query` (GET)
- `/api/v1/query_range` (GET)
- `/api/v1/labels` (GET)
- `/api/v1/label/{name}/values` (GET)
- `/api/v1/series` (GET)
- `/api/v1/metadata` (GET)
- `/api/v1/alerts` (GET)
- `/api/v1/rules` (GET)

#### Pass-through requests

`query-tee` supports acting as a transparent proxy for requests to routes not matching any of the documented API endpoints above.
When enabled, those requests are passed on to just the configured preferred backend.
To activate this feature it requires setting `-proxy.passthrough-non-registered-routes=true` flag and configuring a preferred backend.

### Authentication

`query-tee` supports [HTTP basic authentication](https://developer.mozilla.org/en-US/docs/Web/HTTP/Authentication). It allows either to configure username and password in the backend URL, to forward the request auth to the backend or merge the two.

The request sent from the `query-tee` to the backend includes HTTP basic authentication when one of the following conditions are met:

- If the endpoint URL has username and password, `query-tee` uses it.
- If the endpoint URL has username only, `query-tee` keeps the username and inject the password received in the incoming request (if any).
- If the endpoint URL has no username and no password, `query-tee` forwards the incoming request basic authentication (if any).

### Backend response selection

`query-tee` allows to configure a preferred backend from which picking the response to send back to the client. The preferred backend can be configured via the CLI flag `-backend.preferred=<hostname>`, setting it to the hostname of the preferred backend.

When a preferred backend **is set**, `query-tee` sends back to the client:

- The preferred backend response if the status code is 2xx or 4xx
- Otherwise, the first received 2xx or 4xx response if at least a backend succeeded
- Otherwise, the first received response

When a preferred backend **is not set**, `query-tee` sends back to the client:

- The first received 2xx or 4xx response if at least a backend succeeded
- Otherwise, the first received response

_Note: from the `query-tee` perspective, a backend request is considered successful even if the status code is 4xx because it generally means the error is due to an invalid request and not to a backend issue._

### Backend results comparison

`query-tee` allows to optionally enable the query results comparison between two backends. The results comparison can be enabled via the CLI flag `-proxy.compare-responses=true` and requires exactly two configured backends with a preferred one.

When the comparison is enabled, the `query-tee` compares the response received from the two configured backends and logs a message for each query whose results don't match, as well as keeps track of the number of successful and failed comparison through the metric `cortex_querytee_responses_compared_total`.

Floating point sample values are compared with a small tolerance that can be configured via `-proxy.value-comparison-tolerance`. This prevents false positives due to differences in floating point values _rounding_ introduced by the non deterministic series ordering within the Prometheus PromQL engine.

### Slow backends

`query-tee` sends back to the client the first viable response as soon as available, without waiting to receive a response from all backends.

### Exported metrics

`query-tee` exposes the following Prometheus metrics on the port configured via the CLI flag `-server.metrics-port`:

```bash
# HELP cortex_querytee_request_duration_seconds Time (in seconds) spent serving HTTP requests.
# TYPE cortex_querytee_request_duration_seconds histogram
cortex_querytee_request_duration_seconds_bucket{backend="<hostname>",method="<method>",route="<route>",status_code="<status>",le="<bucket>"}
cortex_querytee_request_duration_seconds_sum{backend="<hostname>",method="<method>",route="<route>",status_code="<status>"}
cortex_querytee_request_duration_seconds_count{backend="<hostname>",method="<method>",route="<route>",status_code="<status>"}

# HELP cortex_querytee_responses_total Total number of responses sent back to the client by the selected backend.
# TYPE cortex_querytee_responses_total counter
cortex_querytee_responses_total{backend="<hostname>",method="<method>",route="<route>"}

# HELP cortex_querytee_responses_compared_total Total number of responses compared per route name by result.
# TYPE cortex_querytee_responses_compared_total counter
cortex_querytee_responses_compared_total{route="<route>",result="<success|fail>"}
```
