---
title: "Query Range Response Trimming"
linkTitle: "Query Range Response Trimming"
weight: 1
slug: query-range-response-trimming
---

- Author: @chinmay venkat 
- Date: February 2026
- Status: Proposed

## Problem

When `querier.align-querier-with-step: true` is configured, the query frontend modifies the user's requested `start` time by flooring it to the nearest `step` boundary **before** passing the request to the results cache and downstream querier.

This causes the response to contain data points **outside** the user's originally requested time range.

### Example

| Config | Value |
|---|---|
| `querier.align-querier-with-step` | `true` |
| `querier.split-queries-by-interval` | `1h` |
| `step` | `10m` |

User requests: `start=09:01, end=10:00`

`StepAlignMiddleware` transforms this to: `start=09:00, end=10:00`

The user receives data from `09:00` — **1 minute earlier than requested**.

With larger step values (e.g., `30m`), the delta is proportionally larger. Additionally, `SplitByIntervalMiddleware`'s `nextIntervalBoundary` calculation can cause sub-request start/end to drift from the user's requested window, compounding the effect.

## Root Cause

The middleware chain in `query_range_middlewares.go` is ordered as:

```
LimitsMiddleware
→ StepAlignMiddleware         ← mutates: start = floor(start / step) * step
→ SplitByIntervalMiddleware
→ ResultsCacheMiddleware      ← sees aligned start; original start is permanently lost
→ ShardByMiddleware
```

Once `StepAlignMiddleware` calls `r.WithStartEnd(alignedStart, alignedEnd)`, the original user-requested `start`/`end` values are **permanently gone from the request object** and no downstream middleware can use them to trim the final response.

The `extractSampleStreams(start, end, ...)` function that trims samples by timestamp already exists in `results_cache.go` and is used inside `partition()` — but only with the post-alignment `start`, not the original.

## Proposed Solution

Add a `RangeTrimMiddleware` as the **outermost** middleware in the chain, controlled by a new opt-in configuration flag `querier.trim-response-to-requested-range`.

When enabled, it captures the original `start`/`end` before any mutation, lets the full middleware stack execute internally (alignment, splitting, caching, sharding), then trims the final response back to the user's original window using the existing `Extractor.Extract()` interface.

### 1. Config flag — `pkg/querier/tripperware/queryrange/query_range_middlewares.go`

Add `TrimResponseToRequestedRange` to `Config`, following the same pattern as `AlignQueriesWithStep` and `CacheResults`:

```go
// Config for query_range middleware chain.
type Config struct {
    SplitQueriesByInterval   time.Duration            `yaml:"split_queries_by_interval"`
    DynamicQuerySplitsConfig DynamicQuerySplitsConfig `yaml:"dynamic_query_splits"`
    AlignQueriesWithStep     bool `yaml:"align_queries_with_step"`
    TrimResponseToRequestedRange bool `yaml:"trim_response_to_requested_range"` // ← NEW
    ResultsCacheConfig   `yaml:"results_cache"`
    CacheResults         bool `yaml:"cache_results"`
    MaxRetries           int  `yaml:"max_retries"`
    ForwardHeaders       flagext.StringSlice `yaml:"forward_headers_list"`
    VerticalShardSize    int `yaml:"-"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
    // ... existing flags ...
    f.BoolVar(&cfg.TrimResponseToRequestedRange,
        "querier.trim-response-to-requested-range", false,
        "When enabled, the query frontend trims the response to exactly the "+
        "[start, end] range requested by the user, removing any extra data "+
        "introduced by align_queries_with_step or interval split-boundary rounding.")
}
```

### 2. New File: `pkg/querier/tripperware/queryrange/range_trim.go`

```go
package queryrange

import (
    "context"
    "github.com/cortexproject/cortex/pkg/querier/tripperware"
)

// NewRangeTrimMiddleware returns a middleware that clips the final response
// to the original [start, end] requested by the user. Enable via
// querier.trim-response-to-requested-range.
func NewRangeTrimMiddleware(extractor Extractor) tripperware.Middleware {
    return tripperware.MiddlewareFunc(func(next tripperware.Handler) tripperware.Handler {
        return &rangeTrimHandler{next: next, extractor: extractor}
    })
}

type rangeTrimHandler struct {
    next      tripperware.Handler
    extractor Extractor
}

func (h rangeTrimHandler) Do(ctx context.Context, r tripperware.Request) (tripperware.Response, error) {
    origStart := r.GetStart()
    origEnd   := r.GetEnd()

    resp, err := h.next.Do(ctx, r)
    if err != nil {
        return nil, err
    }

    return h.extractor.Extract(origStart, origEnd, resp), nil
}
```

### 3. Register conditionally in `Middlewares()`

Register as the **first** entry so it wraps the entire chain:

```go
func Middlewares(cfg Config, ...) ([]tripperware.Middleware, cache.Cache, error) {
    metrics := tripperware.NewInstrumentMiddlewareMetrics(registerer)

    // Outermost: trim response to original user-requested range
    queryRangeMiddleware := []tripperware.Middleware{}
    if cfg.TrimResponseToRequestedRange {                            // ← NEW
        queryRangeMiddleware = append(queryRangeMiddleware,
            tripperware.InstrumentMiddleware("range_trim", metrics),
            NewRangeTrimMiddleware(cacheExtractor))
    }

    queryRangeMiddleware = append(queryRangeMiddleware,
        NewLimitsMiddleware(limits, lookbackDelta))
    if cfg.AlignQueriesWithStep {
        queryRangeMiddleware = append(queryRangeMiddleware,
            tripperware.InstrumentMiddleware("step_align", metrics),
            StepAlignMiddleware)
    }
    // ... rest unchanged
}
```

Because middlewares compose like an onion, the first entry in the slice is the outermost layer — it sees the original request and intercepts the final response after all inner middlewares have run.

## Why This Is Safe

| Concern | Answer |
|---|---|
| Breaks results caching? | No — cache operates internally with aligned times, untouched |
| Breaks query splitting? | No — splitting happens inside, RangeTrim only touches the final response |
| `cacheExtractor` already available? | ✅ Already a parameter of `Middlewares()` |
| `Extract()` correct for trimming? | ✅ `extractSampleStreams(start, end, ...)` filters by sample timestamps, same mechanism used in `partition()` |
| Affects non-aligned mode? | No — if start/end are not mutated, `Extract(origStart, origEnd)` is a no-op |
| Stats trimming? | ✅ `extractStats(start, end, ...)` already handles per-step stats correctly |

## Files Changed

| File | Type | Description |
|---|---|---|
| `pkg/querier/tripperware/queryrange/range_trim.go` | New | `RangeTrimMiddleware` + `rangeTrimHandler` |
| `pkg/querier/tripperware/queryrange/query_range_middlewares.go` | Modified | Add `TrimResponseToRequestedRange` to `Config`, `RegisterFlags`, and `Middlewares()` |
| `pkg/querier/tripperware/queryrange/range_trim_test.go` | New | Unit tests |
| `CHANGELOG.md` | Modified | Entry under `## main / unreleased` |

## Test Plan

New unit tests in `range_trim_test.go`:

1. **Flag disabled (default)**: middleware not registered, response unchanged
2. **Alignment trim**: `trim_response_to_requested_range=true` + `align_queries_with_step=true`, `step=10m`, `start=09:01` → verify no samples before `09:01`
3. **Already aligned (no-op)**: `start=09:00` with `step=10m` → response unchanged
4. **Split boundary trim**: `split_queries_by_interval=1h`, `start=09:01` → verify no data before `09:01`
5. **End trim**: `end=09:59` with internal boundary at `10:00` → verify no data after `09:59`

```bash
# Run new tests
go test ./pkg/querier/tripperware/queryrange/... -run TestRangeTrim -v

# Run full suite to confirm no regressions
go test ./pkg/querier/tripperware/queryrange/... -v
```

