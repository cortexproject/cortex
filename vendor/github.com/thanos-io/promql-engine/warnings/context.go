// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package warnings

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"
)

// MixedFloatsHistogramsAggWarning is used when an aggregation encounters both floats and histograms.
// We define this here because Prometheus's NewMixedFloatsHistogramsAggWarning requires a posrange
// which we don't have at the accumulator level.
//
//lint:ignore faillint We need fmt.Errorf to match Prometheus error format exactly.
var MixedFloatsHistogramsAggWarning = fmt.Errorf("%w aggregation", annotations.MixedFloatsHistogramsWarning)

// Warnings is a bitset of warning flags that can be returned by functions
// to indicate warning conditions. The actual warning messages with metric
// names are emitted by operators that have access to series labels.
type Warnings uint32

const (
	WarnNotCounter Warnings = 1 << iota
	WarnNotGauge
	WarnMixedFloatsHistograms
	WarnMixedExponentialCustomBuckets
	WarnHistogramIgnoredInMixedRange  // for _over_time functions, only when both floats and histograms
	WarnHistogramIgnoredInAggregation // for aggregations (max, min, stddev, etc.), always when histograms ignored
	WarnCounterResetCollision
	WarnNHCBBoundsReconciled    // for subtraction operations (rate, irate, delta)
	WarnNHCBBoundsReconciledAgg // for aggregation operations (sum, avg, sum_over_time, avg_over_time)
	WarnIncompatibleTypesInBinOp
)

type warningKey string

const key warningKey = "promql-warnings"

type warnings struct {
	mu    sync.Mutex
	warns annotations.Annotations
}

func newWarnings() *warnings {
	return &warnings{warns: annotations.Annotations{}}
}

func (w *warnings) add(warns error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warns = w.warns.Add(warns)
}

func (w *warnings) get() annotations.Annotations {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.warns
}

func (w *warnings) merge(anno annotations.Annotations) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.warns = w.warns.Merge(anno)
}

func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, key, newWarnings())
}

func AddToContext(warn error, ctx context.Context) {
	w, ok := ctx.Value(key).(*warnings)
	if !ok {
		return
	}
	w.add(warn)
}

func MergeToContext(annos annotations.Annotations, ctx context.Context) {
	w, ok := ctx.Value(key).(*warnings)
	if !ok {
		return
	}
	w.merge(annos)
}

func FromContext(ctx context.Context) annotations.Annotations {
	warns := ctx.Value(key).(*warnings).get()

	return maps.Clone(warns)
}

// ConvertHistogramError converts histogram operation errors to appropriate annotation warnings.
// Returns nil if the error is not a histogram error.
func ConvertHistogramError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
		return annotations.MixedExponentialCustomHistogramsWarning
	}
	return err
}
