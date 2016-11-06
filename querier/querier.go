package querier

import (
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/chunk"
	"github.com/weaveworks/cortex/util"
)

// A Querier allows querying all samples in a given time range that match a set
// of label matchers.
type Querier interface {
	Query(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) (model.Matrix, error)
	LabelValuesForLabelName(context.Context, model.LabelName) (model.LabelValues, error)
}

// A ChunkQuerier is a Querier that fetches samples from a ChunkStore.
type ChunkQuerier struct {
	Store chunk.Store
}

// Query implements Querier and transforms a list of chunks into sample
// matrices.
func (q *ChunkQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) (model.Matrix, error) {
	// Get chunks for all matching series from ChunkStore.
	chunks, err := q.Store.Get(ctx, from, to, matchers...)
	if err != nil {
		return nil, err
	}

	return chunk.ChunksToMatrix(chunks)
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (q *ChunkQuerier) LabelValuesForLabelName(ctx context.Context, ln model.LabelName) (model.LabelValues, error) {
	// TODO: Support querying historical label values at some point?
	return nil, nil
}

type Queryable struct {
	Q local.Querier
}

func (q Queryable) Querier() (local.Querier, error) {
	return q.Q, nil
}

// A MergeQuerier is a promql.Querier that merges the results of multiple
// cortex.Queriers for the same query.
type MergeQuerier struct {
	Queriers []Querier
}

// QueryRange fetches series for a given time range and label matchers from multiple
// promql.Queriers and returns the merged results as a map of series iterators.
func (qm MergeQuerier) QueryRange(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	fpToIt := map[model.Fingerprint]local.SeriesIterator{}

	// Fetch samples from all queriers and group them by fingerprint (unsorted
	// and with overlap).
	for _, q := range qm.Queriers {
		matrix, err := q.Query(ctx, from, to, matchers...)
		if err != nil {
			return nil, err
		}

		for _, ss := range matrix {
			fp := ss.Metric.Fingerprint()
			if it, ok := fpToIt[fp]; !ok {
				fpToIt[fp] = sampleStreamIterator{
					ss: ss,
				}
			} else {
				ssIt := it.(sampleStreamIterator)
				ssIt.ss.Values = util.MergeSamples(ssIt.ss.Values, ss.Values)
			}
		}
	}

	iterators := make([]local.SeriesIterator, 0, len(fpToIt))
	for _, it := range fpToIt {
		iterators = append(iterators, it)
	}

	return iterators, nil
}

// QueryInstant fetches series for a given instant and label matchers from multiple
// promql.Queriers and returns the merged results as a map of series iterators.
func (qm MergeQuerier) QueryInstant(ctx context.Context, ts model.Time, stalenessDelta time.Duration, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	// For now, just fall back to QueryRange, as QueryInstant is merely allows
	// for instant-specific optimization.
	return qm.QueryRange(ctx, ts.Add(-stalenessDelta), ts, matchers...)
}

// MetricsForLabelMatchers Implements local.Querier.
func (qm MergeQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...metric.LabelMatchers) ([]metric.Metric, error) {
	// TODO: Implement.
	return nil, nil
}

// LastSampleForLabelMatchers implements local.Querier.
func (qm MergeQuerier) LastSampleForLabelMatchers(ctx context.Context, cutoff model.Time, matcherSets ...metric.LabelMatchers) (model.Vector, error) {
	// TODO: Implement.
	return nil, nil
}

// LabelValuesForLabelName implements local.Querier.
func (qm MergeQuerier) LabelValuesForLabelName(ctx context.Context, name model.LabelName) (model.LabelValues, error) {
	valueSet := map[model.LabelValue]struct{}{}
	for _, q := range qm.Queriers {
		vals, err := q.LabelValuesForLabelName(ctx, name)
		if err != nil {
			return nil, err
		}
		for _, v := range vals {
			valueSet[v] = struct{}{}
		}
	}

	values := make(model.LabelValues, 0, len(valueSet))
	for v := range valueSet {
		values = append(values, v)
	}
	return values, nil
}

func (qm MergeQuerier) Close() error {
	return nil
}

// TODO(juliusv): Remove all the dummy local.Storage methods below
// once the upstream web API expects a leaner interface.

type DummyStorage struct {
	Queryable
}

// Append implements local.Storage. Needed to satisfy interface
// requirements for usage with the Prometheus web API.
func (DummyStorage) Append(*model.Sample) error {
	panic("MergeQuerier.Append() should never be called")
}

// NeedsThrottling implements local.Storage. Needed to satisfy
// interface requirements for usage with the Prometheus web API.
func (DummyStorage) NeedsThrottling() bool {
	panic("MergeQuerier.NeedsThrottling() should never be called")
}

// DropMetricsForLabelMatchers implements local.Storage. Needed
// to satisfy interface requirements for usage with the Prometheus
// web API.
func (DummyStorage) DropMetricsForLabelMatchers(context.Context, ...*metric.LabelMatcher) (int, error) {
	return 0, fmt.Errorf("dropping metrics is not supported")
}

// Start implements local.Storage. Needed to satisfy interface
// requirements for usage with the Prometheus web API.
func (DummyStorage) Start() error {
	panic("MergeQuerier.Start() should never be called")
}

// Stop implements local.Storage. Needed to satisfy interface
// requirements for usage with the Prometheus web API.
func (DummyStorage) Stop() error {
	panic("MergeQuerier.Stop() should never be called")
}

// WaitForIndexing implements local.Storage. Needed to satisfy
// interface requirements for usage with the Prometheus
// web API.
func (DummyStorage) WaitForIndexing() {
	panic("MergeQuerier.WaitForIndexing() should never be called")
}
