// Copyright 2016 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cortex

import (
	"fmt"
	"sort"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local"
	prom_chunk "github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/chunk"
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

	// Group chunks by series, sort and dedupe samples.
	sampleStreams := map[model.Fingerprint]*model.SampleStream{}

	for _, c := range chunks {
		fp := c.Metric.Fingerprint()
		ss, ok := sampleStreams[fp]
		if !ok {
			ss = &model.SampleStream{
				Metric: c.Metric,
			}
			sampleStreams[fp] = ss
		}

		samples, err := decodeChunk(c.Data)
		if err != nil {
			return nil, err
		}

		ss.Values = append(ss.Values, samples...)
	}

	for _, ss := range sampleStreams {
		sort.Sort(timeSortableSamplePairs(ss.Values))
		// TODO: should we also dedupe samples here or leave that to the upper layers?
	}

	matrix := make(model.Matrix, 0, len(sampleStreams))
	for _, ss := range sampleStreams {
		matrix = append(matrix, ss)
	}

	return matrix, nil
}

func decodeChunk(buf []byte) ([]model.SamplePair, error) {
	lc, err := prom_chunk.NewForEncoding(prom_chunk.DoubleDelta)
	if err != nil {
		return nil, err
	}
	lc.UnmarshalFromBuf(buf)
	it := lc.NewIterator()
	// TODO(juliusv): Pre-allocate this with the right length again once we
	// add a method upstream to get the number of samples in a chunk.
	var samples []model.SamplePair
	for it.Scan() {
		samples = append(samples, it.Value())
	}
	return samples, nil
}

type timeSortableSamplePairs []model.SamplePair

func (ts timeSortableSamplePairs) Len() int {
	return len(ts)
}

func (ts timeSortableSamplePairs) Less(i, j int) bool {
	return ts[i].Timestamp < ts[j].Timestamp
}

func (ts timeSortableSamplePairs) Swap(i, j int) {
	ts[i], ts[j] = ts[j], ts[i]
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (q *ChunkQuerier) LabelValuesForLabelName(ctx context.Context, ln model.LabelName) (model.LabelValues, error) {
	// TODO: Support querying historical label values at some point?
	return nil, nil
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
				ssIt.ss.Values = append(ssIt.ss.Values, ss.Values...)
			}
		}
	}

	// Sort and dedupe samples.
	for _, it := range fpToIt {
		sortable := timeSortableSamplePairs(it.(sampleStreamIterator).ss.Values)
		sort.Sort(sortable)
		// TODO: Dedupe samples. Not strictly necessary.
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

// TODO(juliusv): Remove all the dummy local.Storage methods below
// once the upstream web API expects a leaner interface.

// Append implements local.Storage. Needed to satisfy interface
// requirements for usage with the Prometheus web API.
func (qm MergeQuerier) Append(*model.Sample) error {
	panic("MergeQuerier.Append() should never be called")
}

// NeedsThrottling implements local.Storage. Needed to satisfy
// interface requirements for usage with the Prometheus web API.
func (qm MergeQuerier) NeedsThrottling() bool {
	panic("MergeQuerier.NeedsThrottling() should never be called")
}

// DropMetricsForLabelMatchers implements local.Storage. Needed
// to satisfy interface requirements for usage with the Prometheus
// web API.
func (qm MergeQuerier) DropMetricsForLabelMatchers(context.Context, ...*metric.LabelMatcher) (int, error) {
	return 0, fmt.Errorf("dropping metrics is not supported")
}

// Start implements local.Storage. Needed to satisfy interface
// requirements for usage with the Prometheus web API.
func (qm MergeQuerier) Start() error {
	panic("MergeQuerier.Start() should never be called")
}

// Stop implements local.Storage. Needed to satisfy interface
// requirements for usage with the Prometheus web API.
func (qm MergeQuerier) Stop() error {
	panic("MergeQuerier.Stop() should never be called")
}

// WaitForIndexing implements local.Storage. Needed to satisfy
// interface requirements for usage with the Prometheus
// web API.
func (qm MergeQuerier) WaitForIndexing() {
	panic("MergeQuerier.WaitForIndexing() should never be called")
}
