package querier

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage/local"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util"
)

// ChunkStore is the interface we need to get chunks
type ChunkStore interface {
	Get(ctx context.Context, from, through model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error)
}

// NewEngine creates a new promql.Engine for cortex.
func NewEngine(distributor Querier, chunkStore ChunkStore) *promql.Engine {
	queryable := NewQueryable(distributor, chunkStore)
	return promql.NewEngine(queryable, nil)
}

// NewQueryable creates a new Queryable for cortex.
func NewQueryable(distributor Querier, chunkStore ChunkStore) Queryable {
	return Queryable{
		Q: MergeQuerier{
			Queriers: []Querier{
				distributor,
				&ChunkQuerier{
					Store: chunkStore,
				},
			},
		},
	}
}

// A Querier allows querying all samples in a given time range that match a set
// of label matchers.
type Querier interface {
	Query(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error)
	LabelValuesForLabelName(context.Context, model.LabelName) (model.LabelValues, error)
	MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...metric.LabelMatchers) ([]metric.Metric, error)
}

// A ChunkQuerier is a Querier that fetches samples from a ChunkStore.
type ChunkQuerier struct {
	Store ChunkStore
}

// Query implements Querier and transforms a list of chunks into sample
// matrices.
func (q *ChunkQuerier) Query(ctx context.Context, from, to model.Time, matchers ...*metric.LabelMatcher) ([]local.SeriesIterator, error) {
	// Get iterators for all matching series from ChunkStore.
	iterators, err := q.Store.Get(ctx, from, to, matchers...)
	if err != nil {
		return nil, promql.ErrStorage(err)
	}

	return iterators, nil
}

// LabelValuesForLabelName returns all of the label values that are associated with a given label name.
func (q *ChunkQuerier) LabelValuesForLabelName(ctx context.Context, ln model.LabelName) (model.LabelValues, error) {
	// TODO: Support querying historical label values at some point?
	return nil, nil
}

// MetricsForLabelMatchers is a noop for chunk querier.
func (q *ChunkQuerier) MetricsForLabelMatchers(ctx context.Context, from, through model.Time, matcherSets ...metric.LabelMatchers) ([]metric.Metric, error) {
	return nil, nil
}

// Queryable is an adapter between Prometheus' Queryable and Querier.
type Queryable struct {
	Q MergeQuerier
}

// Querier implements Queryable
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
	incomingIterators := make(chan []local.SeriesIterator)
	incomingErrors := make(chan error)

	for _, q := range qm.Queriers {
		go func(q Querier) {
			iterators, err := q.Query(ctx, from, to, matchers...)
			if err != nil {
				incomingErrors <- err
			} else {
				incomingIterators <- iterators
			}
		}(q)
	}

	mergeIterators, err := createMergeIterators(incomingIterators, incomingErrors, len(qm.Queriers))
	if err != nil {
		log.Errorf("Error in MergeQuerier.Query: %v", err)
	}
	return mergeIterators, err
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
	// NB we don't do this in parallel, as in practice we only have 2 queriers,
	// one of which is the chunk store which doesn't implement this.

	metrics := map[model.Fingerprint]metric.Metric{}
	for _, q := range qm.Queriers {
		ms, err := q.MetricsForLabelMatchers(ctx, from, through, matcherSets...)
		if err != nil {
			return nil, err
		}
		for _, m := range ms {
			metrics[m.Metric.Fingerprint()] = m
		}
	}

	result := make([]metric.Metric, 0, len(metrics))
	for _, m := range metrics {
		result = append(result, m)
	}
	return result, nil
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

// Close is a noop
func (qm MergeQuerier) Close() error {
	return nil
}

// RemoteReadHandler handles Prometheus remote read requests.
func (qm MergeQuerier) RemoteReadHandler(w http.ResponseWriter, r *http.Request) {
	compressionType := util.CompressionTypeFor(r.Header.Get("X-Prometheus-Remote-Read-Version"))

	ctx := r.Context()
	var req client.ReadRequest
	if _, err := util.ParseProtoRequest(ctx, r, &req, compressionType); err != nil {
		log.Errorf(err.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Fetch samples for all queries in parallel.
	resp := client.ReadResponse{
		Results: make([]*client.QueryResponse, len(req.Queries)),
	}
	errors := make(chan error, len(req.Queries))
	for i, q := range req.Queries {
		go func(i int, q *client.QueryRequest) {
			from, to, matchers, err := util.FromQueryRequest(q)
			if err != nil {
				errors <- err
				return
			}

			iterators, err := qm.QueryRange(ctx, from, to, matchers...)
			if err != nil {
				errors <- err
				return
			}

			in := metric.Interval{
				OldestInclusive: from,
				NewestInclusive: to,
			}

			// Convert iterators to matrix
			matrix := make(model.Matrix, 0, len(iterators))
			for _, it := range iterators {
				ss := &model.SampleStream{
					Metric: it.Metric().Metric,
					Values: it.RangeValues(in),
				}
				matrix = append(matrix, ss)
			}

			resp.Results[i] = util.ToQueryResponse(matrix)
			errors <- nil
		}(i, q)
	}

	for err := range errors {
		if err != nil {
			log.Errorf("Query error: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	if err := util.SerializeProtoResponse(w, &resp, compressionType); err != nil {
		log.Errorf("error sending remote read response: %v", err)
	}
}

func createMergeIterators(incomingIterators chan []local.SeriesIterator, incomingErrors chan error, n int) ([]local.SeriesIterator, error) {
	// Group iterators by fingerprint.
	fpToIts := map[model.Fingerprint][]local.SeriesIterator{}
	var lastErr error
	for i := 0; i < n; i++ {
		select {
		case err := <-incomingErrors:
			lastErr = err

		case iterators := <-incomingIterators:
			for _, it := range iterators {
				fp := it.Metric().Metric.Fingerprint()
				if _, ok := fpToIts[fp]; !ok {
					fpToIts[fp] = []local.SeriesIterator{it}
				} else {
					fpToIts[fp] = append(fpToIts[fp], it)
				}
			}
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}

	var mergeIterators []local.SeriesIterator
	for _, its := range fpToIts {
		mergeIterators = append(mergeIterators, util.NewMergeSeriesIterator(its))
	}
	return mergeIterators, nil
}

func mergeMatrices(matrices chan model.Matrix, errors chan error, n int) (model.Matrix, error) {
	// Group samples from all matrices by fingerprint.
	fpToSS := map[model.Fingerprint]*model.SampleStream{}
	var lastErr error
	for i := 0; i < n; i++ {
		select {
		case err := <-errors:
			lastErr = err

		case matrix := <-matrices:
			for _, ss := range matrix {
				fp := ss.Metric.Fingerprint()
				if fpSS, ok := fpToSS[fp]; !ok {
					fpToSS[fp] = ss
				} else {
					fpSS.Values = util.MergeSampleSets(fpSS.Values, ss.Values)
				}
			}
		}
	}
	if lastErr != nil {
		return nil, lastErr
	}

	matrix := make(model.Matrix, 0, len(fpToSS))
	for _, ss := range fpToSS {
		matrix = append(matrix, ss)
	}
	return matrix, nil
}

// DummyStorage creates a local.Storage compatible struct from a
// Queryable, such that it can be used with web.NewAPI.
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
