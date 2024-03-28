// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package remote

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/warnings"
	"github.com/thanos-io/promql-engine/query"
	promstorage "github.com/thanos-io/promql-engine/storage/prometheus"
)

type Execution struct {
	storage         *storageAdapter
	query           promql.Query
	opts            *query.Options
	queryRangeStart time.Time
	vectorSelector  model.VectorOperator
	model.OperatorTelemetry
}

func NewExecution(query promql.Query, pool *model.VectorPool, queryRangeStart time.Time, opts *query.Options, hints storage.SelectHints) *Execution {
	storage := newStorageFromQuery(query, opts)
	return &Execution{
		storage:           storage,
		query:             query,
		opts:              opts,
		queryRangeStart:   queryRangeStart,
		vectorSelector:    promstorage.NewVectorSelector(pool, storage, opts, 0, 0, false, 0, 1),
		OperatorTelemetry: model.NewTelemetry("[remoteExec]", opts.EnableAnalysis),
	}
}

func (e *Execution) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { e.AddExecutionTimeTaken(time.Since(start)) }()

	return e.vectorSelector.Series(ctx)
}

func (e *Execution) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { e.AddExecutionTimeTaken(time.Since(start)) }()

	next, err := e.vectorSelector.Next(ctx)
	if next == nil {
		// Closing the storage prematurely can lead to results from the query
		// engine to be recycled. Because of this, we close the storage only
		// when we are done with processing all samples returned by the query.
		e.storage.Close()
	}
	return next, err
}

func (e *Execution) GetPool() *model.VectorPool {
	return e.vectorSelector.GetPool()
}

func (e *Execution) Explain() (me string, next []model.VectorOperator) {
	return fmt.Sprintf("[remoteExec] %s (%d, %d)", e.query, e.queryRangeStart.Unix(), e.opts.End.Unix()), nil
}

type storageAdapter struct {
	query promql.Query
	opts  *query.Options

	once   sync.Once
	err    error
	series []promstorage.SignedSeries
}

func newStorageFromQuery(query promql.Query, opts *query.Options) *storageAdapter {
	return &storageAdapter{
		query: query,
		opts:  opts,
	}
}

func (s *storageAdapter) Matchers() []*labels.Matcher { return nil }

func (s *storageAdapter) GetSeries(ctx context.Context, _, _ int) ([]promstorage.SignedSeries, error) {
	s.once.Do(func() { s.executeQuery(ctx) })
	if s.err != nil {
		return nil, s.err
	}

	return s.series, nil
}

func (s *storageAdapter) executeQuery(ctx context.Context) {
	result := s.query.Exec(ctx)
	warnings.AddToContext(result.Warnings, ctx)
	if result.Err != nil {
		s.err = result.Err
		return
	}
	switch val := result.Value.(type) {
	case promql.Matrix:
		s.series = make([]promstorage.SignedSeries, len(val))
		for i, series := range val {
			s.series[i] = promstorage.SignedSeries{
				Signature: uint64(i),
				Series:    promql.NewStorageSeries(series),
			}
		}
	case promql.Vector:
		s.series = make([]promstorage.SignedSeries, len(val))
		for i, sample := range val {
			series := promql.Series{Metric: sample.Metric}
			if sample.H == nil {
				series.Floats = []promql.FPoint{{T: sample.T, F: sample.F}}
			} else {
				series.Histograms = []promql.HPoint{{T: sample.T, H: sample.H}}
			}
			s.series[i] = promstorage.SignedSeries{
				Signature: uint64(i),
				Series:    promql.NewStorageSeries(series),
			}
		}
	}
}

func (s *storageAdapter) Close() {
	s.query.Close()
}
