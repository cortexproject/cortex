// Copyright The Prometheus Authors
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

package search

import (
	"context"
	"runtime"
	"sort"

	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type ShardsFinderFunction func(ctx context.Context, mint, maxt int64) ([]*storage.ParquetShard, error)

type queryableOpts struct {
	concurrency                int
	pagePartitioningMaxGapSize int
}

var DefaultQueryableOpts = queryableOpts{
	concurrency:                runtime.GOMAXPROCS(0),
	pagePartitioningMaxGapSize: 10 * 1024,
}

type QueryableOpts func(*queryableOpts)

// WithConcurrency set the concurrency that can be used to run the query
func WithConcurrency(concurrency int) QueryableOpts {
	return func(opts *queryableOpts) {
		opts.concurrency = concurrency
	}
}

// WithPageMaxGapSize set the max gap size between pages that should be downloaded together in a single read call
func WithPageMaxGapSize(pagePartitioningMaxGapSize int) QueryableOpts {
	return func(opts *queryableOpts) {
		opts.pagePartitioningMaxGapSize = pagePartitioningMaxGapSize
	}
}

type parquetQueryable struct {
	shardsFinder ShardsFinderFunction
	d            *schema.PrometheusParquetChunksDecoder
	opts         *queryableOpts
}

func NewParquetQueryable(d *schema.PrometheusParquetChunksDecoder, shardFinder ShardsFinderFunction, opts ...QueryableOpts) (prom_storage.Queryable, error) {
	cfg := DefaultQueryableOpts

	for _, opt := range opts {
		opt(&cfg)
	}

	return &parquetQueryable{
		shardsFinder: shardFinder,
		d:            d,
		opts:         &cfg,
	}, nil
}

func (p parquetQueryable) Querier(mint, maxt int64) (prom_storage.Querier, error) {
	return &parquetQuerier{
		mint:         mint,
		maxt:         maxt,
		shardsFinder: p.shardsFinder,
		d:            p.d,
		opts:         p.opts,
	}, nil
}

type parquetQuerier struct {
	mint, maxt   int64
	shardsFinder ShardsFinderFunction
	d            *schema.PrometheusParquetChunksDecoder
	opts         *queryableOpts
}

func (p parquetQuerier) LabelValues(ctx context.Context, name string, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	shards, err := p.queryableShards(ctx, p.mint, p.maxt)
	if err != nil {
		return nil, nil, err
	}

	limit := int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameValues := [][]string{}

	for _, s := range shards {
		r, err := s.LabelValues(ctx, name, matchers)
		if err != nil {
			return nil, nil, err
		}

		resNameValues = append(resNameValues, r...)
	}

	return util.MergeUnsortedSlices(int(limit), resNameValues...), nil, nil
}

func (p parquetQuerier) LabelNames(ctx context.Context, hints *prom_storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	shards, err := p.queryableShards(ctx, p.mint, p.maxt)
	if err != nil {
		return nil, nil, err
	}

	limit := int64(0)

	if hints != nil {
		limit = int64(hints.Limit)
	}

	resNameSets := [][]string{}

	for _, s := range shards {
		r, err := s.LabelNames(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		resNameSets = append(resNameSets, r...)
	}

	return util.MergeUnsortedSlices(int(limit), resNameSets...), nil, nil
}

func (p parquetQuerier) Close() error {
	return nil
}

func (p parquetQuerier) Select(ctx context.Context, sorted bool, sp *prom_storage.SelectHints, matchers ...*labels.Matcher) prom_storage.SeriesSet {
	shards, err := p.queryableShards(ctx, p.mint, p.maxt)
	if err != nil {
		return prom_storage.ErrSeriesSet(err)
	}
	seriesSet := make([]prom_storage.ChunkSeriesSet, len(shards))

	minT, maxT := p.mint, p.maxt
	if sp != nil {
		minT, maxT = sp.Start, sp.End
	}
	skipChunks := sp != nil && sp.Func == "series"

	for i, shard := range shards {
		ss, err := shard.Query(ctx, sorted, minT, maxT, skipChunks, matchers)
		if err != nil {
			return prom_storage.ErrSeriesSet(err)
		}
		seriesSet[i] = ss
	}
	ss := convert.NewMergeChunkSeriesSet(seriesSet, labels.Compare, prom_storage.NewConcatenatingChunkSeriesMerger())

	return convert.NewSeriesSetFromChunkSeriesSet(ss, skipChunks)
}

func (p parquetQuerier) queryableShards(ctx context.Context, mint, maxt int64) ([]*queryableShard, error) {
	shards, err := p.shardsFinder(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	qBlocks := make([]*queryableShard, len(shards))
	for i, shard := range shards {
		qb, err := newQueryableShard(p.opts, shard, p.d)
		if err != nil {
			return nil, err
		}
		qBlocks[i] = qb
	}
	return qBlocks, nil
}

type queryableShard struct {
	shard *storage.ParquetShard
	m     *Materializer
}

func newQueryableShard(opts *queryableOpts, block *storage.ParquetShard, d *schema.PrometheusParquetChunksDecoder) (*queryableShard, error) {
	s, err := block.TSDBSchema()
	if err != nil {
		return nil, err
	}
	m, err := NewMaterializer(s, d, block, opts.concurrency, opts.pagePartitioningMaxGapSize)
	if err != nil {
		return nil, err
	}

	return &queryableShard{
		shard: block,
		m:     m,
	}, nil
}

func (b queryableShard) Query(ctx context.Context, sorted bool, mint, maxt int64, skipChunks bool, matchers []*labels.Matcher) (prom_storage.ChunkSeriesSet, error) {
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.shard.LabelsFile(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([]prom_storage.ChunkSeries, 0, 1024)
	for i, group := range b.shard.LabelsFile().RowGroups() {
		rr, err := Filter(ctx, group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.Materialize(ctx, i, mint, maxt, skipChunks, rr)
		if err != nil {
			return nil, err
		}
		results = append(results, series...)
	}

	if sorted {
		sort.Sort(byLabels(results))
	}
	return convert.NewChunksSeriesSet(results), nil
}

func (b queryableShard) LabelNames(ctx context.Context, matchers []*labels.Matcher) ([][]string, error) {
	if len(matchers) == 0 {
		return [][]string{b.m.MaterializeAllLabelNames()}, nil
	}
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.shard.LabelsFile(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))
	for i, group := range b.shard.LabelsFile().RowGroups() {
		rr, err := Filter(ctx, group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.MaterializeLabelNames(ctx, i, rr)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

func (b queryableShard) LabelValues(ctx context.Context, name string, matchers []*labels.Matcher) ([][]string, error) {
	if len(matchers) == 0 {
		return b.allLabelValues(ctx, name)
	}
	cs, err := MatchersToConstraint(matchers...)
	if err != nil {
		return nil, err
	}
	err = Initialize(b.shard.LabelsFile(), cs...)
	if err != nil {
		return nil, err
	}

	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))
	for i, group := range b.shard.LabelsFile().RowGroups() {
		rr, err := Filter(ctx, group, cs...)
		if err != nil {
			return nil, err
		}
		series, err := b.m.MaterializeLabelValues(ctx, name, i, rr)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

func (b queryableShard) allLabelValues(ctx context.Context, name string) ([][]string, error) {
	results := make([][]string, len(b.shard.LabelsFile().RowGroups()))
	for i := range b.shard.LabelsFile().RowGroups() {
		series, err := b.m.MaterializeAllLabelValues(ctx, name, i)
		if err != nil {
			return nil, err
		}
		results[i] = series
	}

	return results, nil
}

type byLabels []prom_storage.ChunkSeries

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i].Labels(), b[j].Labels()) < 0 }
