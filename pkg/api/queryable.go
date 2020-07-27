package api

import (
	"context"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

type errorTranslateQueryable struct {
	q storage.SampleAndChunkQueryable
}

func (e errorTranslateQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := e.q.Querier(ctx, mint, maxt)
	return errorTranslateQuerier{q: q}, err
}

func (e errorTranslateQueryable) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	q, err := e.q.ChunkQuerier(ctx, mint, maxt)
	return errorTranslateChunkQuerier{q: q}, err
}

func translateError(err error) error {
	if err == nil {
		return err
	}

	// TODO:...
}

type errorTranslateQuerier struct {
	q storage.Querier
}

func (e errorTranslateQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelValues(name)
	return values, warnings, translateError(err)
}

func (e errorTranslateQuerier) LabelNames() ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelNames()
	return values, warnings, translateError(err)
}

func (e errorTranslateQuerier) Close() error {
	return translateError(e.q.Close())
}

func (e errorTranslateQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	s := e.q.Select(sortSeries, hints, matchers...)
	return errorTranslateSeriesSet{s}
}

type errorTranslateChunkQuerier struct {
	q storage.ChunkQuerier
}

func (e errorTranslateChunkQuerier) LabelValues(name string) ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelValues(name)
	return values, warnings, translateError(err)
}

func (e errorTranslateChunkQuerier) LabelNames() ([]string, storage.Warnings, error) {
	values, warnings, err := e.q.LabelNames()
	return values, warnings, translateError(err)
}

func (e errorTranslateChunkQuerier) Close() error {
	return translateError(e.q.Close())
}

func (e errorTranslateChunkQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.ChunkSeriesSet {
	s := e.q.Select(sortSeries, hints, matchers...)
	return errorTranslateChunkSeriesSet{s}
}

type errorTranslateSeriesSet struct {
	s storage.SeriesSet
}

func (e errorTranslateSeriesSet) Next() bool {
	return e.s.Next()
}

func (e errorTranslateSeriesSet) At() storage.Series {
	return e.s.At()
}

func (e errorTranslateSeriesSet) Err() error {
	return translateError(e.s.Err())
}

func (e errorTranslateSeriesSet) Warnings() storage.Warnings {
	return e.s.Warnings()
}

type errorTranslateChunkSeriesSet struct {
	s storage.ChunkSeriesSet
}

func (e errorTranslateChunkSeriesSet) Next() bool {
	return e.s.Next()
}

func (e errorTranslateChunkSeriesSet) At() storage.ChunkSeries {
	return e.s.At()
}

func (e errorTranslateChunkSeriesSet) Err() error {
	return translateError(e.s.Err())
}

func (e errorTranslateChunkSeriesSet) Warnings() storage.Warnings {
	return e.s.Warnings()
}
