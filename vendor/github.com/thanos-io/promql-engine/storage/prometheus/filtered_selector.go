// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
)

type filteredSelector struct {
	selector *seriesSelector
	filter   Filter

	once   sync.Once
	series []SignedSeries
}

func NewFilteredSelector(selector *seriesSelector, filter Filter) SeriesSelector {
	return &filteredSelector{
		selector: selector,
		filter:   filter,
	}
}

func (f *filteredSelector) Matchers() []*labels.Matcher {
	return append(f.selector.matchers, f.filter.Matchers()...)
}

func (f *filteredSelector) GetSeries(ctx context.Context, shard, numShards int) ([]SignedSeries, error) {
	var err error
	f.once.Do(func() { err = f.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}

	return seriesShard(f.series, shard, numShards), nil
}

func (f *filteredSelector) loadSeries(ctx context.Context) error {
	series, err := f.selector.GetSeries(ctx, 0, 1)
	if err != nil {
		return err
	}

	var i uint64
	f.series = make([]SignedSeries, 0, len(series))
	for _, s := range series {
		if f.filter.Matches(s) {
			f.series = append(f.series, SignedSeries{
				Series:    s.Series,
				Signature: i,
			})
			i++
		}
	}

	return nil
}
