// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package prometheus

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

type histogramStatsIterator struct {
	chunkenc.Iterator
	hReader  *histogram.Histogram
	fhReader *histogram.FloatHistogram
}

func NewHistogramStatsIterator(it chunkenc.Iterator) chunkenc.Iterator {
	return histogramStatsIterator{
		Iterator: it,
		hReader:  &histogram.Histogram{},
		fhReader: &histogram.FloatHistogram{},
	}
}

func (f histogramStatsIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	var t int64
	t, f.hReader = f.Iterator.AtHistogram(f.hReader)
	if value.IsStaleNaN(f.hReader.Sum) {
		return t, &histogram.Histogram{Sum: f.hReader.Sum}
	}

	if h == nil {
		return t, &histogram.Histogram{
			CounterResetHint: f.hReader.CounterResetHint,
			Count:            f.hReader.Count,
			Sum:              f.hReader.Sum,
		}
	}

	h.CounterResetHint = f.hReader.CounterResetHint
	h.Count = f.hReader.Count
	h.Sum = f.hReader.Sum
	return t, h
}

func (f histogramStatsIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	var t int64
	t, f.fhReader = f.Iterator.AtFloatHistogram(f.fhReader)
	if value.IsStaleNaN(f.fhReader.Sum) {
		return t, &histogram.FloatHistogram{Sum: f.fhReader.Sum}
	}

	if fh == nil {
		return t, &histogram.FloatHistogram{
			CounterResetHint: f.fhReader.CounterResetHint,
			Count:            f.fhReader.Count,
			Sum:              f.fhReader.Sum,
		}
	}

	fh.CounterResetHint = f.fhReader.CounterResetHint
	fh.Count = f.fhReader.Count
	fh.Sum = f.fhReader.Sum
	return t, fh
}
