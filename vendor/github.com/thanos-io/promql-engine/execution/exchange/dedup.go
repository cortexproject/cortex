// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/query"

	"github.com/cespare/xxhash/v2"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
)

type dedupSample struct {
	t int64
	v float64
	h *histogram.FloatHistogram
}

// The dedupCache is an internal cache used to deduplicate samples inside a single step vector.
type dedupCache []dedupSample

// dedupOperator is a model.VectorOperator that deduplicates samples with
// same IDs inside a single model.StepVector.
// Deduplication is done using a last-sample-wins strategy, which means that
// if multiple samples with the same ID are present in a StepVector, dedupOperator
// will keep the last sample in that vector.
type dedupOperator struct {
	once   sync.Once
	series []labels.Labels

	next model.VectorOperator
	// outputIndex is a slice that is used as an index from input sample ID to output sample ID.
	outputIndex []uint64
	dedupCache  dedupCache
}

func NewDedupOperator(next model.VectorOperator, opts *query.Options) model.VectorOperator {
	oper := &dedupOperator{
		next: next,
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (d *dedupOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	var err error
	d.once.Do(func() { err = d.loadSeries(ctx) })
	if err != nil {
		return 0, err
	}

	n, err := d.next.Next(ctx, buf)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}

	// Process each input vector and overwrite it with the deduplicated output
	for idx := range n {
		vector := &buf[idx]

		// Update dedup cache with all samples from this vector
		for i, inputSampleID := range vector.SampleIDs {
			outputSampleID := d.outputIndex[inputSampleID]
			d.dedupCache[outputSampleID].t = vector.T
			d.dedupCache[outputSampleID].v = vector.Samples[i]
		}

		for i, inputSampleID := range vector.HistogramIDs {
			outputSampleID := d.outputIndex[inputSampleID]
			d.dedupCache[outputSampleID].t = vector.T
			d.dedupCache[outputSampleID].h = vector.Histograms[i]
		}

		// Clear the vector and rebuild it with deduplicated data
		t := vector.T
		buf[idx].Reset(t)

		hint := len(d.series)
		for outputSampleID, sample := range d.dedupCache {
			// To avoid clearing the dedup cache for each step vector, we use the `t` field
			// to detect whether a sample for the current step should be mapped to the output.
			// If the timestamp of the sample does not match the input vector timestamp, it means that
			// the sample was added in a previous iteration and should be skipped.
			if sample.t == t {
				if sample.h == nil {
					buf[idx].AppendSampleWithSizeHint(uint64(outputSampleID), sample.v, hint)
				} else {
					buf[idx].AppendHistogramWithSizeHint(uint64(outputSampleID), sample.h, hint)
				}
			}
		}
	}

	return n, nil
}

func (d *dedupOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	d.once.Do(func() { err = d.loadSeries(ctx) })
	if err != nil {
		return nil, err
	}
	return d.series, nil
}

func (d *dedupOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{d.next}
}

func (d *dedupOperator) String() string {
	return "[dedup]"
}

func (d *dedupOperator) loadSeries(ctx context.Context) error {
	series, err := d.next.Series(ctx)
	if err != nil {
		return err
	}

	outputIndex := make(map[uint64]uint64)
	inputIndex := make([]uint64, len(series))
	hashBuf := make([]byte, 0, 128)
	for inputSeriesID, inputSeries := range series {
		hash := hashSeries(hashBuf, inputSeries)

		inputIndex[inputSeriesID] = hash
		outputSeriesID, ok := outputIndex[hash]
		if !ok {
			outputSeriesID = uint64(len(d.series))
			d.series = append(d.series, inputSeries)
		}
		outputIndex[hash] = outputSeriesID
	}

	d.outputIndex = make([]uint64, len(inputIndex))
	for inputSeriesID, hash := range inputIndex {
		outputSeriesID := outputIndex[hash]
		d.outputIndex[inputSeriesID] = outputSeriesID
	}
	d.dedupCache = make(dedupCache, len(outputIndex))
	for i := range d.dedupCache {
		d.dedupCache[i].t = -1
	}

	return nil
}

func hashSeries(hashBuf []byte, inputSeries labels.Labels) uint64 {
	hashBuf = hashBuf[:0]
	hash := xxhash.Sum64(inputSeries.Bytes(hashBuf))
	return hash
}
