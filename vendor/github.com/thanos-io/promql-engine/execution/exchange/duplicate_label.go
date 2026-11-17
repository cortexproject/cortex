// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package exchange

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	"github.com/prometheus/prometheus/model/labels"
)

type duplicateLabelCheckOperator struct {
	once    sync.Once
	initErr error
	next    model.VectorOperator
	series  []labels.Labels

	// Equal labelsets share an output ID. A nil mapping is the no-alias fast path.
	outputIDs []uint64
	// Each output ID has one occupancy mask shared by floats and histograms.
	seen []uint64

	step      int64
	origin    int64
	hasOrigin bool
}

func NewDuplicateLabelCheck(next model.VectorOperator, opts *query.Options) model.VectorOperator {
	oper := &duplicateLabelCheckOperator{
		next: next,
		step: max(opts.Step.Milliseconds(), 1),
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (d *duplicateLabelCheckOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := d.init(ctx); err != nil {
		return 0, err
	}

	n, err := d.next.Next(ctx, buf)
	if err != nil || n == 0 || d.outputIDs == nil {
		return n, err
	}

	first := buf[0].T
	if d.hasOrigin {
		delta := first - d.origin
		if delta < 0 || delta%d.step != 0 {
			return 0, errors.New("duplicate label check: batch starts must be nondecreasing and step-aligned")
		}
		// Series chunks may cover the same timestamps in successive calls.
		// Only advance occupancy when the batch start advances, preserving any
		// overlap between batches of different lengths.
		if shift := uint64(delta / d.step); shift != 0 {
			for i := range d.seen {
				d.seen[i] >>= shift
			}
		}
	}
	d.origin = first
	d.hasOrigin = true

	for i := range n {
		offset := buf[i].T - first
		if offset < 0 || offset%d.step != 0 || offset/d.step >= 64 {
			return 0, errors.New("duplicate label check: samples must fit in a step-aligned 64-step window")
		}
		bit := uint64(1) << uint64(offset/d.step)
		for _, ids := range [][]uint64{buf[i].SampleIDs, buf[i].HistogramIDs} {
			for j, inputID := range ids {
				outputID := d.outputIDs[inputID]
				if d.seen[outputID]&bit != 0 {
					return 0, extlabels.ErrDuplicateLabelSet
				}
				d.seen[outputID] |= bit
				ids[j] = outputID
			}
		}
	}
	return n, nil
}

func (d *duplicateLabelCheckOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := d.init(ctx); err != nil {
		return nil, err
	}
	return d.series, nil
}

func (d *duplicateLabelCheckOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{d.next}
}

func (d *duplicateLabelCheckOperator) String() string {
	return "[duplicateLabelCheck]"
}

func (d *duplicateLabelCheckOperator) init(ctx context.Context) error {
	d.once.Do(func() {
		input, err := d.next.Series(ctx)
		if err != nil {
			d.initErr = err
			return
		}
		d.series = input
		if len(input) < 2 {
			return
		}

		outputIDs := make(map[uint64]uint64, len(input))
		for i, metric := range input {
			hash := metric.Hash()
			outputID, exists := outputIDs[hash]
			if !exists {
				outputID = uint64(i)
				if d.outputIDs != nil {
					outputID = uint64(len(d.series))
					d.series = append(d.series, metric)
				}
				outputIDs[hash] = outputID
			} else if d.outputIDs == nil {
				// Allocate remapping state only after finding the first alias.
				// Earlier input IDs are already their canonical output IDs.
				d.outputIDs = make([]uint64, len(input))
				for j := range i {
					d.outputIDs[j] = uint64(j)
				}
				d.series = make([]labels.Labels, i, len(input))
				copy(d.series, input[:i])
			}
			if d.outputIDs != nil {
				d.outputIDs[i] = outputID
			}
		}
		if d.outputIDs != nil {
			d.seen = make([]uint64, len(d.series))
		}
	})
	return d.initErr
}
