// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/labels"
)

type timestampOperator struct {
	next model.VectorOperator

	series []labels.Labels
	once   sync.Once
}

func newTimestampOperator(next model.VectorOperator, opts *query.Options) model.VectorOperator {
	oper := &timestampOperator{
		next: next,
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(oper, opts), oper)
}

func (o *timestampOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *timestampOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := o.loadSeries(ctx); err != nil {
		return nil, err
	}
	return o.series, nil
}

func (o *timestampOperator) String() string {
	return "[timestamp]"
}

func (o *timestampOperator) loadSeries(ctx context.Context) error {
	var err error
	o.once.Do(func() {
		series, loadErr := o.next.Series(ctx)
		if loadErr != nil {
			err = loadErr
			return
		}
		o.series = make([]labels.Labels, len(series))

		var b labels.ScratchBuilder
		for i, s := range series {
			lbls := extlabels.DropReserved(s, b)
			o.series[i] = lbls
		}
	})

	return err
}

func (o *timestampOperator) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := o.next.Next(ctx, buf)
	if err != nil {
		return 0, err
	}
	for i := range n {
		vector := &buf[i]
		for j := range vector.Samples {
			vector.Samples[j] = float64(vector.T / 1000)
		}
	}
	return n, nil
}
