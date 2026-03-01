// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package unary

import (
	"context"
	"sync"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/extlabels"
	"github.com/thanos-io/promql-engine/query"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"gonum.org/v1/gonum/floats"
)

type unaryNegation struct {
	next model.VectorOperator
	once sync.Once

	series []labels.Labels
}

func NewUnaryNegation(next model.VectorOperator, opts *query.Options) (model.VectorOperator, error) {
	u := &unaryNegation{
		next: next,
	}
	return telemetry.NewOperator(telemetry.NewTelemetry(u, opts), u), nil
}

func (u *unaryNegation) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{u.next}
}

func (u *unaryNegation) String() string {
	return "[unaryNegation]"
}

func (u *unaryNegation) Series(ctx context.Context) ([]labels.Labels, error) {
	if err := u.loadSeries(ctx); err != nil {
		return nil, err
	}
	return u.series, nil
}

func (u *unaryNegation) loadSeries(ctx context.Context) error {
	var err error
	u.once.Do(func() {
		var series []labels.Labels
		series, err = u.next.Series(ctx)
		if err != nil {
			return
		}
		u.series = make([]labels.Labels, len(series))
		var b labels.ScratchBuilder
		for i := range series {
			lbls := extlabels.DropReserved(series[i], b)
			u.series[i] = lbls
		}
	})
	return err
}

func (u *unaryNegation) Next(ctx context.Context, buf []model.StepVector) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	n, err := u.next.Next(ctx, buf)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, nil
	}
	for i := range n {
		floats.Scale(-1, buf[i].Samples)
		negateHistograms(buf[i].Histograms)
	}
	return n, nil
}

func negateHistograms(hists []*histogram.FloatHistogram) {
	for i := range hists {
		hists[i] = hists[i].Copy().Mul(-1)
	}
}
