// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/execution/telemetry"
	"github.com/thanos-io/promql-engine/logicalplan"
	"github.com/thanos-io/promql-engine/query"

	"github.com/efficientgo/core/errors"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

type relabelOperator struct {
	telemetry.OperatorTelemetry

	next     model.VectorOperator
	funcExpr *logicalplan.FunctionCall
	once     sync.Once
	series   []labels.Labels
}

func newRelabelOperator(
	next model.VectorOperator,
	funcExpr *logicalplan.FunctionCall,
	opts *query.Options,
) *relabelOperator {
	oper := &relabelOperator{
		next:     next,
		funcExpr: funcExpr,
	}
	oper.OperatorTelemetry = telemetry.NewTelemetry(oper, opts)

	return oper
}

func (o *relabelOperator) String() string {
	return "[relabel]"
}

func (o *relabelOperator) Explain() (next []model.VectorOperator) {
	return []model.VectorOperator{o.next}
}

func (o *relabelOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	return o.series, err
}

func (o *relabelOperator) GetPool() *model.VectorPool {
	return o.next.GetPool()
}

func (o *relabelOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	defer func() { o.AddExecutionTimeTaken(time.Since(start)) }()

	return o.next.Next(ctx)
}

func (o *relabelOperator) loadSeries(ctx context.Context) (err error) {
	series, err := o.next.Series(ctx)
	if err != nil {
		return err
	}
	o.series = make([]labels.Labels, len(series))

	switch o.funcExpr.Func.Name {
	case "label_join":
		err = o.loadSeriesForLabelJoin(series)
	case "label_replace":
		err = o.loadSeriesForLabelReplace(series)
	default:
		err = errors.Newf("invalid function name for relabel operator: %s", o.funcExpr.Func.Name)
	}
	return err
}

func (o *relabelOperator) loadSeriesForLabelJoin(series []labels.Labels) error {
	labelJoinDst, err := logicalplan.UnwrapString(o.funcExpr.Args[1])
	if err != nil {
		return errors.Wrap(err, "unable to unwrap string argument")
	}
	if !prommodel.LabelName(labelJoinDst).IsValid() {
		return errors.Newf("invalid destination label name in label_join: %s", labelJoinDst)
	}

	var labelJoinSrcLabels []string
	labelJoinSep, err := logicalplan.UnwrapString(o.funcExpr.Args[2])
	if err != nil {
		return errors.Wrap(err, "unable to unwrap string argument")
	}
	for j := 3; j < len(o.funcExpr.Args); j++ {
		srcLabel, err := logicalplan.UnwrapString(o.funcExpr.Args[j])
		if err != nil {
			return errors.Wrap(err, "unable to unwrap string argument")
		}
		labelJoinSrcLabels = append(labelJoinSrcLabels, srcLabel)
	}
	for i, s := range series {
		lbls := s
		srcVals := make([]string, len(labelJoinSrcLabels))

		for j, src := range labelJoinSrcLabels {
			srcVals[j] = lbls.Get(src)
		}
		lb := labels.NewBuilder(lbls)
		if strval := strings.Join(srcVals, labelJoinSep); strval == "" {
			lb.Del(labelJoinDst)
		} else {
			lb.Set(labelJoinDst, strval)
		}
		o.series[i] = lb.Labels()
	}
	return nil
}
func (o *relabelOperator) loadSeriesForLabelReplace(series []labels.Labels) error {
	labelReplaceDst, err := logicalplan.UnwrapString(o.funcExpr.Args[1])
	if err != nil {
		return errors.Wrap(err, "unable to unwrap string argument")
	}
	if !prommodel.LabelName(labelReplaceDst).IsValid() {
		return errors.Newf("invalid destination label name in label_replace: %s", labelReplaceDst)
	}
	labelReplaceRepl, err := logicalplan.UnwrapString(o.funcExpr.Args[2])
	if err != nil {
		return errors.Wrap(err, "unable to unwrap string argument")
	}
	labelReplaceSrc, err := logicalplan.UnwrapString(o.funcExpr.Args[3])
	if err != nil {
		return errors.Wrap(err, "unable to unwrap string argument")
	}
	labelReplaceRegexVal, err := logicalplan.UnwrapString(o.funcExpr.Args[4])
	if err != nil {
		return errors.Wrap(err, "unable to unwrap string argument")
	}
	labelReplaceRegex, err := regexp.Compile("^(?:" + labelReplaceRegexVal + ")$")
	if err != nil {
		return errors.Newf("invalid regular expression in label_replace(): %s", labelReplaceRegexVal)
	}

	if !prommodel.LabelNameRE.MatchString(labelReplaceDst) {
		return errors.Newf("invalid destination label name in label_replace(): %s", labelReplaceDst)
	}

	for i, s := range series {
		lbls := s

		srcVal := lbls.Get(labelReplaceSrc)
		matches := labelReplaceRegex.FindStringSubmatchIndex(srcVal)
		if len(matches) == 0 {
			o.series[i] = lbls
			continue
		}
		res := labelReplaceRegex.ExpandString([]byte{}, labelReplaceRepl, srcVal, matches)
		lb := labels.NewBuilder(lbls).Del(labelReplaceDst)
		if len(res) > 0 {
			lb.Set(labelReplaceDst, string(res))
		}
		o.series[i] = lb.Labels()
	}

	return nil
}
