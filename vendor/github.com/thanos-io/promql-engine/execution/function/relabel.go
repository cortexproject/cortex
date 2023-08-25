// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package function

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/efficientgo/core/errors"
	prommodel "github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/thanos-io/promql-engine/execution/model"
	"github.com/thanos-io/promql-engine/parser"
)

type relabelFunctionOperator struct {
	next     model.VectorOperator
	funcExpr *parser.Call
	once     sync.Once
	series   []labels.Labels
	model.OperatorTelemetry
}

func (o *relabelFunctionOperator) Analyze() (model.OperatorTelemetry, []model.ObservableVectorOperator) {
	o.SetName("[*relabelFunctionOperator]")
	next := make([]model.ObservableVectorOperator, 0, 1)
	if obsnext, ok := o.next.(model.ObservableVectorOperator); ok {
		next = append(next, obsnext)
	}
	return o, next
}

func (o *relabelFunctionOperator) Explain() (me string, next []model.VectorOperator) {
	return "[*relabelFunctionOperator]", []model.VectorOperator{}
}

func (o *relabelFunctionOperator) Series(ctx context.Context) ([]labels.Labels, error) {
	var err error
	o.once.Do(func() { err = o.loadSeries(ctx) })
	return o.series, err
}

func (o *relabelFunctionOperator) GetPool() *model.VectorPool {
	return o.next.GetPool()
}

func (o *relabelFunctionOperator) Next(ctx context.Context) ([]model.StepVector, error) {
	start := time.Now()
	next, err := o.next.Next(ctx)
	o.AddExecutionTimeTaken(time.Since(start))
	return next, err
}

func (o *relabelFunctionOperator) loadSeries(ctx context.Context) (err error) {
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

func (o *relabelFunctionOperator) loadSeriesForLabelJoin(series []labels.Labels) error {
	labelJoinDst := o.funcExpr.Args[1].(*parser.StringLiteral).Val
	if !prommodel.LabelName(labelJoinDst).IsValid() {
		return errors.Newf("invalid destination label name in label_join: %s", labelJoinDst)
	}

	var labelJoinSrcLabels []string
	labelJoinSep := o.funcExpr.Args[2].(*parser.StringLiteral).Val
	for j := 3; j < len(o.funcExpr.Args); j++ {
		labelJoinSrcLabels = append(labelJoinSrcLabels, o.funcExpr.Args[j].(*parser.StringLiteral).Val)
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
func (o *relabelFunctionOperator) loadSeriesForLabelReplace(series []labels.Labels) error {
	labelReplaceDst := o.funcExpr.Args[1].(*parser.StringLiteral).Val
	if !prommodel.LabelName(labelReplaceDst).IsValid() {
		return errors.Newf("invalid destination label name in label_replace: %s", labelReplaceDst)
	}
	labelReplaceRepl := o.funcExpr.Args[2].(*parser.StringLiteral).Val
	labelReplaceSrc := o.funcExpr.Args[3].(*parser.StringLiteral).Val
	labelReplaceRegexVal := o.funcExpr.Args[4].(*parser.StringLiteral).Val
	labelReplaceRegex, err := regexp.Compile("^(?:" + labelReplaceRegexVal + ")$")
	if err != nil {
		return errors.Newf("invalid regular expression in label_replace(): %s", labelReplaceRegexVal)
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
