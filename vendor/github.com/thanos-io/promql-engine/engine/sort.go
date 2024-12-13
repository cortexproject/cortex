// Copyright (c) The Thanos Community Authors.
// Licensed under the Apache License 2.0.

package engine

import (
	"math"

	"github.com/facette/natsort"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

type sortOrder bool

const (
	sortOrderAsc  sortOrder = false
	sortOrderDesc sortOrder = true
)

type resultSorter interface {
	comparer(samples *promql.Vector) func(i, j int) bool
}

type sortFuncResultSort struct {
	sortOrder sortOrder
}

type sortByLabelFuncResult struct {
	sortingLabels []string

	sortOrder sortOrder
}

type aggregateResultSort struct {
	sortingLabels []string
	groupBy       bool

	sortOrder sortOrder
}

type noSortResultSort struct {
}

func extractSortingLabels(f *parser.Call) []string {
	args := f.Args[1:]

	res := make([]string, 0)
	for i := range args {
		res = append(res, args[i].(*parser.StringLiteral).Val)
	}
	return res
}

func newResultSort(expr parser.Expr) resultSorter {
	switch texpr := expr.(type) {
	case *parser.Call:
		switch texpr.Func.Name {
		case "sort":
			return sortFuncResultSort{sortOrder: sortOrderAsc}
		case "sort_desc":
			return sortFuncResultSort{sortOrder: sortOrderDesc}
		case "sort_by_label":
			return sortByLabelFuncResult{sortOrder: sortOrderAsc, sortingLabels: extractSortingLabels(texpr)}
		case "sort_by_label_desc":
			return sortByLabelFuncResult{sortOrder: sortOrderDesc, sortingLabels: extractSortingLabels(texpr)}
		}
	case *parser.AggregateExpr:
		switch texpr.Op {
		case parser.TOPK:
			return aggregateResultSort{
				sortingLabels: texpr.Grouping,
				sortOrder:     sortOrderDesc,
				groupBy:       !texpr.Without,
			}
		case parser.BOTTOMK:
			return aggregateResultSort{
				sortingLabels: texpr.Grouping,
				sortOrder:     sortOrderAsc,
				groupBy:       !texpr.Without,
			}
		}
	}
	return noSortResultSort{}
}

func (s noSortResultSort) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i, j int) bool { return i < j }
}

func valueCompare(order sortOrder, l, r float64) bool {
	if math.IsNaN(r) {
		return true
	}
	if order == sortOrderAsc {
		return l < r
	}
	return l > r
}

func (s sortFuncResultSort) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i, j int) bool {
		return valueCompare(s.sortOrder, (*samples)[i].F, (*samples)[j].F)
	}
}

func (s sortByLabelFuncResult) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i, j int) bool {
		iLb := labels.NewBuilder((*samples)[i].Metric)
		jLb := labels.NewBuilder((*samples)[j].Metric)

		for _, label := range s.sortingLabels {
			lv1 := iLb.Get(label)
			lv2 := jLb.Get(label)

			if lv1 == lv2 {
				continue
			}
			if natsort.Compare(lv1, lv2) {
				return s.sortOrder == sortOrderAsc
			} else {
				return s.sortOrder == sortOrderDesc
			}
		}
		return valueCompare(s.sortOrder, (*samples)[i].F, (*samples)[j].F)
	}
}

func (s aggregateResultSort) comparer(samples *promql.Vector) func(i, j int) bool {
	return func(i int, j int) bool {
		var iLbls labels.Labels
		var jLbls labels.Labels
		iLb := labels.NewBuilder((*samples)[i].Metric)
		jLb := labels.NewBuilder((*samples)[j].Metric)
		if s.groupBy {
			iLbls = iLb.Keep(s.sortingLabels...).Labels()
			jLbls = jLb.Keep(s.sortingLabels...).Labels()
		} else {
			iLbls = iLb.Del(s.sortingLabels...).Labels()
			jLbls = jLb.Del(s.sortingLabels...).Labels()
		}

		lblsCmp := labels.Compare(iLbls, jLbls)
		if lblsCmp != 0 {
			return lblsCmp < 0
		}
		return valueCompare(s.sortOrder, (*samples)[i].F, (*samples)[j].F)
	}
}
