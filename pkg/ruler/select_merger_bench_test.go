package ruler

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
)

// BenchmarkSelectMerger simulates 114 rules (38 metrics × 3 deploy_color variants)
// and compares query count with vs without merging.
func BenchmarkSelectMerger(b *testing.B) {
	const (
		numMetrics = 38
		numColors  = 3
		seriesPer  = 50
	)
	colors := []string{"blue", "green", "canary"}

	// Build 114 rules: 38 metrics × 3 deploy_color variants.
	// One variant uses =~".*" (superset) to enable merging.
	rls := make([]rules.Rule, 0, numMetrics*numColors)
	for i := range numMetrics {
		for ci, c := range colors {
			var expr string
			if ci == numColors-1 {
				// Last color variant uses regex superset — enables merging.
				expr = fmt.Sprintf(`sum(metric_%d{deploy_color=~".*",job="svc"})`, i)
			} else {
				expr = fmt.Sprintf(`sum(metric_%d{deploy_color="%s",job="svc"})`, i, c)
			}
			_ = c
			e, err := parser.ParseExpr(expr)
			if err != nil {
				b.Fatal(err)
			}
			rls = append(rls, &fakeRule{expr: e})
		}
	}

	// Mock QueryFunc: returns seriesPer samples with varying deploy_color.
	mockQueryFunc := func(_ context.Context, qs string, _ time.Time) (promql.Vector, error) {
		vec := make(promql.Vector, seriesPer)
		for i := range vec {
			vec[i] = promql.Sample{
				Metric: labels.FromStrings(
					"__name__", "metric_0",
					"deploy_color", colors[i%numColors],
					"job", "svc",
					"instance", fmt.Sprintf("host-%d", i),
				),
				T: 0,
				F: float64(i),
			}
		}
		return vec, nil
	}

	ts := time.Now()
	ctx := context.Background()

	b.Run("without_merging", func(b *testing.B) {
		var calls atomic.Int64
		qf := func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
			calls.Add(1)
			return mockQueryFunc(ctx, qs, t)
		}
		for b.Loop() {
			calls.Store(0)
			for _, r := range rls {
				_, err := qf(ctx, r.Query().String(), ts)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(float64(calls.Load()), "queries/op")
	})

	b.Run("with_merging", func(b *testing.B) {
		plan := planMergedSelects(rls, 2)
		var calls atomic.Int64
		qf := selectMergerQueryFunc(func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
			calls.Add(1)
			return mockQueryFunc(ctx, qs, t)
		})
		for b.Loop() {
			calls.Store(0)
			evalCtx := withSelectMergerPlan(ctx, plan)
			for _, r := range rls {
				_, err := qf(evalCtx, r.Query().String(), ts)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
		b.ReportMetric(float64(calls.Load()), "queries/op")
	})
}
