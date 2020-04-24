package e2e

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

var (
	errTooManyVectorElements = "query returned vector with %d values"
	errNotScalarOrVector     = errors.New("not a scalar or vector with single value")
)

func evaluatePromQLWithTextMetrics(expr string, metrics string) (float64, error) {
	var tp expfmt.TextParser
	families, err := tp.TextToMetricFamilies(strings.NewReader(metrics))
	if err != nil {
		return 0, err
	}

	now := time.Now()

	samples := convertMetricFamiliesToSamples(int64(model.TimeFromUnixNano(now.UnixNano())), families)

	eng := promql.NewEngine(promql.EngineOpts{
		Logger:             nil,
		Reg:                nil,
		MaxSamples:         1e6,
		Timeout:            time.Hour, // time.Second,
		ActiveQueryTracker: nil,
	})

	queryable := samplesQueryable(samples)
	q, err := eng.NewInstantQuery(&queryable, expr, now)
	if err != nil {
		return 0, err
	}

	r := q.Exec(context.Background())
	if r.Err != nil {
		return 0, r.Err
	}

	switch s := r.Value.(type) {
	case promql.Scalar:
		return s.V, nil

	case promql.Vector:
		if len(s) == 0 {
			return 0, nil
		}

		if len(s) == 1 {
			return s[0].V, nil
		}

		return 0, fmt.Errorf(errTooManyVectorElements, len(s))
	}

	return 0, errNotScalarOrVector
}

type sample struct {
	labels map[string]string
	ts     int64
	val    float64
}

func convertMetricFamiliesToSamples(ts int64, mfs map[string]*dto.MetricFamily) []sample {
	result := []sample{}

	for _, mf := range mfs {
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			for _, m := range mf.GetMetric() {
				result = append(result, sample{
					labels: getLabels(mf.GetName(), m.GetLabel()),
					ts:     getTimestamp(ts, m.GetTimestampMs()),
					val:    m.GetCounter().GetValue(),
				})
			}

		case dto.MetricType_GAUGE:
			for _, m := range mf.GetMetric() {
				result = append(result, sample{
					labels: getLabels(mf.GetName(), m.GetLabel()),
					ts:     getTimestamp(ts, m.GetTimestampMs()),
					val:    m.GetGauge().GetValue(),
				})
			}

		case dto.MetricType_SUMMARY:
			for _, m := range mf.GetMetric() {
				result = append(result, sample{
					labels: getLabels(mf.GetName()+"_count", m.GetLabel()),
					ts:     getTimestamp(ts, m.GetTimestampMs()),
					val:    float64(m.GetSummary().GetSampleCount()),
				})

				result = append(result, sample{
					labels: getLabels(mf.GetName()+"_sum", m.GetLabel()),
					ts:     getTimestamp(ts, m.GetTimestampMs()),
					val:    m.GetSummary().GetSampleSum(),
				})
			}

		case dto.MetricType_HISTOGRAM:
			for _, m := range mf.GetMetric() {
				for _, b := range m.GetHistogram().GetBucket() {
					result = append(result, sample{
						labels: getLabelsExt(mf.GetName()+"_bucket", m.GetLabel(), true, b.GetUpperBound()),
						ts:     getTimestamp(ts, m.GetTimestampMs()),
						val:    float64(b.GetCumulativeCount()),
					})
				}

				result = append(result, sample{
					labels: getLabels(mf.GetName()+"_count", m.GetLabel()),
					ts:     getTimestamp(ts, m.GetTimestampMs()),
					val:    float64(m.GetHistogram().GetSampleCount()),
				})

				result = append(result, sample{
					labels: getLabels(mf.GetName()+"_sum", m.GetLabel()),
					ts:     getTimestamp(ts, m.GetTimestampMs()),
					val:    m.GetHistogram().GetSampleSum(),
				})
			}
		}
	}

	return result
}

func getTimestamp(ts int64, mts int64) int64 {
	if mts != 0 {
		return mts
	}
	return ts
}

func getLabels(name string, pairs []*dto.LabelPair) map[string]string {
	return getLabelsExt(name, pairs, false, 0)
}

func getLabelsExt(name string, pairs []*dto.LabelPair, bucket bool, upperBound float64) map[string]string {
	result := map[string]string{}
	result[labels.MetricName] = name
	if bucket {
		result[labels.BucketLabel] = fmt.Sprintf("%g", upperBound)
	}

	for _, p := range pairs {
		result[p.GetName()] = p.GetValue()
	}
	return result
}

// implements Queryable and Querier
type samplesQueryable []sample

func (sq *samplesQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return sq, nil
}

func (sq samplesQueryable) SelectSorted(params *storage.SelectParams, matcher ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	var series []storage.Series

	for _, s := range sq {
		if s.matches(matcher...) {
			series = append(series, s)
		}
	}

	return &seriesSet{series: series}, nil, nil
}

func (sq samplesQueryable) Select(params *storage.SelectParams, matcher ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	return sq.SelectSorted(params, matcher...)
}

func (sq *samplesQueryable) LabelValues(name string) ([]string, storage.Warnings, error) {
	panic("implement me")
}

func (sq *samplesQueryable) LabelNames() ([]string, storage.Warnings, error) {
	panic("implement me")
}

func (sq *samplesQueryable) Close() error {
	return nil
}

// seriesSet implements storage.SeriesSet.
type seriesSet struct {
	cur    int
	series []storage.Series
}

func (c *seriesSet) Next() bool {
	c.cur++
	return c.cur-1 < len(c.series)
}

func (c *seriesSet) At() storage.Series {
	return c.series[c.cur-1]
}

func (c *seriesSet) Err() error {
	return nil
}

// sample implements storage.Series
func (c sample) Labels() labels.Labels {
	b := labels.NewBuilder(nil)
	for k, v := range c.labels {
		b.Set(k, v)
	}
	return b.Labels()
}

func (c sample) Iterator() storage.SeriesIterator {
	return newSampleIterator(c.ts, c.val)
}

func (c sample) matches(matcher ...*labels.Matcher) bool {
	for _, m := range matcher {
		if !m.Matches(c.labels[m.Name]) {
			return false
		}
	}

	return true
}

// concreteSeriesIterator implements storage.SeriesIterator.
type singleSampleIterator struct {
	nextCalled bool
	ts         int64
	val        float64
}

func newSampleIterator(ts int64, val float64) storage.SeriesIterator {
	return &singleSampleIterator{
		ts:  ts,
		val: val,
	}
}

// Seek implements storage.SeriesIterator.
func (c *singleSampleIterator) Seek(t int64) bool {
	if t <= c.ts {
		return true
	}
	return false
}

// At implements storage.SeriesIterator.
func (c *singleSampleIterator) At() (t int64, v float64) {
	return c.ts, c.val
}

// Next implements storage.SeriesIterator.
func (c *singleSampleIterator) Next() bool {
	if c.nextCalled {
		return false
	}
	c.nextCalled = true
	return true
}

// Err implements storage.SeriesIterator.
func (c *singleSampleIterator) Err() error {
	return nil
}
