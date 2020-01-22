package util

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type SingleValueWithLabels struct {
	Value       float64
	LabelValues []string
}

type SingleValueWithLabelsMap map[string]SingleValueWithLabels

func (m SingleValueWithLabelsMap) aggregateFn(labelsKey string, labelValues []string, value float64) {
	r := m[labelsKey]
	if r.LabelValues == nil {
		r.LabelValues = labelValues
	}

	r.Value += value
	m[labelsKey] = r
}

func (m SingleValueWithLabelsMap) WriteToMetricChannel(out chan<- prometheus.Metric, desc *prometheus.Desc, valueType prometheus.ValueType) {
	for _, cr := range m {
		out <- prometheus.MustNewConstMetric(desc, valueType, cr.Value, cr.LabelValues...)
	}
}

// MetricFamilyMap is a map of metric names to their family (metrics with same name, but different labels)
// Keeping map of metric name to its family makes it easier to do searches later.
type MetricFamilyMap map[string]*dto.MetricFamily

// NewMetricFamilyMap sorts output from Gatherer.Gather method into a map.
// Gatherer.Gather specifies that there metric families are uniquely named, and we use that fact here.
// If they are not, this method returns error.
func NewMetricFamilyMap(metrics []*dto.MetricFamily) (MetricFamilyMap, error) {
	perMetricName := MetricFamilyMap{}

	for _, m := range metrics {
		name := m.GetName()
		// these errors should never happen when passing Gatherer.Gather() output.
		if name == "" {
			return nil, errors.New("empty name for metric family")
		}
		if perMetricName[name] != nil {
			return nil, fmt.Errorf("non-unique name for metric family: %q", name)
		}

		perMetricName[name] = m
	}

	return perMetricName, nil
}

func (mfm MetricFamilyMap) SumCounters(name string) float64 {
	return sum(mfm[name], counterValue)
}

func (mfm MetricFamilyMap) SumCountersWithLabels(name string, labelNames ...string) SingleValueWithLabelsMap {
	result := SingleValueWithLabelsMap{}
	mfm.sumOfSingleValuesWithLabels(name, labelNames, counterValue, result.aggregateFn)
	return result
}

func (mfm MetricFamilyMap) SumGauges(name string) float64 {
	return sum(mfm[name], gaugeValue)
}

func (mfm MetricFamilyMap) SumGaugesWithLabels(name string, labelNames ...string) map[string]SingleValueWithLabels {
	result := SingleValueWithLabelsMap{}
	mfm.sumOfSingleValuesWithLabels(name, labelNames, gaugeValue, result.aggregateFn)
	return result
}

func (mfm MetricFamilyMap) sumOfSingleValuesWithLabels(metric string, labelNames []string, extractFn func(*dto.Metric) float64, aggregateFn func(labelsKey string, labelValues []string, value float64)) {
	metricsPerLabelValue := getMetricsWithLabelNames(mfm[metric], labelNames)

	for key, mlv := range metricsPerLabelValue {
		for _, m := range mlv.metrics {
			val := extractFn(m)
			aggregateFn(key, mlv.labelValues, val)
		}
	}
}

// MetricFamiliesPerUser is a collection of metrics gathered via calling Gatherer.Gather() method on different
// gatherers, one per user.
type MetricFamiliesPerUser map[string]MetricFamilyMap

func BuildMetricFamiliesPerUserFromUserRegistries(regs map[string]*prometheus.Registry) MetricFamiliesPerUser {
	data := MetricFamiliesPerUser{}
	for userID, r := range regs {
		m, err := r.Gather()
		if err == nil {
			var mfm MetricFamilyMap = nil
			mfm, err = NewMetricFamilyMap(m)
			if err == nil {
				data[userID] = mfm
			}
		}

		if err != nil {
			level.Warn(Logger).Log("msg", "failed to gather metrics from registry", "user", userID, "err", err)
			continue
		}
	}
	return data
}

func (d MetricFamiliesPerUser) SendSumOfCounters(out chan<- prometheus.Metric, desc *prometheus.Desc, counter string) {
	result := float64(0)
	for _, perUser := range d {
		result += perUser.SumCounters(counter)
	}
	out <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, result)
}

func (d MetricFamiliesPerUser) SendSumOfCountersWithLabels(out chan<- prometheus.Metric, desc *prometheus.Desc, counter string, labelNames ...string) {
	d.sumOfSingleValuesWithLabels(counter, counterValue, labelNames).WriteToMetricChannel(out, desc, prometheus.CounterValue)
}

func (d MetricFamiliesPerUser) SendSumOfCountersPerUser(out chan<- prometheus.Metric, desc *prometheus.Desc, counter string) {
	for user, perMetric := range d {
		v := perMetric.SumCounters(counter)

		out <- prometheus.MustNewConstMetric(desc, prometheus.CounterValue, v, user)
	}
}

func (d MetricFamiliesPerUser) SendSumOfGauges(out chan<- prometheus.Metric, desc *prometheus.Desc, gauge string) {
	result := float64(0)
	for _, perMetric := range d {
		result += perMetric.SumGauges(gauge)
	}
	out <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, result)
}

func (d MetricFamiliesPerUser) SendSumOfGaugesWithLabels(out chan<- prometheus.Metric, desc *prometheus.Desc, gauge string, labelNames ...string) {
	d.sumOfSingleValuesWithLabels(gauge, gaugeValue, labelNames).WriteToMetricChannel(out, desc, prometheus.GaugeValue)
}

func (d MetricFamiliesPerUser) sumOfSingleValuesWithLabels(metric string, fn func(*dto.Metric) float64, labelNames []string) SingleValueWithLabelsMap {
	result := SingleValueWithLabelsMap{}
	for _, userMetrics := range d {
		userMetrics.sumOfSingleValuesWithLabels(metric, labelNames, fn, result.aggregateFn)
	}
	return result
}

func (d MetricFamiliesPerUser) SendSumOfSummaries(out chan<- prometheus.Metric, desc *prometheus.Desc, summaryName string) {
	var (
		sampleCount uint64
		sampleSum   float64
		quantiles   map[float64]float64
	)

	for _, userMetrics := range d {
		for _, m := range userMetrics[summaryName].GetMetric() {
			summary := m.GetSummary()
			sampleCount += summary.GetSampleCount()
			sampleSum += summary.GetSampleSum()
			quantiles = mergeSummaryQuantiles(quantiles, summary.GetQuantile())
		}
	}

	out <- prometheus.MustNewConstSummary(desc, sampleCount, sampleSum, quantiles)
}

func (d MetricFamiliesPerUser) SendSumOfSummariesWithLabels(out chan<- prometheus.Metric, desc *prometheus.Desc, summaryName string, labelNames ...string) {
	type summaryResult struct {
		sampleCount uint64
		sampleSum   float64
		quantiles   map[float64]float64
		labelValues []string
	}

	result := map[string]summaryResult{}

	for _, userMetrics := range d {
		metricsPerLabelValue := getMetricsWithLabelNames(userMetrics[summaryName], labelNames)

		for key, mwl := range metricsPerLabelValue {
			for _, m := range mwl.metrics {
				r := result[key]
				if r.labelValues == nil {
					r.labelValues = mwl.labelValues
				}

				summary := m.GetSummary()
				r.sampleCount += summary.GetSampleCount()
				r.sampleSum += summary.GetSampleSum()
				r.quantiles = mergeSummaryQuantiles(r.quantiles, summary.GetQuantile())

				result[key] = r
			}
		}
	}

	for _, sr := range result {
		out <- prometheus.MustNewConstSummary(desc, sr.sampleCount, sr.sampleSum, sr.quantiles, sr.labelValues...)
	}
}

func (d MetricFamiliesPerUser) SendSumOfHistograms(out chan<- prometheus.Metric, desc *prometheus.Desc, histogramName string) {
	var (
		sampleCount uint64
		sampleSum   float64
		buckets     map[float64]uint64
	)

	for _, userMetrics := range d {
		for _, m := range userMetrics[histogramName].GetMetric() {
			histo := m.GetHistogram()
			sampleCount += histo.GetSampleCount()
			sampleSum += histo.GetSampleSum()
			buckets = mergeHistogramBuckets(buckets, histo.GetBucket())
		}
	}

	out <- prometheus.MustNewConstHistogram(desc, sampleCount, sampleSum, buckets)
}

func mergeSummaryQuantiles(quantiles map[float64]float64, summaryQuantiles []*dto.Quantile) map[float64]float64 {
	if len(summaryQuantiles) == 0 {
		return quantiles
	}

	out := quantiles
	if out == nil {
		out = map[float64]float64{}
	}

	for _, q := range summaryQuantiles {
		// we assume that all summaries have same quantiles
		out[q.GetQuantile()] += q.GetValue()
	}
	return out
}

func mergeHistogramBuckets(buckets map[float64]uint64, histogramBuckets []*dto.Bucket) map[float64]uint64 {
	if len(histogramBuckets) == 0 {
		return buckets
	}

	out := buckets
	if out == nil {
		out = map[float64]uint64{}
	}

	for _, q := range histogramBuckets {
		// we assume that all histograms have same buckets
		out[q.GetUpperBound()] += q.GetCumulativeCount()
	}
	return out
}

type metricsWithLabels struct {
	labelValues []string
	metrics     []*dto.Metric
}

func getMetricsWithLabelNames(mf *dto.MetricFamily, labelNames []string) map[string]metricsWithLabels {
	result := map[string]metricsWithLabels{}

	for _, m := range mf.GetMetric() {
		lbls, include := getLabelValues(m, labelNames)
		if !include {
			continue
		}

		key := getLabelsString(lbls)
		r := result[key]
		if r.labelValues == nil {
			r.labelValues = lbls
		}
		r.metrics = append(r.metrics, m)
		result[key] = r
	}
	return result
}

func getLabelValues(m *dto.Metric, labelNames []string) ([]string, bool) {
	all := map[string]string{}
	for _, lp := range m.GetLabel() {
		all[lp.GetName()] = lp.GetValue()
	}

	result := make([]string, 0, len(labelNames))
	for _, ln := range labelNames {
		lv, ok := all[ln]
		if !ok {
			// required labels not found
			return nil, false
		}
		result = append(result, lv)
	}
	return result, true
}

func getLabelsString(labelValues []string) string {
	buf := bytes.Buffer{}
	for _, v := range labelValues {
		buf.WriteString(v)
		buf.WriteByte(0) // separator, not used in prometheus labels
	}
	return buf.String()
}

// sum returns sum of values from all metrics from same metric family (= series with the same metric name, but different labels)
// Supplied function extracts value.
func sum(mf *dto.MetricFamily, fn func(*dto.Metric) float64) float64 {
	result := float64(0)
	for _, m := range mf.GetMetric() {
		result += fn(m)
	}
	return result
}

// This works even if m is nil, m.Counter is nil or m.Counter.Value is nil (it returns 0 in those cases)
func counterValue(m *dto.Metric) float64 { return m.GetCounter().GetValue() }
func gaugeValue(m *dto.Metric) float64   { return m.GetGauge().GetValue() }
