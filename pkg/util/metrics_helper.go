package util

import (
	"bytes"
	"errors"
	"fmt"

	dto "github.com/prometheus/client_model/go"
)

// MetricFamiliesPerUser is a collection of metrics gathered via calling Gatherer.Gather() method on different
// gatherers, one per user.
// First key = userID, second key = metric name.
// Value = slice of gathered values with the same metric name.
type MetricFamiliesPerUser map[string]map[string]*dto.MetricFamily

func NewMetricFamiliersPerUser() MetricFamiliesPerUser {
	return MetricFamiliesPerUser{}
}

// AddGatheredDataForUser adds user-specific output of Gatherer.Gather method.
// Gatherer.Gather specifies that there metric families are uniquely named, and we use that fact here.
// If they are not, this method returns error.
func (d MetricFamiliesPerUser) AddGatheredDataForUser(userID string, metrics []*dto.MetricFamily) error {
	// first, create new map which maps metric names to a slice of MetricFamily instances.
	// That makes it easier to do searches later.
	perMetricName := map[string]*dto.MetricFamily{}

	for _, m := range metrics {
		name := m.GetName()
		// these errors should never happen when passing Gatherer.Gather() output.
		if name == "" {
			return errors.New("empty name for metric family")
		}
		if perMetricName[name] != nil {
			return fmt.Errorf("non-unique name for metric family: %q", name)
		}

		perMetricName[name] = m
	}

	d[userID] = perMetricName
	return nil
}

// SumCountersAcrossAllUsers returns sum(counter).
func (d MetricFamiliesPerUser) SumCountersAcrossAllUsers(counter string) float64 {
	result := float64(0)
	for _, perMetric := range d {
		result += sum(perMetric[counter], counterValue)
	}
	return result
}

// SumCountersPerUser returns sum(counter) by (userID), where userID will be the map key.
func (d MetricFamiliesPerUser) SumCountersPerUser(counter string) map[string]float64 {
	result := map[string]float64{}
	for user, perMetric := range d {
		v := sum(perMetric[counter], counterValue)
		result[user] = v
	}
	return result
}

// SumCountersAcrossAllUsers returns sum(counter).
func (d MetricFamiliesPerUser) SumGaugesAcrossAllUsers(gauge string) float64 {
	result := float64(0)
	for _, perMetric := range d {
		result += sum(perMetric[gauge], gaugeValue)
	}
	return result
}

type SummaryResult struct {
	SampleCount uint64
	SampleSum   float64
	Quantiles   map[float64]float64
	LabelValues []string
}

func (d MetricFamiliesPerUser) SummariersAcrossAllUsers(metricName string, labelNames ...string) []SummaryResult {
	result := map[string]SummaryResult{}

	for _, perMetric := range d { // for each user
		mf := perMetric[metricName]
		for _, m := range mf.GetMetric() {
			lbls, include := getLabelValues(m, labelNames)
			if !include {
				continue
			}

			key := getLabelsString(lbls)
			r := result[key]
			if r.LabelValues == nil {
				r.LabelValues = lbls
			}
			summary := m.GetSummary()
			r.SampleCount += summary.GetSampleCount()
			r.SampleSum += summary.GetSampleSum()
			r.Quantiles = mergeSummaryQuantiles(r.Quantiles, summary.GetQuantile())

			result[key] = r
		}
	}

	out := make([]SummaryResult, 0, len(result))
	for _, sr := range result {
		out = append(out, sr)
	}
	return out
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

func getLabelValues(m *dto.Metric, labelNames []string) ([]string, bool) {
	if len(labelNames) == 0 {
		return nil, true
	}

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
	if len(labelValues) == 0 {
		return ""
	}

	buf := bytes.Buffer{}
	for _, v := range labelValues {
		buf.WriteString(v)
		buf.WriteByte(0) // separator, not used in prometheus labels
	}
	return buf.String()
}

func sum(mf *dto.MetricFamily, fn func(*dto.Metric) float64) float64 {
	result := float64(0)
	for _, m := range mf.Metric {
		result += fn(m)
	}
	return result
}

// This works even if m is nil, m.Counter is nil or m.Counter.Value is nil (it returns 0 in those cases)
func counterValue(m *dto.Metric) float64 { return m.GetCounter().GetValue() }
func gaugeValue(m *dto.Metric) float64   { return m.GetGauge().GetValue() }
