package util

import (
	"errors"
)

type ExcludeLabels map[string]metrics
type metrics []Metric
type Metric struct {
	MetricName string `yaml:"metric_name"`
	LabelName  string `yaml:"label_name"`
}

var (
	errLabelNameMissing  = errors.New("invalid Label Name: Label Name in Exclude Labels Configuration is missing")
	errMetricNameMissing = errors.New("invalid Metric Name: Metric Name in Exclude Labels Configuration is missing")
)

func (excfg ExcludeLabels) Validate() error {
	for _, metrics := range excfg {
		for _, metric := range metrics {
			if metric.LabelName == "" {
				return errLabelNameMissing
			}
			if metric.MetricName == "" {
				return errMetricNameMissing
			}
		}
	}
	return nil
}

func (excfg ExcludeLabels) SkipLabel(lbName string, metricName string, userID string) bool {
	exlbls := excfg[userID]
	for _, lbl := range exlbls {
		if lbl.MetricName == metricName && lbl.LabelName == lbName {
			return true
		}
	}
	return false
}
