package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/cortexproject/cortex/integration/e2ecortex"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func testMetadataQueriesWithBlocksStorage(
	t *testing.T,
	c *e2ecortex.Client,
	lastSeriesInStorage prompb.TimeSeries,
	lastSeriesInIngesterBlocks prompb.TimeSeries,
	firstSeriesInIngesterHead prompb.TimeSeries,
	blockRangePeriod time.Duration,
) {
	var (
		lastSeriesInIngesterBlocksName = getMetricName(lastSeriesInIngesterBlocks.Labels)
		firstSeriesInIngesterHeadName  = getMetricName(firstSeriesInIngesterHead.Labels)
		lastSeriesInStorageName        = getMetricName(lastSeriesInStorage.Labels)

		lastSeriesInStorageTs        = util.TimeFromMillis(lastSeriesInStorage.Samples[0].Timestamp)
		lastSeriesInIngesterBlocksTs = util.TimeFromMillis(lastSeriesInIngesterBlocks.Samples[0].Timestamp)
		firstSeriesInIngesterHeadTs  = util.TimeFromMillis(firstSeriesInIngesterHead.Samples[0].Timestamp)
	)

	type seriesTest struct {
		lookup string
		ok     bool
		resp   []prompb.Label
	}
	type labelValuesTest struct {
		label   string
		matches []string
		resp    []string
	}

	testCases := map[string]struct {
		from time.Time
		to   time.Time

		seriesTests []seriesTest

		labelValuesTests []labelValuesTest

		labelNames []string
	}{
		"query metadata entirely inside the head range": {
			from: firstSeriesInIngesterHeadTs,
			to:   firstSeriesInIngesterHeadTs.Add(blockRangePeriod),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     false,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     false,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{firstSeriesInIngesterHeadName},
					matches: []string{firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{},
					matches: []string{lastSeriesInStorageName},
				},
			},
			labelNames: []string{labels.MetricName, firstSeriesInIngesterHeadName},
		},
		"query metadata entirely inside the ingester range but outside the head range": {
			from: lastSeriesInIngesterBlocksTs,
			to:   lastSeriesInIngesterBlocksTs.Add(blockRangePeriod / 2),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     false,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     true,
					resp:   lastSeriesInIngesterBlocks.Labels,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     false,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{lastSeriesInIngesterBlocksName},
				},

				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInIngesterBlocksName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{},
					matches: []string{firstSeriesInIngesterHeadName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInIngesterBlocksName},
		},
		"query metadata partially inside the ingester range": {
			from: lastSeriesInStorageTs.Add(-blockRangePeriod),
			to:   firstSeriesInIngesterHeadTs.Add(blockRangePeriod),
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     true,
					resp:   lastSeriesInIngesterBlocks.Labels,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     true,
					resp:   lastSeriesInStorage.Labels,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName, firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInStorageName},
					matches: []string{lastSeriesInStorageName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInIngesterBlocksName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName},
					matches: []string{lastSeriesInStorageName, lastSeriesInIngesterBlocksName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInStorageName, lastSeriesInIngesterBlocksName, firstSeriesInIngesterHeadName},
		},
		"query metadata entirely outside the ingester range should return the head data as well": {
			from: lastSeriesInStorageTs.Add(-2 * blockRangePeriod),
			to:   lastSeriesInStorageTs,
			seriesTests: []seriesTest{
				{
					lookup: firstSeriesInIngesterHeadName,
					ok:     true,
					resp:   firstSeriesInIngesterHead.Labels,
				},
				{
					lookup: lastSeriesInIngesterBlocksName,
					ok:     false,
				},
				{
					lookup: lastSeriesInStorageName,
					ok:     true,
					resp:   lastSeriesInStorage.Labels,
				},
			},
			labelValuesTests: []labelValuesTest{
				{
					label: labels.MetricName,
					resp:  []string{lastSeriesInStorageName, firstSeriesInIngesterHeadName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{lastSeriesInStorageName},
					matches: []string{lastSeriesInStorageName},
				},
				{
					label:   labels.MetricName,
					resp:    []string{firstSeriesInIngesterHeadName},
					matches: []string{firstSeriesInIngesterHeadName},
				},
			},
			labelNames: []string{labels.MetricName, lastSeriesInStorageName, firstSeriesInIngesterHeadName},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, st := range tc.seriesTests {
				seriesRes, err := c.Series([]string{st.lookup}, tc.from, tc.to)
				require.NoError(t, err)
				if st.ok {
					require.Equal(t, 1, len(seriesRes))
					require.Equal(t, model.LabelSet(prompbLabelsToModelMetric(st.resp)), seriesRes[0])
				} else {
					require.Equal(t, 0, len(seriesRes))
				}
			}

			for _, lvt := range tc.labelValuesTests {
				labelsRes, err := c.LabelValues(lvt.label, tc.from, tc.to, lvt.matches)
				require.NoError(t, err)
				exp := model.LabelValues{}
				for _, val := range lvt.resp {
					exp = append(exp, model.LabelValue(val))
				}
				require.Equal(t, exp, labelsRes)
			}

			labelNames, err := c.LabelNames(tc.from, tc.to)
			require.NoError(t, err)
			require.Equal(t, tc.labelNames, labelNames)
		})
	}
}

func getMetricName(lbls []prompb.Label) string {
	for _, lbl := range lbls {
		if lbl.Name == labels.MetricName {
			return lbl.Value
		}
	}

	panic(fmt.Sprintf("series %v has no metric name", lbls))
}

func prompbLabelsToModelMetric(pbLabels []prompb.Label) model.Metric {
	metric := model.Metric{}

	for _, l := range pbLabels {
		metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
	}

	return metric
}
