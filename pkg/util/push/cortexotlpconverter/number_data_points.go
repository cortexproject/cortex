package cortexotlpconverter

import (
	"context"
	"math"

	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/prometheus/common/model"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/prometheus/prometheus/model/value"
)

func (c *CortexConverter) addGaugeNumberDataPoints(ctx context.Context, dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, settings Settings, metadata *cortexpb.MetricMetadata, scope scope,
) error {
	for x := 0; x < dataPoints.Len(); x++ {
		if err := c.everyN.checkContext(ctx); err != nil {
			return err
		}

		pt := dataPoints.At(x)
		labels, err := createAttributes(
			resource,
			pt.Attributes(),
			scope,
			settings,
			nil,
			true,
			metadata,
			model.MetricNameLabel,
			metadata.MetricFamilyName,
		)
		if err != nil {
			return err
		}
		sample := &cortexpb.Sample{
			// convert ns to ms
			TimestampMs: convertTimeStamp(pt.Timestamp()),
		}
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			sample.Value = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			sample.Value = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			sample.Value = math.Float64frombits(value.StaleNaN)
		}

		c.addSample(sample, labels)
	}

	return nil
}

func (c *CortexConverter) addSumNumberDataPoints(ctx context.Context, dataPoints pmetric.NumberDataPointSlice,
	resource pcommon.Resource, settings Settings, metadata *cortexpb.MetricMetadata, scope scope,
) error {
	for x := 0; x < dataPoints.Len(); x++ {
		if err := c.everyN.checkContext(ctx); err != nil {
			return err
		}

		pt := dataPoints.At(x)
		lbls, err := createAttributes(
			resource,
			pt.Attributes(),
			scope,
			settings,
			nil,
			true,
			metadata,
			model.MetricNameLabel,
			metadata.MetricFamilyName,
		)
		if err != nil {
			return err
		}
		sample := &cortexpb.Sample{
			// convert ns to ms
			TimestampMs: convertTimeStamp(pt.Timestamp()),
		}
		switch pt.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			sample.Value = float64(pt.IntValue())
		case pmetric.NumberDataPointValueTypeDouble:
			sample.Value = pt.DoubleValue()
		}
		if pt.Flags().NoRecordedValue() {
			sample.Value = math.Float64frombits(value.StaleNaN)
		}

		ts := c.addSample(sample, lbls)
		if ts != nil {
			exemplars, err := getPromExemplars[pmetric.NumberDataPoint](ctx, &c.everyN, pt)
			if err != nil {
				return err
			}
			ts.Exemplars = append(ts.Exemplars, exemplars...)
		}
	}

	return nil
}
