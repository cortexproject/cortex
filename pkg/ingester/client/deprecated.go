package client

import (
	"github.com/cortexproject/cortex/pkg/cortexpb"
)

// Deprecated. Use types in cortexpb package instead.
type PreallocWriteRequest = cortexpb.PreallocWriteRequest
type PreallocTimeseries = cortexpb.PreallocTimeseries
type LabelAdapter = cortexpb.LabelAdapter
type Sample = cortexpb.Sample
type MetricMetadata = cortexpb.MetricMetadata
type WriteRequest = cortexpb.WriteRequest
type WriteRequest_SourceEnum = cortexpb.WriteRequest_SourceEnum
type WriteResponse = cortexpb.WriteResponse
type TimeSeries = cortexpb.TimeSeries
type Metric = cortexpb.Metric
type MetricMetadata_MetricType = cortexpb.MetricMetadata_MetricType

// Deprecated. Use functions and values from cortexpb package instead.
var MetricMetadataMetricTypeToMetricType = cortexpb.MetricMetadataMetricTypeToMetricType
var FromLabelAdaptersToLabels = cortexpb.FromLabelAdaptersToLabels
var FromLabelAdaptersToLabelsWithCopy = cortexpb.FromLabelAdaptersToLabelsWithCopy
var CopyLabels = cortexpb.CopyLabels
var FromLabelsToLabelAdapters = cortexpb.FromLabelsToLabelAdapters
var FromLabelAdaptersToMetric = cortexpb.FromLabelAdaptersToMetric
var FromMetricsToLabelAdapters = cortexpb.FromMetricsToLabelAdapters

var ReuseSlice = cortexpb.ReuseSlice
var API = cortexpb.API
var RULE = cortexpb.RULE
var COUNTER = cortexpb.COUNTER
var GAUGE = cortexpb.GAUGE
var STATESET = cortexpb.STATESET
