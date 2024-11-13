package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/thanos/pkg/shipper"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/middleware"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/cortexproject/cortex/pkg/chunk/encoding"
	"github.com/cortexproject/cortex/pkg/cortexpb"
	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/ring"
	cortex_tsdb "github.com/cortexproject/cortex/pkg/storage/tsdb"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/cortexproject/cortex/pkg/util/services"
	"github.com/cortexproject/cortex/pkg/util/test"
	"github.com/cortexproject/cortex/pkg/util/validation"
)

func TestIngesterPRW2_Push(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
	}
	userID := "test"

	testHistogram := cortexpb.HistogramToHistogramProto(10, tsdbutil.GenerateTestHistogram(1))
	testFloatHistogram := cortexpb.FloatHistogramToHistogramProto(11, tsdbutil.GenerateTestFloatHistogram(1))
	tests := map[string]struct {
		reqs                      []*cortexpb.WriteRequestV2
		expectedErr               error
		expectedIngested          []cortexpb.TimeSeries
		expectedMetadataIngested  []*cortexpb.MetricMetadata
		expectedExemplarsIngested []cortexpb.TimeSeries
		expectedMetrics           string
		additionalMetrics         []string
		disableActiveSeries       bool
		maxExemplars              int
		oooTimeWindow             time.Duration
		disableNativeHistogram    bool
	}{
		"should record native histogram discarded": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					[]cortexpb.Histogram{{TimestampMs: 10}},
					[]cortexpb.MetadataV2{{Type: cortexpb.GAUGE, HelpRef: 3}},
					cortexpb.API,
					"a help for metric_name_2"),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
			},
			expectedMetadataIngested: []*cortexpb.MetricMetadata{
				{MetricFamilyName: "test", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
			},
			additionalMetrics:      []string{"cortex_discarded_samples_total", "cortex_ingester_active_series"},
			disableNativeHistogram: true,
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="native-histogram-sample",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed on valid series and metadata": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					[]cortexpb.MetadataV2{{HelpRef: 3, Type: cortexpb.COUNTER}},
					cortexpb.API,
					"a help for metric_name_1"),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					[]cortexpb.MetadataV2{{HelpRef: 3, Type: cortexpb.GAUGE}},
					cortexpb.API,
					"a help for metric_name_2"),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetadataIngested: []*cortexpb.MetricMetadata{
				{MetricFamilyName: "test", Help: "a help for metric_name_2", Unit: "", Type: cortexpb.GAUGE},
				{MetricFamilyName: "test", Help: "a help for metric_name_1", Unit: "", Type: cortexpb.COUNTER},
			},
			additionalMetrics: []string{
				// Metadata.
				"cortex_ingester_memory_metadata",
				"cortex_ingester_memory_metadata_created_total",
				"cortex_ingester_ingested_metadata_total",
				"cortex_ingester_ingested_metadata_failures_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_metadata_failures_total The total number of metadata that errored on ingestion.
				# TYPE cortex_ingester_ingested_metadata_failures_total counter
				cortex_ingester_ingested_metadata_failures_total 0
				# HELP cortex_ingester_ingested_metadata_total The total number of metadata ingested.
				# TYPE cortex_ingester_ingested_metadata_total counter
				cortex_ingester_ingested_metadata_total 2
				# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
				# TYPE cortex_ingester_memory_metadata gauge
				cortex_ingester_memory_metadata 2
				# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
				# TYPE cortex_ingester_memory_metadata_created_total counter
				cortex_ingester_memory_metadata_created_total{user="test"} 2
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed on valid series with exemplars": {
			maxExemplars: 2,
			reqs: []*cortexpb.WriteRequestV2{
				// Ingesting an exemplar requires a sample to create the series first
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					cortexpb.API),
				{
					Symbols: []string{"", "__name__", "test", "traceID", "123", "456"},
					Timeseries: []cortexpb.PreallocTimeseriesV2{
						{
							TimeSeriesV2: &cortexpb.TimeSeriesV2{
								LabelsRefs: []uint32{1, 2},
								Exemplars: []cortexpb.ExemplarV2{
									{
										LabelsRefs: []uint32{3, 4},
										Timestamp:  1000,
										Value:      1000,
									},
									{
										LabelsRefs: []uint32{3, 5},
										Timestamp:  1001,
										Value:      1001,
									},
								},
							},
						},
					},
				},
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}}},
			},
			expectedExemplarsIngested: []cortexpb.TimeSeries{
				{
					Labels: metricLabelAdapters,
					Exemplars: []cortexpb.Exemplar{
						{
							Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "123"}},
							TimestampMs: 1000,
							Value:       1000,
						},
						{
							Labels:      []cortexpb.LabelAdapter{{Name: "traceID", Value: "456"}},
							TimestampMs: 1001,
							Value:       1001,
						},
					},
				},
			},
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total 2

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 2

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 1

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out of order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
			`,
		},
		"successful push, active series disabled": {
			disableActiveSeries: true,
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 9}, {Value: 2, TimestampMs: 10}}},
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
			`,
		},
		"ooo disabled, should soft fail on sample out of order": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
					[]cortexpb.Histogram{
						cortexpb.HistogramToHistogramProto(9, tsdbutil.GenerateTestHistogram(1)),
					},
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfOrderSample, model.Time(9), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_out_of_order_samples_total",
				"cortex_ingester_tsdb_head_out_of_order_samples_appended_total",
				"cortex_discarded_samples_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 2
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_out_of_order_samples_appended_total Total number of appended out of order samples.
        	    # TYPE cortex_ingester_tsdb_head_out_of_order_samples_appended_total counter
        	    cortex_ingester_tsdb_head_out_of_order_samples_appended_total{type="float",user="test"} 0
        	    # HELP cortex_ingester_tsdb_out_of_order_samples_total Total number of out of order samples ingestion failed attempts due to out of order being disabled.
        	    # TYPE cortex_ingester_tsdb_out_of_order_samples_total counter
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="float",user="test"} 1
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-order",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"ooo disabled, should soft fail on sample out of bound": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (86400 * 1000)}},
					[]cortexpb.Histogram{
						cortexpb.HistogramToHistogramProto(1575043969-(86400*1000), tsdbutil.GenerateTestHistogram(1)),
					},
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrOutOfBounds, model.Time(1575043969-(86400*1000)), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{"cortex_ingester_active_series"},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 2
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-out-of-bounds",user="test"} 2
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"ooo enabled, should soft fail on sample too old": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (600 * 1000)}},
					nil,
					nil,
					cortexpb.API),
			},
			oooTimeWindow: 5 * time.Minute,
			expectedErr:   httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.ErrTooOldSample, model.Time(1575043969-(600*1000)), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{
				"cortex_discarded_samples_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="sample-too-old",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"ooo enabled, should succeed": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (60 * 1000)}},
					nil,
					nil,
					cortexpb.API),
			},
			oooTimeWindow: 5 * time.Minute,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 1575043969 - (60 * 1000)}, {Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{"cortex_ingester_active_series"},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on two different sample values at the same timestamp": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 1, TimestampMs: 1575043969}},
					nil,
					nil,
					cortexpb.API),
			},
			expectedErr: httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(storage.NewDuplicateFloatErr(1575043969, 2, 1), model.Time(1575043969), cortexpb.FromLabelsToLabelAdapters(metricLabels)), userID).Error()),
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 1575043969}}},
			},
			additionalMetrics: []string{"cortex_discarded_samples_total", "cortex_ingester_active_series"},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 1
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="new-value-for-timestamp",user="test"} 1
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should soft fail on exemplar with unknown series": {
			maxExemplars: 1,
			reqs: []*cortexpb.WriteRequestV2{
				// Ingesting an exemplar requires a sample to create the series first
				// This is not done here.
				{
					Symbols: []string{"", "__name__", "test", "traceID", "123"},
					Timeseries: []cortexpb.PreallocTimeseriesV2{
						{
							TimeSeriesV2: &cortexpb.TimeSeriesV2{
								LabelsRefs: []uint32{1, 2},
								Exemplars: []cortexpb.ExemplarV2{
									{
										LabelsRefs: []uint32{3, 4},
										Timestamp:  1000,
										Value:      1000,
									},
								},
							},
						},
					},
				},
			},
			expectedErr:              httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestExemplarErr(errExemplarRef, model.Time(1000), cortexpb.FromLabelsToLabelAdapters(metricLabels), []cortexpb.LabelAdapter{{Name: "traceID", Value: "123"}}), userID).Error()),
			expectedIngested:         nil,
			expectedMetadataIngested: nil,
			additionalMetrics: []string{
				"cortex_ingester_tsdb_exemplar_exemplars_appended_total",
				"cortex_ingester_tsdb_exemplar_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage",
				"cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds",
				"cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 0
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 0
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 0
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_appended_total Total number of TSDB exemplars appended.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_appended_total counter
				cortex_ingester_tsdb_exemplar_exemplars_appended_total 0

				# HELP cortex_ingester_tsdb_exemplar_exemplars_in_storage Number of TSDB exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_exemplars_in_storage 0

				# HELP cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage Number of TSDB series with exemplars currently in storage.
				# TYPE cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage gauge
				cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds The timestamp of the oldest exemplar stored in circular storage. Useful to check for what time range the current exemplar buffer limit allows. This usually means the last timestamp for all exemplars for a typical setup. This is not true though if one of the series timestamp is in future compared to rest series.
				# TYPE cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds gauge
				cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="test"} 0

				# HELP cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total Total number of out of order exemplar ingestion failed attempts.
				# TYPE cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total counter
				cortex_ingester_tsdb_exemplar_out_of_order_exemplars_total 0
			`,
		},
		"should succeed when only native histogram present if enabled": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					nil,
					[]cortexpb.Histogram{testHistogram},
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Histograms: []cortexpb.Histogram{testHistogram}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_samples_appended_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_out_of_order_samples_appended_total Total number of appended out of order samples.
        	    # TYPE cortex_ingester_tsdb_head_out_of_order_samples_appended_total counter
        	    cortex_ingester_tsdb_head_out_of_order_samples_appended_total{type="float",user="test"} 0
        	    # HELP cortex_ingester_tsdb_head_samples_appended_total Total number of appended samples.
        	    # TYPE cortex_ingester_tsdb_head_samples_appended_total counter
        	    cortex_ingester_tsdb_head_samples_appended_total{type="float",user="test"} 0
        	    cortex_ingester_tsdb_head_samples_appended_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should succeed when only float native histogram present if enabled": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					nil,
					[]cortexpb.Histogram{testFloatHistogram},
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Histograms: []cortexpb.Histogram{testFloatHistogram}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_samples_appended_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 1
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_out_of_order_samples_appended_total Total number of appended out of order samples.
        	    # TYPE cortex_ingester_tsdb_head_out_of_order_samples_appended_total counter
        	    cortex_ingester_tsdb_head_out_of_order_samples_appended_total{type="float",user="test"} 0
        	    # HELP cortex_ingester_tsdb_head_samples_appended_total Total number of appended samples.
        	    # TYPE cortex_ingester_tsdb_head_samples_appended_total counter
        	    cortex_ingester_tsdb_head_samples_appended_total{type="float",user="test"} 0
        	    cortex_ingester_tsdb_head_samples_appended_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
		"should fail to ingest histogram due to OOO native histogram. Sample and histogram has same timestamp but sample got ingested first": {
			reqs: []*cortexpb.WriteRequestV2{
				cortexpb.ToWriteRequestV2(
					[]labels.Labels{metricLabels},
					[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
					[]cortexpb.Histogram{testHistogram},
					nil,
					cortexpb.API),
			},
			expectedErr: nil,
			expectedIngested: []cortexpb.TimeSeries{
				{Labels: metricLabelAdapters, Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 10}}},
			},
			additionalMetrics: []string{
				"cortex_ingester_tsdb_head_samples_appended_total",
				"cortex_ingester_tsdb_out_of_order_samples_total",
				"cortex_ingester_active_series",
			},
			expectedMetrics: `
				# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
				# TYPE cortex_ingester_ingested_samples_total counter
				cortex_ingester_ingested_samples_total 2
				# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
				# TYPE cortex_ingester_ingested_samples_failures_total counter
				cortex_ingester_ingested_samples_failures_total 0
				# HELP cortex_ingester_memory_users The current number of users in memory.
				# TYPE cortex_ingester_memory_users gauge
				cortex_ingester_memory_users 1
        	    # HELP cortex_ingester_tsdb_head_samples_appended_total Total number of appended samples.
        	    # TYPE cortex_ingester_tsdb_head_samples_appended_total counter
        	    cortex_ingester_tsdb_head_samples_appended_total{type="float",user="test"} 1
        	    cortex_ingester_tsdb_head_samples_appended_total{type="histogram",user="test"} 0
        	    # HELP cortex_ingester_tsdb_out_of_order_samples_total Total number of out of order samples ingestion failed attempts due to out of order being disabled.
        	    # TYPE cortex_ingester_tsdb_out_of_order_samples_total counter
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="float",user="test"} 0
        	    cortex_ingester_tsdb_out_of_order_samples_total{type="histogram",user="test"} 1
				# HELP cortex_ingester_memory_series The current number of series in memory.
				# TYPE cortex_ingester_memory_series gauge
				cortex_ingester_memory_series 1
				# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
				# TYPE cortex_ingester_memory_series_created_total counter
				cortex_ingester_memory_series_created_total{user="test"} 1
				# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
				# TYPE cortex_ingester_memory_series_removed_total counter
				cortex_ingester_memory_series_removed_total{user="test"} 0
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				# HELP cortex_ingester_active_series Number of currently active series per user.
				# TYPE cortex_ingester_active_series gauge
				cortex_ingester_active_series{user="test"} 1
			`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.ActiveSeriesMetricsEnabled = !testData.disableActiveSeries

			limits := defaultLimitsTestConfig()
			limits.MaxExemplars = testData.maxExemplars
			limits.OutOfOrderTimeWindow = model.Duration(testData.oooTimeWindow)
			i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry, !testData.disableNativeHistogram)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Push timeseries
			for idx, req := range testData.reqs {
				_, err := i.PushV2(ctx, req)

				// We expect no error on any request except the last one
				// which may error (and in that case we assert on it)
				if idx < len(testData.reqs)-1 {
					assert.NoError(t, err)
				} else {
					assert.Equal(t, testData.expectedErr, err)
				}
			}

			// Read back samples to see what has been really ingested
			s := &mockQueryStreamServer{ctx: ctx}
			err = i.QueryStream(&client.QueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers:         []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}},
			}, s)
			require.NoError(t, err)
			set, err := seriesSetFromResponseStream(s)
			require.NoError(t, err)

			require.NotNil(t, set)
			r, err := client.SeriesSetToQueryResponse(set)
			require.NoError(t, err)
			assert.Equal(t, testData.expectedIngested, r.Timeseries)

			// Read back samples to see what has been really ingested
			exemplarRes, err := i.QueryExemplars(ctx, &client.ExemplarQueryRequest{
				StartTimestampMs: math.MinInt64,
				EndTimestampMs:   math.MaxInt64,
				Matchers: []*client.LabelMatchers{
					{Matchers: []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: labels.MetricName, Value: ".*"}}},
				},
			})

			require.NoError(t, err)
			require.NotNil(t, exemplarRes)
			assert.Equal(t, testData.expectedExemplarsIngested, exemplarRes.Timeseries)

			// Read back metadata to see what has been really ingested.
			mres, err := i.MetricsMetadata(ctx, &client.MetricsMetadataRequest{})

			require.NoError(t, err)
			require.NotNil(t, mres)

			// Order is never guaranteed.
			assert.ElementsMatch(t, testData.expectedMetadataIngested, mres.Metadata)

			// Update active series for metrics check.
			if !testData.disableActiveSeries {
				i.updateActiveSeries(ctx)
			}

			// Append additional metrics to assert on.
			mn := append(metricNames, testData.additionalMetrics...)

			// Check tracked Prometheus metrics
			err = testutil.GatherAndCompare(registry, strings.NewReader(testData.expectedMetrics), mn...)
			assert.NoError(t, err)
		})
	}
}

func TestIngesterPRW2_MetricLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerMetric = 1
	limits.MaxLocalMetadataPerMetric = 1

	dir := t.TempDir()

	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	blocksIngesterGenerator := func() *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, blocksDir, prometheus.NewRegistry(), true)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
		// Wait until it's ACTIVE
		test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
			return ing.lifecycler.GetState()
		})

		return ing
	}

	tests := []string{"chunks", "blocks"}
	for i, ingGenerator := range []func() *Ingester{blocksIngesterGenerator} {
		t.Run(tests[i], func(t *testing.T) {
			ing := ingGenerator()

			userID := "1"
			labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
			sample1 := cortexpb.Sample{
				TimestampMs: 0,
				Value:       1,
			}
			sample2 := cortexpb.Sample{
				TimestampMs: 1,
				Value:       2,
			}
			labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "biz"}}
			sample3 := cortexpb.Sample{
				TimestampMs: 1,
				Value:       3,
			}

			// Append only one series and one metadata first, expect no error.
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := ing.PushV2(ctx, cortexpb.ToWriteRequestV2([]labels.Labels{labels1}, []cortexpb.Sample{sample1}, nil, []cortexpb.MetadataV2{{HelpRef: 5, Type: cortexpb.COUNTER}}, cortexpb.API, "a help for testmetric"))
			require.NoError(t, err)

			testLimits := func() {
				// Append two series, expect series-exceeded error.
				_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2([]labels.Labels{labels1, labels3}, []cortexpb.Sample{sample2, sample3}, nil, nil, cortexpb.API))
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
				assert.Equal(t, wrapWithUser(makeMetricLimitError(perMetricSeriesLimit, labels3, ing.limiter.FormatError(userID, errMaxSeriesPerMetricLimitExceeded)), userID).Error(), string(httpResp.Body))

				// Append two metadata for the same metric. Drop the second one, and expect no error since metadata is a best effort approach.
				_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2([]labels.Labels{labels1, labels3}, nil, nil, []cortexpb.MetadataV2{{HelpRef: 6, Type: cortexpb.COUNTER}, {HelpRef: 7, Type: cortexpb.COUNTER}}, cortexpb.API, "a help for testmetric", "a help for testmetric2"))
				require.NoError(t, err)

				// Read samples back via ingester queries.
				res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
				require.NoError(t, err)

				// Verify Series
				expected := model.Matrix{
					{
						Metric: cortexpb.FromLabelAdaptersToMetric(cortexpb.FromLabelsToLabelAdapters(labels1)),
						Values: []model.SamplePair{
							{
								Timestamp: model.Time(sample1.TimestampMs),
								Value:     model.SampleValue(sample1.Value),
							},
							{
								Timestamp: model.Time(sample2.TimestampMs),
								Value:     model.SampleValue(sample2.Value),
							},
						},
					},
				}

				assert.Equal(t, expected, res)

				// Verify metadata
				m, err := ing.MetricsMetadata(ctx, nil)
				require.NoError(t, err)
				resultMetadata := &cortexpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: cortexpb.COUNTER}
				assert.Equal(t, []*cortexpb.MetricMetadata{resultMetadata}, m.Metadata)
			}

			testLimits()

			// Limits should hold after restart.
			services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
			ing = ingGenerator()
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			testLimits()
		})
	}
}

func TestIngesterPRW2_UserLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.MaxLocalSeriesPerUser = 1
	limits.MaxLocalMetricsWithMetadataPerUser = 1

	dir := t.TempDir()

	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	blocksIngesterGenerator := func() *Ingester {
		ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, nil, blocksDir, prometheus.NewRegistry(), true)
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
		// Wait until it's ACTIVE
		test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
			return ing.lifecycler.GetState()
		})

		return ing
	}

	tests := []string{"blocks"}
	for i, ingGenerator := range []func() *Ingester{blocksIngesterGenerator} {
		t.Run(tests[i], func(t *testing.T) {
			ing := ingGenerator()

			userID := "1"
			// Series
			labels1 := labels.Labels{{Name: labels.MetricName, Value: "testmetric"}, {Name: "foo", Value: "bar"}}
			sample1 := cortexpb.Sample{
				TimestampMs: 0,
				Value:       1,
			}
			sample2 := cortexpb.Sample{
				TimestampMs: 1,
				Value:       2,
			}
			labels3 := labels.Labels{{Name: labels.MetricName, Value: "testmetric2"}, {Name: "foo", Value: "biz"}}
			sample3 := cortexpb.Sample{
				TimestampMs: 1,
				Value:       3,
			}

			// Append only one series and one metadata first, expect no error.
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := ing.PushV2(ctx, cortexpb.ToWriteRequestV2([]labels.Labels{labels1}, []cortexpb.Sample{sample1}, nil, []cortexpb.MetadataV2{{HelpRef: 5, Type: cortexpb.COUNTER}}, cortexpb.API, "a help for testmetric"))
			require.NoError(t, err)

			testLimits := func() {
				// Append to two series, expect series-exceeded error.
				_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2([]labels.Labels{labels1, labels3}, []cortexpb.Sample{sample2, sample3}, nil, nil, cortexpb.API))
				httpResp, ok := httpgrpc.HTTPResponseFromError(err)
				require.True(t, ok, "returned error is not an httpgrpc response")
				assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
				assert.Equal(t, wrapWithUser(makeLimitError(perUserSeriesLimit, ing.limiter.FormatError(userID, errMaxSeriesPerUserLimitExceeded)), userID).Error(), string(httpResp.Body))

				// Append two metadata, expect no error since metadata is a best effort approach.
				_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2([]labels.Labels{labels1, labels3}, nil, nil, []cortexpb.MetadataV2{{HelpRef: 7, Type: cortexpb.COUNTER}, {HelpRef: 8, Type: cortexpb.COUNTER}}, cortexpb.API, "a help for testmetric", "a help for testmetric2"))
				require.NoError(t, err)

				// Read samples back via ingester queries.
				res, _, err := runTestQuery(ctx, t, ing, labels.MatchEqual, model.MetricNameLabel, "testmetric")
				require.NoError(t, err)

				expected := model.Matrix{
					{
						Metric: cortexpb.FromLabelAdaptersToMetric(cortexpb.FromLabelsToLabelAdapters(labels1)),
						Values: []model.SamplePair{
							{
								Timestamp: model.Time(sample1.TimestampMs),
								Value:     model.SampleValue(sample1.Value),
							},
							{
								Timestamp: model.Time(sample2.TimestampMs),
								Value:     model.SampleValue(sample2.Value),
							},
						},
					},
				}

				// Verify samples
				require.Equal(t, expected, res)

				// Verify metadata
				m, err := ing.MetricsMetadata(ctx, nil)
				require.NoError(t, err)
				resultMetadata := &cortexpb.MetricMetadata{MetricFamilyName: "testmetric", Help: "a help for testmetric", Type: cortexpb.COUNTER}
				assert.Equal(t, []*cortexpb.MetricMetadata{resultMetadata}, m.Metadata)
			}

			testLimits()

			// Limits should hold after restart.
			services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
			ing = ingGenerator()
			defer services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

			testLimits()
		})
	}

}

func TestIngesterPRW2_PerLabelsetLimitExceeded(t *testing.T) {
	limits := defaultLimitsTestConfig()
	userID := "1"
	registry := prometheus.NewRegistry()

	limits.LimitsPerLabelSet = []validation.LimitsPerLabelSet{
		{
			LabelSet: labels.FromMap(map[string]string{
				"label1": "value1",
			}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 3,
			},
		},
		{
			LabelSet: labels.FromMap(map[string]string{
				"label2": "value2",
			}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 2,
			},
		},
	}
	tenantLimits := newMockTenantLimits(map[string]*validation.Limits{userID: &limits})

	b, err := json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))

	dir := t.TempDir()
	chunksDir := filepath.Join(dir, "chunks")
	blocksDir := filepath.Join(dir, "blocks")
	require.NoError(t, os.Mkdir(chunksDir, os.ModePerm))
	require.NoError(t, os.Mkdir(blocksDir, os.ModePerm))

	ing, err := prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, tenantLimits, blocksDir, registry, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	// Wait until it's ACTIVE
	test.Poll(t, time.Second, ring.ACTIVE, func() interface{} {
		return ing.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	samples := []cortexpb.Sample{{Value: 2, TimestampMs: 10}}

	// Create first series within the limits
	for _, set := range limits.LimitsPerLabelSet {
		lbls := []string{labels.MetricName, "metric_name"}
		for _, lbl := range set.LabelSet {
			lbls = append(lbls, lbl.Name, lbl.Value)
		}
		for i := 0; i < set.Limits.MaxSeries; i++ {
			_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
				[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", fmt.Sprintf("extraValue%v", i))...)}, samples, nil, nil, cortexpb.API))
			require.NoError(t, err)
		}
	}

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Should impose limits
	for _, set := range limits.LimitsPerLabelSet {
		lbls := []string{labels.MetricName, "metric_name"}
		for _, lbl := range set.LabelSet {
			lbls = append(lbls, lbl.Name, lbl.Value)
		}
		_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
			[]labels.Labels{labels.FromStrings(append(lbls, "newLabel", "newValue")...)}, samples, nil, nil, cortexpb.API))
		httpResp, ok := httpgrpc.HTTPResponseFromError(err)
		require.True(t, ok, "returned error is not an httpgrpc response")
		assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
		require.ErrorContains(t, err, set.Id)
	}

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 2
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset", "cortex_discarded_samples_total"))

	// Should apply composite limits
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet,
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"comp1": "compValue1",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 10,
			},
		},
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"comp2": "compValue2",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 10,
			},
		},
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			"comp1": "compValue1",
			"comp2": "compValue2",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 2,
			},
		},
	)

	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)

	// Should backfill
	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 2
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 0
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 0
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 0
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset", "cortex_discarded_samples_total"))

	// Adding 5 metrics with only 1 label
	for i := 0; i < 5; i++ {
		lbls := []string{labels.MetricName, "metric_name", "comp1", "compValue1"}
		_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
			[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", fmt.Sprintf("extraValue%v", i))...)}, samples, nil, nil, cortexpb.API))
		require.NoError(t, err)
	}

	// Adding 2 metrics with both labels (still below the limit)
	lbls := []string{labels.MetricName, "metric_name", "comp1", "compValue1", "comp2", "compValue2"}
	for i := 0; i < 2; i++ {
		_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
			[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", fmt.Sprintf("extraValue%v", i))...)}, samples, nil, nil, cortexpb.API))
		require.NoError(t, err)
	}

	// Now we should hit the limit as we already have 2 metrics with comp1=compValue1, comp2=compValue2
	_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
		[]labels.Labels{labels.FromStrings(append(lbls, "newLabel", "newValue")...)}, samples, nil, nil, cortexpb.API))
	httpResp, ok := httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok, "returned error is not an httpgrpc response")
	assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
	require.ErrorContains(t, err, labels.FromStrings("comp1", "compValue1", "comp2", "compValue2").String())

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_discarded_samples_total The total number of samples that were discarded.
				# TYPE cortex_discarded_samples_total counter
				cortex_discarded_samples_total{reason="per_labelset_series_limit",user="1"} 3
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 7
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 2
		`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset", "cortex_discarded_samples_total"))

	// Should bootstrap and apply limits when configuration change
	limits.LimitsPerLabelSet = append(limits.LimitsPerLabelSet,
		validation.LimitsPerLabelSet{LabelSet: labels.FromMap(map[string]string{
			labels.MetricName: "metric_name",
			"comp2":           "compValue2",
		}),
			Limits: validation.LimitsPerLabelSetEntry{
				MaxSeries: 3, // we already have 2 so we need to allow 1 more
			},
		},
	)

	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)

	lbls = []string{labels.MetricName, "metric_name", "comp2", "compValue2"}
	_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
		[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", "extraValueUpdate")...)}, samples, nil, nil, cortexpb.API))
	require.NoError(t, err)

	_, err = ing.PushV2(ctx, cortexpb.ToWriteRequestV2(
		[]labels.Labels{labels.FromStrings(append(lbls, "extraLabel", "extraValueUpdate2")...)}, samples, nil, nil, cortexpb.API))
	httpResp, ok = httpgrpc.HTTPResponseFromError(err)
	require.True(t, ok, "returned error is not an httpgrpc response")
	assert.Equal(t, http.StatusBadRequest, int(httpResp.Code))
	require.ErrorContains(t, err, labels.FromStrings(lbls...).String())

	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_limits_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 10
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{__name__=\"metric_name\", comp2=\"compValue2\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\", comp2=\"compValue2\"}",limit="max_series",user="1"} 2
				cortex_ingester_usage_per_labelset{labelset="{comp1=\"compValue1\"}",limit="max_series",user="1"} 7
				cortex_ingester_usage_per_labelset{labelset="{comp2=\"compValue2\"}",limit="max_series",user="1"} 3
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Should remove metrics when the limits is removed
	limits.LimitsPerLabelSet = limits.LimitsPerLabelSet[:2]
	b, err = json.Marshal(limits)
	require.NoError(t, err)
	require.NoError(t, limits.UnmarshalJSON(b))
	tenantLimits.setLimits(userID, &limits)
	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))

	// Should persist between restarts
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck
	registry = prometheus.NewRegistry()
	ing, err = prepareIngesterWithBlocksStorageAndLimits(t, defaultIngesterTestConfig(t), limits, tenantLimits, blocksDir, registry, true)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ing))
	ing.updateActiveSeries(ctx)
	require.NoError(t, testutil.GatherAndCompare(registry, bytes.NewBufferString(`
				# HELP cortex_ingester_limits_per_labelset Limits per user and labelset.
				# TYPE cortex_ingester_limits_per_labelset gauge
				cortex_ingester_limits_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_limits_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
				# HELP cortex_ingester_usage_per_labelset Current usage per user and labelset.
				# TYPE cortex_ingester_usage_per_labelset gauge
				cortex_ingester_usage_per_labelset{labelset="{label1=\"value1\"}",limit="max_series",user="1"} 3
				cortex_ingester_usage_per_labelset{labelset="{label2=\"value2\"}",limit="max_series",user="1"} 2
	`), "cortex_ingester_usage_per_labelset", "cortex_ingester_limits_per_labelset"))
	services.StopAndAwaitTerminated(context.Background(), ing) //nolint:errcheck

}

// Referred from https://github.com/prometheus/prometheus/blob/v2.52.1/model/histogram/histogram_test.go#L985.
func TestIngesterPRW2_PushNativeHistogramErrors(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	for _, tc := range []struct {
		name        string
		histograms  []cortexpb.Histogram
		expectedErr error
	}{
		{
			name: "rejects histogram with NaN observations that has its Count (2) lower than the actual total of buckets (2 + 1)",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					ZeroCount:       2,
					Count:           2,
					Sum:             math.NaN(),
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("3 observations found in buckets, but the Count field is 2: %w", histogram.ErrHistogramCountNotBigEnough),
		},
		{
			name: "rejects histogram without NaN observations that has its Count (4) higher than the actual total of buckets (2 + 1)",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					ZeroCount:       2,
					Count:           4,
					Sum:             333,
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("3 observations found in buckets, but the Count field is 4: %w", histogram.ErrHistogramCountMismatch),
		},
		{
			name: "rejects histogram that has too few negative buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					NegativeBuckets: []int64{},
				}),
			},
			expectedErr: fmt.Errorf("negative side: spans need 1 buckets, have 0 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects histogram that has too few positive buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{},
				}),
			},
			expectedErr: fmt.Errorf("positive side: spans need 1 buckets, have 0 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects histogram that has too many negative buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					NegativeBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("negative side: spans need 1 buckets, have 2 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects histogram that has too many positive buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: 0, Length: 1}},
					PositiveBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("positive side: spans need 1 buckets, have 2 buckets: %w", histogram.ErrHistogramSpansBucketsMismatch),
		},
		{
			name: "rejects a histogram that has a negative span with a negative offset",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}, {Offset: -1, Length: 1}},
					NegativeBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("negative side: span number 2 with offset -1: %w", histogram.ErrHistogramSpanNegativeOffset),
		},
		{
			name: "rejects a histogram that has a positive span with a negative offset",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}, {Offset: -1, Length: 1}},
					PositiveBuckets: []int64{1, 2},
				}),
			},
			expectedErr: fmt.Errorf("positive side: span number 2 with offset -1: %w", histogram.ErrHistogramSpanNegativeOffset),
		},
		{
			name: "rejects a histogram that has a negative span with a negative count",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					NegativeBuckets: []int64{-1},
				}),
			},
			expectedErr: fmt.Errorf("negative side: bucket number 1 has observation count of -1: %w", histogram.ErrHistogramNegativeBucketCount),
		},
		{
			name: "rejects a histogram that has a positive span with a negative count",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					PositiveBuckets: []int64{-1},
				}),
			},
			expectedErr: fmt.Errorf("positive side: bucket number 1 has observation count of -1: %w", histogram.ErrHistogramNegativeBucketCount),
		},
		{
			name: "rejects a histogram that has a lower count than count in buckets",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					Count:           0,
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					NegativeBuckets: []int64{1},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("2 observations found in buckets, but the Count field is 0: %w", histogram.ErrHistogramCountMismatch),
		},
		{
			name: "rejects a histogram that doesn't count the zero bucket in its count",
			histograms: []cortexpb.Histogram{
				cortexpb.HistogramToHistogramProto(10, &histogram.Histogram{
					Count:           2,
					ZeroCount:       1,
					NegativeSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					PositiveSpans:   []histogram.Span{{Offset: -1, Length: 1}},
					NegativeBuckets: []int64{1},
					PositiveBuckets: []int64{1},
				}),
			},
			expectedErr: fmt.Errorf("3 observations found in buckets, but the Count field is 2: %w", histogram.ErrHistogramCountMismatch),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0

			limits := defaultLimitsTestConfig()
			i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry, true)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			ctx := user.InjectOrgID(context.Background(), userID)

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			req := cortexpb.ToWriteRequestV2([]labels.Labels{metricLabels}, nil, tc.histograms, nil, cortexpb.API)
			// Push timeseries
			_, err = i.PushV2(ctx, req)
			assert.Equal(t, httpgrpc.Errorf(http.StatusBadRequest, wrapWithUser(wrappedTSDBIngestErr(tc.expectedErr, model.Time(10), metricLabelAdapters), userID).Error()), err)

			require.Equal(t, testutil.ToFloat64(i.metrics.ingestedSamplesFail), float64(1))
		})
	}
}

func TestIngesterPRW2_Push_ShouldCorrectlyTrackMetricsInMultiTenantScenario(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_ingested_samples_total",
		"cortex_ingester_ingested_samples_failures_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_users",
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*cortexpb.WriteRequestV2{
			cortexpb.ToWriteRequestV2(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				cortexpb.API),
			cortexpb.ToWriteRequestV2(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				cortexpb.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.PushV2(ctx, req)
			require.NoError(t, err)
		}
	}

	// Update active series for metrics check.
	i.updateActiveSeries(context.Background())

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total 4
		# HELP cortex_ingester_ingested_samples_failures_total The total number of samples that errored on ingestion.
		# TYPE cortex_ingester_ingested_samples_failures_total counter
		cortex_ingester_ingested_samples_failures_total 0
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 2
		# HELP cortex_ingester_memory_series The current number of series in memory.
		# TYPE cortex_ingester_memory_series gauge
		cortex_ingester_memory_series 2
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 1
		cortex_ingester_active_series{user="test-2"} 1
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngesterPRW2_Push_DecreaseInactiveSeries(t *testing.T) {
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	metricNames := []string{
		"cortex_ingester_memory_series_created_total",
		"cortex_ingester_memory_series_removed_total",
		"cortex_ingester_active_series",
	}

	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetricsIdleTimeout = 100 * time.Millisecond
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push timeseries for each user
	for _, userID := range []string{"test-1", "test-2"} {
		reqs := []*cortexpb.WriteRequestV2{
			cortexpb.ToWriteRequestV2(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
				nil,
				nil,
				cortexpb.API),
			cortexpb.ToWriteRequestV2(
				[]labels.Labels{metricLabels},
				[]cortexpb.Sample{{Value: 2, TimestampMs: 10}},
				nil,
				nil,
				cortexpb.API),
		}

		for _, req := range reqs {
			ctx := user.InjectOrgID(context.Background(), userID)
			_, err := i.PushV2(ctx, req)
			require.NoError(t, err)
		}
	}

	// Wait a bit to make series inactive (set to 100ms above).
	time.Sleep(200 * time.Millisecond)

	// Update active series for metrics check. This will remove inactive series.
	i.updateActiveSeries(context.Background())

	// Check tracked Prometheus metrics
	expectedMetrics := `
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="test-1"} 1
		cortex_ingester_memory_series_created_total{user="test-2"} 1
		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="test-1"} 0
		cortex_ingester_memory_series_removed_total{user="test-2"} 0
		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="test-1"} 0
		cortex_ingester_active_series{user="test-2"} 0
	`

	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func BenchmarkIngesterPRW2Push(b *testing.B) {
	limits := defaultLimitsTestConfig()
	benchmarkIngesterPRW2Push(b, limits, false)
}

func benchmarkIngesterPRW2Push(b *testing.B, limits validation.Limits, errorsExpected bool) {
	registry := prometheus.NewRegistry()
	ctx := user.InjectOrgID(context.Background(), userID)

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(b)
	cfg.LifecyclerConfig.JoinAfter = 0

	ingester, err := prepareIngesterWithBlocksStorage(b, cfg, registry)
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
	defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(b, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return ingester.lifecycler.GetState()
	})

	// Push a single time series to set the TSDB min time.
	metricLabelAdapters := []cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}}
	metricLabels := cortexpb.FromLabelAdaptersToLabels(metricLabelAdapters)
	startTime := util.TimeToMillis(time.Now())

	currTimeReq := cortexpb.ToWriteRequestV2(
		[]labels.Labels{metricLabels},
		[]cortexpb.Sample{{Value: 1, TimestampMs: startTime}},
		nil,
		nil,
		cortexpb.API)
	_, err = ingester.PushV2(ctx, currTimeReq)
	require.NoError(b, err)

	const (
		series  = 10000
		samples = 10
	)

	allLabels, allSamples := benchmarkData(series)

	b.ResetTimer()
	for iter := 0; iter < b.N; iter++ {
		// Bump the timestamp on each of our test samples each time round the loop
		for j := 0; j < samples; j++ {
			for i := range allSamples {
				allSamples[i].TimestampMs = startTime + int64(iter*samples+j+1)
			}
			_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(allLabels, allSamples, nil, nil, cortexpb.API))
			if !errorsExpected {
				require.NoError(b, err)
			}
		}
	}
}

func Benchmark_IngesterPRW2_PushOnError(b *testing.B) {
	var (
		ctx             = user.InjectOrgID(context.Background(), userID)
		sampleTimestamp = int64(100)
		metricName      = "test"
	)

	scenarios := map[string]struct {
		numSeriesPerRequest  int
		numConcurrentClients int
	}{
		"no concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 1,
		},
		"low concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 100,
		},
		"high concurrency": {
			numSeriesPerRequest:  1000,
			numConcurrentClients: 1000,
		},
	}

	instanceLimits := map[string]*InstanceLimits{
		"no limits":  nil,
		"limits set": {MaxIngestionRate: 1000, MaxInMemoryTenants: 1, MaxInMemorySeries: 1000, MaxInflightPushRequests: 1000}, // these match max values from scenarios
	}

	tests := map[string]struct {
		// If this returns false, test is skipped.
		prepareConfig   func(limits *validation.Limits, instanceLimits *InstanceLimits) bool
		beforeBenchmark func(b *testing.B, ingester *Ingester, numSeriesPerRequest int)
		runBenchmark    func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample)
	}{
		"out of bound samples": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a single time series to set the TSDB min time.
				currTimeReq := cortexpb.ToWriteRequestV2(
					[]labels.Labels{{{Name: labels.MetricName, Value: metricName}}},
					[]cortexpb.Sample{{Value: 1, TimestampMs: util.TimeToMillis(time.Now())}},
					nil,
					nil,
					cortexpb.API)
				_, err := ingester.PushV2(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := storage.ErrOutOfBounds.Error()
				// Push out of bound samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"out of order samples": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool { return true },
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// For each series, push a single sample with a timestamp greater than next pushes.
				for i := 0; i < numSeriesPerRequest; i++ {
					currTimeReq := cortexpb.ToWriteRequestV2(
						[]labels.Labels{{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}}},
						[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
						nil,
						nil,
						cortexpb.API)

					_, err := ingester.PushV2(ctx, currTimeReq)
					require.NoError(b, err)
				}
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				expectedErr := storage.ErrOutOfOrderSample.Error()

				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				// Push out of order samples.
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck

					verifyErrorString(b, err, expectedErr)
				}
			},
		},
		"per-user series limit reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				limits.MaxLocalSeriesPerUser = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with a metric name different than the one used during the benchmark.
				currTimeReq := cortexpb.ToWriteRequestV2(
					[]labels.Labels{labels.FromStrings(labels.MetricName, "another")},
					[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					cortexpb.API)
				_, err := ingester.PushV2(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				// Push series with a different name than the one already pushed.

				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-user series limit")
				}
			},
		},
		"per-metric series limit reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				limits.MaxLocalSeriesPerMetric = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Push a series with the same metric name but different labels than the one used during the benchmark.
				currTimeReq := cortexpb.ToWriteRequestV2(
					[]labels.Labels{labels.FromStrings(labels.MetricName, metricName, "cardinality", "another")},
					[]cortexpb.Sample{{Value: 1, TimestampMs: sampleTimestamp + 1}},
					nil,
					nil,
					cortexpb.API)
				_, err := ingester.PushV2(ctx, currTimeReq)
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API)) // nolint:errcheck
					verifyErrorString(b, err, "per-metric series limit")
				}
			},
		},
		"very low ingestion rate limit": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxIngestionRate = 0.00001 // very low
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Send a lot of samples
				_, err := ingester.PushV2(ctx, generateSamplesForLabelV2(labels.FromStrings(labels.MetricName, "test"), 10000))
				require.NoError(b, err)

				ingester.ingestionRate.Tick()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "push rate reached")
				}
			},
		},
		"max number of tenants reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemoryTenants = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				// Send some samples for one tenant (not the same that is used during the test)
				ctx := user.InjectOrgID(context.Background(), "different_tenant")
				_, err := ingester.PushV2(ctx, generateSamplesForLabelV2(labels.FromStrings(labels.MetricName, "test"), 10000))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				// Push series with different labels than the one already pushed.
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "max tenants limit reached")
				}
			},
		},
		"max number of series reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInMemorySeries = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				_, err := ingester.PushV2(ctx, generateSamplesForLabelV2(labels.FromStrings(labels.MetricName, "test"), 10000))
				require.NoError(b, err)
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "max series limit reached")
				}
			},
		},
		"max inflight requests reached": {
			prepareConfig: func(limits *validation.Limits, instanceLimits *InstanceLimits) bool {
				if instanceLimits == nil {
					return false
				}
				instanceLimits.MaxInflightPushRequests = 1
				return true
			},
			beforeBenchmark: func(b *testing.B, ingester *Ingester, numSeriesPerRequest int) {
				ingester.inflightPushRequests.Inc()
			},
			runBenchmark: func(b *testing.B, ingester *Ingester, metrics []labels.Labels, samples []cortexpb.Sample) {
				st := writev2.NewSymbolTable()
				for _, lbs := range metrics {
					st.SymbolizeLabels(lbs, nil)
				}
				for n := 0; n < b.N; n++ {
					_, err := ingester.PushV2(ctx, cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API))
					verifyErrorString(b, err, "too many inflight push requests")
				}
			},
		},
	}

	for testName, testData := range tests {
		for scenarioName, scenario := range scenarios {
			for limitsName, limits := range instanceLimits {
				b.Run(fmt.Sprintf("failure: %s, scenario: %s, limits: %s", testName, scenarioName, limitsName), func(b *testing.B) {
					registry := prometheus.NewRegistry()

					instanceLimits := limits
					if instanceLimits != nil {
						// make a copy, to avoid changing value in the instanceLimits map.
						newLimits := &InstanceLimits{}
						*newLimits = *instanceLimits
						instanceLimits = newLimits
					}

					// Create a mocked ingester
					cfg := defaultIngesterTestConfig(b)
					cfg.LifecyclerConfig.JoinAfter = 0

					limits := defaultLimitsTestConfig()
					if !testData.prepareConfig(&limits, instanceLimits) {
						b.SkipNow()
					}

					cfg.InstanceLimitsFn = func() *InstanceLimits {
						return instanceLimits
					}

					ingester, err := prepareIngesterWithBlocksStorageAndLimits(b, cfg, limits, nil, "", registry, true)
					require.NoError(b, err)
					require.NoError(b, services.StartAndAwaitRunning(context.Background(), ingester))
					defer services.StopAndAwaitTerminated(context.Background(), ingester) //nolint:errcheck

					// Wait until the ingester is ACTIVE
					test.Poll(b, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
						return ingester.lifecycler.GetState()
					})

					testData.beforeBenchmark(b, ingester, scenario.numSeriesPerRequest)

					// Prepare the request.
					metrics := make([]labels.Labels, 0, scenario.numSeriesPerRequest)
					samples := make([]cortexpb.Sample, 0, scenario.numSeriesPerRequest)
					for i := 0; i < scenario.numSeriesPerRequest; i++ {
						metrics = append(metrics, labels.Labels{{Name: labels.MetricName, Value: metricName}, {Name: "cardinality", Value: strconv.Itoa(i)}})
						samples = append(samples, cortexpb.Sample{Value: float64(i), TimestampMs: sampleTimestamp})
					}

					// Run the benchmark.
					wg := sync.WaitGroup{}
					wg.Add(scenario.numConcurrentClients)
					start := make(chan struct{})

					b.ReportAllocs()
					b.ResetTimer()

					for c := 0; c < scenario.numConcurrentClients; c++ {
						go func() {
							defer wg.Done()
							<-start

							testData.runBenchmark(b, ingester, metrics, samples)
						}()
					}

					b.ResetTimer()
					close(start)
					wg.Wait()
				})
			}
		}
	}
}

func TestIngesterPRW2_LabelNames(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := []string{"__name__", "route", "status"}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		limit    int
		expected []string
	}{
		"should return all label names if no limit is set": {
			expected: expected,
		},
		"should return limited label names if a limit is set": {
			limit:    2,
			expected: expected[:2],
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Get label names
			res, err := i.LabelNames(ctx, &client.LabelNamesRequest{Limit: int64(testData.limit)})
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.LabelNames)
		})
	}
}

func TestIngesterPRW2_LabelValues(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	expected := map[string][]string{
		"__name__": {"test_1", "test_2"},
		"status":   {"200", "500"},
		"route":    {"get_user"},
		"unknown":  {},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	tests := map[string]struct {
		limit int64
		match []*labels.Matcher
	}{
		"should return all label values if no limit is set": {
			limit: 0,
		},
		"should return limited label values if a limit is set": {
			limit: 1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for labelName, expectedValues := range expected {
				req := &client.LabelValuesRequest{LabelName: labelName, Limit: testData.limit}
				res, err := i.LabelValues(ctx, req)
				require.NoError(t, err)
				if testData.limit > 0 && len(expectedValues) > int(testData.limit) {
					expectedValues = expectedValues[:testData.limit]
				}
				assert.ElementsMatch(t, expectedValues, res.LabelValues)
			}
		})
	}

}

func TestIngesterPRW2_LabelValue_MaxInflightQueryRequest(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.DefaultLimits.MaxInflightQueryRequests = 1
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	i.inflightQueryRequests.Add(1)

	// Mock request
	ctx := user.InjectOrgID(context.Background(), "test")

	wreq, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000)
	_, err = i.PushV2(ctx, wreq)
	require.NoError(t, err)

	rreq := &client.LabelValuesRequest{}
	_, err = i.LabelValues(ctx, rreq)
	require.Error(t, err)
	require.Equal(t, err, errTooManyInflightQueryRequests)
}

func Test_IngesterPRW2_Query(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "route", Value: "get_user"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "route", Value: "get_user"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	tests := map[string]struct {
		from     int64
		to       int64
		matchers []*client.LabelMatcher
		expected []cortexpb.TimeSeries
	}{
		"should return an empty response if no metric matches": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
			},
			expected: []cortexpb.TimeSeries{},
		},
		"should filter series by == matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by != matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.NOT_EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[2].lbls), Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by =~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by !~ matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.REGEX_NO_MATCH, Name: model.MetricNameLabel, Value: ".*_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[2].lbls), Samples: []cortexpb.Sample{{Value: 2, TimestampMs: 200000}}},
			},
		},
		"should filter series by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				{Type: client.REGEX_MATCH, Name: "status", Value: "5.."},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[1].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 110000}}},
			},
		},
		"should filter series by matcher and time range": {
			from: 100000,
			to:   100000,
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
			},
			expected: []cortexpb.TimeSeries{
				{Labels: cortexpb.FromLabelsToLabelAdapters(series[0].lbls), Samples: []cortexpb.Sample{{Value: 1, TimestampMs: 100000}}},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			req := &client.QueryRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				Matchers:         testData.matchers,
			}

			s := &mockQueryStreamServer{ctx: ctx}
			err = i.QueryStream(req, s)
			require.NoError(t, err)
			set, err := seriesSetFromResponseStream(s)
			require.NoError(t, err)
			r, err := client.SeriesSetToQueryResponse(set)
			require.NoError(t, err)
			fmt.Println("testData.expected", testData.expected)
			fmt.Println("r.Timeseries", r.Timeseries)
			assert.ElementsMatch(t, testData.expected, r.Timeseries)
		})
	}
}

func TestIngesterPRW2_Query_MaxInflightQueryRequest(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.DefaultLimits.MaxInflightQueryRequests = 1
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	i.inflightQueryRequests.Add(1)

	// Mock request
	ctx := user.InjectOrgID(context.Background(), "test")

	wreq, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000)
	_, err = i.PushV2(ctx, wreq)
	require.NoError(t, err)

	rreq := &client.QueryRequest{}
	s := &mockQueryStreamServer{ctx: ctx}
	err = i.QueryStream(rreq, s)
	require.Error(t, err)
	require.Equal(t, err, errTooManyInflightQueryRequests)
}

func TestIngesterPRW2_Push_ShouldNotCreateTSDBIfNotInActiveState(t *testing.T) {
	// Configure the lifecycler to not immediately join the ring, to make sure
	// the ingester will NOT be in the ACTIVE state when we'll push samples.
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 10 * time.Second

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck
	require.Equal(t, ring.PENDING, i.lifecycler.GetState())

	// Mock request
	userID := "test"
	ctx := user.InjectOrgID(context.Background(), userID)
	req := &cortexpb.WriteRequestV2{}

	res, err := i.PushV2(ctx, req)
	assert.Equal(t, wrapWithUser(fmt.Errorf(errTSDBCreateIncompatibleState, "PENDING"), userID).Error(), err.Error())
	assert.Nil(t, res)

	// Check if the TSDB has been created
	_, tsdbCreated := i.TSDBState.dbs[userID]
	assert.False(t, tsdbCreated)
}

func TestIngesterPRW2_MetricsForLabelMatchers(t *testing.T) {
	fixtures := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
		// The two following series have the same FastFingerprint=e002a3a451262627
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "l"}, {Name: "uniq0", Value: "0"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
		{labels.Labels{{Name: labels.MetricName, Value: "collision"}, {Name: "app", Value: "m"}, {Name: "uniq0", Value: "1"}, {Name: "uniq1", Value: "1"}}, 1, 300000},
	}

	tests := map[string]struct {
		from                 int64
		to                   int64
		limit                int64
		matchers             []*client.LabelMatchers
		expected             []*cortexpb.Metric
		queryIngestersWithin time.Duration
	}{
		"should return an empty response if no metric match": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "unknown"},
				},
			}},
			expected: []*cortexpb.Metric{},
		},
		"should filter metrics by single matcher": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by multiple matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should NOT filter metrics by time range to always return known metrics even when queried for older time ranges": {
			from: 100,
			to:   1000,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
			},
		},
		"should filter metrics by time range if queryIngestersWithin is enabled": {
			from: 99999,
			to:   100001,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
			},
			queryIngestersWithin: time.Hour,
		},
		"should not return duplicated metrics on overlapping matchers": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_1"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
					},
				},
			},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[1].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[2].lbls)},
			},
		},
		"should return all matching metrics even if their FastFingerprint collide": {
			from: math.MinInt64,
			to:   math.MaxInt64,
			matchers: []*client.LabelMatchers{{
				Matchers: []*client.LabelMatcher{
					{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "collision"},
				},
			}},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[3].lbls)},
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[4].lbls)},
			},
		},
		"should return only limited results": {
			from:  math.MinInt64,
			to:    math.MaxInt64,
			limit: 1,
			matchers: []*client.LabelMatchers{
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: "status", Value: "200"},
					},
				},
				{
					Matchers: []*client.LabelMatcher{
						{Type: client.EQUAL, Name: model.MetricNameLabel, Value: "test_2"},
					},
				},
			},
			expected: []*cortexpb.Metric{
				{Labels: cortexpb.FromLabelsToLabelAdapters(fixtures[0].lbls)},
			},
		},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range fixtures {
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	// Run tests
	for testName, testData := range tests {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			req := &client.MetricsForLabelMatchersRequest{
				StartTimestampMs: testData.from,
				EndTimestampMs:   testData.to,
				MatchersSet:      testData.matchers,
				Limit:            testData.limit,
			}
			i.cfg.QueryIngestersWithin = testData.queryIngestersWithin
			res, err := i.MetricsForLabelMatchers(ctx, req)
			require.NoError(t, err)
			assert.ElementsMatch(t, testData.expected, res.Metric)
		})
	}
}

func TestIngesterPRW2_MetricsForLabelMatchers_Deduplication(t *testing.T) {
	const (
		userID    = "test"
		numSeries = 100000
	)

	now := util.TimeToMillis(time.Now())
	i := createIngesterWithSeriesV2(t, userID, numSeries, 1, now, 1)
	ctx := user.InjectOrgID(context.Background(), "test")

	req := &client.MetricsForLabelMatchersRequest{
		StartTimestampMs: now,
		EndTimestampMs:   now,
		// Overlapping matchers to make sure series are correctly deduplicated.
		MatchersSet: []*client.LabelMatchers{
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}},
			{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*0"},
			}},
		},
	}

	res, err := i.MetricsForLabelMatchers(ctx, req)
	require.NoError(t, err)
	require.Len(t, res.GetMetric(), numSeries)
}

func BenchmarkIngesterPRW2_MetricsForLabelMatchers(b *testing.B) {
	var (
		userID              = "test"
		numSeries           = 10000
		numSamplesPerSeries = 60 * 6 // 6h on 1 sample per minute
		startTimestamp      = util.TimeToMillis(time.Now())
		step                = int64(60000) // 1 sample per minute
	)

	i := createIngesterWithSeriesV2(b, userID, numSeries, numSamplesPerSeries, startTimestamp, step)
	ctx := user.InjectOrgID(context.Background(), "test")

	// Flush the ingester to ensure blocks have been compacted, so we'll test
	// fetching labels from blocks.
	i.Flush()

	b.ResetTimer()
	b.ReportAllocs()

	for n := 0; n < b.N; n++ {
		req := &client.MetricsForLabelMatchersRequest{
			StartTimestampMs: math.MinInt64,
			EndTimestampMs:   math.MaxInt64,
			MatchersSet: []*client.LabelMatchers{{Matchers: []*client.LabelMatcher{
				{Type: client.REGEX_MATCH, Name: model.MetricNameLabel, Value: "test.*"},
			}}},
		}

		res, err := i.MetricsForLabelMatchers(ctx, req)
		require.NoError(b, err)
		require.Len(b, res.GetMetric(), numSeries)
	}
}

func TestIngesterPRW2_QueryStream(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)

	for _, enc := range encodings {
		t.Run(enc.String(), func(t *testing.T) {
			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until it's ACTIVE.
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Push series.
			ctx := user.InjectOrgID(context.Background(), userID)
			lbls := labels.Labels{{Name: labels.MetricName, Value: "foo"}}
			var (
				req                    *cortexpb.WriteRequestV2
				expectedResponseChunks *client.QueryStreamResponse
			)
			switch enc {
			case encoding.PrometheusXorChunk:
				req, expectedResponseChunks = mockWriteRequestV2(t, lbls, 123000, 456)
			case encoding.PrometheusHistogramChunk:
				req, expectedResponseChunks = mockHistogramWriteRequestV2(t, lbls, 123000, 456, false)
			case encoding.PrometheusFloatHistogramChunk:
				req, expectedResponseChunks = mockHistogramWriteRequestV2(t, lbls, 123000, 456, true)
			}
			_, err = i.PushV2(ctx, req)
			require.NoError(t, err)

			// Create a GRPC server used to query back the data.
			serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
			defer serv.GracefulStop()
			client.RegisterIngesterServer(serv, i)

			listener, err := net.Listen("tcp", "localhost:0")
			require.NoError(t, err)

			go func() {
				require.NoError(t, serv.Serve(listener))
			}()

			// Query back the series using GRPC streaming.
			c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
			require.NoError(t, err)
			defer c.Close()

			queryRequest := &client.QueryRequest{
				StartTimestampMs: 0,
				EndTimestampMs:   200000,
				Matchers: []*client.LabelMatcher{{
					Type:  client.EQUAL,
					Name:  model.MetricNameLabel,
					Value: "foo",
				}},
			}

			chunksTest := func(t *testing.T) {
				s, err := c.QueryStream(ctx, queryRequest)
				require.NoError(t, err)

				count := 0
				var lastResp *client.QueryStreamResponse
				for {
					resp, err := s.Recv()
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					count += len(resp.Chunkseries)
					lastResp = resp
				}
				require.Equal(t, 1, count)
				require.Equal(t, expectedResponseChunks, lastResp)
			}

			t.Run("chunks", chunksTest)
		})
	}
}

func TestIngesterPRW2_QueryStreamManySamplesChunks(t *testing.T) {
	// Create ingester.
	cfg := defaultIngesterTestConfig(t)

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 1000000
	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	// 100k samples in chunks use about 154 KiB,
	_, err = i.PushV2(ctx, writeRequestSingleSeriesV2(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "1"}}, samples[0:100000]))
	require.NoError(t, err)

	// 1M samples in chunks use about 1.51 MiB,
	_, err = i.PushV2(ctx, writeRequestSingleSeriesV2(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "2"}}, samples))
	require.NoError(t, err)

	// 500k samples in chunks need 775 KiB,
	_, err = i.PushV2(ctx, writeRequestSingleSeriesV2(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: "3"}}, samples[0:500000]))
	require.NoError(t, err)

	// Create a GRPC server used to query back the data.
	serv := grpc.NewServer(grpc.StreamInterceptor(middleware.StreamServerUserHeaderInterceptor))
	defer serv.GracefulStop()
	client.RegisterIngesterServer(serv, i)

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		require.NoError(t, serv.Serve(listener))
	}()

	// Query back the series using GRPC streaming.
	c, err := client.MakeIngesterClient(listener.Addr().String(), defaultClientTestConfig())
	require.NoError(t, err)
	defer c.Close()

	s, err := c.QueryStream(ctx, &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	})
	require.NoError(t, err)

	recvMsgs := 0
	series := 0
	totalSamples := 0

	for {
		resp, err := s.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.True(t, len(resp.Chunkseries) > 0) // No empty messages.

		recvMsgs++
		series += len(resp.Chunkseries)

		for _, ts := range resp.Chunkseries {
			for _, c := range ts.Chunks {
				enc := encoding.Encoding(c.Encoding).PromChunkEncoding()
				require.True(t, enc != chunkenc.EncNone)
				chk, err := chunkenc.FromData(enc, c.Data)
				require.NoError(t, err)
				totalSamples += chk.NumSamples()
			}
		}
	}

	// As ingester doesn't guarantee sorting of series, we can get 2 (100k + 500k in first, 1M in second)
	// or 3 messages (100k or 500k first, 1M second, and 500k or 100k last).

	require.True(t, 2 <= recvMsgs && recvMsgs <= 3)
	require.Equal(t, 3, series)
	require.Equal(t, 100000+500000+samplesCount, totalSamples)
}

func BenchmarkIngesterPRW2_QueryStream_Chunks(b *testing.B) {
	benchmarkQueryStreamV2(b)
}

func benchmarkQueryStreamV2(b *testing.B) {
	cfg := defaultIngesterTestConfig(b)

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(b, cfg, prometheus.NewRegistry())
	require.NoError(b, err)
	require.NoError(b, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE.
	test.Poll(b, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series.
	ctx := user.InjectOrgID(context.Background(), userID)

	const samplesCount = 1000
	samples := make([]cortexpb.Sample, 0, samplesCount)

	for i := 0; i < samplesCount; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
	}

	const seriesCount = 100
	for s := 0; s < seriesCount; s++ {
		_, err = i.PushV2(ctx, writeRequestSingleSeriesV2(labels.Labels{{Name: labels.MetricName, Value: "foo"}, {Name: "l", Value: strconv.Itoa(s)}}, samples))
		require.NoError(b, err)
	}

	req := &client.QueryRequest{
		StartTimestampMs: 0,
		EndTimestampMs:   samplesCount + 1,

		Matchers: []*client.LabelMatcher{{
			Type:  client.EQUAL,
			Name:  model.MetricNameLabel,
			Value: "foo",
		}},
	}

	mockStream := &mockQueryStreamServer{ctx: ctx}

	b.ResetTimer()

	for ix := 0; ix < b.N; ix++ {
		err := i.QueryStream(req, mockStream)
		require.NoError(b, err)
	}
}

func TestIngesterPRW2_dontShipBlocksWhenTenantDeletionMarkerIsPresent(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	i.TSDBState.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadataV2(t, i)
	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())
	i.compactBlocks(context.Background(), true, nil)
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjects := len(bucket.Objects())
	require.NotZero(t, numObjects)

	require.NoError(t, cortex_tsdb.WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bucket), userID, cortex_tsdb.NewTenantDeletionMark(time.Now())))
	numObjects++ // For deletion marker

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	db.lastDeletionMarkCheck.Store(0)

	// After writing tenant deletion mark,
	pushSingleSampleWithMetadataV2(t, i)
	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())
	i.compactBlocks(context.Background(), true, nil)
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load())
	i.shipBlocks(context.Background(), nil)

	numObjectsAfterMarkingTenantForDeletion := len(bucket.Objects())
	require.Equal(t, numObjects, numObjectsAfterMarkingTenantForDeletion)
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))
}

func TestIngesterPRW2_seriesCountIsCorrectAfterClosingTSDBForDeletedTenant(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 2

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	// Use in-memory bucket.
	bucket := objstore.NewInMemBucket()

	// Write tenant deletion mark.
	require.NoError(t, cortex_tsdb.WriteTenantDeletionMark(context.Background(), objstore.WithNoopInstr(bucket), userID, cortex_tsdb.NewTenantDeletionMark(time.Now())))

	i.TSDBState.bucket = bucket
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadataV2(t, i)
	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())

	// We call shipBlocks to check for deletion marker (it happens inside this method).
	i.shipBlocks(context.Background(), nil)

	// Verify that tenant deletion mark was found.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.deletionMarkFound.Load())

	// If we try to close TSDB now, it should succeed, even though TSDB is not idle and empty.
	require.Equal(t, uint64(1), db.Head().NumSeries())
	require.Equal(t, tsdbTenantMarkedForDeletion, i.closeAndDeleteUserTSDBIfIdle(userID))

	// Closing should decrease series count.
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load())
}

func TestIngesterPRW2_invalidSamplesDontChangeLastUpdateTime(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), userID)
	sampleTimestamp := int64(model.Now())

	{
		req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, sampleTimestamp)
		_, err = i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	lastUpdate := db.lastUpdate.Load()

	// Wait until 1 second passes.
	test.Poll(t, 1*time.Second, time.Now().Unix()+1, func() interface{} {
		return time.Now().Unix()
	})

	// Push another sample to the same metric and timestamp, with different value. We expect to get error.
	{
		req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 1, sampleTimestamp)
		_, err = i.PushV2(ctx, req)
		require.Error(t, err)
	}

	// Make sure last update hasn't changed.
	require.Equal(t, lastUpdate, db.lastUpdate.Load())
}

func TestIngesterPRW2_flushing(t *testing.T) {
	for name, tc := range map[string]struct {
		setupIngester func(cfg *Config)
		action        func(t *testing.T, i *Ingester, reg *prometheus.Registry)
	}{
		"ingesterShutdown": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = true
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},
			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadataV2(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Shutdown ingester. This triggers flushing of the block.
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

				verifyCompactedHead(t, i, true)

				// Verify that block has been shipped.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"shutdownHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
				cfg.BlocksStorageConfig.TSDB.KeepUserTSDBOpenOnShutdown = true
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadataV2(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

				i.ShutdownHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/shutdown", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandler": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadataV2(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushHandlerWithListOfTenants": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				pushSingleSampleWithMetadataV2(t, i)

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				users := url.Values{}
				users.Add(tenantParam, "unknown-user")
				users.Add(tenantParam, "another-unknown-user")

				// Using wait=true makes this a synchronous call.
				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true&"+users.Encode(), nil))

				// Still nothing shipped or compacted.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))
				verifyCompactedHead(t, i, false)

				users = url.Values{}
				users.Add(tenantParam, "different-user")
				users.Add(tenantParam, userID) // Our user
				users.Add(tenantParam, "yet-another-user")

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true&"+users.Encode(), nil))

				verifyCompactedHead(t, i, true)
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 1
				`), "cortex_ingester_shipper_uploads_total"))
			},
		},

		"flushMultipleBlocksWithDataSpanning3Days": {
			setupIngester: func(cfg *Config) {
				cfg.BlocksStorageConfig.TSDB.FlushBlocksOnShutdown = false
			},

			action: func(t *testing.T, i *Ingester, reg *prometheus.Registry) {
				// Pushing 5 samples, spanning over 3 days.
				// First block
				pushSingleSampleAtTimeV2(t, i, 23*time.Hour.Milliseconds())
				pushSingleSampleAtTimeV2(t, i, 24*time.Hour.Milliseconds()-1)

				// Second block
				pushSingleSampleAtTimeV2(t, i, 24*time.Hour.Milliseconds()+1)
				pushSingleSampleAtTimeV2(t, i, 25*time.Hour.Milliseconds())

				// Third block, far in the future.
				pushSingleSampleAtTimeV2(t, i, 50*time.Hour.Milliseconds())

				// Nothing shipped yet.
				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 0
				`), "cortex_ingester_shipper_uploads_total"))

				i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush?wait=true", nil))

				verifyCompactedHead(t, i, true)

				require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
					# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
					# TYPE cortex_ingester_shipper_uploads_total counter
					cortex_ingester_shipper_uploads_total 3
				`), "cortex_ingester_shipper_uploads_total"))

				userDB := i.getTSDB(userID)
				require.NotNil(t, userDB)

				blocks := userDB.Blocks()
				require.Equal(t, 3, len(blocks))
				require.Equal(t, 23*time.Hour.Milliseconds(), blocks[0].Meta().MinTime)
				require.Equal(t, 24*time.Hour.Milliseconds(), blocks[0].Meta().MaxTime) // Block maxt is exclusive.

				require.Equal(t, 24*time.Hour.Milliseconds()+1, blocks[1].Meta().MinTime)
				require.Equal(t, 26*time.Hour.Milliseconds(), blocks[1].Meta().MaxTime)

				require.Equal(t, 50*time.Hour.Milliseconds()+1, blocks[2].Meta().MaxTime) // Block maxt is exclusive.
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
			cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Minute // Long enough to not be reached during the test.

			if tc.setupIngester != nil {
				tc.setupIngester(&cfg)
			}

			// Create ingester
			reg := prometheus.NewPedanticRegistry()
			i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			t.Cleanup(func() {
				_ = services.StopAndAwaitTerminated(context.Background(), i)
			})

			// Wait until it's ACTIVE
			test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// mock user's shipper
			tc.action(t, i, reg)
		})
	}
}

func TestIngesterPRW2_ForFlush(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 10 * time.Minute // Long enough to not be reached during the test.

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSampleWithMetadataV2(t, i)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Nothing shipped yet.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 0
	`), "cortex_ingester_shipper_uploads_total"))

	// Restart ingester in "For Flusher" mode. We reuse the same config (esp. same dir)
	reg = prometheus.NewPedanticRegistry()
	i, err = NewForFlusher(i.cfg, i.limits, reg, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))

	// Our single sample should be reloaded from WAL
	verifyCompactedHead(t, i, false)
	i.Flush()

	// Head should be empty after flushing.
	verifyCompactedHead(t, i, true)

	// Verify that block has been shipped.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
		# HELP cortex_ingester_shipper_uploads_total Total number of uploaded TSDB blocks
		# TYPE cortex_ingester_shipper_uploads_total counter
		cortex_ingester_shipper_uploads_total 1
	`), "cortex_ingester_shipper_uploads_total"))

	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
}

func TestIngesterPRW2_UserStats(t *testing.T) {
	series := []struct {
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{labels.Labels{{Name: labels.MetricName, Value: "test_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push series
	ctx := user.InjectOrgID(context.Background(), "test")

	for _, series := range series {
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.UserStats(ctx, &client.UserStatsRequest{})
	require.NoError(t, err)
	assert.InDelta(t, 0.2, res.ApiIngestionRate, 0.0001)
	assert.InDelta(t, float64(0), res.RuleIngestionRate, 0.0001)
	assert.Equal(t, uint64(3), res.NumSeries)
}

func TestIngesterPRW2_AllUserStats(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_2"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_1"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	res, err := i.AllUserStats(context.Background(), &client.UserStatsRequest{})
	require.NoError(t, err)

	expect := []*client.UserIDStatsResponse{
		{
			UserId: "user-1",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.2,
				NumSeries:         3,
				ApiIngestionRate:  0.2,
				RuleIngestionRate: 0,
				ActiveSeries:      3,
				LoadedBlocks:      0,
			},
		},
		{
			UserId: "user-2",
			Data: &client.UserStatsResponse{
				IngestionRate:     0.13333333333333333,
				NumSeries:         2,
				ApiIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
				ActiveSeries:      2,
				LoadedBlocks:      0,
			},
		},
	}
	assert.ElementsMatch(t, expect, res.Stats)
}

func TestIngesterPRW2_AllUserStatsHandler(t *testing.T) {
	series := []struct {
		user      string
		lbls      labels.Labels
		value     float64
		timestamp int64
	}{
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_1"}, {Name: "status", Value: "500"}, {Name: "route", Value: "get_user"}}, 1, 110000},
		{"user-1", labels.Labels{{Name: labels.MetricName, Value: "test_1_2"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_1"}}, 2, 200000},
		{"user-2", labels.Labels{{Name: labels.MetricName, Value: "test_2_2"}}, 2, 200000},
	}

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})
	for _, series := range series {
		ctx := user.InjectOrgID(context.Background(), series.user)
		req, _ := mockWriteRequestV2(t, series.lbls, series.value, series.timestamp)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	// Force compaction to test loaded blocks
	compactionCallbackCh := make(chan struct{})
	i.TSDBState.forceCompactTrigger <- requestWithUsersAndCallback{users: nil, callback: compactionCallbackCh}
	<-compactionCallbackCh

	// force update statistics
	for _, db := range i.TSDBState.dbs {
		db.ingestedAPISamples.Tick()
		db.ingestedRuleSamples.Tick()
	}

	// Get label names
	response := httptest.NewRecorder()
	request := httptest.NewRequest("GET", "/all_user_stats", nil)
	request.Header.Add("Accept", "application/json")
	i.AllUserStatsHandler(response, request)
	var resp UserStatsByTimeseries
	err = json.Unmarshal(response.Body.Bytes(), &resp)
	require.NoError(t, err)

	expect := UserStatsByTimeseries{
		{
			UserID: "user-1",
			UserStats: UserStats{
				IngestionRate:     0.2,
				NumSeries:         0,
				APIIngestionRate:  0.2,
				RuleIngestionRate: 0,
				ActiveSeries:      3,
				LoadedBlocks:      1,
			},
		},
		{
			UserID: "user-2",
			UserStats: UserStats{
				IngestionRate:     0.13333333333333333,
				NumSeries:         0,
				APIIngestionRate:  0.13333333333333333,
				RuleIngestionRate: 0,
				ActiveSeries:      2,
				LoadedBlocks:      1,
			},
		},
	}
	assert.ElementsMatch(t, expect, resp)
}

func TestIngesterPRW2_CompactIdleBlock(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Hour      // Long enough to not be reached during the test.
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second // Testing this.

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadataV2(t, i)

	i.compactBlocks(context.Background(), false, nil)
	verifyCompactedHead(t, i, false)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))

	// wait one second (plus maximum jitter) -- TSDB is now idle.
	time.Sleep(time.Duration(float64(cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout) * (1 + compactionIdleTimeoutJitter)))

	i.compactBlocks(context.Background(), false, nil)
	verifyCompactedHead(t, i, true)
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))

	// Pushing another sample still works.
	pushSingleSampleWithMetadataV2(t, i)
	verifyCompactedHead(t, i, false)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 2

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 1

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1
    `), memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_memory_users"))
}

func TestIngesterPRW2_CompactAndCloseIdleTSDB(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0
	cfg.BlocksStorageConfig.TSDB.ShipInterval = 1 * time.Second // Required to enable shipping.
	cfg.BlocksStorageConfig.TSDB.ShipConcurrency = 1
	cfg.BlocksStorageConfig.TSDB.HeadCompactionInterval = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.HeadCompactionIdleTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBTimeout = 1 * time.Second
	cfg.BlocksStorageConfig.TSDB.CloseIdleTSDBInterval = 100 * time.Millisecond

	r := prometheus.NewRegistry()

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, r)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	pushSingleSampleWithMetadataV2(t, i)
	i.updateActiveSeries(context.Background())

	require.Equal(t, int64(1), i.TSDBState.seriesCount.Load())

	userMetrics := []string{memSeriesCreatedTotalName, memSeriesRemovedTotalName, "cortex_ingester_active_series"}

	globalMetrics := []string{"cortex_ingester_memory_users", "cortex_ingester_memory_metadata"}
	metricsToCheck := append(userMetrics, globalMetrics...)

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))

	// Wait until TSDB has been closed and removed.
	test.Poll(t, 10*time.Second, 0, func() interface{} {
		i.stoppedMtx.Lock()
		defer i.stoppedMtx.Unlock()
		return len(i.TSDBState.dbs)
	})

	require.Greater(t, testutil.ToFloat64(i.TSDBState.idleTsdbChecks.WithLabelValues(string(tsdbIdleClosed))), float64(0))
	i.updateActiveSeries(context.Background())
	require.Equal(t, int64(0), i.TSDBState.seriesCount.Load()) // Flushing removed all series from memory.

	// Verify that user has disappeared from metrics.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(""), userMetrics...))

	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 0

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 0
    `), "cortex_ingester_memory_users", "cortex_ingester_memory_metadata"))

	// Pushing another sample will recreate TSDB.
	pushSingleSampleWithMetadataV2(t, i)
	i.updateActiveSeries(context.Background())

	// User is back.
	require.NoError(t, testutil.GatherAndCompare(r, strings.NewReader(`
		# HELP cortex_ingester_memory_series_created_total The total number of series that were created per user.
		# TYPE cortex_ingester_memory_series_created_total counter
		cortex_ingester_memory_series_created_total{user="1"} 1

		# HELP cortex_ingester_memory_series_removed_total The total number of series that were removed per user.
		# TYPE cortex_ingester_memory_series_removed_total counter
		cortex_ingester_memory_series_removed_total{user="1"} 0

		# HELP cortex_ingester_memory_users The current number of users in memory.
		# TYPE cortex_ingester_memory_users gauge
		cortex_ingester_memory_users 1

		# HELP cortex_ingester_active_series Number of currently active series per user.
		# TYPE cortex_ingester_active_series gauge
		cortex_ingester_active_series{user="1"} 1

		# HELP cortex_ingester_memory_metadata The current number of metadata in memory.
		# TYPE cortex_ingester_memory_metadata gauge
		cortex_ingester_memory_metadata 1

		# HELP cortex_ingester_memory_metadata_created_total The total number of metadata that were created per user
		# TYPE cortex_ingester_memory_metadata_created_total counter
		cortex_ingester_memory_metadata_created_total{user="1"} 1
    `), metricsToCheck...))
}

func TestIngesterPRW2_CloseTSDBsOnShutdown(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push some data.
	pushSingleSampleWithMetadataV2(t, i)

	db := i.getTSDB(userID)
	require.NotNil(t, db)

	// Stop ingester.
	require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))

	// Verify that DB is no longer in memory, but was closed
	db = i.getTSDB(userID)
	require.Nil(t, db)
}

func TestIngesterPRW2_NotDeleteUnshippedBlocks(t *testing.T) {
	chunkRange := 2 * time.Hour
	chunkRangeMilliSec := chunkRange.Milliseconds()
	cfg := defaultIngesterTestConfig(t)
	cfg.BlocksStorageConfig.TSDB.BlockRanges = []time.Duration{chunkRange}
	cfg.BlocksStorageConfig.TSDB.Retention = time.Millisecond // Which means delete all but first block.
	cfg.LifecyclerConfig.JoinAfter = 0

	// Create ingester
	reg := prometheus.NewPedanticRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, cfg, reg)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds 0
	`), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Push some data to create 3 blocks.
	ctx := user.InjectOrgID(context.Background(), userID)
	for j := int64(0); j < 5; j++ {
		req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}

	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.Nil(t, db.Compact(ctx))

	oldBlocks := db.Blocks()
	require.Equal(t, 3, len(oldBlocks))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, oldBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Saying that we have shipped the second block, so only that should get deleted.
	require.Nil(t, shipper.WriteMetaFile(nil, db.shipperMetadataFilePath, &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(5); j < 6; j++ {
		req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact(ctx))

	// Only the second block should be gone along with a new block.
	newBlocks := db.Blocks()
	require.Equal(t, 3, len(newBlocks))
	require.Equal(t, oldBlocks[0].Meta().ULID, newBlocks[0].Meta().ULID)    // First block remains same.
	require.Equal(t, oldBlocks[2].Meta().ULID, newBlocks[1].Meta().ULID)    // 3rd block becomes 2nd now.
	require.NotEqual(t, oldBlocks[1].Meta().ULID, newBlocks[2].Meta().ULID) // The new block won't match previous 2nd block.

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))

	// Shipping 2 more blocks, hence all the blocks from first round.
	require.Nil(t, shipper.WriteMetaFile(nil, db.shipperMetadataFilePath, &shipper.Meta{
		Version:  shipper.MetaVersion1,
		Uploaded: []ulid.ULID{oldBlocks[1].Meta().ULID, newBlocks[0].Meta().ULID, newBlocks[1].Meta().ULID},
	}))
	require.NoError(t, db.updateCachedShippedBlocks())

	// Add more samples that could trigger another compaction and hence reload of blocks.
	for j := int64(6); j < 7; j++ {
		req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, j*chunkRangeMilliSec)
		_, err := i.PushV2(ctx, req)
		require.NoError(t, err)
	}
	require.Nil(t, db.Compact(ctx))

	// All blocks from the old blocks should be gone now.
	newBlocks2 := db.Blocks()
	require.Equal(t, 2, len(newBlocks2))

	require.Equal(t, newBlocks[2].Meta().ULID, newBlocks2[0].Meta().ULID) // Block created in last round.
	for _, b := range oldBlocks {
		// Second block is not one among old blocks.
		require.NotEqual(t, b.Meta().ULID, newBlocks2[1].Meta().ULID)
	}

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_ingester_oldest_unshipped_block_timestamp_seconds Unix timestamp of the oldest TSDB block not shipped to the storage yet. 0 if ingester has no blocks or all blocks have been shipped.
		# TYPE cortex_ingester_oldest_unshipped_block_timestamp_seconds gauge
		cortex_ingester_oldest_unshipped_block_timestamp_seconds %d
	`, newBlocks2[0].Meta().ULID.Time()/1000)), "cortex_ingester_oldest_unshipped_block_timestamp_seconds"))
}

func TestIngesterPRW2_PushErrorDuringForcedCompaction(t *testing.T) {
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push a sample, it should succeed.
	pushSingleSampleWithMetadataV2(t, i)

	// We mock a flushing by setting the boolean.
	db := i.getTSDB(userID)
	require.NotNil(t, db)
	require.True(t, db.casState(active, forceCompacting))

	// Ingestion should fail with a 503.
	req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()))
	ctx := user.InjectOrgID(context.Background(), userID)
	_, err = i.PushV2(ctx, req)
	require.Equal(t, httpgrpc.Errorf(http.StatusServiceUnavailable, wrapWithUser(errors.New("forced compaction in progress"), userID).Error()), err)

	// Ingestion is successful after a flush.
	require.True(t, db.casState(forceCompacting, active))
	pushSingleSampleWithMetadata(t, i)
}

func TestIngesterPRW2_NoFlushWithInFlightRequest(t *testing.T) {
	registry := prometheus.NewRegistry()
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), registry)
	require.NoError(t, err)

	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), i)
	})

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push few samples.
	for j := 0; j < 5; j++ {
		pushSingleSampleWithMetadataV2(t, i)
	}

	// Verifying that compaction won't happen when a request is in flight.

	// This mocks a request in flight.
	db := i.getTSDB(userID)
	require.NoError(t, db.acquireAppendLock())

	// Flush handler only triggers compactions, but doesn't wait for them to finish. We cannot use ?wait=true here,
	// because it would deadlock -- flush will wait for appendLock to be released.
	i.FlushHandler(httptest.NewRecorder(), httptest.NewRequest("POST", "/flush", nil))

	// Flushing should not have succeeded even after 5 seconds.
	time.Sleep(5 * time.Second)
	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 0
	`), "cortex_ingester_tsdb_compactions_total"))

	// No requests in flight after this.
	db.releaseAppendLock()

	// Let's wait until all head series have been flushed.
	test.Poll(t, 5*time.Second, uint64(0), func() interface{} {
		db := i.getTSDB(userID)
		if db == nil {
			return false
		}
		return db.Head().NumSeries()
	})

	require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(`
		# HELP cortex_ingester_tsdb_compactions_total Total number of TSDB compactions that were executed.
		# TYPE cortex_ingester_tsdb_compactions_total counter
		cortex_ingester_tsdb_compactions_total 1
	`), "cortex_ingester_tsdb_compactions_total"))
}

func TestIngesterPRW2_PushInstanceLimits(t *testing.T) {
	tests := map[string]struct {
		limits          InstanceLimits
		reqs            map[string][]*cortexpb.WriteRequestV2
		expectedErr     error
		expectedErrType interface{}
	}{
		"should succeed creating one user and series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},
			reqs: map[string][]*cortexpb.WriteRequestV2{
				"test": {
					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						[]cortexpb.MetadataV2{
							{Type: cortexpb.COUNTER, HelpRef: 3},
						},
						cortexpb.API,
						"a help for metric_name_1"),
				},
			},
			expectedErr: nil,
		},

		"should fail creating two series": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},
			reqs: map[string][]*cortexpb.WriteRequestV2{
				"test": {
					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						cortexpb.API),

					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test2"}})}, // another series
						[]cortexpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						cortexpb.API),
				},
			},

			expectedErr: wrapWithUser(errMaxSeriesLimitReached, "test"),
		},

		"should fail creating two users": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1},
			reqs: map[string][]*cortexpb.WriteRequestV2{
				"user1": {
					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						cortexpb.API),
				},

				"user2": {
					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test2"}})}, // another series
						[]cortexpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						cortexpb.API),
				},
			},
			expectedErr: wrapWithUser(errMaxUsersLimitReached, "user2"),
		},

		"should fail pushing samples in two requests due to rate limit": {
			limits: InstanceLimits{MaxInMemorySeries: 1, MaxInMemoryTenants: 1, MaxIngestionRate: 0.001},
			reqs: map[string][]*cortexpb.WriteRequestV2{
				"user1": {
					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						cortexpb.API),

					cortexpb.ToWriteRequestV2(
						[]labels.Labels{cortexpb.FromLabelAdaptersToLabels([]cortexpb.LabelAdapter{{Name: labels.MetricName, Value: "test1"}})},
						[]cortexpb.Sample{{Value: 1, TimestampMs: 10}},
						nil,
						nil,
						cortexpb.API),
				},
			},
			expectedErr: errMaxSamplesPushRateLimitReached,
		},
	}

	defaultInstanceLimits = nil

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.LifecyclerConfig.JoinAfter = 0
			cfg.InstanceLimitsFn = func() *InstanceLimits {
				return &testData.limits
			}

			i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is ACTIVE
			test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
				return i.lifecycler.GetState()
			})

			// Iterate through users in sorted order (by username).
			uids := []string{}
			totalPushes := 0
			for uid, requests := range testData.reqs {
				uids = append(uids, uid)
				totalPushes += len(requests)
			}
			sort.Strings(uids)

			pushIdx := 0
			for _, uid := range uids {
				ctx := user.InjectOrgID(context.Background(), uid)

				for _, req := range testData.reqs[uid] {
					pushIdx++
					_, err := i.PushV2(ctx, req)

					if pushIdx < totalPushes {
						require.NoError(t, err)
					} else {
						// Last push may expect error.
						if testData.expectedErr != nil {
							assert.Equal(t, testData.expectedErr, err)
						} else if testData.expectedErrType != nil {
							assert.True(t, errors.As(err, testData.expectedErrType), "expected error type %T, got %v", testData.expectedErrType, err)
						} else {
							assert.NoError(t, err)
						}
					}

					// imitate time ticking between each push
					i.ingestionRate.Tick()

					rate := testutil.ToFloat64(i.metrics.ingestionRate)
					require.NotZero(t, rate)
				}
			}
		})
	}
}

func TestIngesterPRW2_inflightPushRequests(t *testing.T) {
	limits := InstanceLimits{MaxInflightPushRequests: 1}

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.InstanceLimitsFn = func() *InstanceLimits { return &limits }
	cfg.LifecyclerConfig.JoinAfter = 0

	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is ACTIVE
	test.Poll(t, 100*time.Millisecond, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	startCh := make(chan struct{})

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		count := 3500000
		req := generateSamplesForLabelV2(labels.FromStrings(labels.MetricName, fmt.Sprintf("real-%d", count)), count)
		// Signal that we're going to do the real push now.
		close(startCh)

		_, err := i.PushV2(ctx, req)
		return err
	})

	g.Go(func() error {
		select {
		case <-ctx.Done():
		// failed to setup
		case <-startCh:
			// we can start the test.
		}

		time.Sleep(10 * time.Millisecond) // Give first goroutine a chance to start pushing...
		req := generateSamplesForLabelV2(labels.FromStrings(labels.MetricName, "testcase"), 1024)

		_, err := i.PushV2(ctx, req)
		require.Equal(t, errTooManyInflightPushRequests, err)
		return nil
	})

	require.NoError(t, g.Wait())
}

func TestIngesterPRW2_QueryExemplar_MaxInflightQueryRequest(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.DefaultLimits.MaxInflightQueryRequests = 1
	i, err := prepareIngesterWithBlocksStorage(t, cfg, prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until it's ACTIVE
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	i.inflightQueryRequests.Add(1)

	// Mock request
	ctx := user.InjectOrgID(context.Background(), "test")

	wreq, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test_1"}, {Name: "status", Value: "200"}, {Name: "route", Value: "get_user"}}, 1, 100000)
	_, err = i.PushV2(ctx, wreq)
	require.NoError(t, err)

	rreq := &client.ExemplarQueryRequest{}
	_, err = i.QueryExemplars(ctx, rreq)
	require.Error(t, err)
	require.Equal(t, err, errTooManyInflightQueryRequests)
}

func generateSamplesForLabelV2(lbs labels.Labels, count int) *cortexpb.WriteRequestV2 {
	var lbls = make([]labels.Labels, 0, count)
	var samples = make([]cortexpb.Sample, 0, count)

	for i := 0; i < count; i++ {
		samples = append(samples, cortexpb.Sample{
			Value:       float64(i),
			TimestampMs: int64(i),
		})
		lbls = append(lbls, lbs)
	}

	return cortexpb.ToWriteRequestV2(lbls, samples, nil, nil, cortexpb.API)
}

func mockWriteRequestWithMetadataV2(t *testing.T, lbls labels.Labels, value float64, timestamp int64, metadata cortexpb.MetadataV2, additionalSymbols ...string) (*cortexpb.WriteRequestV2, *client.QueryStreamResponse) {
	samples := []cortexpb.Sample{
		{
			TimestampMs: timestamp,
			Value:       value,
		},
	}

	req := cortexpb.ToWriteRequestV2([]labels.Labels{lbls}, samples, nil, []cortexpb.MetadataV2{metadata}, cortexpb.API, additionalSymbols...)

	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	require.NoError(t, err)
	app.Append(timestamp, value)
	chunk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestamp,
						EndTimestampMs:   timestamp,
						Encoding:         int32(encoding.PrometheusXorChunk),
						Data:             chunk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryStreamResChunks
}

func mockHistogramWriteRequestV2(t *testing.T, lbls labels.Labels, value int, timestampMs int64, float bool) (*cortexpb.WriteRequestV2, *client.QueryStreamResponse) {
	var (
		histograms []cortexpb.Histogram
		h          *histogram.Histogram
		fh         *histogram.FloatHistogram
		c          chunkenc.Chunk
	)
	if float {
		fh = tsdbutil.GenerateTestFloatHistogram(value)
		histograms = []cortexpb.Histogram{
			cortexpb.FloatHistogramToHistogramProto(timestampMs, fh),
		}
		c = chunkenc.NewFloatHistogramChunk()
	} else {
		h = tsdbutil.GenerateTestHistogram(value)
		histograms = []cortexpb.Histogram{
			cortexpb.HistogramToHistogramProto(timestampMs, h),
		}
		c = chunkenc.NewHistogramChunk()
	}

	app, err := c.Appender()
	require.NoError(t, err)
	if float {
		_, _, _, err = app.AppendFloatHistogram(nil, timestampMs, fh, true)
	} else {
		_, _, _, err = app.AppendHistogram(nil, timestampMs, h, true)
	}
	require.NoError(t, err)
	c.Compact()

	req := cortexpb.ToWriteRequestV2([]labels.Labels{lbls}, nil, histograms, nil, cortexpb.API)
	enc := int32(encoding.PrometheusHistogramChunk)
	if float {
		enc = int32(encoding.PrometheusFloatHistogramChunk)
	}
	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestampMs,
						EndTimestampMs:   timestampMs,
						Encoding:         enc,
						Data:             c.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryStreamResChunks
}

func mockWriteRequestV2(t *testing.T, lbls labels.Labels, value float64, timestamp int64) (*cortexpb.WriteRequestV2, *client.QueryStreamResponse) {
	samples := []cortexpb.Sample{
		{
			TimestampMs: timestamp,
			Value:       value,
		},
	}

	req := cortexpb.ToWriteRequestV2([]labels.Labels{lbls}, samples, nil, nil, cortexpb.API)

	chunk := chunkenc.NewXORChunk()
	app, err := chunk.Appender()
	require.NoError(t, err)
	app.Append(timestamp, value)
	chunk.Compact()

	expectedQueryStreamResChunks := &client.QueryStreamResponse{
		Chunkseries: []client.TimeSeriesChunk{
			{
				Labels: cortexpb.FromLabelsToLabelAdapters(lbls),
				Chunks: []client.Chunk{
					{
						StartTimestampMs: timestamp,
						EndTimestampMs:   timestamp,
						Encoding:         int32(encoding.PrometheusXorChunk),
						Data:             chunk.Bytes(),
					},
				},
			},
		},
	}

	return req, expectedQueryStreamResChunks
}

func pushSingleSampleWithMetadataV2(t *testing.T, i *Ingester) {
	ctx := user.InjectOrgID(context.Background(), userID)
	metadata := cortexpb.MetadataV2{
		Type:    cortexpb.COUNTER,
		HelpRef: 3,
		UnitRef: 0,
	}

	req, _ := mockWriteRequestWithMetadataV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, util.TimeToMillis(time.Now()), metadata, "a help for metric")
	_, err := i.PushV2(ctx, req)
	require.NoError(t, err)
}

func pushSingleSampleAtTimeV2(t *testing.T, i *Ingester, ts int64) {
	ctx := user.InjectOrgID(context.Background(), userID)
	req, _ := mockWriteRequestV2(t, labels.Labels{{Name: labels.MetricName, Value: "test"}}, 0, ts)
	_, err := i.PushV2(ctx, req)
	require.NoError(t, err)
}

func writeRequestSingleSeriesV2(lbls labels.Labels, samples []cortexpb.Sample) *cortexpb.WriteRequestV2 {
	req := &cortexpb.WriteRequestV2{
		Source: cortexpb.API,
	}

	st := writev2.NewSymbolTable()
	ts := cortexpb.TimeSeriesV2{}
	ts.Samples = samples
	ts.LabelsRefs = st.SymbolizeLabels(lbls, nil)
	req.Timeseries = append(req.Timeseries, cortexpb.PreallocTimeseriesV2{TimeSeriesV2: &ts})
	req.Symbols = st.Symbols()

	return req
}

// createIngesterWithSeries creates an ingester and push numSeries with numSamplesPerSeries each.
func createIngesterWithSeriesV2(t testing.TB, userID string, numSeries, numSamplesPerSeries int, startTimestamp, step int64) *Ingester {
	const maxBatchSize = 1000

	// Create ingester.
	i, err := prepareIngesterWithBlocksStorage(t, defaultIngesterTestConfig(t), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), i))
	})

	// Wait until it's ACTIVE.
	test.Poll(t, 1*time.Second, ring.ACTIVE, func() interface{} {
		return i.lifecycler.GetState()
	})

	// Push fixtures.
	ctx := user.InjectOrgID(context.Background(), userID)

	for ts := startTimestamp; ts < startTimestamp+(step*int64(numSamplesPerSeries)); ts += step {
		for o := 0; o < numSeries; o += maxBatchSize {
			batchSize := min(maxBatchSize, numSeries-o)

			// Generate metrics and samples (1 for each series).
			metrics := make([]labels.Labels, 0, batchSize)
			samples := make([]cortexpb.Sample, 0, batchSize)

			for s := 0; s < batchSize; s++ {
				metrics = append(metrics, labels.Labels{
					{Name: labels.MetricName, Value: fmt.Sprintf("test_%d", o+s)},
				})

				samples = append(samples, cortexpb.Sample{
					TimestampMs: ts,
					Value:       1,
				})
			}

			// Send metrics to the ingester.
			req := cortexpb.ToWriteRequestV2(metrics, samples, nil, nil, cortexpb.API)
			_, err := i.PushV2(ctx, req)
			require.NoError(t, err)
		}
	}

	return i
}
