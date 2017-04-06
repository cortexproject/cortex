package chunk

import (
	"crypto/sha1"

	"fmt"
	"strings"
	"time"

	"github.com/prometheus/common/model"
	"github.com/weaveworks/cortex/util"
)

const (
	secondsInHour      = int64(time.Hour / time.Second)
	secondsInDay       = int64(24 * time.Hour / time.Second)
	millisecondsInHour = int64(time.Hour / time.Millisecond)
	millisecondsInDay  = int64(24 * time.Hour / time.Millisecond)
)

var (
	rangeKeyV1 = []byte{'1'}
	rangeKeyV2 = []byte{'2'}
	rangeKeyV3 = []byte{'3'}
	rangeKeyV4 = []byte{'4'}
	rangeKeyV5 = []byte{'5'}
	rangeKeyV6 = []byte{'6'}
)

// Schema interface defines methods to calculate the hash and range keys needed
// to write or read chunks from the external index.
type Schema interface {
	// When doing a write, use this method to return the list of entries you should write to.
	GetWriteEntries(from, through model.Time, userID string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error)

	// When doing a read, use these methods to return the list of entries you should query
	GetReadEntriesForMetric(from, through model.Time, userID string, metricName model.LabelValue) ([]IndexEntry, error)
	GetReadEntriesForMetricLabel(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error)
	GetReadEntriesForMetricLabelValue(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error)
}

// IndexEntry describes an entry in the chunk index
type IndexEntry struct {
	TableName string
	HashValue string

	// For writes, RangeValue will always be set.
	RangeValue []byte

	// New for v6 schema, label value is not written as part of the range key.
	Value []byte

	// For reads, one of RangeValuePrefix or RangeValueStart might be set:
	// - If RangeValuePrefix is not nil, must read all keys with that prefix.
	// - If RangeValueStart is not nil, must read all keys from there onwards.
	// - If neither is set, must read all keys for that row.
	RangeValuePrefix []byte
	RangeValueStart  []byte
}

// v1Schema was:
// - hash key: <userid>:<hour bucket>:<metric name>
// - range key: <label name>\0<label value>\0<chunk name>
func v1Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.hourlyBuckets,
		originalEntries{},
	}
}

// v2Schema went to daily buckets in the hash key
// - hash key: <userid>:d<day bucket>:<metric name>
func v2Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.dailyBuckets,
		originalEntries{},
	}
}

// v3Schema went to base64 encoded label values & a version ID
// - range key: <label name>\0<base64(label value)>\0<chunk name>\0<version 1>
func v3Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.dailyBuckets,
		base64Entries{originalEntries{}},
	}
}

// v4 schema went to two schemas in one:
// 1) - hash key: <userid>:<hour bucket>:<metric name>:<label name>
//    - range key: \0<base64(label value)>\0<chunk name>\0<version 2>
// 2) - hash key: <userid>:<hour bucket>:<metric name>
//    - range key: \0\0<chunk name>\0<version 3>
func v4Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.dailyBuckets,
		labelNameInHashKeyEntries{},
	}
}

// v5 schema is an extension of v4, with the chunk end time in the
// range key to improve query latency.  However, it did it wrong
// so the chunk end times are ignored.
func v5Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.dailyBuckets,
		v5Entries{},
	}
}

// v6 schema is an extension of v5, with correct chunk end times, and
// the label value moved out of the range key.
func v6Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.dailyBuckets,
		v6Entries{},
	}
}

// v7 schema is an extension of v6, with support for queries with no metric names
func v7Schema(cfg SchemaConfig) Schema {
	return schema{
		cfg.dailyBuckets,
		v7Entries{},
	}
}

// schema implements Schema given a bucketing function and and set of range key callbacks
type schema struct {
	buckets func(from, through model.Time, userID string, metricName model.LabelValue, callback bucketCallback) ([]IndexEntry, error)
	entries entries
}

func (s schema) GetWriteEntries(from, through model.Time, userID string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, bucketHashKey string) ([]IndexEntry, error) {
		return s.entries.GetWriteEntries(bucketFrom, bucketThrough, tableName, bucketHashKey, metricName, labels, chunkID)
	})
}

func (s schema) GetReadEntriesForMetric(from, through model.Time, userID string, metricName model.LabelValue) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, bucketHashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricEntries(bucketFrom, bucketThrough, tableName, bucketHashKey, metricName)
	})
}

func (s schema) GetReadEntriesForMetricLabel(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, bucketHashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricLabelEntries(bucketFrom, bucketThrough, tableName, bucketHashKey, metricName, labelName)
	})
}

func (s schema) GetReadEntriesForMetricLabelValue(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, bucketHashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricLabelValueEntries(bucketFrom, bucketThrough, tableName, bucketHashKey, metricName, labelName, labelValue)
	})
}

type entries interface {
	GetWriteEntries(from, through uint32, tableName, hashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error)
	GetReadMetricEntries(from, through uint32, tableName, hashKey string, metricName model.LabelValue) ([]IndexEntry, error)
	GetReadMetricLabelEntries(from, through uint32, tableName, hashKey string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error)
	GetReadMetricLabelValueEntries(from, through uint32, tableName, hashKey string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error)
}

type originalEntries struct{}

func (originalEntries) GetWriteEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	result := []IndexEntry{}
	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		if strings.ContainsRune(string(value), '\x00') {
			return nil, fmt.Errorf("label values cannot contain null byte")
		}
		result = append(result, IndexEntry{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName),
			RangeValue: buildRangeKey([]byte(key), []byte(value), chunkIDBytes),
		})
	}
	return result, nil
}

func (originalEntries) GetReadMetricEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        bucketHashKey + ":" + string(metricName),
			RangeValuePrefix: nil,
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        bucketHashKey + ":" + string(metricName),
			RangeValuePrefix: buildRangeKey([]byte(labelName)),
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	if strings.ContainsRune(string(labelValue), '\x00') {
		return nil, fmt.Errorf("label values cannot contain null byte")
	}
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        bucketHashKey + ":" + string(metricName),
			RangeValuePrefix: buildRangeKey([]byte(labelName), []byte(labelValue)),
		},
	}, nil
}

type base64Entries struct {
	originalEntries
}

func (base64Entries) GetWriteEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	result := []IndexEntry{}
	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}

		encodedBytes := encodeBase64Value(value)
		result = append(result, IndexEntry{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName),
			RangeValue: buildRangeKey([]byte(key), encodedBytes, chunkIDBytes, rangeKeyV1),
		})
	}
	return result, nil
}

func (base64Entries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedBytes := encodeBase64Value(labelValue)
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        bucketHashKey + ":" + string(metricName),
			RangeValuePrefix: buildRangeKey([]byte(labelName), encodedBytes),
		},
	}, nil
}

type labelNameInHashKeyEntries struct{}

func (labelNameInHashKeyEntries) GetWriteEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName),
			RangeValue: buildRangeKey(nil, nil, chunkIDBytes, rangeKeyV2),
		},
	}

	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		encodedBytes := encodeBase64Value(value)
		entries = append(entries, IndexEntry{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName) + ":" + string(key),
			RangeValue: buildRangeKey(nil, encodedBytes, chunkIDBytes, rangeKeyV1),
		})
	}

	return entries, nil
}

func (labelNameInHashKeyEntries) GetReadMetricEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: bucketHashKey + ":" + string(metricName),
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: bucketHashKey + ":" + string(metricName) + ":" + string(labelName),
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedBytes := encodeBase64Value(labelValue)
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        bucketHashKey + ":" + string(metricName) + ":" + string(labelName),
			RangeValuePrefix: buildRangeKey(nil, encodedBytes),
		},
	}, nil
}

// v5Entries includes chunk end time in range key - see #298.
type v5Entries struct{}

func (v5Entries) GetWriteEntries(_, through uint32, tableName, bucketHashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(through)

	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName),
			RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV3),
		},
	}

	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		encodedValueBytes := encodeBase64Value(value)
		entries = append(entries, IndexEntry{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName) + ":" + string(key),
			RangeValue: buildRangeKey(encodedThroughBytes, encodedValueBytes, chunkIDBytes, rangeKeyV4),
		})
	}

	return entries, nil
}

func (v5Entries) GetReadMetricEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: bucketHashKey + ":" + string(metricName),
		},
	}, nil
}

func (v5Entries) GetReadMetricLabelEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: bucketHashKey + ":" + string(metricName) + ":" + string(labelName),
		},
	}, nil
}

func (v5Entries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName, _ model.LabelValue) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: bucketHashKey + ":" + string(metricName) + ":" + string(labelName),
		},
	}, nil
}

// v6Entries fixes issues with v5 time encoding being wrong (see #337), and
// moves label value out of range key (see #199).
type v6Entries struct{}

func (v6Entries) GetWriteEntries(_, through uint32, tableName, bucketHashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(through)

	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName),
			RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV3),
		},
	}

	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		entries = append(entries, IndexEntry{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName) + ":" + string(key),
			RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV5),
			Value:      []byte(value),
		})
	}

	return entries, nil
}

func (v6Entries) GetReadMetricEntries(from, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue) ([]IndexEntry, error) {
	encodedFromBytes := encodeTime(from)
	return []IndexEntry{
		{
			TableName:       tableName,
			HashValue:       bucketHashKey + ":" + string(metricName),
			RangeValueStart: buildRangeKey(encodedFromBytes),
		},
	}, nil
}

func (v6Entries) GetReadMetricLabelEntries(from, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	encodedFromBytes := encodeTime(from)
	return []IndexEntry{
		{
			TableName:       tableName,
			HashValue:       bucketHashKey + ":" + string(metricName) + ":" + string(labelName),
			RangeValueStart: buildRangeKey(encodedFromBytes),
		},
	}, nil
}

func (v6Entries) GetReadMetricLabelValueEntries(from, _ uint32, tableName, bucketHashKey string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedFromBytes := encodeTime(from)
	return []IndexEntry{
		{
			TableName:       tableName,
			HashValue:       bucketHashKey + ":" + string(metricName) + ":" + string(labelName),
			RangeValueStart: buildRangeKey(encodedFromBytes),
		},
	}, nil
}

// v7Entries supports queries with no metric name
type v7Entries struct {
	v6Entries
}

func (v7Entries) GetWriteEntries(_, through uint32, tableName, bucketHashKey string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	metricName, err := util.ExtractMetricNameFromMetric(labels)
	if err != nil {
		return nil, err
	}

	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(through)
	metricNameHashBytes := sha1.Sum([]byte(metricName))

	// Add IndexEntry with userID:bigBucket HashValue
	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  bucketHashKey,
			RangeValue: buildRangeKey(nil, nil, metricNameHashBytes[:], rangeKeyV6),
			Value:      []byte(metricName),
		},
	}

	// Add IndexEntry with userID:bigBucket:metricName HashValue
	entries = append(entries, IndexEntry{
		TableName:  tableName,
		HashValue:  bucketHashKey + ":" + string(metricName),
		RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV3),
	})

	// Add IndexEntries with userID:bigBucket:metricName:labelName HashValue
	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		entries = append(entries, IndexEntry{
			TableName:  tableName,
			HashValue:  bucketHashKey + ":" + string(metricName) + ":" + string(key),
			RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV5),
			Value:      []byte(value),
		})
	}

	return entries, nil
}

func (v7Entries) GetReadEntries(from, _ uint32, tableName, bucketHashKey string) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: bucketHashKey,
		},
	}, nil
}
