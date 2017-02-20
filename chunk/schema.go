package chunk

import (
	"encoding/base64"
	"flag"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"

	"github.com/weaveworks/cortex/util"
)

const (
	secondsInHour = int64(time.Hour / time.Second)
	secondsInDay  = int64(24 * time.Hour / time.Second)
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
	HashKey   string
	RangeKey  []byte
}

// SchemaConfig contains the config for our chunk index schemas
type SchemaConfig struct {
	PeriodicTableConfig
	OriginalTableName string

	// After midnight on this day, we start bucketing indexes by day instead of by
	// hour.  Only the day matters, not the time within the day.
	DailyBucketsFrom util.DayValue

	// After this time, we will only query for base64-encoded label values.
	Base64ValuesFrom util.DayValue

	// After this time, we will read and write v4 schemas.
	V4SchemaFrom util.DayValue
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *SchemaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.PeriodicTableConfig.RegisterFlags(f)

	f.Var(&cfg.DailyBucketsFrom, "dynamodb.daily-buckets-from", "The date (in the format YYYY-MM-DD) of the first day for which DynamoDB index buckets should be day-sized vs. hour-sized.")
	f.Var(&cfg.Base64ValuesFrom, "dynamodb.base64-buckets-from", "The date (in the format YYYY-MM-DD) after which we will stop querying to non-base64 encoded values.")
	f.Var(&cfg.V4SchemaFrom, "dynamodb.v4-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v4 schema.")
}

func (cfg *SchemaConfig) tableForBucket(bucketStart int64) string {
	if !cfg.UsePeriodicTables || bucketStart < (cfg.PeriodicTableStartAt.Unix()) {
		return cfg.OriginalTableName
	}
	// TODO remove reference to time package here
	return cfg.TablePrefix + strconv.Itoa(int(bucketStart/int64(cfg.TablePeriod/time.Second)))
}

type bucketCallback func(from, through model.Time, tableName, hashKey string) ([]IndexEntry, error)

func (cfg SchemaConfig) hourlyBuckets(from, through model.Time, userID string, metricName model.LabelValue, callback bucketCallback) ([]IndexEntry, error) {
	var (
		fromHour    = from.Unix() / secondsInHour
		throughHour = through.Unix() / secondsInHour
		result      = []IndexEntry{}
	)

	for i := fromHour; i <= throughHour; i++ {
		from := model.TimeFromUnix(i * secondsInHour)
		through := model.TimeFromUnix((i + 1) * secondsInHour)
		entries, err := callback(from, through, cfg.tableForBucket(i*secondsInHour), fmt.Sprintf("%s:%d:%s", userID, i, metricName))
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

func (cfg SchemaConfig) dailyBuckets(from, through model.Time, userID string, metricName model.LabelValue, callback bucketCallback) ([]IndexEntry, error) {
	var (
		fromDay    = from.Unix() / secondsInDay
		throughDay = through.Unix() / secondsInDay
		result     = []IndexEntry{}
	)

	for i := fromDay; i <= throughDay; i++ {
		from := model.TimeFromUnix(i * secondsInDay)
		through := model.TimeFromUnix((i + 1) * secondsInDay)
		entries, err := callback(from, through, cfg.tableForBucket(i*secondsInDay), fmt.Sprintf("%s:d%d:%s", userID, i, metricName))
		if err != nil {
			return nil, err
		}
		result = append(result, entries...)
	}
	return result, nil
}

// compositeSchema is a Schema which delegates to various schemas depending
// on when they were activated.
type compositeSchema struct {
	schemas []compositeSchemaEntry
}

type compositeSchemaEntry struct {
	start model.Time
	Schema
}

type byStart []compositeSchemaEntry

func (a byStart) Len() int           { return len(a) }
func (a byStart) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byStart) Less(i, j int) bool { return a[i].start < a[j].start }

func newCompositeSchema(cfg SchemaConfig) (Schema, error) {
	schemas := []compositeSchemaEntry{
		{0, v1Schema(cfg)},
	}

	if cfg.DailyBucketsFrom.IsSet() {
		schemas = append(schemas, compositeSchemaEntry{cfg.DailyBucketsFrom.Time, v2Schema(cfg)})
	}

	if cfg.Base64ValuesFrom.IsSet() {
		schemas = append(schemas, compositeSchemaEntry{cfg.Base64ValuesFrom.Time, v3Schema(cfg)})
	}

	if cfg.V4SchemaFrom.IsSet() {
		schemas = append(schemas, compositeSchemaEntry{cfg.V4SchemaFrom.Time, v4Schema(cfg)})
	}

	if !sort.IsSorted(byStart(schemas)) {
		return nil, fmt.Errorf("schemas not in time-sorted order")
	}

	return compositeSchema{schemas}, nil
}

func (c compositeSchema) forSchemas(from, through model.Time, callback func(from, through model.Time, schema Schema) ([]IndexEntry, error)) ([]IndexEntry, error) {
	if len(c.schemas) == 0 {
		return nil, nil
	}

	// first, find the schema with the highest start _before or at_ from
	i := sort.Search(len(c.schemas), func(i int) bool {
		return c.schemas[i].start > from
	})
	if i > 0 {
		i--
	} else {
		// This could happen if we get passed a sample from before 1970.
		i = 0
		from = c.schemas[0].start
	}

	// next, find the schema with the lowest start _after_ through
	j := sort.Search(len(c.schemas), func(j int) bool {
		return c.schemas[j].start > through
	})

	min := func(a, b model.Time) model.Time {
		if a < b {
			return a
		}
		return b
	}

	start := from
	result := []IndexEntry{}
	for ; i < j; i++ {
		nextSchemaStarts := model.Latest
		if i+1 < len(c.schemas) {
			nextSchemaStarts = c.schemas[i+1].start
		}

		// If the next schema starts at the same time as this one,
		// skip this one.
		if nextSchemaStarts == c.schemas[i].start {
			continue
		}

		end := min(through, nextSchemaStarts-1)
		entries, err := callback(start, end, c.schemas[i].Schema)
		if err != nil {
			return nil, err
		}

		result = append(result, entries...)
		start = nextSchemaStarts
	}

	return result, nil
}

func (c compositeSchema) GetWriteEntries(from, through model.Time, userID string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	return c.forSchemas(from, through, func(from, through model.Time, schema Schema) ([]IndexEntry, error) {
		return schema.GetWriteEntries(from, through, userID, metricName, labels, chunkID)
	})
}

func (c compositeSchema) GetReadEntriesForMetric(from, through model.Time, userID string, metricName model.LabelValue) ([]IndexEntry, error) {
	return c.forSchemas(from, through, func(from, through model.Time, schema Schema) ([]IndexEntry, error) {
		return schema.GetReadEntriesForMetric(from, through, userID, metricName)
	})
}

func (c compositeSchema) GetReadEntriesForMetricLabel(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return c.forSchemas(from, through, func(from, through model.Time, schema Schema) ([]IndexEntry, error) {
		return schema.GetReadEntriesForMetricLabel(from, through, userID, metricName, labelName)
	})
}

func (c compositeSchema) GetReadEntriesForMetricLabelValue(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	return c.forSchemas(from, through, func(from, through model.Time, schema Schema) ([]IndexEntry, error) {
		return schema.GetReadEntriesForMetricLabelValue(from, through, userID, metricName, labelName, labelValue)
	})
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

// v2Schame went to daily buckets in the hash key
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

// schema implements Schema given a bucketing function and and set of range key callbacks
type schema struct {
	buckets func(from, through model.Time, userID string, metricName model.LabelValue, callback bucketCallback) ([]IndexEntry, error)
	entries entries
}

func (s schema) GetWriteEntries(from, through model.Time, userID string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(_, _ model.Time, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetWriteEntries(tableName, hashKey, labels, chunkID)
	})
}

func (s schema) GetReadEntriesForMetric(from, through model.Time, userID string, metricName model.LabelValue) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, s.entries.GetReadMetricEntries)
}

func (s schema) GetReadEntriesForMetricLabel(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(from, through model.Time, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricLabelEntries(from, through, tableName, hashKey, labelName)
	})
}

func (s schema) GetReadEntriesForMetricLabelValue(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(from, through model.Time, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricLabelValueEntries(from, through, tableName, hashKey, labelName, labelValue)
	})
}

type entries interface {
	GetWriteEntries(tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error)
	GetReadMetricEntries(from, through model.Time, tableName, hashKey string) ([]IndexEntry, error)
	GetReadMetricLabelEntries(from, through model.Time, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error)
	GetReadMetricLabelValueEntries(from, through model.Time, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error)
}

type originalEntries struct{}

func (originalEntries) GetWriteEntries(tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	result := []IndexEntry{}
	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		if strings.ContainsRune(string(value), '\x00') {
			return nil, fmt.Errorf("label values cannot contain null byte")
		}
		result = append(result, IndexEntry{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  buildRangeKey(string(key), string(value), chunkID),
		})
	}
	return result, nil
}

func (originalEntries) GetReadMetricEntries(_, _ model.Time, tableName, hashKey string) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  nil,
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelEntries(_, _ model.Time, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  buildRangeKey(string(labelName)),
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelValueEntries(_, _ model.Time, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	if strings.ContainsRune(string(labelValue), '\x00') {
		return nil, fmt.Errorf("label values cannot contain null byte")
	}
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  buildRangeKey(string(labelName), string(labelValue)),
		},
	}, nil
}

type base64Entries struct {
	originalEntries
}

func (base64Entries) GetWriteEntries(tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	result := []IndexEntry{}
	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		encodedValue := base64.RawStdEncoding.EncodeToString([]byte(value))
		result = append(result, IndexEntry{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  buildRangeKey(string(key), encodedValue, chunkID, "1"),
		})
	}
	return result, nil
}

func (base64Entries) GetReadMetricLabelValueEntries(_, _ model.Time, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedValue := base64.RawStdEncoding.EncodeToString([]byte(labelValue))
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  buildRangeKey(string(labelName), string(encodedValue)),
		},
	}, nil
}

type labelNameInHashKeyEntries struct{}

func (labelNameInHashKeyEntries) GetWriteEntries(tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	entries := []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  buildRangeKey("", "", chunkID, "2"),
		},
	}

	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		encodedValue := base64.RawStdEncoding.EncodeToString([]byte(value))
		entries = append(entries, IndexEntry{
			TableName: tableName,
			HashKey:   hashKey + ":" + string(key),
			RangeKey:  buildRangeKey("", encodedValue, chunkID, "1"),
		})
	}

	return entries, nil
}

func (labelNameInHashKeyEntries) GetReadMetricEntries(_, _ model.Time, tableName, hashKey string) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey,
			RangeKey:  nil,
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelEntries(_, _ model.Time, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey + ":" + string(labelName),
			RangeKey:  buildRangeKey(""),
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelValueEntries(_, _ model.Time, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedValue := base64.RawStdEncoding.EncodeToString([]byte(labelValue))
	return []IndexEntry{
		{
			TableName: tableName,
			HashKey:   hashKey + ":" + string(labelName),
			RangeKey:  buildRangeKey("", encodedValue),
		},
	}, nil
}

func buildRangeKey(ss ...string) []byte {
	length := 0
	for _, s := range ss {
		length += len(s) + 1
	}
	output, i := make([]byte, length, length), 0
	for _, s := range ss {
		copy(output[i:i+len(s)], s)
		i += len(s) + 1
	}
	return output
}

func parseRangeValue(v []byte) (model.LabelValue, string, error) {
	var (
		valueBytes   []byte
		chunkIDBytes []byte
		version      []byte
		i, j         = 0, 0
	)
	next := func(output *[]byte) error {
		for ; j < len(v); j++ {
			if v[j] != 0 {
				continue
			}

			if output != nil {
				*output = v[i:j]
			}

			j++
			i = j
			return nil
		}
		return fmt.Errorf("invalid range value: %x", v)
	}
	if err := next(nil); err != nil {
		return "", "", err
	}
	if err := next(&valueBytes); err != nil {
		return "", "", err
	}
	if err := next(&chunkIDBytes); err != nil {
		return "", "", err
	}
	if err := next(&version); err == nil {
		// We read a version, need to decode value
		decodedValueLen := base64.RawStdEncoding.DecodedLen(len(valueBytes))
		decodedValueBytes := make([]byte, decodedValueLen, decodedValueLen)
		if _, err := base64.RawStdEncoding.Decode(decodedValueBytes, valueBytes); err != nil {
			return "", "", err
		}
		valueBytes = decodedValueBytes
	}
	return model.LabelValue(valueBytes), string(chunkIDBytes), nil
}
