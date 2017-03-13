package chunk

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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

	// After this time, we will read and write v5 schemas.
	V5SchemaFrom util.DayValue
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *SchemaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.PeriodicTableConfig.RegisterFlags(f)

	f.Var(&cfg.DailyBucketsFrom, "dynamodb.daily-buckets-from", "The date (in the format YYYY-MM-DD) of the first day for which DynamoDB index buckets should be day-sized vs. hour-sized.")
	f.Var(&cfg.Base64ValuesFrom, "dynamodb.base64-buckets-from", "The date (in the format YYYY-MM-DD) after which we will stop querying to non-base64 encoded values.")
	f.Var(&cfg.V4SchemaFrom, "dynamodb.v4-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v4 schema.")
	f.Var(&cfg.V5SchemaFrom, "dynamodb.v5-schema-from", "The date (in the format YYYY-MM-DD) after which we enable v5 schema.")
}

func (cfg *SchemaConfig) tableForBucket(bucketStart int64) string {
	if !cfg.UsePeriodicTables || bucketStart < (cfg.PeriodicTableStartAt.Unix()) {
		return cfg.OriginalTableName
	}
	// TODO remove reference to time package here
	return cfg.TablePrefix + strconv.Itoa(int(bucketStart/int64(cfg.TablePeriod/time.Second)))
}

type bucketCallback func(from, through uint32, tableName, hashKey string) ([]IndexEntry, error)

func (cfg SchemaConfig) hourlyBuckets(from, through model.Time, userID string, metricName model.LabelValue, callback bucketCallback) ([]IndexEntry, error) {
	var (
		fromHour    = from.Unix() / secondsInHour
		throughHour = through.Unix() / secondsInHour
		result      = []IndexEntry{}
	)

	for i := fromHour; i <= throughHour; i++ {
		relativeFrom := util.Max64(0, int64(from)-(i*millisecondsInHour))
		relativeThrough := util.Min64(millisecondsInHour, int64(through)-(i*millisecondsInDay))
		entries, err := callback(uint32(relativeFrom), uint32(relativeThrough), cfg.tableForBucket(i*secondsInHour), fmt.Sprintf("%s:%d:%s", userID, i, metricName))
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
		// The idea here is that the hash key contains the bucket start time (rounded to
		// the nearest day).  The range key can contain the offset from that, to the
		// (start/end) of the chunk. For chunks that span multiple buckets, these
		// offsets will be capped to the bucket boundaries, i.e. start will be
		// positive in the first bucket, then zero in the next etc.
		//
		// The reason for doing all this is to reduce the size of the time stamps we
		// include in the range keys - we use a uint32 - as we then have to base 32
		// encode it.

		relativeFrom := util.Max64(0, int64(from)-(i*millisecondsInDay))
		relativeThrough := util.Min64(millisecondsInDay, int64(through)-(i*millisecondsInDay))
		entries, err := callback(uint32(relativeFrom), uint32(relativeThrough), cfg.tableForBucket(i*secondsInDay), fmt.Sprintf("%s:d%d:%s", userID, i, metricName))
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

	if cfg.V5SchemaFrom.IsSet() {
		schemas = append(schemas, compositeSchemaEntry{cfg.V5SchemaFrom.Time, v5Schema(cfg)})
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

// schema implements Schema given a bucketing function and and set of range key callbacks
type schema struct {
	buckets func(from, through model.Time, userID string, metricName model.LabelValue, callback bucketCallback) ([]IndexEntry, error)
	entries entries
}

func (s schema) GetWriteEntries(from, through model.Time, userID string, metricName model.LabelValue, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetWriteEntries(bucketFrom, bucketThrough, tableName, hashKey, labels, chunkID)
	})
}

func (s schema) GetReadEntriesForMetric(from, through model.Time, userID string, metricName model.LabelValue) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricEntries(bucketFrom, bucketThrough, tableName, hashKey)
	})
}

func (s schema) GetReadEntriesForMetricLabel(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricLabelEntries(bucketFrom, bucketThrough, tableName, hashKey, labelName)
	})
}

func (s schema) GetReadEntriesForMetricLabelValue(from, through model.Time, userID string, metricName model.LabelValue, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	return s.buckets(from, through, userID, metricName, func(bucketFrom, bucketThrough uint32, tableName, hashKey string) ([]IndexEntry, error) {
		return s.entries.GetReadMetricLabelValueEntries(bucketFrom, bucketThrough, tableName, hashKey, labelName, labelValue)
	})
}

type entries interface {
	GetWriteEntries(from, through uint32, tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error)
	GetReadMetricEntries(from, through uint32, tableName, hashKey string) ([]IndexEntry, error)
	GetReadMetricLabelEntries(from, through uint32, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error)
	GetReadMetricLabelValueEntries(from, through uint32, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error)
}

type originalEntries struct{}

func (originalEntries) GetWriteEntries(_, _ uint32, tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
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
			HashValue:  hashKey,
			RangeValue: buildRangeKey([]byte(key), []byte(value), chunkIDBytes),
		})
	}
	return result, nil
}

func (originalEntries) GetReadMetricEntries(_, _ uint32, tableName, hashKey string) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        hashKey,
			RangeValuePrefix: nil,
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        hashKey,
			RangeValuePrefix: buildRangeKey([]byte(labelName)),
		},
	}, nil
}

func (originalEntries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	if strings.ContainsRune(string(labelValue), '\x00') {
		return nil, fmt.Errorf("label values cannot contain null byte")
	}
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        hashKey,
			RangeValuePrefix: buildRangeKey([]byte(labelName), []byte(labelValue)),
		},
	}, nil
}

type base64Entries struct {
	originalEntries
}

func (base64Entries) GetWriteEntries(_, _ uint32, tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	result := []IndexEntry{}
	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}

		encodedBytes := encodeBase64Value(value)
		result = append(result, IndexEntry{
			TableName:  tableName,
			HashValue:  hashKey,
			RangeValue: buildRangeKey([]byte(key), encodedBytes, chunkIDBytes, rangeKeyV1),
		})
	}
	return result, nil
}

func (base64Entries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedBytes := encodeBase64Value(labelValue)
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        hashKey,
			RangeValuePrefix: buildRangeKey([]byte(labelName), encodedBytes),
		},
	}, nil
}

type labelNameInHashKeyEntries struct{}

func (labelNameInHashKeyEntries) GetWriteEntries(_, _ uint32, tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  hashKey,
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
			HashValue:  hashKey + ":" + string(key),
			RangeValue: buildRangeKey(nil, encodedBytes, chunkIDBytes, rangeKeyV1),
		})
	}

	return entries, nil
}

func (labelNameInHashKeyEntries) GetReadMetricEntries(_, _ uint32, tableName, hashKey string) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: hashKey,
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: hashKey + ":" + string(labelName),
		},
	}, nil
}

func (labelNameInHashKeyEntries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedBytes := encodeBase64Value(labelValue)
	return []IndexEntry{
		{
			TableName:        tableName,
			HashValue:        hashKey + ":" + string(labelName),
			RangeValuePrefix: buildRangeKey(nil, encodedBytes),
		},
	}, nil
}

// v5Entries includes chunk end time in range key - see #298.
type v5Entries struct{}

func encodeTime(t uint32) []byte {
	// timestamps are hex encoded such that it doesn't contain null byte,
	// but is still lexicographically sortable.
	throughBytes := make([]byte, 4, 4)
	binary.BigEndian.PutUint32(throughBytes, t)
	encodedThroughBytes := make([]byte, 8, 8)
	hex.Encode(encodedThroughBytes, throughBytes)
	return encodedThroughBytes
}

func decodeTime(bs []byte) uint32 {
	buf := make([]byte, 4, 4)
	hex.Decode(buf, bs)
	return binary.BigEndian.Uint32(buf)
}

func (v5Entries) GetWriteEntries(_, through uint32, tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(through)

	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  hashKey,
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
			HashValue:  hashKey + ":" + string(key),
			RangeValue: buildRangeKey(encodedThroughBytes, encodedValueBytes, chunkIDBytes, rangeKeyV4),
		})
	}

	return entries, nil
}

func (v5Entries) GetReadMetricEntries(_, _ uint32, tableName, hashKey string) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: hashKey,
		},
	}, nil
}

func (v5Entries) GetReadMetricLabelEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: hashKey + ":" + string(labelName),
		},
	}, nil
}

func (v5Entries) GetReadMetricLabelValueEntries(_, _ uint32, tableName, hashKey string, labelName model.LabelName, _ model.LabelValue) ([]IndexEntry, error) {
	return []IndexEntry{
		{
			TableName: tableName,
			HashValue: hashKey + ":" + string(labelName),
		},
	}, nil
}

// v6Entries fixes issues with v5 time encoding being wrong (see #337), and
// moves label value out of range key (see #199).
type v6Entries struct{}

func (v6Entries) GetWriteEntries(_, through uint32, tableName, hashKey string, labels model.Metric, chunkID string) ([]IndexEntry, error) {
	chunkIDBytes := []byte(chunkID)
	encodedThroughBytes := encodeTime(through)

	entries := []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  hashKey,
			RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV3),
		},
	}

	for key, value := range labels {
		if key == model.MetricNameLabel {
			continue
		}
		entries = append(entries, IndexEntry{
			TableName:  tableName,
			HashValue:  hashKey + ":" + string(key),
			RangeValue: buildRangeKey(encodedThroughBytes, nil, chunkIDBytes, rangeKeyV5),
			Value:      []byte(value),
		})
	}

	return entries, nil
}

func (v6Entries) GetReadMetricEntries(_, through uint32, tableName, hashKey string) ([]IndexEntry, error) {
	encodedThroughBytes := encodeTime(through)
	return []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  hashKey,
			RangeValue: buildRangeKey(encodedThroughBytes),
		},
	}, nil
}

func (v6Entries) GetReadMetricLabelEntries(_, through uint32, tableName, hashKey string, labelName model.LabelName) ([]IndexEntry, error) {
	encodedThroughBytes := encodeTime(through)
	return []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  hashKey + ":" + string(labelName),
			RangeValue: buildRangeKey(encodedThroughBytes),
		},
	}, nil
}

func (v6Entries) GetReadMetricLabelValueEntries(_, through uint32, tableName, hashKey string, labelName model.LabelName, labelValue model.LabelValue) ([]IndexEntry, error) {
	encodedThroughBytes := encodeTime(through)
	return []IndexEntry{
		{
			TableName:  tableName,
			HashValue:  hashKey + ":" + string(labelName),
			RangeValue: buildRangeKey(encodedThroughBytes),
		},
	}, nil
}

func buildRangeKey(ss ...[]byte) []byte {
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

func encodeBase64Value(value model.LabelValue) []byte {
	encodedLen := base64.RawStdEncoding.EncodedLen(len(value))
	encoded := make([]byte, encodedLen, encodedLen)
	base64.RawStdEncoding.Encode(encoded, []byte(value))
	return encoded
}

func decodeBase64Value(bs []byte) (model.LabelValue, error) {
	decodedLen := base64.RawStdEncoding.DecodedLen(len(bs))
	decoded := make([]byte, decodedLen, decodedLen)
	if _, err := base64.RawStdEncoding.Decode(decoded, bs); err != nil {
		return "", err
	}
	return model.LabelValue(decoded), nil
}

func parseRangeValue(rangeValue []byte, value []byte) (Chunk, model.LabelValue, error) {
	components := make([][]byte, 0, 5)
	i, j := 0, 0
	for j < len(rangeValue) {
		if rangeValue[j] != 0 {
			j++
			continue
		}

		components = append(components, rangeValue[i:j])
		j++
		i = j
	}

	switch {
	case len(components) < 3:
		return Chunk{}, "", fmt.Errorf("invalid range value: %x", rangeValue)

	// v1 & v2 schema had three components - label name, label value and chunk ID.
	// No version number.
	case len(components) == 3:
		chunk := Chunk{ID: string(components[2])}

		if value != nil {
			if err := json.Unmarshal(value, &chunk); err != nil {
				return Chunk{}, "", err
			}
			chunk.metadataInIndex = true
		}

		return chunk, model.LabelValue(components[1]), nil

	// v3 schema had four components - label name, label value, chunk ID and version.
	// "version" is 1 and label value is base64 encoded.
	case bytes.Equal(components[3], rangeKeyV1):
		chunk := Chunk{ID: string(components[2])}
		labelValue, err := decodeBase64Value(components[1])
		return chunk, labelValue, err

	// v4 schema wrote v3 range keys and a new range key - version 2,
	// with four components - <empty>, <empty>, chunk ID and version.
	case bytes.Equal(components[3], rangeKeyV2):
		chunk := Chunk{ID: string(components[2])}
		return chunk, model.LabelValue(""), nil

	// v5 schema version 3 range key is chunk end time, <empty>, chunk ID, version
	case bytes.Equal(components[3], rangeKeyV3):
		chunk := Chunk{ID: string(components[2])}
		return chunk, model.LabelValue(""), nil

	// v5 schema version 4 range key is chunk end time, label value, chunk ID, version
	case bytes.Equal(components[3], rangeKeyV4):
		chunk := Chunk{ID: string(components[2])}
		labelValue, err := decodeBase64Value(components[1])
		return chunk, labelValue, err

	// v6 schema added version 5 range keys, which have the label value written in
	// to the value, not the range key. So they are [chunk end time, <empty>, chunk ID, version].
	case bytes.Equal(components[3], rangeKeyV5):
		chunk := Chunk{ID: string(components[2])}
		labelValue := model.LabelValue(value)
		return chunk, labelValue, nil

	default:
		return Chunk{}, model.LabelValue(""), fmt.Errorf("unrecognised version: '%v'", string(components[3]))
	}

}
