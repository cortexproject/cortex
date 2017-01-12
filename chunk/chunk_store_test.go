package chunk

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/metric"
	"golang.org/x/net/context"

	"github.com/weaveworks/cortex/user"
)

func init() {
	spew.Config.SortKeys = true // :\
}

func setupDynamodb(t *testing.T, dynamoDB DynamoDBClient) {
	tableManager, err := NewDynamoTableManager(TableManagerConfig{
		DynamoDB: dynamoDB,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tableManager.syncTables(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestChunkStoreUnprocessed(t *testing.T) {
	dynamoDB := NewMockDynamoDB(2, 2)
	setupDynamodb(t, dynamoDB)
	store := NewAWSStore(StoreConfig{
		DynamoDB: dynamoDB,
		S3:       NewMockS3(),
	})

	ctx := user.WithID(context.Background(), "0")
	now := model.Now()
	chunks, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})
	chunk := NewChunk(
		model.Fingerprint(1),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "baz",
			"toms": "code",
		},
		chunks[0],
		now.Add(-time.Hour),
		now,
	)
	want := []Chunk{chunk}
	if err := store.Put(ctx, want); err != nil {
		t.Fatal(err)
	}
	have, err := store.Get(ctx, now.Add(-time.Hour), now, mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, have) {
		t.Fatalf("wrong chunks - %s", diff(want, have))
	}
}

func TestChunkStore(t *testing.T) {
	dynamoDB := NewMockDynamoDB(0, 0)
	setupDynamodb(t, dynamoDB)
	store := NewAWSStore(StoreConfig{
		DynamoDB: dynamoDB,
		S3:       NewMockS3(),
	})

	ctx := user.WithID(context.Background(), "0")
	now := model.Now()
	chunks, _ := chunk.New().Add(model.SamplePair{Timestamp: now, Value: 0})

	chunk1 := NewChunk(
		model.Fingerprint(1),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "baz",
			"toms": "code",
		},
		chunks[0],
		now.Add(-time.Hour),
		now,
	)
	chunk2 := NewChunk(
		model.Fingerprint(2),
		model.Metric{
			model.MetricNameLabel: "foo",
			"bar":  "beep",
			"toms": "code",
		},
		chunks[0],
		now.Add(-time.Hour),
		now,
	)

	if err := store.Put(ctx, []Chunk{chunk1, chunk2}); err != nil {
		t.Fatal(err)
	}

	test := func(name string, expect []Chunk, matchers ...*metric.LabelMatcher) {
		log.Infof(">>> %s", name)
		chunks, err := store.Get(ctx, now.Add(-time.Hour), now, matchers...)
		if err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(expect, chunks) {
			t.Fatalf("%s: wrong chunks - %s", name, diff(expect, chunks))
		}
	}

	nameMatcher := mustNewLabelMatcher(metric.Equal, model.MetricNameLabel, "foo")

	test("Just name label", []Chunk{chunk1, chunk2}, nameMatcher)
	test("Empty matcher", nil, nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", ""))
	test("Equal bar=baz", []Chunk{chunk1}, nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "baz"))
	test("Equal bar=beep", []Chunk{chunk2}, nameMatcher, mustNewLabelMatcher(metric.Equal, "bar", "beep"))
	test("Equal toms=code", []Chunk{chunk1, chunk2}, nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"))
	test("Not equal", []Chunk{chunk2}, nameMatcher, mustNewLabelMatcher(metric.NotEqual, "bar", "baz"))
	test("Regex match", []Chunk{chunk1, chunk2}, nameMatcher, mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz"))
	test("Multiple matchers", []Chunk{chunk1, chunk2}, nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.RegexMatch, "bar", "beep|baz"))
	test("Multiple matchers II", []Chunk{chunk1}, nameMatcher, mustNewLabelMatcher(metric.Equal, "toms", "code"), mustNewLabelMatcher(metric.Equal, "bar", "baz"))
}

func mustNewLabelMatcher(matchType metric.MatchType, name model.LabelName, value model.LabelValue) *metric.LabelMatcher {
	matcher, err := metric.NewLabelMatcher(matchType, name, value)
	if err != nil {
		panic(err)
	}
	return matcher
}

func diff(want, have interface{}) string {
	text, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(spew.Sdump(want)),
		B:        difflib.SplitLines(spew.Sdump(have)),
		FromFile: "want",
		ToFile:   "have",
		Context:  3,
	})
	return "\n" + text
}

func TestBigBuckets(t *testing.T) {
	const (
		tableName      = "table"
		periodicPrefix = "periodic"
	)

	buckets := func(tableName string, bs []string) []bucketSpec {
		result := []bucketSpec{}
		for _, b := range bs {
			result = append(result, bucketSpec{
				tableName: tableName,
				bucket:    b,
			})
		}
		return result
	}

	mergeBuckets := func(bss ...[]bucketSpec) []bucketSpec {
		result := []bucketSpec{}
		for _, bs := range bss {
			result = append(result, bs...)
		}
		return result
	}

	firstDayBuckets := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23"}

	scenarios := []struct {
		from, through, dailyBucketsFrom model.Time
		periodicTablesFrom              time.Time
		periodicTablesPeriod            time.Duration
		buckets                         []bucketSpec
	}{
		// Buckets are by hour until we reach the `dailyBucketsFrom`, after which they are by day.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0).Add(1 * 24 * time.Hour),
			buckets:          buckets(tableName, append(firstDayBuckets, "d1", "d2")),
		},

		// Only the day part of `dailyBucketsFrom` matters, not the time part.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0).Add(2*24*time.Hour) - 1,
			buckets:          buckets(tableName, append(firstDayBuckets, "d1", "d2")),
		},

		// Moving dailyBucketsFrom to the previous day compared to the above makes 24 1-hour buckets disappear.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0).Add(1*24*time.Hour) - 1,
			buckets:          buckets(tableName, []string{"d0", "d1", "d2"}),
		},

		// If `dailyBucketsFrom` is after the interval, everything will be bucketed by hour.
		{
			from:             model.TimeFromUnix(0),
			through:          model.TimeFromUnix(0).Add(2 * 24 * time.Hour),
			dailyBucketsFrom: model.TimeFromUnix(0).Add(99 * 24 * time.Hour),
			buckets:          buckets(tableName, append(firstDayBuckets, "24", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46", "47", "48")),
		},

		// Should only return daily buckets when dailyBucketsFrom is before the interval.
		{
			from:             model.TimeFromUnix(0).Add(1 * 24 * time.Hour),
			through:          model.TimeFromUnix(0).Add(3*24*time.Hour) - 1,
			dailyBucketsFrom: model.TimeFromUnix(0),
			buckets:          buckets(tableName, []string{"d1", "d2"}),
		},

		// Basic weekly- ables.
		{
			from:                 model.TimeFromUnix(0),
			through:              model.TimeFromUnix(0).Add(4*24*time.Hour) - 1,
			dailyBucketsFrom:     model.TimeFromUnix(0),
			periodicTablesFrom:   time.Unix(0, 0),
			periodicTablesPeriod: 2 * 24 * time.Hour,
			buckets: mergeBuckets(
				buckets(periodicPrefix+"0", []string{"d0", "d1"}),
				buckets(periodicPrefix+"1", []string{"d2", "d3"}),
			),
		},

		// Daily buckets + weekly tables.
		{
			from:                 model.TimeFromUnix(0),
			through:              model.TimeFromUnix(0).Add(4*24*time.Hour) - 1,
			dailyBucketsFrom:     model.TimeFromUnix(0).Add(2*24*time.Hour) - 1,
			periodicTablesFrom:   time.Unix(0, 0).Add(1 * 24 * time.Hour),
			periodicTablesPeriod: 2 * 24 * time.Hour,
			buckets: mergeBuckets(
				buckets(tableName, firstDayBuckets),
				buckets(periodicPrefix+"0", []string{"d1"}),
				buckets(periodicPrefix+"1", []string{"d2", "d3"}),
			),
		},
	}
	for i, s := range scenarios {
		t.Run(fmt.Sprintf("Case %d", i), func(t *testing.T) {
			cs := &AWSStore{
				cfg: StoreConfig{
					TableName:        tableName,
					DailyBucketsFrom: s.dailyBucketsFrom,
					PeriodicTableConfig: PeriodicTableConfig{
						UsePeriodicTables:    !s.periodicTablesFrom.IsZero(),
						TablePeriod:          s.periodicTablesPeriod,
						TablePrefix:          periodicPrefix,
						PeriodicTableStartAt: s.periodicTablesFrom,
					},
				},
			}
			buckets := cs.bigBuckets(s.from, s.through)
			if !reflect.DeepEqual(buckets, s.buckets) {
				t.Fatalf("%d. unexpected buckets; want %v, got %v", i, s.buckets, buckets)
			}
		})
	}
}
